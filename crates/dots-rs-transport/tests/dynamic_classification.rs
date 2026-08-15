//! End-to-end classification tests for dynamic subscriptions on
//! cached types: `subscribe_dynamic` / `subscribe_all_types` must
//! report create / update / remove exactly like a typed subscriber —
//! the dynamic counterpart of dots-cpp `Dispatcher::dispatchEvent`
//! routing every `descriptor.cached()` type through the container
//! pool. Regression tests for `Event<DynamicStruct>` always reporting
//! `Create`.
//!
//! Uses the scripted-server pattern from `descriptor_learning.rs`:
//! "foreign" types are compiled into the test as a convenient way to
//! build wire bytes, but (unless a test says otherwise) NOT registered
//! in the guest's registry — the guest learns them from wire
//! descriptors, so their instances decode to `Payload::Wire`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use dots_rs_core::{DynamicStruct, Transmittable, dots, encode_to_vec};
// Glob import: the `dots!` macro resolves per-type `__dots_ctor_*`
// helper macros exported at the crate root.
#[allow(unused_imports)]
use dots_rs_model::*;
use dots_rs_model::{
    DotsHeader, DotsMsgConnectResponse, DotsMsgHello, Registry, StructDescriptorData,
    encode_transmission, StaticTypePolicy,
};
use dots_rs_transport::{
    Connection, GuestDriver, GuestTransceiver, Operation, TransmissionCodec,
};
use futures_util::StreamExt;
use tokio::io::{AsyncWriteExt, DuplexStream};
use tokio::time::timeout;
use tokio_util::codec::Framed;

mod foreign {
    use dots_rs_derive::DotsStruct;

    /// Cached: dynamic subscribers must get container-backed
    /// create / update / remove classification.
    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "TraceItem", cached)]
    pub struct TraceItem {
        #[dots(tag = 1, key)]
        pub id: Option<u32>,
        #[dots(tag = 2)]
        pub message: Option<String>,
        #[dots(tag = 3)]
        pub sequence: Option<u64>,
    }

    /// Uncached: dynamic subscribers keep container-less
    /// always-create semantics.
    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "TraceNote")]
    pub struct TraceNote {
        #[dots(tag = 1, key)]
        pub id: Option<u32>,
        #[dots(tag = 2)]
        pub note: Option<String>,
    }
}
use foreign::*;

async fn run_handshake_server(framed: &mut Framed<DuplexStream, TransmissionCodec>) {
    let hello = dots!(DotsMsgHello {
        server_name: "classification-test-dotsd",
        auth_challenge: 0_u64,
        authentication_required: false,
    });
    push_typed(framed, "DotsMsgHello", &hello).await;
    let _connect = framed.next().await.unwrap().unwrap();
    let response = dots!(DotsMsgConnectResponse {
        server_name: "classification-test-dotsd",
        client_id: 1_u32,
        accepted: true,
        preload: false,
    });
    push_typed(framed, "DotsMsgConnectResponse", &response).await;
}

/// Push a typed transmission as raw frame bytes — the scripted-server
/// equivalent of the broker streaming an instance. No `attributes` in
/// the header, so the receiver's merge mask falls back to the
/// payload's valid set (partial-update semantics).
async fn push_typed<T: Transmittable + Sync>(
    framed: &mut Framed<DuplexStream, TransmissionCodec>,
    type_name: &str,
    value: &T,
) {
    let header = dots!(DotsHeader {
        type_name: type_name,
        sender: 1_u32,
    });
    let frame = encode_transmission(&header, value);
    framed.get_mut().write_all(&frame).await.unwrap();
}

/// Push a removal (`remove_obj = true`) for `value`'s key.
async fn push_remove<T: Transmittable + Sync>(
    framed: &mut Framed<DuplexStream, TransmissionCodec>,
    type_name: &str,
    value: &T,
) {
    let header = dots!(DotsHeader {
        type_name: type_name,
        sender: 1_u32,
        remove_obj: true,
    });
    let frame = encode_transmission(&header, value);
    framed.get_mut().write_all(&frame).await.unwrap();
}

async fn connect_guest() -> (
    Arc<GuestTransceiver>,
    GuestDriver<DuplexStream>,
    Framed<DuplexStream, TransmissionCodec>,
    Arc<Registry>,
) {
    let (client_io, server_io) = tokio::io::duplex(65536);
    // Internal types only — the foreign types stay unknown until
    // learned from the wire (or registered explicitly by a test).
    let reg = Arc::new(Registry::with_static_types(StaticTypePolicy::All));

    let server_reg = reg.clone();
    let server = tokio::spawn(async move {
        let mut framed = Framed::new(server_io, TransmissionCodec::new(server_reg));
        run_handshake_server(&mut framed).await;
        framed
    });

    let conn = Connection::establish(client_io, "classification-test-guest", reg.clone())
        .await
        .expect("handshake succeeds");
    let framed = server.await.unwrap();
    let (transceiver, driver) = GuestTransceiver::from_connection(reg.clone(), conn, [], []);
    (transceiver, driver, framed, reg)
}

async fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !cond() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for: {what}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Look up the wire-learned dynamic descriptor for `name`, waiting for
/// the import to happen.
async fn learned_descriptor(
    reg: &Arc<Registry>,
    name: &str,
) -> Arc<dots_rs_core::DynamicStructDescriptor> {
    wait_until(&format!("{name} descriptor import"), || {
        reg.lookup(name).is_some()
    })
    .await;
    match reg.lookup(name) {
        Some(dots_rs_model::DescriptorEntry::Struct(d)) => d,
        other => panic!("expected struct descriptor for {name}, got {other:?}"),
    }
}

/// One observed event: the operation plus the deterministic encoding
/// of `Event::updated()` (comparable against `encode_to_vec` of an
/// expected typed value).
type Seen = Arc<Mutex<Vec<(Operation, Vec<u8>)>>>;

fn recording_handler(
    seen: &Seen,
) -> impl FnMut(&dots_rs_transport::Event<DynamicStruct>) + Send + 'static {
    let seen = seen.clone();
    move |ev| {
        seen.lock()
            .unwrap()
            .push((ev.operation(), ev.updated().encode()));
    }
}

/// The headline regression: a dynamic subscription on a wire-learned
/// **cached** type classifies create / update / remove against a local
/// container (instead of reporting create for everything), merges
/// partial updates, and suppresses removes of absent keys.
#[tokio::test]
async fn subscribe_dynamic_classifies_cached_type_end_to_end() {
    let (transceiver, driver, mut server, reg) = connect_guest().await;
    let driver_handle = tokio::spawn(driver.run());

    let data = StructDescriptorData::from_static(TraceItem::DESCRIPTOR);
    push_typed(&mut server, "StructDescriptorData", &data).await;
    let descriptor = learned_descriptor(&reg, "TraceItem").await;
    assert!(descriptor.flags.is_cached(), "wire flags carry `cached`");

    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let _sub = transceiver.subscribe_dynamic(descriptor, recording_handler(&seen));

    // Create (full value).
    let created = TraceItem {
        id: Some(1),
        message: Some("first".into()),
        sequence: Some(100),
    };
    push_typed(&mut server, "TraceItem", &created).await;

    // Partial update touching only `message` — `sequence` must survive.
    let update = TraceItem {
        id: Some(1),
        message: Some("second".into()),
        sequence: None,
    };
    push_typed(&mut server, "TraceItem", &update).await;

    // Remove (key-only), then a REDUNDANT remove of the same key.
    let key_only = TraceItem {
        id: Some(1),
        ..Default::default()
    };
    push_remove(&mut server, "TraceItem", &key_only).await;
    push_remove(&mut server, "TraceItem", &key_only).await;

    // Barrier create with a different key: once it arrives, everything
    // before it has been dispatched — proving the redundant remove
    // fired no handler.
    let barrier = TraceItem {
        id: Some(2),
        message: Some("barrier".into()),
        sequence: Some(1),
    };
    push_typed(&mut server, "TraceItem", &barrier).await;

    wait_until("4 events (redundant remove suppressed)", || {
        seen.lock().unwrap().len() >= 4
    })
    .await;

    let events = seen.lock().unwrap().clone();
    assert_eq!(events.len(), 4, "redundant remove must not fire the handler");

    assert_eq!(events[0].0, Operation::Create);
    assert_eq!(events[0].1, encode_to_vec(&created));

    let merged = TraceItem {
        id: Some(1),
        message: Some("second".into()),
        sequence: Some(100),
    };
    assert_eq!(events[1].0, Operation::Update, "second event is an update, not a create");
    assert_eq!(events[1].1, encode_to_vec(&merged), "updated() carries the merged state");

    assert_eq!(events[2].0, Operation::Remove);
    assert_eq!(events[2].1, encode_to_vec(&merged), "remove carries the pre-remove state");

    assert_eq!(events[3].0, Operation::Create);
    assert_eq!(events[3].1, encode_to_vec(&barrier));

    transceiver.exit();
    let _ = timeout(Duration::from_secs(2), driver_handle).await;
}

/// `subscribe_all_types` (the dots-trace scenario): cached types
/// classify against a container, uncached types keep always-create —
/// container creation is gated on the descriptor's `cached` flag.
#[tokio::test]
async fn subscribe_all_types_classifies_cached_and_gates_uncached() {
    let (transceiver, driver, mut server, _reg) = connect_guest().await;

    let item_seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let note_seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let (item_in, note_in) = (item_seen.clone(), note_seen.clone());
    let _all = transceiver.subscribe_all_types(move |ev| {
        let record = (ev.operation(), ev.updated().encode());
        match ev.updated().descriptor.name.as_str() {
            "TraceItem" => item_in.lock().unwrap().push(record),
            "TraceNote" => note_in.lock().unwrap().push(record),
            _ => {}
        }
    });
    let driver_handle = tokio::spawn(driver.run());

    let item_data = StructDescriptorData::from_static(TraceItem::DESCRIPTOR);
    push_typed(&mut server, "StructDescriptorData", &item_data).await;
    let note_data = StructDescriptorData::from_static(TraceNote::DESCRIPTOR);
    push_typed(&mut server, "StructDescriptorData", &note_data).await;

    // Cached type: create then (partial) update of the same key.
    let item = TraceItem {
        id: Some(1),
        message: Some("v1".into()),
        sequence: Some(1),
    };
    push_typed(&mut server, "TraceItem", &item).await;
    let item_v2 = TraceItem {
        id: Some(1),
        message: Some("v2".into()),
        sequence: None,
    };
    push_typed(&mut server, "TraceItem", &item_v2).await;

    // Uncached type: two transmissions of the same key.
    let note = TraceNote {
        id: Some(1),
        note: Some("n1".into()),
    };
    push_typed(&mut server, "TraceNote", &note).await;
    let note_v2 = TraceNote {
        id: Some(1),
        note: Some("n2".into()),
    };
    push_typed(&mut server, "TraceNote", &note_v2).await;

    wait_until("2 TraceItem + 2 TraceNote events", || {
        item_seen.lock().unwrap().len() >= 2 && note_seen.lock().unwrap().len() >= 2
    })
    .await;

    let items = item_seen.lock().unwrap().clone();
    assert_eq!(items[0].0, Operation::Create);
    assert_eq!(items[1].0, Operation::Update, "cached type classifies updates");
    let merged = TraceItem {
        id: Some(1),
        message: Some("v2".into()),
        sequence: Some(1),
    };
    assert_eq!(items[1].1, encode_to_vec(&merged), "cached update merges partials");

    let notes = note_seen.lock().unwrap().clone();
    assert_eq!(notes[0].0, Operation::Create);
    assert_eq!(
        notes[1].0,
        Operation::Create,
        "uncached type stays always-create (no container)"
    );
    assert_eq!(notes[1].1, encode_to_vec(&note_v2), "uncached updated() == transmitted");

    transceiver.exit();
    let _ = timeout(Duration::from_secs(2), driver_handle).await;
}

/// Mixed subscribers, typed first: a typed `subscribe::<T>` (which
/// creates the static-representation pool container) plus a dynamic
/// subscription on the same type agree on classification and on the
/// merged `updated()` contents — both served by the one container.
#[tokio::test]
async fn typed_then_dynamic_agree_on_classification() {
    let (transceiver, driver, mut server, reg) = connect_guest().await;

    let typed_seen: Arc<Mutex<Vec<(Operation, TraceItem)>>> = Arc::new(Mutex::new(Vec::new()));
    let typed_in = typed_seen.clone();
    let _typed_sub = transceiver.subscribe::<TraceItem>(move |ev| {
        typed_in
            .lock()
            .unwrap()
            .push((ev.operation(), ev.updated().clone()));
    });

    // `subscribe::<T>` registered the static descriptor; the registry
    // entry is its dynamic projection — the Arc a dynamic subscriber
    // converges on.
    let descriptor = match reg.lookup("TraceItem") {
        Some(dots_rs_model::DescriptorEntry::Struct(d)) => d,
        other => panic!("expected TraceItem projection, got {other:?}"),
    };
    let dyn_seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let _dyn_sub = transceiver.subscribe_dynamic(descriptor, recording_handler(&dyn_seen));

    let driver_handle = tokio::spawn(driver.run());

    let created = TraceItem {
        id: Some(7),
        message: Some("v1".into()),
        sequence: Some(10),
    };
    push_typed(&mut server, "TraceItem", &created).await;
    let update = TraceItem {
        id: Some(7),
        message: Some("v2".into()),
        sequence: None,
    };
    push_typed(&mut server, "TraceItem", &update).await;

    wait_until("2 typed + 2 dynamic events", || {
        typed_seen.lock().unwrap().len() >= 2 && dyn_seen.lock().unwrap().len() >= 2
    })
    .await;

    let typed = typed_seen.lock().unwrap().clone();
    let dynamic = dyn_seen.lock().unwrap().clone();
    assert_eq!(typed[0].0, Operation::Create);
    assert_eq!(typed[1].0, Operation::Update);
    assert_eq!(dynamic[0].0, Operation::Create);
    assert_eq!(dynamic[1].0, Operation::Update);

    // Field-for-field agreement between the two views of `updated()`.
    assert_eq!(dynamic[0].1, encode_to_vec(&typed[0].1));
    assert_eq!(dynamic[1].1, encode_to_vec(&typed[1].1));
    assert_eq!(typed[1].1.sequence, Some(10), "merge preserved the unsent field");

    transceiver.exit();
    let _ = timeout(Duration::from_secs(2), driver_handle).await;
}

/// Mixed subscribers, dynamic first: `subscribe_dynamic` on a
/// wire-learned cached type populates a dynamic-representation
/// container; a later typed `container::<T>()` promotes it in place —
/// the accumulated entries become typed-readable and subsequent
/// transmissions keep classifying for both subscriber kinds.
#[tokio::test]
async fn dynamic_then_typed_promotes_container() {
    let (transceiver, driver, mut server, reg) = connect_guest().await;
    let driver_handle = tokio::spawn(driver.run());

    let data = StructDescriptorData::from_static(TraceItem::DESCRIPTOR);
    push_typed(&mut server, "StructDescriptorData", &data).await;
    let descriptor = learned_descriptor(&reg, "TraceItem").await;

    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let _sub = transceiver.subscribe_dynamic(descriptor, recording_handler(&seen));

    // Populate the (dynamic-representation) container over the wire.
    let one = TraceItem {
        id: Some(1),
        message: Some("one".into()),
        sequence: Some(10),
    };
    let two = TraceItem {
        id: Some(2),
        message: Some("two".into()),
        sequence: Some(20),
    };
    push_typed(&mut server, "TraceItem", &one).await;
    push_typed(&mut server, "TraceItem", &two).await;
    wait_until("2 creates", || seen.lock().unwrap().len() >= 2).await;

    // The typed pool path finds the dynamic container and promotes it:
    // the wire-accumulated entries are readable through the typed view.
    let container = transceiver.container::<TraceItem>();
    let snapshot = container.snapshot();
    assert_eq!(snapshot.len(), 2, "promotion preserves accumulated entries");
    assert_eq!(snapshot[0].value, one);
    assert_eq!(snapshot[1].value, two);
    assert_eq!(snapshot[0].clone_info.last_operation, Operation::Create);

    // A further update still classifies for the dynamic subscriber
    // (and lands in the now-static container). The static registration
    // above also flips the codec to `Payload::Typed` for this type, so
    // this exercises the typed-payload-into-promoted-container path.
    let update = TraceItem {
        id: Some(1),
        message: Some("one-updated".into()),
        sequence: None,
    };
    push_typed(&mut server, "TraceItem", &update).await;
    wait_until("update event", || seen.lock().unwrap().len() >= 3).await;

    let events = seen.lock().unwrap().clone();
    let merged = TraceItem {
        id: Some(1),
        message: Some("one-updated".into()),
        sequence: Some(10),
    };
    assert_eq!(events[2].0, Operation::Update, "classification survives promotion");
    assert_eq!(events[2].1, encode_to_vec(&merged));

    let entry = container.get(&merged).expect("entry present after update");
    assert_eq!(&*entry, &merged);
    drop(entry);

    transceiver.exit();
    let _ = timeout(Duration::from_secs(2), driver_handle).await;
}
