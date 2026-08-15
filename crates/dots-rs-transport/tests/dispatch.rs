//! Dispatch-semantics tests, porting expectations from dots-cpp's
//! `tests/src/TestDispatcher.cpp` onto the real guest pipeline.
//!
//! Each test wires a [`GuestTransceiver`] to a scripted fake server on
//! the other end of `tokio::io::duplex` (the pubsub.rs pattern): the
//! server runs the standard no-preload handshake and then pushes raw
//! transmissions, which the spawned [`GuestDriver`] run loop reads and
//! dispatches through `dispatch_external_owned` — the same path a real
//! connection uses.
//!
//! The mid-dispatch unregister tests are backed by
//! `DispatchState::pending_unregister` (the dots-cpp `m_removeIds`
//! analogue) and the dynamic-classification test by the dynamic entry
//! reading the container's `DispatchShared` outcome.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dots_rs_core::{DynamicStructDescriptor, StructValue, dots};
#[allow(unused_imports)]
use dots_rs_model::*;
use dots_rs_model::{
    DotsHeader, DotsMsgConnectResponse, DotsMsgHello, Registry, Transmission,
    encode_transmission, StaticTypePolicy,
};
use dots_rs_transport::{
    Connection, Event, GuestDriver, GuestTransceiver, Operation, Subscription,
    SubscriptionHandle, TransmissionCodec,
};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncWriteExt, DuplexStream};
use tokio::time::timeout;
use tokio_util::codec::Framed;

mod model {
    use dots_rs_derive::DotsStruct;

    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "Pinger", cached)]
    pub struct Pinger {
        #[dots(tag = 1, key)]
        pub id: Option<u32>,
        #[dots(tag = 2)]
        pub message: Option<String>,
    }
}
use model::*;

fn registry() -> Arc<Registry> {
    let reg = Registry::with_static_types(StaticTypePolicy::All);
    reg.register_struct_static(Pinger::DESCRIPTOR);
    Arc::new(reg)
}

fn dynamic_for(reg: &Registry, type_name: &str, payload: &dyn StructValue) -> Transmission {
    let header = dots!(DotsHeader {
        type_name: type_name,
    });
    let descriptor = match reg.lookup(type_name).expect("type registered") {
        dots_rs_model::DescriptorEntry::Struct(d) => d.clone(),
        _ => panic!("expected struct entry for {type_name}"),
    };
    let bytes = dots_rs_core::encode_to_vec(payload);
    let payload = dots_rs_core::DynamicStruct::decode(descriptor, &bytes).unwrap();
    Transmission { header, payload: dots_rs_model::Payload::Wire(payload) }
}

/// Run the standard no-preload handshake from the server side.
async fn run_handshake_server(
    framed: &mut Framed<DuplexStream, TransmissionCodec>,
    reg: &Arc<Registry>,
) {
    let hello = dots!(DotsMsgHello {
        server_name: "dispatch-test-dotsd",
        auth_challenge: 0_u64,
        authentication_required: false,
    });
    framed
        .send(dynamic_for(reg, "DotsMsgHello", &hello))
        .await
        .unwrap();
    let _connect = framed.next().await.unwrap().unwrap();
    let response = dots!(DotsMsgConnectResponse {
        server_name: "dispatch-test-dotsd",
        client_id: 1_u32,
        accepted: true,
        preload: false,
    });
    framed
        .send(dynamic_for(reg, "DotsMsgConnectResponse", &response))
        .await
        .unwrap();
}

/// Handshake a guest transceiver against a scripted server. Returns the
/// transceiver, its (not yet spawned) driver, and the server-side
/// framed stream for pushing transmissions.
async fn connect_guest() -> (
    Arc<GuestTransceiver>,
    GuestDriver<DuplexStream>,
    Framed<DuplexStream, TransmissionCodec>,
) {
    let (client_io, server_io) = tokio::io::duplex(65536);
    let reg = registry();

    let server_reg = reg.clone();
    let server = tokio::spawn(async move {
        let mut framed = Framed::new(server_io, TransmissionCodec::new(server_reg.clone()));
        run_handshake_server(&mut framed, &server_reg).await;
        framed
    });

    let conn = Connection::establish(client_io, "dispatch-test-guest", reg.clone())
        .await
        .expect("handshake succeeds");
    let framed = server.await.unwrap();
    let (transceiver, driver) = GuestTransceiver::from_connection(reg, conn, [], []);
    (transceiver, driver, framed)
}

/// Push a Pinger transmission from the server side. `remove == true`
/// makes it a removal (the payload should then be key-only, as a real
/// broker sends).
async fn push_pinger(
    framed: &mut Framed<DuplexStream, TransmissionCodec>,
    pinger: &Pinger,
    remove: bool,
) {
    let header = dots!(DotsHeader {
        type_name: "Pinger",
        attributes: pinger.valid_set(),
        sender: 42_u32,
        remove_obj: remove,
    });
    let frame = encode_transmission(&header, pinger);
    framed.get_mut().write_all(&frame).await.unwrap();
}

/// Receive with a bounded wait; `None` if nothing arrives in time.
async fn recv_timeout<T: Send + 'static>(
    sub: &mut Subscription<T>,
    wait: Duration,
) -> Option<Event<T>> {
    timeout(wait, sub.recv()).await.ok().flatten()
}

const GENEROUS: Duration = Duration::from_secs(2);
const SETTLE: Duration = Duration::from_millis(100);

/// Poll until `cond` holds or the (generous) deadline passes; panics
/// with `what` on timeout.
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

// ----- 1. subscribe_tasked: basic delivery + serialization + shutdown -----

/// No direct cpp counterpart — covers the `subscribe_tasked` API added
/// in commit eaf0349: an async handler receives published events, its
/// invocations are serialized one-at-a-time (per the documented
/// contract "each handler future is awaited to completion before the
/// next event is dequeued"), and the spawned task "ends on its own when
/// the subscription closes (e.g. at shutdown)".
#[tokio::test]
async fn subscribe_tasked_delivers_serially_and_ends_at_shutdown() {
    let (transceiver, driver, mut server) = connect_guest().await;

    struct Probe {
        in_flight: AtomicUsize,
        max_in_flight: AtomicUsize,
        done: AtomicUsize,
        ids: Mutex<Vec<u32>>,
    }
    let probe = Arc::new(Probe {
        in_flight: AtomicUsize::new(0),
        max_in_flight: AtomicUsize::new(0),
        done: AtomicUsize::new(0),
        ids: Mutex::new(Vec::new()),
    });

    let task = transceiver.subscribe_tasked::<Pinger, _, _, _>(
        probe.clone(),
        |p, evt: Event<Pinger>| async move {
            let now = p.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            p.max_in_flight.fetch_max(now, Ordering::SeqCst);
            if let Some(id) = evt.updated().id {
                p.ids.lock().unwrap().push(id);
            }
            // Hold each invocation open long enough that a second
            // in-flight handler would be observable if invocations
            // were allowed to overlap.
            tokio::time::sleep(Duration::from_millis(25)).await;
            p.in_flight.fetch_sub(1, Ordering::SeqCst);
            p.done.fetch_add(1, Ordering::SeqCst);
        },
    );

    let driver_handle = tokio::spawn(driver.run());

    for id in 1..=3u32 {
        let p = dots!(Pinger {
            id: id,
            message: format!("msg-{id}"),
        });
        push_pinger(&mut server, &p, false).await;
    }

    let probe_for_wait = probe.clone();
    wait_until("all three tasked handler invocations to complete", move || {
        probe_for_wait.done.load(Ordering::SeqCst) >= 3
    })
    .await;

    assert_eq!(
        *probe.ids.lock().unwrap(),
        vec![1, 2, 3],
        "events arrive in publish order"
    );
    assert_eq!(
        probe.max_in_flight.load(Ordering::SeqCst),
        1,
        "handler invocations must be serialized one-at-a-time"
    );

    // Shutdown: the driver exits, and once the dispatch state drops the
    // subscription closes — the documented end-of-life of the task.
    transceiver.exit();
    timeout(GENEROUS, driver_handle)
        .await
        .expect("driver exits after exit()")
        .unwrap()
        .unwrap();
    drop(transceiver);
    timeout(GENEROUS, task)
        .await
        .expect("tasked handler ends on its own when the subscription closes")
        .unwrap();
}

// ----- 2. Redundant remove dispatches nothing (end-to-end) -----

/// Ports `dispatch_NoEventAfterRedundantRemoveForCachedType`: a remove
/// for a key the local cache never held must dispatch no event at all.
/// Goes through the real guest pipeline (scripted server → driver run
/// loop → dispatch), not the container unit API.
#[tokio::test]
async fn redundant_remove_for_cached_type_dispatches_nothing() {
    let (transceiver, driver, mut server) = connect_guest().await;

    // Container-backed subscription, as any cached-type subscriber in
    // dots-cpp is.
    let mut sub = transceiver.subscribe_stream::<Pinger>();
    let driver_handle = tokio::spawn(driver.run());

    // Remove for a key that was never published ...
    let ghost = dots!(Pinger { id: 1_u32 });
    push_pinger(&mut server, &ghost, true).await;

    // ... then a normal create.
    let created = dots!(Pinger {
        id: 2_u32,
        message: "created",
    });
    push_pinger(&mut server, &created, false).await;

    // The FIRST (and only) event the subscriber sees is the create —
    // the redundant remove was suppressed.
    let event = recv_timeout(&mut sub, GENEROUS)
        .await
        .expect("the create event arrives");
    assert!(
        event.is_create(),
        "expected the create, got {:?} (a remove event leaked through)",
        event.operation()
    );
    assert_eq!(event.updated().id, Some(2));
    assert_ne!(event.header.remove_obj, Some(true));

    assert!(
        recv_timeout(&mut sub, SETTLE).await.is_none(),
        "no further event may follow the create"
    );

    transceiver.exit();
    let _ = timeout(GENEROUS, driver_handle).await;
}

// ----- 3. Remove-current-handler during dispatch -----

/// Ports `dispatch_RemoveCurrentEventHandlerDuringDispatch`: a handler
/// that removes (drops) its own subscription from inside the handler
/// must not run for any later event — it runs exactly once here.
#[tokio::test]
async fn remove_current_handler_during_dispatch_stops_it() {
    let (transceiver, driver, mut server) = connect_guest().await;

    let count = Arc::new(AtomicUsize::new(0));
    let slot: Arc<Mutex<Option<SubscriptionHandle>>> = Arc::new(Mutex::new(None));

    let count_in = count.clone();
    let slot_in = slot.clone();
    let handle = transceiver.subscribe::<Pinger>(move |_evt| {
        count_in.fetch_add(1, Ordering::SeqCst);
        // Drop our own handle from inside the handler — the cpp
        // equivalent of `removeEventHandler(descriptor, id)` on the
        // currently-running handler.
        drop(slot_in.lock().unwrap().take());
    });
    *slot.lock().unwrap() = Some(handle);

    // Barrier subscription, registered AFTER the callback so it sees
    // each event only once the callback pass for it has completed.
    let mut barrier = transceiver.subscribe_stream::<Pinger>();
    let driver_handle = tokio::spawn(driver.run());

    for id in 1..=2u32 {
        push_pinger(&mut server, &dots!(Pinger { id: id }), false).await;
    }
    recv_timeout(&mut barrier, GENEROUS).await.expect("event 1 dispatched");
    recv_timeout(&mut barrier, GENEROUS).await.expect("event 2 dispatched");

    assert_eq!(
        count.load(Ordering::SeqCst),
        1,
        "a handler that removed itself during dispatch must run exactly once"
    );

    transceiver.exit();
    let _ = timeout(GENEROUS, driver_handle).await;
}

// ----- 4. Remove-other-handler during dispatch -----

/// Ports `dispatch_RemoveOtherEventHandlerDuringDispatch`: handler A
/// (registered first) removes handler B's subscription while the first
/// event is being dispatched. dots-cpp pins that B must NOT be invoked
/// for the current event (removal is effective immediately), and of
/// course not for later ones either — so B never fires.
#[tokio::test]
async fn remove_other_handler_during_dispatch_prevents_its_invocation() {
    let (transceiver, driver, mut server) = connect_guest().await;

    let a_count = Arc::new(AtomicUsize::new(0));
    let b_count = Arc::new(AtomicUsize::new(0));
    let b_slot: Arc<Mutex<Option<SubscriptionHandle>>> = Arc::new(Mutex::new(None));

    // Handler A: registered first, drops B's handle on the first event.
    let a_count_in = a_count.clone();
    let b_slot_in = b_slot.clone();
    let _a = transceiver.subscribe::<Pinger>(move |_evt| {
        a_count_in.fetch_add(1, Ordering::SeqCst);
        drop(b_slot_in.lock().unwrap().take());
    });

    // Handler B: registered second (after A), so per registration order
    // it would run after A within the same event's dispatch.
    let b_count_in = b_count.clone();
    let b = transceiver.subscribe::<Pinger>(move |_evt| {
        b_count_in.fetch_add(1, Ordering::SeqCst);
    });
    *b_slot.lock().unwrap() = Some(b);

    let mut barrier = transceiver.subscribe_stream::<Pinger>();
    let driver_handle = tokio::spawn(driver.run());

    for id in 1..=2u32 {
        push_pinger(&mut server, &dots!(Pinger { id: id }), false).await;
    }
    recv_timeout(&mut barrier, GENEROUS).await.expect("event 1 dispatched");
    recv_timeout(&mut barrier, GENEROUS).await.expect("event 2 dispatched");

    assert_eq!(a_count.load(Ordering::SeqCst), 2, "A fires for both events");
    assert_eq!(
        b_count.load(Ordering::SeqCst),
        0,
        "cpp contract: a handler removed during dispatch must not fire for the current event (nor any later one)"
    );

    transceiver.exit();
    let _ = timeout(GENEROUS, driver_handle).await;
}

// ----- 5. Add-handler during dispatch -----

/// Ports `dispatch_AddEventHandlerDuringDispatch`. dots-cpp invokes a
/// handler added during dispatch for the CURRENT event; dots-rust does
/// not hand it the in-flight transmission (per the `dispatch_external`
/// docs in connection.rs), but replay-on-subscribe closes the gap: the
/// container was updated before the subscriber phase, so registering C
/// replays the post-update state to it synchronously. Observable
/// outcome matches dots-cpp — C sees the current event's state (as a
/// replayed create) and every later event live.
#[tokio::test]
async fn add_handler_during_dispatch_sees_current_state_via_replay() {
    let (transceiver, driver, mut server) = connect_guest().await;

    let a_count = Arc::new(AtomicUsize::new(0));
    let c_count = Arc::new(AtomicUsize::new(0));
    // Keep C's handle alive past the registering handler's scope.
    let added: Arc<Mutex<Vec<SubscriptionHandle>>> = Arc::new(Mutex::new(Vec::new()));

    let a_count_in = a_count.clone();
    let c_count_for_a = c_count.clone();
    let added_in = added.clone();
    let transceiver_in = transceiver.clone();
    let _a = transceiver.subscribe::<Pinger>(move |_evt| {
        if a_count_in.fetch_add(1, Ordering::SeqCst) == 0 {
            // First event: register handler C from inside the dispatch.
            let c_count_in = c_count_for_a.clone();
            let c = transceiver_in.subscribe::<Pinger>(move |_evt| {
                c_count_in.fetch_add(1, Ordering::SeqCst);
            });
            added_in.lock().unwrap().push(c);
        }
    });

    let mut barrier = transceiver.subscribe_stream::<Pinger>();
    let driver_handle = tokio::spawn(driver.run());

    // Event 1: A registers C mid-dispatch; the subscribe replays the
    // already-updated container to C before returning.
    push_pinger(&mut server, &dots!(Pinger { id: 1_u32 }), false).await;
    recv_timeout(&mut barrier, GENEROUS).await.expect("event 1 dispatched");
    tokio::time::sleep(SETTLE).await;
    assert_eq!(
        c_count.load(Ordering::SeqCst),
        1,
        "cpp-parity via replay: a handler added during dispatch sees the current event's state"
    );

    // Event 2: C is installed and fires live.
    push_pinger(&mut server, &dots!(Pinger { id: 2_u32 }), false).await;
    recv_timeout(&mut barrier, GENEROUS).await.expect("event 2 dispatched");
    // C was appended to the entry list after the barrier, so its
    // event-2 invocation may complete just after the barrier's — poll.
    let c_count_for_wait = c_count.clone();
    wait_until("handler C to fire for the next event", move || {
        c_count_for_wait.load(Ordering::SeqCst) == 2
    })
    .await;
    assert_eq!(a_count.load(Ordering::SeqCst), 2);

    transceiver.exit();
    let _ = timeout(GENEROUS, driver_handle).await;
}

// ----- 6. Dynamic subscription classification on a cached type -----

/// Ports `dispatch_UpdateEventWhenDynamicallAddedHandlerForCachedType`
/// (and its Create sibling): a dynamically-added handler on a cached
/// type must see create for the first publish of a key and UPDATE for
/// the second publish of the same key — classification against the
/// type's cache, exactly like a statically-typed handler.
#[tokio::test]
async fn dynamic_subscription_classifies_second_publish_as_update() {
    let (transceiver, driver, mut server) = connect_guest().await;

    let ops: Arc<Mutex<Vec<Operation>>> = Arc::new(Mutex::new(Vec::new()));
    let ops_in = ops.clone();
    let descriptor = Arc::new(DynamicStructDescriptor::from_static(Pinger::DESCRIPTOR));
    let _dyn_sub = transceiver.subscribe_dynamic(descriptor, move |evt| {
        ops_in.lock().unwrap().push(evt.operation());
    });

    // Cached type: a container-backed subscription keeps the local
    // cache live, matching the cpp fixture where the Dispatcher always
    // caches the type. Registered after the dynamic handler, it also
    // serves as the per-event barrier.
    let mut barrier = transceiver.subscribe_stream::<Pinger>();
    let driver_handle = tokio::spawn(driver.run());

    let first = dots!(Pinger {
        id: 1_u32,
        message: "first",
    });
    push_pinger(&mut server, &first, false).await;
    let second = dots!(Pinger {
        id: 1_u32,
        message: "second",
    });
    push_pinger(&mut server, &second, false).await;

    recv_timeout(&mut barrier, GENEROUS).await.expect("event 1 dispatched");
    recv_timeout(&mut barrier, GENEROUS).await.expect("event 2 dispatched");

    let ops = ops.lock().unwrap().clone();
    assert_eq!(ops.len(), 2, "dynamic handler sees both publishes");
    assert_eq!(ops[0], Operation::Create, "first publish of a key is a create");
    assert_eq!(
        ops[1],
        Operation::Update,
        "cpp contract: second publish of a cached key is an UPDATE, also for dynamically added handlers"
    );

    transceiver.exit();
    let _ = timeout(GENEROUS, driver_handle).await;
}
