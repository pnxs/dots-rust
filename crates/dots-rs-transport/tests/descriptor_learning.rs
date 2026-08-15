//! Wire descriptor-learning tests: a dynamic guest (the dots-trace
//! scenario) receives `EnumDescriptorData` / `StructDescriptorData`
//! for types it has NOT compiled in and must be able to build and
//! register the dynamic descriptors.
//!
//! Mirrors dots-cpp `Channel::processReceive` →
//! `Channel::importDependencies` (Channel.cpp): descriptor import is
//! unconditional at the receive layer — before dispatch, independent
//! of any subscription — and reuse-if-present (an already-registered
//! name, e.g. a compiled-in static type echoed back by the broker, is
//! never replaced). Regression tests for the guest-side gap where
//! incoming `EnumDescriptorData` was never registered, so every
//! `StructDescriptorData` referencing an enum failed with
//! `UnknownType`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use dots_rs_core::{Transmittable, dots};
#[allow(unused_imports)]
use dots_rs_model::*;
use dots_rs_model::{
    DotsHeader, DotsMsgConnectResponse, DotsMsgHello, EnumDescriptorData, Registry,
    StructDescriptorData, encode_transmission, StaticTypePolicy,
};
use dots_rs_transport::{Connection, GuestDriver, GuestTransceiver, TransmissionCodec};
use futures_util::{StreamExt};
use tokio::io::{AsyncWriteExt, DuplexStream};
use tokio::time::timeout;
use tokio_util::codec::Framed;

/// "Foreign" types: compiled into the test as a convenient way to
/// build wire-form descriptor data, but deliberately NOT registered
/// in the guest's registry — the guest must learn them from the wire.
mod foreign {
    use dots_rs_derive::{DotsEnum, DotsStruct};

    #[derive(DotsEnum, Default, Debug, Clone, Copy, PartialEq, Eq)]
    #[dots(name = "TraceMood")]
    pub enum TraceMood {
        #[default]
        #[dots(tag = 1)]
        Happy,
        #[dots(tag = 2)]
        Grumpy,
    }

    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "TracePerson", cached)]
    pub struct TracePerson {
        #[dots(tag = 1, key)]
        pub id: Option<u32>,
        #[dots(tag = 2)]
        pub mood: Option<TraceMood>,
    }
}
use foreign::*;

async fn run_handshake_server(framed: &mut Framed<DuplexStream, TransmissionCodec>) {
    let hello = dots!(DotsMsgHello {
        server_name: "descriptor-test-dotsd",
        auth_challenge: 0_u64,
        authentication_required: false,
    });
    push_typed(framed, "DotsMsgHello", &hello).await;
    let _connect = framed.next().await.unwrap().unwrap();
    let response = dots!(DotsMsgConnectResponse {
        server_name: "descriptor-test-dotsd",
        client_id: 1_u32,
        accepted: true,
        preload: false,
    });
    push_typed(framed, "DotsMsgConnectResponse", &response).await;
}

/// Push a typed transmission as raw frame bytes — the scripted-server
/// equivalent of the broker streaming a cached instance.
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

async fn connect_guest() -> (
    Arc<GuestTransceiver>,
    GuestDriver<DuplexStream>,
    Framed<DuplexStream, TransmissionCodec>,
    Arc<Registry>,
) {
    let (client_io, server_io) = tokio::io::duplex(65536);
    // Internal types only — TraceMood / TracePerson stay unknown.
    let reg = Arc::new(Registry::with_static_types(StaticTypePolicy::All));

    let server_reg = reg.clone();
    let server = tokio::spawn(async move {
        let mut framed = Framed::new(server_io, TransmissionCodec::new(server_reg));
        run_handshake_server(&mut framed).await;
        framed
    });

    let conn = Connection::establish(client_io, "descriptor-test-guest", reg.clone())
        .await
        .expect("handshake succeeds");
    let framed = server.await.unwrap();
    let (transceiver, driver) = GuestTransceiver::from_connection(reg.clone(), conn, [], []);
    (transceiver, driver, framed, reg)
}

/// Ports the dots-trace failure: the broker streams an enum descriptor
/// followed by a struct descriptor whose property references the enum
/// (descriptor exchange ships enums first). The guest must register
/// the enum so the struct descriptor builds — previously this failed
/// with `UnknownType("TraceMood")`.
#[tokio::test]
async fn wire_enum_descriptor_registers_and_struct_referencing_it_builds() {
    let (transceiver, driver, mut server, reg) = connect_guest().await;

    let learned: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let learned_in = learned.clone();
    let _sub = transceiver.subscribe_new_struct_type(move |descriptor| {
        learned_in.lock().unwrap().push(descriptor.name.clone());
    });
    let driver_handle = tokio::spawn(driver.run());

    // Enum first, then the struct referencing it — the broker's send
    // order (same contract as dots-cpp descriptor exchange).
    let enum_data = EnumDescriptorData::from_static(TraceMood::DESCRIPTOR);
    push_typed(&mut server, "EnumDescriptorData", &enum_data).await;
    let struct_data = StructDescriptorData::from_static(TracePerson::DESCRIPTOR);
    push_typed(&mut server, "StructDescriptorData", &struct_data).await;

    // The struct handler only fires if build_dynamic_struct succeeded.
    let learned_for_wait = learned.clone();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !learned_for_wait.lock().unwrap().iter().any(|n| n == "TracePerson") {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out: TracePerson descriptor was never built \
             (wire enum registration failed?); learned so far: {:?}",
            learned_for_wait.lock().unwrap()
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // The enum itself must be in the registry ...
    match reg.lookup("TraceMood") {
        Some(dots_rs_model::DescriptorEntry::Enum(e)) => {
            assert_eq!(e.elements.len(), 2);
            assert_eq!(e.element_by_tag(2).unwrap().name, "Grumpy");
        }
        other => panic!("expected registered enum TraceMood, got {other:?}"),
    }
    // ... and the struct's property must resolve to it.
    match reg.lookup("TracePerson") {
        Some(dots_rs_model::DescriptorEntry::Struct(s)) => {
            let mood = s.property(2).expect("mood property");
            match &mood.kind {
                dots_rs_core::DynamicFieldKind::Enum(e) => assert_eq!(e.name, "TraceMood"),
                other => panic!("expected DynamicFieldKind::Enum, got {other:?}"),
            }
        }
        other => panic!("expected registered struct TracePerson, got {other:?}"),
    }

    transceiver.exit();
    let _ = timeout(Duration::from_secs(2), driver_handle).await;
}

/// Descriptor import is a receive-layer concern, not a subscription
/// concern (cpp `Channel::importDependencies` runs on EVERY received
/// transmission): even with no `subscribe_new_struct_type` installed,
/// wire descriptors must land in the registry.
#[tokio::test]
async fn wire_descriptors_import_without_any_subscription() {
    let (transceiver, driver, mut server, reg) = connect_guest().await;
    let driver_handle = tokio::spawn(driver.run());

    let enum_data = EnumDescriptorData::from_static(TraceMood::DESCRIPTOR);
    push_typed(&mut server, "EnumDescriptorData", &enum_data).await;
    let struct_data = StructDescriptorData::from_static(TracePerson::DESCRIPTOR);
    push_typed(&mut server, "StructDescriptorData", &struct_data).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while reg.lookup("TracePerson").is_none() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out: TracePerson was never imported without a subscription"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(matches!(
        reg.lookup("TraceMood"),
        Some(dots_rs_model::DescriptorEntry::Enum(_))
    ));

    transceiver.exit();
    let _ = timeout(Duration::from_secs(2), driver_handle).await;
}

/// Import is reuse-if-present (cpp `DescriptorConverter` returns the
/// registry's existing descriptor for a known name): a replayed or
/// echoed descriptor — including the broker echoing back a type this
/// guest has compiled in statically — must NOT replace the existing
/// registry entry. `DynamicStruct` equality compares descriptors by
/// `Arc` identity, so a silent replacement would split the world.
#[tokio::test]
async fn echoed_descriptor_does_not_replace_existing_entry() {
    let (transceiver, driver, mut server, reg) = connect_guest().await;

    // "Compiled-in" type: statically registered before anything
    // arrives on the wire, as GuestDriver::early_subscribe does.
    reg.register_struct_static(TracePerson::DESCRIPTOR);
    reg.register_enum_static(TraceMood::DESCRIPTOR);
    let static_person = match reg.lookup("TracePerson").unwrap() {
        dots_rs_model::DescriptorEntry::Struct(d) => d,
        _ => unreachable!(),
    };

    let driver_handle = tokio::spawn(driver.run());

    // The broker echoes our own descriptors back (it fans descriptor
    // publishes out like any cached type).
    let enum_data = EnumDescriptorData::from_static(TraceMood::DESCRIPTOR);
    push_typed(&mut server, "EnumDescriptorData", &enum_data).await;
    let struct_data = StructDescriptorData::from_static(TracePerson::DESCRIPTOR);
    push_typed(&mut server, "StructDescriptorData", &struct_data).await;

    // Barrier: a later unrelated descriptor proves the echoes above
    // have been processed once it appears in the registry.
    #[derive(dots_rs_derive::DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "TraceBarrier")]
    struct TraceBarrier {
        #[dots(tag = 1)]
        note: Option<String>,
    }
    let barrier_data = StructDescriptorData::from_static(TraceBarrier::DESCRIPTOR);
    push_typed(&mut server, "StructDescriptorData", &barrier_data).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while reg.lookup("TraceBarrier").is_none() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for the barrier descriptor"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let after = match reg.lookup("TracePerson").unwrap() {
        dots_rs_model::DescriptorEntry::Struct(d) => d,
        _ => panic!("TracePerson entry changed kind"),
    };
    assert!(
        Arc::ptr_eq(&static_person, &after),
        "an echoed StructDescriptorData replaced the static registry entry"
    );

    transceiver.exit();
    let _ = timeout(Duration::from_secs(2), driver_handle).await;
}
