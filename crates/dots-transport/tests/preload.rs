//! Builder + cache preload flow tests, paired with a fake-server task.
//!
//! The full lifecycle the builder drives:
//!
//!   client                              server
//!   ──────                              ──────
//!                              ←  Hello (auth_required = false)
//!   Connect (preload_cache=true)  →
//!                              ←  ConnectResponse (accepted, preload=true)
//!   StructDescriptorData(Pinger)  →
//!   [...one per declared type]
//!   ── client now in EarlySubscribe; user adds subscriptions ──
//!   Connect (preload_client_finished=true)  →
//!   [server streams cache transmissions]   ←
//!   [...per-type cached objects, header.from_cache > 0]
//!                              ←  ConnectResponse (preload_finished=true)
//!   ── client transitions to Connected ──

use std::sync::Arc;

use dots_core::{StructValue, decode_typed_from_slice, dots, encode_to_vec};
use dots_model::{
    DotsConnectionState, DotsHeader, DotsMsgConnect, DotsMsgConnectResponse, DotsMsgHello,
    Registry, StructDescriptorData, Transmission, encode_transmission,
    registry_with_internal_types,
};
#[allow(unused_imports)]
use dots_model::*;
use dots_transport::{
    ConnectionBuilder, ConnectionError, GuestTransceiver, TransmissionCodec,
};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncWriteExt, DuplexStream};
use tokio_util::codec::Framed;

mod model {
    use dots_derive::DotsStruct;

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
    let reg = registry_with_internal_types();
    reg.register_struct_static(Pinger::DESCRIPTOR);
    Arc::new(reg)
}

fn dynamic_for(reg: &Registry, type_name: &str, payload: &dyn StructValue) -> Transmission {
    let header = dots!(DotsHeader {
        type_name: type_name,
        sender: 1_u32,
    });
    let descriptor = match reg.lookup(type_name).expect("type registered") {
        dots_model::DescriptorEntry::Struct(d) => d.clone(),
        _ => panic!("expected struct entry for {type_name}"),
    };
    let bytes = encode_to_vec(payload);
    let payload = dots_core::DynamicStruct::decode(descriptor, &bytes).unwrap();
    Transmission { header, payload: dots_model::Payload::Wire(payload) }
}

/// Fake server: receives Connect with preload=true, responds, then
/// expects the StructDescriptorData publish for Pinger, then the
/// DotsMember(Join, Pinger) the driver emits in Phase 1b, then the
/// preloadClientFinished Connect, then streams `cached_pingers` back
/// (each with header.from_cache decreasing), and finally sends the
/// preloadFinished response.
async fn preload_server(
    server_io: DuplexStream,
    reg: Arc<Registry>,
    cached_pingers: Vec<Pinger>,
) {
    let codec = TransmissionCodec::new(reg.clone());
    let mut framed = Framed::new(server_io, codec);

    // Hello.
    let hello = dots!(DotsMsgHello {
        server_name: "preload-test",
        auth_challenge: 0_u64,
        authentication_required: false,
    });
    framed
        .send(dynamic_for(&reg, "DotsMsgHello", &hello))
        .await
        .unwrap();

    // Receive Connect (preload=true).
    let txn = framed.next().await.unwrap().unwrap();
    let bytes = txn.payload.encode();
    let connect: DotsMsgConnect = decode_typed_from_slice(&bytes).unwrap();
    assert_eq!(connect.preload_cache, Some(true));

    // Initial ConnectResponse (preload=true).
    let response = dots!(DotsMsgConnectResponse {
        server_name: "preload-test",
        client_id: 42_u32,
        accepted: true,
        preload: true,
        preload_finished: false,
    });
    framed
        .send(dynamic_for(&reg, "DotsMsgConnectResponse", &response))
        .await
        .unwrap();

    // Receive StructDescriptorData(Pinger).
    let txn = framed.next().await.unwrap().unwrap();
    assert_eq!(
        txn.header.type_name.as_deref(),
        Some("StructDescriptorData")
    );
    let bytes = txn.payload.encode();
    let descriptor_data: StructDescriptorData = decode_typed_from_slice(&bytes).unwrap();
    assert_eq!(descriptor_data.name.as_deref(), Some("Pinger"));

    // Receive DotsMember(Join, Pinger) — Phase 1b of the driver.
    let txn = framed.next().await.unwrap().unwrap();
    assert_eq!(txn.header.type_name.as_deref(), Some("DotsMember"));

    // Receive Connect (preload_client_finished=true).
    let txn = framed.next().await.unwrap().unwrap();
    let bytes = txn.payload.encode();
    let finish: DotsMsgConnect = decode_typed_from_slice(&bytes).unwrap();
    assert_eq!(finish.preload_client_finished, Some(true));

    // Stream cached Pingers.
    let total = cached_pingers.len() as u32;
    for (i, pinger) in cached_pingers.iter().enumerate() {
        let remaining = total - 1 - i as u32;
        let header = dots!(DotsHeader {
            type_name: "Pinger",
            sender: 99_u32,
            from_cache: remaining,
        });
        let frame = encode_transmission(&header, pinger);
        framed.get_mut().write_all(&frame).await.unwrap();
    }

    // Final ConnectResponse (preload_finished=true).
    let response = dots!(DotsMsgConnectResponse {
        server_name: "preload-test",
        client_id: 42_u32,
        accepted: true,
        preload: true,
        preload_finished: true,
    });
    framed
        .send(dynamic_for(&reg, "DotsMsgConnectResponse", &response))
        .await
        .unwrap();
}

// ----- Tests -----

#[tokio::test]
async fn builder_no_preload_lands_directly_in_connected() {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let reg = registry();

    let server_reg = reg.clone();
    let server = tokio::spawn(async move {
        let codec = TransmissionCodec::new(server_reg.clone());
        let mut framed = Framed::new(server_io, codec);
        let hello = dots!(DotsMsgHello {
            server_name: "s",
            auth_challenge: 0_u64,
            authentication_required: false,
        });
        framed
            .send(dynamic_for(&server_reg, "DotsMsgHello", &hello))
            .await
            .unwrap();
        // Receive Connect — preload should be false.
        let txn = framed.next().await.unwrap().unwrap();
        let bytes = txn.payload.encode();
        let connect: DotsMsgConnect = decode_typed_from_slice(&bytes).unwrap();
        assert_eq!(connect.preload_cache, Some(false));
        let response = dots!(DotsMsgConnectResponse {
            client_id: 7_u32,
            accepted: true,
            preload: false,
        });
        framed
            .send(dynamic_for(
                &server_reg,
                "DotsMsgConnectResponse",
                &response,
            ))
            .await
            .unwrap();
    });

    let conn = ConnectionBuilder::new(client_io, "no-preload", reg)
        .preload(false)
        .connect()
        .await
        .unwrap();
    assert_eq!(conn.state(), DotsConnectionState::Connected);
    drop(conn);
    server.await.unwrap();
}

#[tokio::test]
async fn builder_with_preload_lands_in_early_subscribe_then_finishes() {
    let (client_io, server_io) = tokio::io::duplex(8192);
    let reg = registry();

    let cached = vec![
        dots!(Pinger {
            id: 1_u32,
            message: "cached-1",
        }),
        dots!(Pinger {
            id: 2_u32,
            message: "cached-2",
        }),
    ];
    let server = tokio::spawn(preload_server(server_io, reg.clone(), cached.clone()));

    let conn = ConnectionBuilder::new(client_io, "preload-client", reg.clone())
        .preload(true)
        .connect()
        .await
        .unwrap();
    assert_eq!(conn.state(), DotsConnectionState::EarlySubscribe);

    let (gt, driver) = GuestTransceiver::from_connection(
        reg,
        conn,
        [Pinger::DESCRIPTOR],
        [Pinger::DESCRIPTOR],
    );
    let mut sub = gt.subscribe_stream::<Pinger>();
    let driver_handle = tokio::spawn(driver.run());

    // The driver completes Phase 1 (descriptors), Phase 1b (Joins),
    // Phase 2 (finish_preload + cache dispatch), then enters the main
    // loop. Cache events for Pinger flow into `sub`.
    let first = sub.recv().await.expect("cache event 1");
    assert_eq!(first.value, cached[0]);
    assert_eq!(first.header.from_cache, Some(1));

    let second = sub.recv().await.expect("cache event 2");
    assert_eq!(second.value, cached[1]);
    assert_eq!(second.header.from_cache, Some(0));

    gt.exit();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), driver_handle).await;
    server.await.unwrap();
}

#[tokio::test]
async fn finish_preload_errors_when_not_in_early_subscribe() {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let reg = registry();

    let server_reg = reg.clone();
    let server = tokio::spawn(async move {
        let codec = TransmissionCodec::new(server_reg.clone());
        let mut framed = Framed::new(server_io, codec);
        let hello = dots!(DotsMsgHello {
            server_name: "s",
            auth_challenge: 0_u64,
            authentication_required: false,
        });
        framed
            .send(dynamic_for(&server_reg, "DotsMsgHello", &hello))
            .await
            .unwrap();
        let _connect = framed.next().await.unwrap().unwrap();
        let response = dots!(DotsMsgConnectResponse {
            client_id: 1_u32,
            accepted: true,
            preload: false,
        });
        framed
            .send(dynamic_for(
                &server_reg,
                "DotsMsgConnectResponse",
                &response,
            ))
            .await
            .unwrap();
    });

    let mut conn = ConnectionBuilder::new(client_io, "client", reg)
        .preload(false)
        .connect()
        .await
        .unwrap();

    match conn.finish_preload().await {
        Err(ConnectionError::InvalidState { expected, actual }) => {
            assert_eq!(expected, DotsConnectionState::EarlySubscribe);
            assert_eq!(actual, DotsConnectionState::Connected);
        }
        other => panic!("expected InvalidState, got {other:?}"),
    }
    drop(conn);
    server.await.unwrap();
}

#[tokio::test]
async fn driver_ships_enum_descriptors_before_struct_descriptors() {
    use dots_derive::{DotsEnum, DotsStruct};

    #[derive(DotsEnum, Default, Debug, Clone, Copy, PartialEq, Eq)]
    #[dots(name = "Color")]
    enum Color {
        #[default]
        #[dots(tag = 1)]
        Red,
        #[dots(tag = 2)]
        Green,
    }

    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "Painted", cached)]
    struct Painted {
        #[dots(tag = 1, key)]
        id: Option<u32>,
        #[dots(tag = 2)]
        color: Option<Color>,
    }

    let (client_io, server_io) = tokio::io::duplex(4096);
    let reg = registry();
    reg.register_struct_static(Painted::DESCRIPTOR);
    reg.register_enum_static(Color::DESCRIPTOR);

    let server_reg = reg.clone();
    let server = tokio::spawn(async move {
        let codec = TransmissionCodec::new(server_reg.clone());
        let mut framed = Framed::new(server_io, codec);
        let hello = dots!(DotsMsgHello {
            server_name: "s",
            auth_challenge: 0_u64,
            authentication_required: false,
        });
        framed
            .send(dynamic_for(&server_reg, "DotsMsgHello", &hello))
            .await
            .unwrap();
        let _connect = framed.next().await.unwrap().unwrap();
        let response = dots!(DotsMsgConnectResponse {
            client_id: 1_u32,
            accepted: true,
            preload: false,
        });
        framed
            .send(dynamic_for(
                &server_reg,
                "DotsMsgConnectResponse",
                &response,
            ))
            .await
            .unwrap();

        // Expect enums first, then structs: the broker resolves
        // nested type references via the registry as it parses each
        // StructDescriptorData, so any enum referenced as a struct
        // field must already be registered. Same constraint as
        // dots-cpp's descriptor exchange.
        let txn = framed.next().await.unwrap().unwrap();
        assert_eq!(
            txn.header.type_name.as_deref(),
            Some("EnumDescriptorData")
        );

        let txn = framed.next().await.unwrap().unwrap();
        assert_eq!(
            txn.header.type_name.as_deref(),
            Some("StructDescriptorData")
        );
    });

    let conn = ConnectionBuilder::new(client_io, "registrar", reg.clone())
        .preload(false)
        .connect()
        .await
        .unwrap();
    let (gt, driver) = GuestTransceiver::from_connection(
        reg,
        conn,
        [Painted::DESCRIPTOR],
        std::iter::empty(),
    );
    let driver_handle = tokio::spawn(driver.run());
    gt.exit();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), driver_handle).await;
    server.await.unwrap();
}

/// Mirrors a user's `let app = App::new(...).await?; let c =
/// app.container::<MyType>();` flow: the cache replay broker sends
/// during the EarlySubscribe phase has to land in the pool's
/// container *before* App::new returns, so the typed
/// `container::<T>()` call (which happens after) sees it.
///
/// Drives [`GuestDriver::early_subscribe`] directly (the part of
/// `App::new` that runs inside `build_app`), then asks for the
/// typed container. The expectation: container holds the broker's
/// cached entries, mirroring dots-cpp's
/// `ContainerPool::get<MyType>()` returning a populated container.
#[tokio::test]
async fn container_after_early_subscribe_contains_cache_replay() {
    let (client_io, server_io) = tokio::io::duplex(8192);
    let reg = registry();

    let cached = vec![
        dots!(Pinger {
            id: 1_u32,
            message: "cached-1",
        }),
        dots!(Pinger {
            id: 2_u32,
            message: "cached-2",
        }),
    ];
    let server = tokio::spawn(preload_server(server_io, reg.clone(), cached.clone()));

    let conn = ConnectionBuilder::new(client_io, "preload-client", reg.clone())
        .preload(true)
        .connect()
        .await
        .unwrap();

    let (gt, mut driver) = GuestTransceiver::from_connection(
        reg,
        conn,
        [Pinger::DESCRIPTOR],
        [Pinger::DESCRIPTOR],
    );

    // No subscription installed yet — match the user's reported
    // bug pattern. We're only running `early_subscribe`, which is
    // what `App::new` does internally.
    driver.early_subscribe().await.unwrap();

    // Cache replay should now be in the pool's container.
    let pingers = gt.container::<Pinger>();
    assert_eq!(
        pingers.len(),
        2,
        "container should contain the 2 cached Pingers after early_subscribe",
    );

    let snapshot = pingers.snapshot();
    let mut messages: Vec<String> = snapshot
        .iter()
        .filter_map(|e| e.value.message.clone())
        .collect();
    messages.sort();
    assert_eq!(messages, vec!["cached-1".to_string(), "cached-2".to_string()]);

    let driver_handle = tokio::spawn(driver.run());
    gt.exit();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), driver_handle).await;
    server.await.unwrap();
}
