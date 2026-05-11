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

use dots_core::{StructValue, decode_typed_from_slice, encode_to_vec};
use dots_derive::DotsStruct;
use dots_model::{
    DotsConnectionState, DotsHeader, DotsMsgConnect, DotsMsgConnectResponse, DotsMsgHello,
    Registry, StructDescriptorData, Transmission, encode_transmission,
    registry_with_internal_types,
};
use dots_transport::{ConnectionBuilder, ConnectionError, TransmissionCodec};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncWriteExt, DuplexStream};
use tokio_util::codec::Framed;

#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "Pinger", cached)]
struct Pinger {
    #[dots(tag = 1, key)]
    id: Option<u32>,
    #[dots(tag = 2)]
    message: Option<String>,
}

fn registry() -> Arc<Registry> {
    let reg = registry_with_internal_types();
    reg.register_struct_static(Pinger::DESCRIPTOR);
    Arc::new(reg)
}

fn dynamic_for(reg: &Registry, type_name: &str, payload: &dyn StructValue) -> Transmission {
    let header = DotsHeader {
        type_name: Some(type_name.into()),
        sender: Some(1),
        ..Default::default()
    };
    let descriptor = match reg.lookup(type_name).expect("type registered") {
        dots_model::DescriptorEntry::Struct(d) => d.clone(),
        _ => panic!("expected struct entry for {type_name}"),
    };
    let bytes = encode_to_vec(payload);
    let payload = dots_core::DynamicStruct::decode(descriptor, &bytes).unwrap();
    Transmission { header, payload }
}

/// Fake server: receives Connect with preload=true, responds, then
/// expects the StructDescriptorData publish for Pinger, then the
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
    let hello = DotsMsgHello {
        server_name: Some("preload-test".into()),
        auth_challenge: Some(0),
        authentication_required: Some(false),
    };
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
    let response = DotsMsgConnectResponse {
        server_name: Some("preload-test".into()),
        client_id: Some(42),
        accepted: Some(true),
        preload: Some(true),
        preload_finished: Some(false),
    };
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

    // Receive Connect (preload_client_finished=true).
    let txn = framed.next().await.unwrap().unwrap();
    let bytes = txn.payload.encode();
    let finish: DotsMsgConnect = decode_typed_from_slice(&bytes).unwrap();
    assert_eq!(finish.preload_client_finished, Some(true));

    // Stream cached Pingers.
    let total = cached_pingers.len() as u32;
    for (i, pinger) in cached_pingers.iter().enumerate() {
        let remaining = total - 1 - i as u32;
        let header = DotsHeader {
            type_name: Some("Pinger".into()),
            sender: Some(99),
            from_cache: Some(remaining),
            ..Default::default()
        };
        let frame = encode_transmission(&header, pinger);
        framed.get_mut().write_all(&frame).await.unwrap();
    }

    // Final ConnectResponse (preload_finished=true).
    let response = DotsMsgConnectResponse {
        server_name: Some("preload-test".into()),
        client_id: Some(42),
        accepted: Some(true),
        preload: Some(true),
        preload_finished: Some(true),
    };
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
        let hello = DotsMsgHello {
            server_name: Some("s".into()),
            auth_challenge: Some(0),
            authentication_required: Some(false),
        };
        framed
            .send(dynamic_for(&server_reg, "DotsMsgHello", &hello))
            .await
            .unwrap();
        // Receive Connect — preload should be false.
        let txn = framed.next().await.unwrap().unwrap();
        let bytes = txn.payload.encode();
        let connect: DotsMsgConnect = decode_typed_from_slice(&bytes).unwrap();
        assert_eq!(connect.preload_cache, Some(false));
        let response = DotsMsgConnectResponse {
            client_id: Some(7),
            accepted: Some(true),
            preload: Some(false),
            ..Default::default()
        };
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
        Pinger {
            id: Some(1),
            message: Some("cached-1".into()),
        },
        Pinger {
            id: Some(2),
            message: Some("cached-2".into()),
        },
    ];
    let server = tokio::spawn(preload_server(server_io, reg.clone(), cached.clone()));

    let mut conn = ConnectionBuilder::new(client_io, "preload-client", reg)
        .publishes::<Pinger>()
        .preload(true)
        .connect()
        .await
        .unwrap();
    assert_eq!(conn.state(), DotsConnectionState::EarlySubscribe);

    let mut sub = conn.subscribe::<Pinger>();
    conn.finish_preload().await.unwrap();
    assert_eq!(conn.state(), DotsConnectionState::Connected);

    // Drain the cached events from the subscription. They were
    // dispatched during finish_preload's read loop.
    let first = sub.recv().await.expect("cache event 1");
    assert_eq!(first.value, cached[0]);
    assert_eq!(first.header.from_cache, Some(1));

    let second = sub.recv().await.expect("cache event 2");
    assert_eq!(second.value, cached[1]);
    assert_eq!(second.header.from_cache, Some(0));

    drop(conn);
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
        let hello = DotsMsgHello {
            server_name: Some("s".into()),
            auth_challenge: Some(0),
            authentication_required: Some(false),
        };
        framed
            .send(dynamic_for(&server_reg, "DotsMsgHello", &hello))
            .await
            .unwrap();
        let _connect = framed.next().await.unwrap().unwrap();
        let response = DotsMsgConnectResponse {
            client_id: Some(1),
            accepted: Some(true),
            preload: Some(false),
            ..Default::default()
        };
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
async fn builder_publishes_struct_and_enum_descriptors_in_order() {
    use dots_derive::DotsEnum;

    #[derive(DotsEnum, Default, Debug, Clone, Copy, PartialEq, Eq)]
    #[dots(name = "Color")]
    enum Color {
        #[default]
        #[dots(tag = 1)]
        Red,
        #[dots(tag = 2)]
        Green,
    }

    let (client_io, server_io) = tokio::io::duplex(4096);
    let reg = registry();

    let server_reg = reg.clone();
    let server = tokio::spawn(async move {
        let codec = TransmissionCodec::new(server_reg.clone());
        let mut framed = Framed::new(server_io, codec);
        let hello = DotsMsgHello {
            server_name: Some("s".into()),
            auth_challenge: Some(0),
            authentication_required: Some(false),
        };
        framed
            .send(dynamic_for(&server_reg, "DotsMsgHello", &hello))
            .await
            .unwrap();
        let _connect = framed.next().await.unwrap().unwrap();
        let response = DotsMsgConnectResponse {
            client_id: Some(1),
            accepted: Some(true),
            preload: Some(false),
            ..Default::default()
        };
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

    let conn = ConnectionBuilder::new(client_io, "registrar", reg)
        .publishes_struct(Pinger::DESCRIPTOR)
        .publishes_enum(Color::DESCRIPTOR)
        .preload(false)
        .connect()
        .await
        .unwrap();
    drop(conn);
    server.await.unwrap();
}
