//! Subscribe / publish API tests, paired with a fake-server task on
//! the other end of `tokio::io::duplex`.
//!
//! Each test follows the same shape:
//!
//! 1. Spawn a "fake server" task that runs the standard handshake,
//!    then drives whatever publish/receive behavior the test needs.
//! 2. Establish a `Connection`, subscribe to typed events and/or
//!    publish typed values.
//! 3. Drive `Connection::next()` to advance reads (which dispatches
//!    to subscriptions as a side effect).

use std::sync::Arc;

use dots_core::{StructValue, decode_typed_from_slice, encode_to_vec};
use dots_derive::DotsStruct;
use dots_model::{
    DotsHeader, DotsMsgConnectResponse, DotsMsgHello, Registry, Transmission,
    encode_typed_transmission, registry_with_internal_types,
};
use dots_transport::{Connection, TransmissionCodec};
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
    #[dots(tag = 3)]
    sequence: Option<u64>,
}

#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "Bonk")]
struct Bonk {
    #[dots(tag = 1)]
    note: Option<String>,
}

fn registry() -> Arc<Registry> {
    let reg = registry_with_internal_types();
    reg.register_struct_static(Pinger::DESCRIPTOR);
    reg.register_struct_static(Bonk::DESCRIPTOR);
    Arc::new(reg)
}

fn dynamic_for(reg: &Registry, type_name: &str, payload: &dyn StructValue) -> Transmission {
    let header = DotsHeader {
        type_name: Some(type_name.into()),
        sender: Some(99),
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

/// Run the standard happy-path handshake from the server side.
async fn run_handshake_server(
    framed: &mut Framed<DuplexStream, TransmissionCodec>,
    reg: &Arc<Registry>,
) {
    let hello = DotsMsgHello {
        server_name: Some("test-dotsd".into()),
        auth_challenge: Some(0),
        authentication_required: Some(false),
    };
    framed
        .send(dynamic_for(reg, "DotsMsgHello", &hello))
        .await
        .unwrap();
    let _connect = framed.next().await.unwrap().unwrap();
    let response = DotsMsgConnectResponse {
        server_name: Some("test-dotsd".into()),
        client_id: Some(1),
        accepted: Some(true),
        preload: Some(false),
        preload_finished: Some(true),
    };
    framed
        .send(dynamic_for(reg, "DotsMsgConnectResponse", &response))
        .await
        .unwrap();
}

// ----- Subscription receives typed events -----

#[tokio::test]
async fn subscription_receives_event_pushed_by_server() {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let reg = registry();

    let server_reg = reg.clone();
    let server = tokio::spawn(async move {
        let codec = TransmissionCodec::new(server_reg.clone());
        let mut framed = Framed::new(server_io, codec);
        run_handshake_server(&mut framed, &server_reg).await;

        let pinger = Pinger {
            id: Some(7),
            message: Some("hello".into()),
            sequence: Some(1),
        };
        // Send via raw bytes through the underlying stream — the
        // codec's encoder takes Transmission and we want to test the
        // typed-payload arrival path on the receive side.
        let header = DotsHeader {
            type_name: Some("Pinger".into()),
            sender: Some(42),
            ..Default::default()
        };
        let frame = encode_typed_transmission(&header, &pinger);
        framed.get_mut().write_all(&frame).await.unwrap();
    });

    let mut conn = Connection::establish(client_io, "client", reg).await.unwrap();
    let mut sub = conn.subscribe::<Pinger>();

    // Drive one read on the connection — this pulls one transmission
    // off the wire and dispatches it to subscribers.
    conn.next().await.unwrap().unwrap();

    let event = sub.recv().await.expect("subscription receives event");
    assert_eq!(event.value.id, Some(7));
    assert_eq!(event.value.message.as_deref(), Some("hello"));
    assert_eq!(event.header.sender, Some(42));

    drop(conn);
    server.await.unwrap();
}

#[tokio::test]
async fn subscription_filters_by_type_name() {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let reg = registry();

    let server_reg = reg.clone();
    let server = tokio::spawn(async move {
        let codec = TransmissionCodec::new(server_reg.clone());
        let mut framed = Framed::new(server_io, codec);
        run_handshake_server(&mut framed, &server_reg).await;

        // Send a Bonk first (different type), then a Pinger.
        let bonk_header = DotsHeader {
            type_name: Some("Bonk".into()),
            ..Default::default()
        };
        let bonk = Bonk {
            note: Some("noise".into()),
        };
        framed
            .get_mut()
            .write_all(&encode_typed_transmission(&bonk_header, &bonk))
            .await
            .unwrap();

        let pinger_header = DotsHeader {
            type_name: Some("Pinger".into()),
            ..Default::default()
        };
        let pinger = Pinger {
            id: Some(1),
            message: Some("only-this".into()),
            ..Default::default()
        };
        framed
            .get_mut()
            .write_all(&encode_typed_transmission(&pinger_header, &pinger))
            .await
            .unwrap();
    });

    let mut conn = Connection::establish(client_io, "filter-client", reg).await.unwrap();
    let mut pinger_sub = conn.subscribe::<Pinger>();

    // Drive both reads.
    conn.next().await.unwrap().unwrap();
    conn.next().await.unwrap().unwrap();

    let event = pinger_sub.recv().await.unwrap();
    assert_eq!(event.value.message.as_deref(), Some("only-this"));
    // Confirm only one Pinger arrived (no spurious Bonk leaks through).
    let next = tokio::time::timeout(std::time::Duration::from_millis(50), pinger_sub.recv()).await;
    assert!(next.is_err(), "no further Pinger should be queued");

    drop(conn);
    server.await.unwrap();
}

#[tokio::test]
async fn multiple_subscriptions_to_same_type_each_receive() {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let reg = registry();

    let server_reg = reg.clone();
    let server = tokio::spawn(async move {
        let codec = TransmissionCodec::new(server_reg.clone());
        let mut framed = Framed::new(server_io, codec);
        run_handshake_server(&mut framed, &server_reg).await;

        let header = DotsHeader {
            type_name: Some("Pinger".into()),
            ..Default::default()
        };
        let pinger = Pinger {
            id: Some(2),
            message: Some("broadcast".into()),
            ..Default::default()
        };
        framed
            .get_mut()
            .write_all(&encode_typed_transmission(&header, &pinger))
            .await
            .unwrap();
    });

    let mut conn = Connection::establish(client_io, "client", reg).await.unwrap();
    let mut sub_a = conn.subscribe::<Pinger>();
    let mut sub_b = conn.subscribe::<Pinger>();

    conn.next().await.unwrap().unwrap();

    let a = sub_a.recv().await.unwrap();
    let b = sub_b.recv().await.unwrap();
    assert_eq!(a.value.message.as_deref(), Some("broadcast"));
    assert_eq!(b.value.message.as_deref(), Some("broadcast"));

    drop(conn);
    server.await.unwrap();
}

#[tokio::test]
async fn dropping_subscription_stops_receiving() {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let reg = registry();

    let server_reg = reg.clone();
    let server = tokio::spawn(async move {
        let codec = TransmissionCodec::new(server_reg.clone());
        let mut framed = Framed::new(server_io, codec);
        run_handshake_server(&mut framed, &server_reg).await;

        // Two Pingers.
        for i in 1..=2 {
            let header = DotsHeader {
                type_name: Some("Pinger".into()),
                ..Default::default()
            };
            let pinger = Pinger {
                id: Some(i),
                ..Default::default()
            };
            framed
                .get_mut()
                .write_all(&encode_typed_transmission(&header, &pinger))
                .await
                .unwrap();
        }
    });

    let mut conn = Connection::establish(client_io, "drop-client", reg).await.unwrap();
    let mut sub = conn.subscribe::<Pinger>();
    conn.next().await.unwrap().unwrap();
    let first = sub.recv().await.unwrap();
    assert_eq!(first.value.id, Some(1));

    // Drop the subscription. Subsequent dispatches should be no-ops.
    drop(sub);
    conn.next().await.unwrap().unwrap();

    drop(conn);
    server.await.unwrap();
}

// ----- Publish path -----

#[tokio::test]
async fn publish_sends_typed_value_to_server() {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let reg = registry();

    let server_reg = reg.clone();
    let server = tokio::spawn(async move {
        let codec = TransmissionCodec::new(server_reg.clone());
        let mut framed = Framed::new(server_io, codec);
        run_handshake_server(&mut framed, &server_reg).await;

        let txn = framed.next().await.unwrap().unwrap();
        assert_eq!(txn.header.type_name.as_deref(), Some("Pinger"));
        let bytes = txn.payload.encode();
        let received: Pinger = decode_typed_from_slice(&bytes).unwrap();
        assert_eq!(received.id, Some(123));
        assert_eq!(received.message.as_deref(), Some("from publish"));
    });

    let mut conn = Connection::establish(client_io, "publisher", reg).await.unwrap();
    let pinger = Pinger {
        id: Some(123),
        message: Some("from publish".into()),
        sequence: Some(0),
    };
    conn.publish(&pinger).await.unwrap();

    drop(conn);
    server.await.unwrap();
}

// ----- End-to-end: publish loops back via subscription (if dotsd
//       routes it back to us — here we simulate by having the server
//       echo what it receives) -----

#[tokio::test]
async fn publish_then_server_echoes_then_subscription_receives() {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let reg = registry();

    let server_reg = reg.clone();
    let server = tokio::spawn(async move {
        let codec = TransmissionCodec::new(server_reg.clone());
        let mut framed = Framed::new(server_io, codec);
        run_handshake_server(&mut framed, &server_reg).await;

        // Receive the published Pinger ...
        let txn = framed.next().await.unwrap().unwrap();
        let bytes = txn.payload.encode();
        let pinger: Pinger = decode_typed_from_slice(&bytes).unwrap();
        // ... and echo it back.
        let header = DotsHeader {
            type_name: Some("Pinger".into()),
            sender: Some(999),
            ..Default::default()
        };
        framed
            .get_mut()
            .write_all(&encode_typed_transmission(&header, &pinger))
            .await
            .unwrap();
    });

    let mut conn = Connection::establish(client_io, "echo-client", reg).await.unwrap();
    let mut sub = conn.subscribe::<Pinger>();

    let original = Pinger {
        id: Some(11),
        message: Some("echo me".into()),
        sequence: Some(7),
    };
    conn.publish(&original).await.unwrap();
    conn.next().await.unwrap().unwrap();

    let event = sub.recv().await.unwrap();
    assert_eq!(event.value, original);
    assert_eq!(event.header.sender, Some(999));

    drop(conn);
    server.await.unwrap();
}
