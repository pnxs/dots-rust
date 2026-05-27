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

use dots_core::{PropertySet, StructValue, decode_typed_from_slice, dots, encode_to_vec};
use dots_derive::DotsStruct;
use dots_model::{
    DotsHeader, DotsMsgConnectResponse, DotsMsgHello, Registry, Transmission,
    encode_transmission, registry_with_internal_types,
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
    let header = dots!(DotsHeader {
        type_name: type_name,
        sender: 99_u32,
    });
    let descriptor = match reg.lookup(type_name).expect("type registered") {
        dots_model::DescriptorEntry::Struct(d) => d.clone(),
        _ => panic!("expected struct entry for {type_name}"),
    };
    let bytes = encode_to_vec(payload);
    let payload = dots_core::DynamicStruct::decode(descriptor, &bytes).unwrap();
    Transmission { header, payload: dots_model::Payload::Wire(payload) }
}

/// Run the standard happy-path handshake from the server side.
async fn run_handshake_server(
    framed: &mut Framed<DuplexStream, TransmissionCodec>,
    reg: &Arc<Registry>,
) {
    let hello = dots!(DotsMsgHello {
        server_name: "test-dotsd",
        auth_challenge: 0_u64,
        authentication_required: false,
    });
    framed
        .send(dynamic_for(reg, "DotsMsgHello", &hello))
        .await
        .unwrap();
    let _connect = framed.next().await.unwrap().unwrap();
    let response = dots!(DotsMsgConnectResponse {
        server_name: "test-dotsd",
        client_id: 1_u32,
        accepted: true,
        preload: false,
        preload_finished: true,
    });
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

        let pinger = dots!(Pinger {
            id: 7_u32,
            message: "hello",
            sequence: 1_u64,
        });
        // Send via raw bytes through the underlying stream — the
        // codec's encoder takes Transmission and we want to test the
        // typed-payload arrival path on the receive side.
        let header = dots!(DotsHeader {
            type_name: "Pinger",
            sender: 42_u32,
        });
        let frame = encode_transmission(&header, &pinger);
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
        let bonk_header = dots!(DotsHeader {
            type_name: "Bonk",
        });
        let bonk = dots!(Bonk {
            note: "noise",
        });
        framed
            .get_mut()
            .write_all(&encode_transmission(&bonk_header, &bonk))
            .await
            .unwrap();

        let pinger_header = dots!(DotsHeader {
            type_name: "Pinger",
        });
        let pinger = dots!(Pinger {
            id: 1_u32,
            message: "only-this",
        });
        framed
            .get_mut()
            .write_all(&encode_transmission(&pinger_header, &pinger))
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

        let header = dots!(DotsHeader {
            type_name: "Pinger",
        });
        let pinger = dots!(Pinger {
            id: 2_u32,
            message: "broadcast",
        });
        framed
            .get_mut()
            .write_all(&encode_transmission(&header, &pinger))
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
        for i in 1..=2u32 {
            let header = dots!(DotsHeader {
                type_name: "Pinger",
            });
            let pinger = dots!(Pinger {
                id: i,
            });
            framed
                .get_mut()
                .write_all(&encode_transmission(&header, &pinger))
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
    let pinger = dots!(Pinger {
        id: 123_u32,
        message: "from publish",
        sequence: 0_u64,
    });
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
        let header = dots!(DotsHeader {
            type_name: "Pinger",
            sender: 999_u32,
        });
        framed
            .get_mut()
            .write_all(&encode_transmission(&header, &pinger))
            .await
            .unwrap();
    });

    let mut conn = Connection::establish(client_io, "echo-client", reg).await.unwrap();
    let mut sub = conn.subscribe::<Pinger>();

    let original = dots!(Pinger {
        id: 11_u32,
        message: "echo me",
        sequence: 7_u64,
    });
    conn.publish(&original).await.unwrap();
    conn.next().await.unwrap().unwrap();

    let event = sub.recv().await.unwrap();
    assert_eq!(event.value, original);
    assert_eq!(event.header.sender, Some(999));

    drop(conn);
    server.await.unwrap();
}

#[tokio::test]
async fn publish_with_mask_drops_excluded_properties_keeps_keys() {
    // Pinger has tag 1 (key, id), tag 2 (message), tag 3 (sequence).
    // Publish a fully-populated Pinger but mask down to tag 3 only;
    // expect the wire payload to carry tags 1 (key, auto-included)
    // and 3, and to omit tag 2.
    let (client_io, server_io) = tokio::io::duplex(4096);
    let reg = registry();

    let server_reg = reg.clone();
    let server = tokio::spawn(async move {
        let codec = TransmissionCodec::new(server_reg.clone());
        let mut framed = Framed::new(server_io, codec);
        run_handshake_server(&mut framed, &server_reg).await;
        let txn = framed.next().await.unwrap().unwrap();
        let bytes = txn.payload.encode();
        let decoded: Pinger = decode_typed_from_slice(&bytes).unwrap();
        (txn.header, decoded)
    });

    let mut conn = Connection::establish(client_io, "mask-client", reg).await.unwrap();
    let pinger = dots!(Pinger {
        id: 42_u32,
        message: "dropped",
        sequence: 99_u64,
    });
    let mask = PropertySet::EMPTY.with_tag(3);
    conn.publish_with_mask(&pinger, mask).await.unwrap();

    drop(conn);
    let (header, decoded) = server.await.unwrap();

    // Only id (key, tag 1) and sequence (tag 3) — message (tag 2) is gone.
    assert_eq!(decoded.id, Some(42));
    assert_eq!(decoded.message, None);
    assert_eq!(decoded.sequence, Some(99));

    // Header attributes mask should match: bit 1 (key) | bit 3 (sequence).
    let expected = PropertySet::EMPTY.with_tag(1).with_tag(3);
    assert_eq!(header.attributes, Some(expected));
}
