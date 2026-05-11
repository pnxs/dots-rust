//! Container tests with a fake server pushing create/update/remove
//! sequences over `tokio::io::duplex`.

use std::sync::Arc;

use dots_core::{StructValue, Timepoint, encode_to_vec};
use dots_derive::DotsStruct;
use dots_model::{
    DotsHeader, DotsMsgConnectResponse, DotsMsgHello, Registry, Transmission,
    encode_transmission, registry_with_internal_types,
};
use dots_transport::{Connection, Operation, TransmissionCodec};
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

fn registry() -> Arc<Registry> {
    let reg = registry_with_internal_types();
    reg.register_struct_static(Pinger::DESCRIPTOR);
    Arc::new(reg)
}

fn dynamic_for(reg: &Registry, type_name: &str, payload: &dyn StructValue) -> Transmission {
    let header = DotsHeader {
        type_name: Some(type_name.into()),
        ..Default::default()
    };
    let descriptor = match reg.lookup(type_name).expect("type registered") {
        dots_model::DescriptorEntry::Struct(d) => d.clone(),
        _ => panic!(),
    };
    let bytes = encode_to_vec(payload);
    let payload = dots_core::DynamicStruct::decode(descriptor, &bytes).unwrap();
    Transmission { header, payload }
}

async fn run_no_preload_handshake(
    framed: &mut Framed<DuplexStream, TransmissionCodec>,
    reg: &Arc<Registry>,
) {
    let hello = DotsMsgHello {
        server_name: Some("s".into()),
        auth_challenge: Some(0),
        authentication_required: Some(false),
    };
    framed
        .send(dynamic_for(reg, "DotsMsgHello", &hello))
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
        .send(dynamic_for(reg, "DotsMsgConnectResponse", &response))
        .await
        .unwrap();
}

/// Push a Pinger transmission with optional remove_obj / sender.
async fn push_pinger(
    framed: &mut Framed<DuplexStream, TransmissionCodec>,
    pinger: &Pinger,
    sender: Option<u32>,
    sent_time: Option<f64>,
    remove: bool,
) {
    let header = DotsHeader {
        type_name: Some("Pinger".into()),
        attributes: Some(pinger.valid_set()),
        sender,
        sent_time: sent_time.map(Timepoint),
        remove_obj: Some(remove),
        ..Default::default()
    };
    let frame = encode_transmission(&header, pinger);
    framed.get_mut().write_all(&frame).await.unwrap();
}

#[tokio::test]
async fn container_starts_empty() {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let reg = registry();

    let server_reg = reg.clone();
    let server = tokio::spawn(async move {
        let codec = TransmissionCodec::new(server_reg.clone());
        let mut framed = Framed::new(server_io, codec);
        run_no_preload_handshake(&mut framed, &server_reg).await;
    });

    let conn = Connection::establish(client_io, "client", reg).await.unwrap();
    let pingers = conn.container::<Pinger>();
    assert!(pingers.is_empty());
    assert_eq!(pingers.len(), 0);

    drop(conn);
    server.await.unwrap();
}

#[tokio::test]
async fn container_create_then_update_preserves_created_metadata() {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let reg = registry();

    let server_reg = reg.clone();
    let server = tokio::spawn(async move {
        let codec = TransmissionCodec::new(server_reg.clone());
        let mut framed = Framed::new(server_io, codec);
        run_no_preload_handshake(&mut framed, &server_reg).await;

        // First publish — Create.
        let p1 = Pinger {
            id: Some(7),
            message: Some("first".into()),
            sequence: Some(1),
        };
        push_pinger(&mut framed, &p1, Some(11), Some(100.0), false).await;

        // Second publish, same key — Update.
        let p2 = Pinger {
            id: Some(7),
            message: Some("second".into()),
            sequence: Some(2),
        };
        push_pinger(&mut framed, &p2, Some(22), Some(200.0), false).await;
    });

    let mut conn = Connection::establish(client_io, "client", reg).await.unwrap();
    let pingers = conn.container::<Pinger>();

    // Drive two reads to ingest the two transmissions.
    conn.next().await.unwrap().unwrap();
    conn.next().await.unwrap().unwrap();

    assert_eq!(pingers.len(), 1);
    let query = Pinger {
        id: Some(7),
        ..Default::default()
    };
    let entry = pingers.get(&query).expect("entry exists");
    assert_eq!(entry.value.message.as_deref(), Some("second"));
    assert_eq!(entry.clone_info.last_operation, Operation::Update);
    assert_eq!(entry.clone_info.last_update_sender, Some(22));
    assert_eq!(entry.clone_info.last_update_time, Some(Timepoint(200.0)));
    // created_* preserved from the first publish.
    assert_eq!(entry.clone_info.created_sender, Some(11));
    assert_eq!(entry.clone_info.created_time, Some(Timepoint(100.0)));

    drop(conn);
    server.await.unwrap();
}

#[tokio::test]
async fn container_remove_deletes_entry() {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let reg = registry();

    let server_reg = reg.clone();
    let server = tokio::spawn(async move {
        let codec = TransmissionCodec::new(server_reg.clone());
        let mut framed = Framed::new(server_io, codec);
        run_no_preload_handshake(&mut framed, &server_reg).await;

        let p = Pinger {
            id: Some(5),
            message: Some("alive".into()),
            ..Default::default()
        };
        push_pinger(&mut framed, &p, None, None, false).await;
        // Same key, remove_obj = true.
        push_pinger(&mut framed, &p, None, None, true).await;
    });

    let mut conn = Connection::establish(client_io, "client", reg).await.unwrap();
    let pingers = conn.container::<Pinger>();

    conn.next().await.unwrap().unwrap();
    assert_eq!(pingers.len(), 1);
    conn.next().await.unwrap().unwrap();
    assert_eq!(pingers.len(), 0);

    drop(conn);
    server.await.unwrap();
}

#[tokio::test]
async fn container_indexes_by_key_only() {
    // Two Pingers with the same id but different non-key fields — the
    // second should overwrite the first (same key).
    let (client_io, server_io) = tokio::io::duplex(4096);
    let reg = registry();

    let server_reg = reg.clone();
    let server = tokio::spawn(async move {
        let codec = TransmissionCodec::new(server_reg.clone());
        let mut framed = Framed::new(server_io, codec);
        run_no_preload_handshake(&mut framed, &server_reg).await;

        let p1 = Pinger {
            id: Some(99),
            message: Some("one".into()),
            sequence: Some(1),
        };
        let p2 = Pinger {
            id: Some(99),
            message: Some("two".into()),
            sequence: Some(2),
        };
        push_pinger(&mut framed, &p1, None, None, false).await;
        push_pinger(&mut framed, &p2, None, None, false).await;
    });

    let mut conn = Connection::establish(client_io, "client", reg).await.unwrap();
    let pingers = conn.container::<Pinger>();

    conn.next().await.unwrap().unwrap();
    conn.next().await.unwrap().unwrap();
    assert_eq!(pingers.len(), 1);

    // Lookup by example.
    let entry = pingers
        .get(&Pinger {
            id: Some(99),
            // Other fields irrelevant for key lookup.
            sequence: Some(99999),
            ..Default::default()
        })
        .expect("found by id");
    assert_eq!(entry.value.sequence, Some(2));

    drop(conn);
    server.await.unwrap();
}

#[tokio::test]
async fn container_holds_multiple_distinct_keys() {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let reg = registry();

    let server_reg = reg.clone();
    let server = tokio::spawn(async move {
        let codec = TransmissionCodec::new(server_reg.clone());
        let mut framed = Framed::new(server_io, codec);
        run_no_preload_handshake(&mut framed, &server_reg).await;

        for id in 1..=3u32 {
            let p = Pinger {
                id: Some(id),
                message: Some(format!("entry-{id}")),
                ..Default::default()
            };
            push_pinger(&mut framed, &p, None, None, false).await;
        }
    });

    let mut conn = Connection::establish(client_io, "client", reg).await.unwrap();
    let pingers = conn.container::<Pinger>();

    for _ in 0..3 {
        conn.next().await.unwrap().unwrap();
    }
    assert_eq!(pingers.len(), 3);

    let snapshot = pingers.snapshot();
    let mut messages: Vec<String> = snapshot
        .iter()
        .map(|e| e.value.message.clone().unwrap_or_default())
        .collect();
    messages.sort();
    assert_eq!(messages, vec!["entry-1", "entry-2", "entry-3"]);

    drop(conn);
    server.await.unwrap();
}

#[tokio::test]
async fn container_and_subscription_both_receive() {
    // Both a Container and a Subscription on the same type — both
    // should see every transmission.
    let (client_io, server_io) = tokio::io::duplex(4096);
    let reg = registry();

    let server_reg = reg.clone();
    let server = tokio::spawn(async move {
        let codec = TransmissionCodec::new(server_reg.clone());
        let mut framed = Framed::new(server_io, codec);
        run_no_preload_handshake(&mut framed, &server_reg).await;

        let p = Pinger {
            id: Some(1),
            message: Some("dual".into()),
            ..Default::default()
        };
        push_pinger(&mut framed, &p, None, None, false).await;
    });

    let mut conn = Connection::establish(client_io, "dual", reg).await.unwrap();
    let pingers = conn.container::<Pinger>();
    let mut sub = conn.subscribe::<Pinger>();

    conn.next().await.unwrap().unwrap();

    assert_eq!(pingers.len(), 1);
    let event = sub.recv().await.expect("subscription receives event");
    assert_eq!(event.value.message.as_deref(), Some("dual"));

    drop(conn);
    server.await.unwrap();
}

#[tokio::test]
async fn dropping_container_stops_updating() {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let reg = registry();

    let server_reg = reg.clone();
    let server = tokio::spawn(async move {
        let codec = TransmissionCodec::new(server_reg.clone());
        let mut framed = Framed::new(server_io, codec);
        run_no_preload_handshake(&mut framed, &server_reg).await;

        for id in 1..=2u32 {
            let p = Pinger {
                id: Some(id),
                ..Default::default()
            };
            push_pinger(&mut framed, &p, None, None, false).await;
        }
    });

    let mut conn = Connection::establish(client_io, "drop-test", reg).await.unwrap();
    let pingers = conn.container::<Pinger>();

    conn.next().await.unwrap().unwrap();
    assert_eq!(pingers.len(), 1);

    drop(pingers);

    // Subsequent dispatches should be no-ops (the container's entry
    // is gone from the dispatch table).
    conn.next().await.unwrap().unwrap();

    drop(conn);
    server.await.unwrap();
}

#[tokio::test]
async fn with_entries_iterates_in_place() {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let reg = registry();

    let server_reg = reg.clone();
    let server = tokio::spawn(async move {
        let codec = TransmissionCodec::new(server_reg.clone());
        let mut framed = Framed::new(server_io, codec);
        run_no_preload_handshake(&mut framed, &server_reg).await;

        for id in 1..=2u32 {
            let p = Pinger {
                id: Some(id),
                ..Default::default()
            };
            push_pinger(&mut framed, &p, None, None, false).await;
        }
    });

    let mut conn = Connection::establish(client_io, "with-test", reg).await.unwrap();
    let pingers = conn.container::<Pinger>();
    for _ in 0..2 {
        conn.next().await.unwrap().unwrap();
    }

    let count = pingers.with_entries(|map| map.len());
    let ids: Vec<u32> = pingers.with_entries(|map| {
        map.values()
            .filter_map(|e| e.value.id)
            .collect()
    });
    assert_eq!(count, 2);
    let mut sorted = ids;
    sorted.sort();
    assert_eq!(sorted, vec![1, 2]);

    drop(conn);
    server.await.unwrap();
}
