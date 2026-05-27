//! Container tests with a fake server pushing create/update/remove
//! sequences over `tokio::io::duplex`.

use std::sync::Arc;

use dots_core::{StructValue, Timepoint, dots, encode_to_vec};
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
    let header = dots!(DotsHeader {
        type_name: type_name,
    });
    let descriptor = match reg.lookup(type_name).expect("type registered") {
        dots_model::DescriptorEntry::Struct(d) => d.clone(),
        _ => panic!(),
    };
    let bytes = encode_to_vec(payload);
    let payload = dots_core::DynamicStruct::decode(descriptor, &bytes).unwrap();
    Transmission { header, payload: dots_model::Payload::Wire(payload) }
}

async fn run_no_preload_handshake(
    framed: &mut Framed<DuplexStream, TransmissionCodec>,
    reg: &Arc<Registry>,
) {
    let hello = dots!(DotsMsgHello {
        server_name: "s",
        auth_challenge: 0_u64,
        authentication_required: false,
    });
    framed
        .send(dynamic_for(reg, "DotsMsgHello", &hello))
        .await
        .unwrap();
    let _connect = framed.next().await.unwrap().unwrap();
    let response = dots!(DotsMsgConnectResponse {
        client_id: 1_u32,
        accepted: true,
        preload: false,
    });
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
    let header = dots!(DotsHeader {
        type_name: "Pinger",
        attributes: pinger.valid_set(),
        sender: sender,
        sent_time: sent_time.map(Timepoint),
        remove_obj: remove,
    });
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
        let p1 = dots!(Pinger {
            id: 7_u32,
            message: "first",
            sequence: 1_u64,
        });
        push_pinger(&mut framed, &p1, Some(11), Some(100.0), false).await;

        // Second publish, same key — Update.
        let p2 = dots!(Pinger {
            id: 7_u32,
            message: "second",
            sequence: 2_u64,
        });
        push_pinger(&mut framed, &p2, Some(22), Some(200.0), false).await;
    });

    let mut conn = Connection::establish(client_io, "client", reg).await.unwrap();
    let pingers = conn.container::<Pinger>();

    // Drive two reads to ingest the two transmissions.
    conn.next().await.unwrap().unwrap();
    conn.next().await.unwrap().unwrap();

    assert_eq!(pingers.len(), 1);
    let query = dots!(Pinger {
        id: 7_u32,
    });
    let entry = pingers.get(&query).expect("entry exists");
    assert_eq!(entry.message.as_deref(), Some("second"));
    let ci = entry.clone_info();
    assert_eq!(ci.last_operation, Operation::Update);
    assert_eq!(ci.last_update_sender, Some(22));
    assert_eq!(ci.last_update_time, Some(Timepoint(200.0)));
    // created_* preserved from the first publish.
    assert_eq!(ci.created_sender, Some(11));
    assert_eq!(ci.created_time, Some(Timepoint(100.0)));

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

        let p = dots!(Pinger {
            id: 5_u32,
            message: "alive",
        });
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

        let p1 = dots!(Pinger {
            id: 99_u32,
            message: "one",
            sequence: 1_u64,
        });
        let p2 = dots!(Pinger {
            id: 99_u32,
            message: "two",
            sequence: 2_u64,
        });
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
        .get(&dots!(Pinger {
            id: 99_u32,
            // Other fields irrelevant for key lookup.
            sequence: 99999_u64,
        }))
        .expect("found by id");
    assert_eq!(entry.sequence, Some(2));

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
            let p = dots!(Pinger {
                id: id,
                message: format!("entry-{id}"),
            });
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

        let p = dots!(Pinger {
            id: 1_u32,
            message: "dual",
        });
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
            let p = dots!(Pinger {
                id: id,
            });
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
            let p = dots!(Pinger {
                id: id,
            });
            push_pinger(&mut framed, &p, None, None, false).await;
        }
    });

    let mut conn = Connection::establish(client_io, "with-test", reg).await.unwrap();
    let pingers = conn.container::<Pinger>();
    for _ in 0..2 {
        conn.next().await.unwrap().unwrap();
    }

    // Borrowed iteration via the read guard — no clones of any Pinger.
    let guard = pingers.lock();
    assert_eq!(guard.len(), 2);
    let mut ids: Vec<u32> = Vec::new();
    for (_k, p, _ci) in &guard {
        if let Some(id) = p.id {
            ids.push(id);
        }
    }
    drop(guard);
    ids.sort();
    assert_eq!(ids, vec![1, 2]);

    // for_each path still works.
    let mut count_via_closure = 0;
    pingers.for_each(|_, _, _| count_via_closure += 1);
    assert_eq!(count_via_closure, 2);

    drop(conn);
    server.await.unwrap();
}
