//! `App` integration tests.
//!
//! Each test pairs the App against a fake server on the other end of
//! a TCP loopback `TcpListener` (since `App::connect` only takes an
//! address string, not a generic stream).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use dots_core::{StructValue, decode_typed_from_slice, encode_to_vec};
use dots_derive::DotsStruct;
use dots_model::{
    DotsHeader, DotsMsgConnect, DotsMsgConnectResponse, DotsMsgHello, Registry,
    StructDescriptorData, Transmission, encode_transmission,
    registry_with_internal_types,
};
use dots_transport::{App, TransmissionCodec};
use futures_util::{SinkExt, StreamExt};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
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

/// Spin up a fake server bound to a free port, return its address and
/// the spawned task that handles one client.
async fn spawn_server<F, Fut>(handler: F) -> std::net::SocketAddr
where
    F: FnOnce(TcpStream, Arc<Registry>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let reg = registry();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        handler(stream, reg).await;
    });
    addr
}

async fn handshake_with_preload(
    framed: &mut Framed<TcpStream, TransmissionCodec>,
    reg: &Arc<Registry>,
) -> Vec<String> {
    // Hello
    let hello = DotsMsgHello {
        server_name: Some("test-dotsd".into()),
        auth_challenge: Some(0),
        authentication_required: Some(false),
        capabilities: None,
    };
    framed
        .send(dynamic_for(reg, "DotsMsgHello", &hello))
        .await
        .unwrap();

    // Receive Connect (preload=true).
    let txn = framed.next().await.unwrap().unwrap();
    let bytes = txn.payload.encode();
    let connect: DotsMsgConnect = decode_typed_from_slice(&bytes).unwrap();
    assert_eq!(connect.preload_cache, Some(true));

    // Initial ConnectResponse (preload=true).
    let response = DotsMsgConnectResponse {
        server_name: Some("test-dotsd".into()),
        client_id: Some(42),
        accepted: Some(true),
        preload: Some(true),
        preload_finished: Some(false),
    };
    framed
        .send(dynamic_for(reg, "DotsMsgConnectResponse", &response))
        .await
        .unwrap();

    // Receive auto-published descriptor data + the preload-finished
    // Connect; collect the type names in order until we see that
    // Connect.
    let mut descriptors_seen = Vec::new();
    loop {
        let txn = framed.next().await.unwrap().unwrap();
        match txn.header.type_name.as_deref() {
            Some("StructDescriptorData") => {
                let bytes = txn.payload.encode();
                let data: StructDescriptorData = decode_typed_from_slice(&bytes).unwrap();
                descriptors_seen.push(data.name.unwrap_or_default());
            }
            Some("EnumDescriptorData") => {
                descriptors_seen.push("<enum>".into());
            }
            Some("DotsMsgConnect") => {
                let bytes = txn.payload.encode();
                let c: DotsMsgConnect = decode_typed_from_slice(&bytes).unwrap();
                assert_eq!(c.preload_client_finished, Some(true));
                break;
            }
            other => panic!("unexpected message during preload: {other:?}"),
        }
    }

    // Final ConnectResponse (preload_finished=true).
    let response = DotsMsgConnectResponse {
        server_name: Some("test-dotsd".into()),
        client_id: Some(42),
        accepted: Some(true),
        preload: Some(true),
        preload_finished: Some(true),
    };
    framed
        .send(dynamic_for(reg, "DotsMsgConnectResponse", &response))
        .await
        .unwrap();

    descriptors_seen
}

// ----- Tests -----

#[tokio::test]
async fn app_connects_and_runs_until_exit() {
    let addr = spawn_server(|stream, reg| async move {
        let mut framed = Framed::new(stream, TransmissionCodec::new(reg.clone()));
        handshake_with_preload(&mut framed, &reg).await;
        // Hold the connection open until the client closes it.
        let _ = framed.next().await;
    })
    .await;

    let app = App::connect_tcp(&addr.to_string(), "client").await.unwrap();
    let client = app.client();
    let run = tokio::spawn(app.run());
    // Give the loop a tick to start, then exit.
    tokio::task::yield_now().await;
    client.exit();
    timeout(Duration::from_secs(1), run)
        .await
        .expect("run terminates after exit()")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn app_auto_publishes_descriptors_for_subscribed_types() {
    let addr = spawn_server(|stream, reg| async move {
        let mut framed = Framed::new(stream, TransmissionCodec::new(reg.clone()));
        let descriptors = handshake_with_preload(&mut framed, &reg).await;
        assert!(
            descriptors.iter().any(|n| n == "Pinger"),
            "expected Pinger descriptor in {descriptors:?}"
        );
    })
    .await;

    let app = App::connect_tcp(&addr.to_string(), "registrar").await.unwrap();
    let _sub = app.subscribe::<Pinger>(|_| {});
    let client = app.client();
    let run = tokio::spawn(app.run());
    // Server closes after handshake; run loop should exit gracefully.
    timeout(Duration::from_secs(1), async {
        let _ = run.await;
        client.exit();
    })
    .await
    .expect("server-side hangup propagates");
}

#[tokio::test]
async fn callback_subscription_receives_events() {
    let counter = Arc::new(AtomicUsize::new(0));

    let addr = spawn_server(|stream, reg| async move {
        let mut framed = Framed::new(stream, TransmissionCodec::new(reg.clone()));
        handshake_with_preload(&mut framed, &reg).await;

        // Push two Pingers.
        for id in 1..=2u32 {
            let p = Pinger {
                id: Some(id),
                message: Some(format!("msg-{id}")),
            };
            let header = DotsHeader {
                type_name: Some("Pinger".into()),
                attributes: Some(p.valid_set()),
                ..Default::default()
            };
            framed
                .get_mut()
                .write_all(&encode_transmission(&header, &p))
                .await
                .unwrap();
        }
        // Hold connection until the client side is done.
        let _ = framed.next().await;
    })
    .await;

    let app = App::connect_tcp(&addr.to_string(), "callback-test").await.unwrap();
    let client = app.client();
    let counter_in_handler = counter.clone();
    app.subscribe::<Pinger>(move |_event| {
        counter_in_handler.fetch_add(1, Ordering::SeqCst);
    })
    .discard();

    let run = tokio::spawn(app.run());

    // Wait for both events to be dispatched.
    for _ in 0..100 {
        if counter.load(Ordering::SeqCst) >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(counter.load(Ordering::SeqCst), 2);

    client.exit();
    let _ = timeout(Duration::from_secs(1), run).await;
}

#[tokio::test]
async fn client_publish_from_handler_reaches_server() {
    let received = Arc::new(tokio::sync::Notify::new());
    let received_in_server = received.clone();

    let addr = spawn_server(move |stream, reg| async move {
        let mut framed = Framed::new(stream, TransmissionCodec::new(reg.clone()));
        handshake_with_preload(&mut framed, &reg).await;

        // Trigger event: push a Pinger that the client's handler will react to.
        let trigger = Pinger {
            id: Some(1),
            message: Some("trigger".into()),
        };
        let header = DotsHeader {
            type_name: Some("Pinger".into()),
            attributes: Some(trigger.valid_set()),
            ..Default::default()
        };
        framed
            .get_mut()
            .write_all(&encode_transmission(&header, &trigger))
            .await
            .unwrap();

        // The client's `subscribe::<Pinger>` auto-publishes a
        // DotsMember(join, "Pinger") to tell us it wants Pinger
        // events; skip that to find the echo Pinger.
        loop {
            let txn = framed.next().await.unwrap().unwrap();
            match txn.header.type_name.as_deref() {
                Some("Pinger") => {
                    let bytes = txn.payload.encode();
                    let echo: Pinger = decode_typed_from_slice(&bytes).unwrap();
                    assert_eq!(echo.id, Some(2));
                    assert_eq!(echo.message.as_deref(), Some("reply"));
                    received_in_server.notify_one();
                    break;
                }
                Some("DotsMember") => continue,
                other => panic!("unexpected message: {other:?}"),
            }
        }
    })
    .await;

    let app = App::connect_tcp(&addr.to_string(), "echoer").await.unwrap();
    let client = app.client();
    let client_in_handler = client.clone();
    app.subscribe::<Pinger>(move |event| {
        if event.value.id == Some(1) {
            let _ = client_in_handler.publish(&Pinger {
                id: Some(2),
                message: Some("reply".into()),
            });
        }
    })
    .discard();

    let run = tokio::spawn(app.run());
    timeout(Duration::from_secs(1), received.notified())
        .await
        .expect("handler-published Pinger reaches server");
    client.exit();
    let _ = timeout(Duration::from_secs(1), run).await;
}

#[tokio::test]
async fn container_alongside_callback_both_update() {
    let counter = Arc::new(AtomicUsize::new(0));

    let addr = spawn_server(|stream, reg| async move {
        let mut framed = Framed::new(stream, TransmissionCodec::new(reg.clone()));
        handshake_with_preload(&mut framed, &reg).await;

        for id in 1..=3u32 {
            let p = Pinger {
                id: Some(id),
                message: Some(format!("msg-{id}")),
            };
            let header = DotsHeader {
                type_name: Some("Pinger".into()),
                attributes: Some(p.valid_set()),
                ..Default::default()
            };
            framed
                .get_mut()
                .write_all(&encode_transmission(&header, &p))
                .await
                .unwrap();
        }
        let _ = framed.next().await;
    })
    .await;

    let app = App::connect_tcp(&addr.to_string(), "dual").await.unwrap();
    let client = app.client();
    let pingers = app.container::<Pinger>();
    let counter_in_handler = counter.clone();
    app.subscribe::<Pinger>(move |_| {
        counter_in_handler.fetch_add(1, Ordering::SeqCst);
    })
    .discard();

    let run = tokio::spawn(app.run());

    for _ in 0..100 {
        if counter.load(Ordering::SeqCst) >= 3 && pingers.len() >= 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(counter.load(Ordering::SeqCst), 3);
    assert_eq!(pingers.len(), 3);

    client.exit();
    let _ = timeout(Duration::from_secs(1), run).await;
}

#[tokio::test]
async fn dropping_subscription_handle_unsubscribes() {
    let counter = Arc::new(AtomicUsize::new(0));

    let addr = spawn_server(|stream, reg| async move {
        let mut framed = Framed::new(stream, TransmissionCodec::new(reg.clone()));
        handshake_with_preload(&mut framed, &reg).await;

        // Two Pingers, with a small gap so the client has time to drop
        // the subscription between them.
        for (id, delay_ms) in [(1u32, 0u64), (2u32, 100u64)] {
            if delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            let p = Pinger {
                id: Some(id),
                message: Some(format!("msg-{id}")),
            };
            let header = DotsHeader {
                type_name: Some("Pinger".into()),
                attributes: Some(p.valid_set()),
                ..Default::default()
            };
            framed
                .get_mut()
                .write_all(&encode_transmission(&header, &p))
                .await
                .unwrap();
        }
        let _ = framed.next().await;
    })
    .await;

    let app = App::connect_tcp(&addr.to_string(), "drop-test").await.unwrap();
    let client = app.client();
    let counter_in_handler = counter.clone();
    let sub = app.subscribe::<Pinger>(move |_| {
        counter_in_handler.fetch_add(1, Ordering::SeqCst);
    });

    let run = tokio::spawn(app.run());

    // Wait for the first event, drop sub before the second arrives.
    for _ in 0..100 {
        if counter.load(Ordering::SeqCst) >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    drop(sub);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(counter.load(Ordering::SeqCst), 1, "second event should not fire");

    client.exit();
    let _ = timeout(Duration::from_secs(1), run).await;
}
