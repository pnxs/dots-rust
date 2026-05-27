//! `App` integration tests.
//!
//! Each test pairs the App against a fake server on the other end of
//! a TCP loopback `TcpListener` (since `App::connect` only takes an
//! address string, not a generic stream).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

/// Process-wide guard so the App tests don't race on the
/// `dots_transport::global` singleton. Each test acquires this at the
/// start; the guard is held for the duration of the test, then
/// released as the test function returns. Tokio's `#[tokio::test]`
/// default current-thread runtime keeps the future pinned to one OS
/// thread, so holding a `!Send` `MutexGuard` across awaits is safe.
static APP_LOCK: Mutex<()> = Mutex::new(());

fn app_lock() -> MutexGuard<'static, ()> {
    // Tolerate a poisoned mutex (a prior test panicking while
    // holding the lock) — the next test can still run.
    APP_LOCK.lock().unwrap_or_else(|p| p.into_inner())
}

use dots_core::{StructValue, decode_typed_from_slice, dots, encode_to_vec};
use dots_derive::DotsStruct;
use dots_model::{
    DotsHeader, DotsMember, DotsMemberEvent, DotsMsgConnect, DotsMsgConnectResponse,
    DotsMsgHello, Registry, StructDescriptorData, Transmission, encode_transmission,
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

/// Used in the early-subscribe wire-flow test below to exercise the
/// publish-only path: monomorphizing `app.publish::<PreloadPubOnly>`
/// puts the type in `PUBLISHED_TYPES` (so its descriptor ships during
/// EarlySubscribe), but nothing in this test binary calls
/// `subscribe::<PreloadPubOnly>` so it stays out of `SUBSCRIBED_TYPES`
/// (so no auto-`DotsMember(Join)` is emitted for it).
#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "PreloadPubOnly", cached)]
struct PreloadPubOnly {
    #[dots(tag = 1, key)]
    id: Option<u32>,
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
    let hello = dots!(DotsMsgHello {
        server_name: "test-dotsd",
        auth_challenge: 0_u64,
        authentication_required: false,
    });
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
    let response = dots!(DotsMsgConnectResponse {
        server_name: "test-dotsd",
        client_id: 42_u32,
        accepted: true,
        preload: true,
        preload_finished: false,
    });
    framed
        .send(dynamic_for(reg, "DotsMsgConnectResponse", &response))
        .await
        .unwrap();

    // Receive auto-published descriptor data, auto-published
    // DotsMember(Join) for SUBSCRIBED_TYPES, and finally the
    // preload-finished Connect; collect descriptor names in order
    // until we see that Connect. DotsMember messages are consumed
    // and skipped — tests that care about specific joins parse them
    // separately.
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
            Some("DotsMember") => {
                // Auto-subscribe Join from the EarlySubscribe phase.
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
    let response = dots!(DotsMsgConnectResponse {
        server_name: "test-dotsd",
        client_id: 42_u32,
        accepted: true,
        preload: true,
        preload_finished: true,
    });
    framed
        .send(dynamic_for(reg, "DotsMsgConnectResponse", &response))
        .await
        .unwrap();

    descriptors_seen
}

// ----- Tests -----

#[tokio::test]
async fn app_connects_and_runs_until_exit() {
    let _guard = app_lock();
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
    let _guard = app_lock();
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
    let _guard = app_lock();
    let counter = Arc::new(AtomicUsize::new(0));

    let addr = spawn_server(|stream, reg| async move {
        let mut framed = Framed::new(stream, TransmissionCodec::new(reg.clone()));
        handshake_with_preload(&mut framed, &reg).await;

        // Push two Pingers.
        for id in 1..=2u32 {
            let p = dots!(Pinger {
                id: id,
                message: format!("msg-{id}"),
            });
            let header = dots!(DotsHeader {
                type_name: "Pinger",
                attributes: p.valid_set(),
            });
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
    let _guard = app_lock();
    let received = Arc::new(tokio::sync::Notify::new());
    let received_in_server = received.clone();

    let addr = spawn_server(move |stream, reg| async move {
        let mut framed = Framed::new(stream, TransmissionCodec::new(reg.clone()));
        handshake_with_preload(&mut framed, &reg).await;

        // Trigger event: push a Pinger that the client's handler will react to.
        let trigger = dots!(Pinger {
            id: 1_u32,
            message: "trigger",
        });
        let header = dots!(DotsHeader {
            type_name: "Pinger",
            attributes: trigger.valid_set(),
        });
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
            let _ = client_in_handler.publish(&dots!(Pinger {
                id: 2_u32,
                message: "reply",
            }));
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
    let _guard = app_lock();
    let counter = Arc::new(AtomicUsize::new(0));

    let addr = spawn_server(|stream, reg| async move {
        let mut framed = Framed::new(stream, TransmissionCodec::new(reg.clone()));
        handshake_with_preload(&mut framed, &reg).await;

        for id in 1..=3u32 {
            let p = dots!(Pinger {
                id: id,
                message: format!("msg-{id}"),
            });
            let header = dots!(DotsHeader {
                type_name: "Pinger",
                attributes: p.valid_set(),
            });
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
    let _guard = app_lock();
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
            let p = dots!(Pinger {
                id: id,
                message: format!("msg-{id}"),
            });
            let header = dots!(DotsHeader {
                type_name: "Pinger",
                attributes: p.valid_set(),
            });
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

/// Wire-level test for the EarlySubscribe phase. Mirrors dots-cpp
/// `GuestTransceiver::handleTransitionImpl`, which transmits a
/// descriptor for every `m_preloadPublishTypes` entry, and a
/// descriptor *plus* `joinGroup` for every `m_preloadSubscribeTypes`
/// entry, before sending `preloadClientFinished`.
///
/// The Rust port pre-collects both `PUBLISHED_TYPES` and
/// `SUBSCRIBED_TYPES` into the `ConnectionBuilder` for the descriptor
/// pass, and the `GuestDriver::run` Phase 1b loops over
/// `SUBSCRIBED_TYPES` to emit one `DotsMember(Join)` per type. We
/// assert:
///
/// 1. Every `StructDescriptorData` and every `DotsMember(Join)` that
///    arrives during EarlySubscribe is followed by exactly one
///    `DotsMsgConnect{ preloadClientFinished = true }` (i.e. the
///    auto-publish work has completed before the client signals
///    "ready").
/// 2. Every recorded `DotsMember` has `event = Join` — Leave/Kill
///    have no place in this phase.
/// 3. The descriptors of both `Pinger` (touched via `subscribe::<T>`)
///    and `PreloadPubOnly` (touched via `publish::<T>`) appear in
///    that descriptor set.
/// 4. A `DotsMember(Join, "Pinger")` is emitted for the subscribed
///    type — the new behavior under test.
///
/// Note on what we **don't** assert: that a publish-only type
/// produces no `DotsMember(Join)`. In debug builds the link-time
/// `linkme` slot for `register_as_subscribed::<T>` is emitted for
/// every `#[derive(DotsStruct)]` type regardless of whether
/// `subscribe::<T>` is ever called — see the build-mode caveat in
/// `dots-derive/tests/global_registration.rs`. Release/LTO tightens
/// this to "only types actually subscribed", but in `cargo test`
/// debug we'd flap on the negative assertion.
#[tokio::test]
async fn early_subscribe_publishes_descriptors_and_joins_for_subscribed_types() {
    let _guard = app_lock();

    let captured: Arc<Mutex<(Vec<String>, Vec<DotsMember>)>> =
        Arc::new(Mutex::new((Vec::new(), Vec::new())));
    let captured_for_server = captured.clone();

    let addr = spawn_server(move |stream, reg| async move {
        let mut framed = Framed::new(stream, TransmissionCodec::new(reg.clone()));

        let hello = dots!(DotsMsgHello {
            server_name: "preload-flow-test",
            auth_challenge: 0_u64,
            authentication_required: false,
        });
        framed
            .send(dynamic_for(&reg, "DotsMsgHello", &hello))
            .await
            .unwrap();

        let txn = framed.next().await.unwrap().unwrap();
        let bytes = txn.payload.encode();
        let connect: DotsMsgConnect = decode_typed_from_slice(&bytes).unwrap();
        assert_eq!(connect.preload_cache, Some(true));

        let response = dots!(DotsMsgConnectResponse {
            server_name: "preload-flow-test",
            client_id: 7_u32,
            accepted: true,
            preload: true,
            preload_finished: false,
        });
        framed
            .send(dynamic_for(&reg, "DotsMsgConnectResponse", &response))
            .await
            .unwrap();

        loop {
            let txn = framed.next().await.unwrap().unwrap();
            match txn.header.type_name.as_deref() {
                Some("StructDescriptorData") => {
                    let bytes = txn.payload.encode();
                    let data: StructDescriptorData =
                        decode_typed_from_slice(&bytes).unwrap();
                    captured_for_server
                        .lock()
                        .unwrap()
                        .0
                        .push(data.name.unwrap_or_default());
                }
                Some("EnumDescriptorData") => {
                    // Not asserted on, but valid here.
                }
                Some("DotsMember") => {
                    let bytes = txn.payload.encode();
                    let member: DotsMember = decode_typed_from_slice(&bytes).unwrap();
                    captured_for_server.lock().unwrap().1.push(member);
                }
                Some("DotsMsgConnect") => {
                    let bytes = txn.payload.encode();
                    let c: DotsMsgConnect = decode_typed_from_slice(&bytes).unwrap();
                    assert_eq!(
                        c.preload_client_finished,
                        Some(true),
                        "expected preloadClientFinished to terminate EarlySubscribe",
                    );
                    break;
                }
                other => panic!("unexpected EarlySubscribe message: {other:?}"),
            }
        }

        let response = dots!(DotsMsgConnectResponse {
            server_name: "preload-flow-test",
            client_id: 7_u32,
            accepted: true,
            preload: true,
            preload_finished: true,
        });
        framed
            .send(dynamic_for(&reg, "DotsMsgConnectResponse", &response))
            .await
            .unwrap();

        // Hold the connection open until the client exits.
        let _ = framed.next().await;
    })
    .await;

    let app = App::connect_tcp(&addr.to_string(), "preload-flow-client")
        .await
        .unwrap();

    // Touch both halves of the link-time intent so the monomorphization
    // and the resulting `register_as_*` symbols land in this binary:
    //   - subscribe::<Pinger>     →  Pinger ∈ SUBSCRIBED_TYPES
    //   - publish::<PreloadPubOnly> → PreloadPubOnly ∈ PUBLISHED_TYPES
    // The `publish()` value is queued on the outbound mpsc and only
    // drained in the driver's Phase 3 (post-preload), so it has no
    // effect on the EarlySubscribe wire flow under test.
    app.subscribe::<Pinger>(|_| {}).discard();
    app.publish(&dots!(PreloadPubOnly { id: 1_u32 }));

    let client = app.client();
    let run = tokio::spawn(app.run());

    // Drive the EarlySubscribe phase to completion. Once we observe
    // any DotsMember (the auto-Join Phase 1b), preloadClientFinished
    // follows immediately in Phase 2 — so a short settle is enough.
    for _ in 0..200 {
        if !captured.lock().unwrap().1.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    client.exit();
    let _ = timeout(Duration::from_secs(1), run).await;

    let (descriptors, members) = {
        let guard = captured.lock().unwrap();
        (guard.0.clone(), guard.1.clone())
    };

    // (2) every captured DotsMember is a Join — Leaves don't belong here.
    for m in &members {
        assert_eq!(
            m.event,
            Some(DotsMemberEvent::Join),
            "unexpected DotsMember event in EarlySubscribe: {m:?}",
        );
    }

    // (3) descriptors include the type we subscribed to and the type
    //     we published.
    assert!(
        descriptors.iter().any(|n| n == "Pinger"),
        "subscribed type's descriptor must be sent in EarlySubscribe; got {descriptors:?}",
    );
    assert!(
        descriptors.iter().any(|n| n == "PreloadPubOnly"),
        "published type's descriptor must be sent in EarlySubscribe; got {descriptors:?}",
    );

    // (4) the new behavior: SUBSCRIBED_TYPES auto-emits Join.
    let join_names: Vec<&str> = members
        .iter()
        .filter_map(|m| m.group_name.as_deref())
        .collect();
    assert!(
        join_names.contains(&"Pinger"),
        "subscribed type must produce a DotsMember(Join) in EarlySubscribe; got {join_names:?}",
    );

    // Sanity: at least one Join was emitted (Phase 1b actually ran).
    assert!(
        !members.is_empty(),
        "expected at least one auto-published DotsMember(Join) in EarlySubscribe",
    );
}
