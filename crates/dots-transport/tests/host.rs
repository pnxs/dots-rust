//! HostTransceiver integration tests.
//!
//! Each test wires a guest [`Connection`] / [`GuestTransceiver`] via
//! `tokio::io::duplex` to a [`HostTransceiver`] in the same process,
//! drives both sides forward concurrently, and asserts the broker
//! routed (or didn't route) traffic as expected.

use std::sync::Arc;
use std::time::Duration;

use dots_derive::DotsStruct;
use dots_model::{Registry, registry_with_internal_types};
use dots_transport::{Connection, ConnectionBuilder, GuestTransceiver, HostTransceiver};

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
    Arc::new(registry_with_internal_types())
}

#[tokio::test]
async fn host_accepts_guest_and_handshake_completes() {
    let host = HostTransceiver::new("test-host");
    let (host_io, guest_io) = tokio::io::duplex(8192);
    host.accept(host_io);

    let conn = Connection::establish(guest_io, "guest-1", registry())
        .await
        .expect("handshake should succeed");
    assert_eq!(conn.client_id(), Some(2)); // HOST_ID is 1, first guest is 2.
    assert_eq!(conn.server_name(), Some("test-host"));

    drop(conn);
    // Give the host task a tick to clean up.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(host.guest_count(), 0);
}

#[tokio::test]
async fn host_routes_pinger_between_two_guests() {
    let host = HostTransceiver::new("test-host");
    let registry = registry();
    registry.register_struct_static(Pinger::DESCRIPTOR);

    let (host_io_a, guest_io_a) = tokio::io::duplex(8192);
    let (host_io_b, guest_io_b) = tokio::io::duplex(8192);
    host.accept(host_io_a);
    host.accept(host_io_b);

    // Guest A: subscribes to Pinger via builder (preload disabled — no
    // cache pool yet, so we don't need to drain a preload phase).
    let conn_a = ConnectionBuilder::new(guest_io_a, "guest-a", registry.clone())
        .preload(false)
        .publishes::<Pinger>()
        .connect()
        .await
        .unwrap();
    let (gt_a, driver_a) = GuestTransceiver::from_connection(
        "guest-a".to_string(),
        registry.clone(),
        conn_a,
    );
    let mut sub_a = gt_a.subscribe_stream::<Pinger>();
    let driver_a_handle = tokio::spawn(driver_a.run());

    // Guest B: just publishes (also subscribes implicitly via auto-join? no,
    // we publish without subscribing here).
    let conn_b = ConnectionBuilder::new(guest_io_b, "guest-b", registry.clone())
        .preload(false)
        .publishes::<Pinger>()
        .connect()
        .await
        .unwrap();
    let (gt_b, driver_b) = GuestTransceiver::from_connection(
        "guest-b".to_string(),
        registry.clone(),
        conn_b,
    );
    let driver_b_handle = tokio::spawn(driver_b.run());

    // Give the host time to receive A's DotsMember(Join).
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if host.group_size("Pinger") >= 1 {
            break;
        }
    }
    assert!(
        host.group_size("Pinger") >= 1,
        "guest-a should have joined Pinger group"
    );

    // B publishes a Pinger.
    gt_b.publish(&Pinger {
        id: Some(7),
        message: Some("hi from B".into()),
        sequence: Some(1),
    })
    .unwrap();

    // A should receive it.
    let event = tokio::time::timeout(Duration::from_secs(2), sub_a.recv())
        .await
        .expect("timed out waiting for routed Pinger")
        .expect("subscription closed");
    assert_eq!(event.value.id, Some(7));
    assert_eq!(event.value.message.as_deref(), Some("hi from B"));
    assert_eq!(event.value.sequence, Some(1));

    gt_a.exit();
    gt_b.exit();
    let _ = tokio::time::timeout(Duration::from_secs(1), driver_a_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(1), driver_b_handle).await;
}

#[tokio::test]
async fn host_publish_reaches_subscribed_guest() {
    let host = HostTransceiver::new("test-host");
    let registry = registry();
    registry.register_struct_static(Pinger::DESCRIPTOR);
    // Host needs Pinger registered too so its publish() can encode.
    host.registry().register_struct_static(Pinger::DESCRIPTOR);

    let (host_io, guest_io) = tokio::io::duplex(8192);
    host.accept(host_io);

    let conn = ConnectionBuilder::new(guest_io, "subscriber", registry.clone())
        .preload(false)
        .publishes::<Pinger>()
        .connect()
        .await
        .unwrap();
    let (gt, driver) =
        GuestTransceiver::from_connection("subscriber".to_string(), registry.clone(), conn);
    let mut sub = gt.subscribe_stream::<Pinger>();
    let driver_handle = tokio::spawn(driver.run());

    // Wait for the join to land.
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if host.group_size("Pinger") >= 1 {
            break;
        }
    }
    assert_eq!(host.group_size("Pinger"), 1);

    // Host publishes directly.
    host.publish(&Pinger {
        id: Some(99),
        message: Some("from host".into()),
        sequence: Some(42),
    });

    let event = tokio::time::timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("timed out")
        .expect("sub closed");
    assert_eq!(event.value.id, Some(99));
    assert_eq!(event.header.sender, Some(dots_transport::HOST_ID));

    gt.exit();
    let _ = tokio::time::timeout(Duration::from_secs(1), driver_handle).await;
}

#[tokio::test]
async fn host_replays_cached_pingers_to_late_subscriber() {
    let host = HostTransceiver::new("test-host");
    let registry = registry();
    registry.register_struct_static(Pinger::DESCRIPTOR);
    host.registry().register_struct_static(Pinger::DESCRIPTOR);

    // Guest A: publishes two Pingers, no subscription.
    let (host_io_a, guest_io_a) = tokio::io::duplex(8192);
    host.accept(host_io_a);
    let conn_a = ConnectionBuilder::new(guest_io_a, "publisher", registry.clone())
        .preload(false)
        .publishes::<Pinger>()
        .connect()
        .await
        .unwrap();
    let (gt_a, driver_a) = GuestTransceiver::from_connection(
        "publisher".to_string(),
        registry.clone(),
        conn_a,
    );
    let driver_a_handle = tokio::spawn(driver_a.run());

    gt_a.publish(&Pinger {
        id: Some(1),
        message: Some("first".into()),
        sequence: Some(1),
    })
    .unwrap();
    gt_a.publish(&Pinger {
        id: Some(2),
        message: Some("second".into()),
        sequence: Some(1),
    })
    .unwrap();

    // Wait for the host to record both in its cache.
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if host.cache_size("Pinger") == 2 {
            break;
        }
    }
    assert_eq!(
        host.cache_size("Pinger"),
        2,
        "host should have cached both Pingers"
    );

    // Guest B: late subscriber; expects to receive both cached Pingers
    // on join, plus a DotsCacheInfo{end_transmission:true}.
    let (host_io_b, guest_io_b) = tokio::io::duplex(8192);
    host.accept(host_io_b);
    let conn_b = ConnectionBuilder::new(guest_io_b, "subscriber", registry.clone())
        .preload(false)
        .publishes::<Pinger>()
        .connect()
        .await
        .unwrap();
    let (gt_b, driver_b) = GuestTransceiver::from_connection(
        "subscriber".to_string(),
        registry.clone(),
        conn_b,
    );
    let mut sub = gt_b.subscribe_stream::<Pinger>();
    let cache = gt_b.container::<Pinger>();
    let driver_b_handle = tokio::spawn(driver_b.run());

    // Receive both replayed pingers.
    let mut got = Vec::new();
    for _ in 0..2 {
        let event = tokio::time::timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("timed out waiting for replayed Pinger")
            .expect("subscription closed");
        got.push(event.value.id.unwrap());
        // Expect from_cache to be set on replayed entries.
        assert!(
            event.header.from_cache.is_some(),
            "replayed event should carry from_cache"
        );
    }
    got.sort();
    assert_eq!(got, vec![1, 2]);

    // Container should end up with both entries.
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if cache.len() == 2 {
            break;
        }
    }
    assert_eq!(cache.len(), 2);

    gt_a.exit();
    gt_b.exit();
    let _ = tokio::time::timeout(Duration::from_secs(1), driver_a_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(1), driver_b_handle).await;
}

#[tokio::test]
async fn host_does_not_loop_back_publisher_to_itself() {
    let host = HostTransceiver::new("test-host");
    let registry = registry();
    registry.register_struct_static(Pinger::DESCRIPTOR);

    let (host_io, guest_io) = tokio::io::duplex(8192);
    host.accept(host_io);

    let conn = ConnectionBuilder::new(guest_io, "solo", registry.clone())
        .preload(false)
        .publishes::<Pinger>()
        .connect()
        .await
        .unwrap();
    let (gt, driver) =
        GuestTransceiver::from_connection("solo".to_string(), registry.clone(), conn);
    let mut sub = gt.subscribe_stream::<Pinger>();
    let driver_handle = tokio::spawn(driver.run());

    // Wait for join.
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if host.group_size("Pinger") >= 1 {
            break;
        }
    }

    gt.publish(&Pinger {
        id: Some(1),
        ..Default::default()
    })
    .unwrap();

    // The publisher is the only subscriber and the host excludes the
    // sender from fan-out, so nothing should arrive.
    let res = tokio::time::timeout(Duration::from_millis(200), sub.recv()).await;
    assert!(res.is_err(), "publisher should not receive its own publish");

    gt.exit();
    let _ = tokio::time::timeout(Duration::from_secs(1), driver_handle).await;
}
