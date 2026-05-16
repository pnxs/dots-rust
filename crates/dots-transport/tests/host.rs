//! HostTransceiver integration tests.
//!
//! Each test wires a guest [`Connection`] / [`GuestTransceiver`] via
//! `tokio::io::duplex` to a [`HostTransceiver`] in the same process,
//! drives both sides forward concurrently, and asserts the broker
//! routed (or didn't route) traffic as expected.

use std::sync::Arc;
use std::time::Duration;

use dots_core::dots;
use dots_derive::DotsStruct;
use dots_model::{Registry, registry_with_internal_types};
use dots_transport::{Connection, ConnectionBuilder, GuestTransceiver, HostTransceiver};
use tokio::net::{UnixListener, UnixStream};

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
    gt_b.publish(&dots!(Pinger {
        id: 7_u32,
        message: "hi from B",
        sequence: 1_u64,
    }));

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
    host.publish(&dots!(Pinger {
        id: 99_u32,
        message: "from host",
        sequence: 42_u64,
    }));

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

    gt_a.publish(&dots!(Pinger {
        id: 1_u32,
        message: "first",
        sequence: 1_u64,
    }));
    gt_a.publish(&dots!(Pinger {
        id: 2_u32,
        message: "second",
        sequence: 1_u64,
    }));

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
async fn dropping_subscription_keeps_group_alive_until_transceiver_drops() {
    // Subscriptions are pure dispatch-handler additions; group
    // membership is owned by the transceiver-level container pool.
    // Drop the last sub → group stays alive (the pool still holds the
    // container). Drop the transceiver → leave fires.
    let host = HostTransceiver::new("test-host");
    let registry = registry();
    registry.register_struct_static(Pinger::DESCRIPTOR);

    let (host_io, guest_io) = tokio::io::duplex(8192);
    host.accept(host_io);

    let conn = ConnectionBuilder::new(guest_io, "guest", registry.clone())
        .preload(false)
        .publishes::<Pinger>()
        .connect()
        .await
        .unwrap();
    let (gt, driver) =
        GuestTransceiver::from_connection("guest".to_string(), registry.clone(), conn);
    let driver_handle = tokio::spawn(driver.run());

    let sub = gt.subscribe_stream::<Pinger>();

    // Wait for the host to register the join.
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if host.group_size("Pinger") == 1 {
            break;
        }
    }
    assert_eq!(host.group_size("Pinger"), 1, "join should have landed");

    // Dropping the subscription does NOT publish a Leave — the pool
    // still owns the container for Pinger, which keeps the group
    // membership alive.
    drop(sub);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        host.group_size("Pinger"),
        1,
        "pool keeps the group alive after subscription drops"
    );

    // Dropping the transceiver itself drops the pool, which drops the
    // container, which fires the group-leave.
    gt.exit();
    let _ = tokio::time::timeout(Duration::from_secs(1), driver_handle).await;
    drop(gt);
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if host.group_size("Pinger") == 0 {
            break;
        }
    }
    assert_eq!(
        host.group_size("Pinger"),
        0,
        "leave should fire when transceiver drops"
    );
}

#[tokio::test]
async fn dropping_one_of_two_subscriptions_keeps_join() {
    let host = HostTransceiver::new("test-host");
    let registry = registry();
    registry.register_struct_static(Pinger::DESCRIPTOR);

    let (host_io, guest_io) = tokio::io::duplex(8192);
    host.accept(host_io);

    let conn = ConnectionBuilder::new(guest_io, "guest", registry.clone())
        .preload(false)
        .publishes::<Pinger>()
        .connect()
        .await
        .unwrap();
    let (gt, driver) =
        GuestTransceiver::from_connection("guest".to_string(), registry.clone(), conn);
    let driver_handle = tokio::spawn(driver.run());

    // Two subscriptions to the same type — the second should NOT
    // publish another Join, and dropping the first should NOT publish
    // a Leave (count is still 1).
    let sub_a = gt.subscribe_stream::<Pinger>();
    let _sub_b = gt.subscribe_stream::<Pinger>();

    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if host.group_size("Pinger") == 1 {
            break;
        }
    }
    assert_eq!(host.group_size("Pinger"), 1);

    drop(sub_a);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        host.group_size("Pinger"),
        1,
        "second subscriber should keep group alive"
    );

    gt.exit();
    let _ = tokio::time::timeout(Duration::from_secs(1), driver_handle).await;
}

#[tokio::test]
async fn guest_remove_drops_entry_from_host_cache() {
    let host = HostTransceiver::new("test-host");
    let registry = registry();
    registry.register_struct_static(Pinger::DESCRIPTOR);

    // Guest A publishes two Pingers, then removes one.
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

    gt_a.publish(&dots!(Pinger {
        id: 1_u32,
        message: "first",
        sequence: 1_u64,
    }));
    gt_a.publish(&dots!(Pinger {
        id: 2_u32,
        message: "second",
        sequence: 1_u64,
    }));

    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if host.cache_size("Pinger") == 2 {
            break;
        }
    }
    assert_eq!(host.cache_size("Pinger"), 2);

    // Remove id=1.
    gt_a.remove(&dots!(Pinger {
        id: 1_u32,
    }));

    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if host.cache_size("Pinger") == 1 {
            break;
        }
    }
    assert_eq!(
        host.cache_size("Pinger"),
        1,
        "remove should have shrunk the cache to 1 entry"
    );

    gt_a.exit();
    let _ = tokio::time::timeout(Duration::from_secs(1), driver_a_handle).await;
}

#[tokio::test]
async fn host_replies_to_dots_echo_request() {
    use dots_model::DotsEcho;

    let host = HostTransceiver::new("test-host");
    let registry = registry();

    let (host_io, guest_io) = tokio::io::duplex(8192);
    host.accept(host_io);
    let conn = ConnectionBuilder::new(guest_io, "echo-client", registry.clone())
        .preload(false)
        .connect()
        .await
        .unwrap();
    let (gt, driver) =
        GuestTransceiver::from_connection("echo-client".to_string(), registry.clone(), conn);
    let mut sub = gt.subscribe_stream::<DotsEcho>();
    let driver_handle = tokio::spawn(driver.run());

    // Wait for the join to land so the host has us in the DotsEcho
    // group (echo replies are sent direct, but the join also keeps
    // the guest's dispatch entry warm).
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if host.group_size("DotsEcho") >= 1 {
            break;
        }
    }

    gt.publish(&dots!(DotsEcho {
        request: true,
        identifier: 7_u32,
        sequence_number: 42_u32,
        data: "ping",
    }));

    let event = tokio::time::timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("timed out waiting for echo reply")
        .expect("subscription closed");
    assert_eq!(event.value.request, Some(false));
    assert_eq!(event.value.identifier, Some(7));
    assert_eq!(event.value.sequence_number, Some(42));
    assert_eq!(event.value.data.as_deref(), Some("ping"));
    assert_eq!(event.header.sender, Some(dots_transport::HOST_ID));

    gt.exit();
    let _ = tokio::time::timeout(Duration::from_secs(1), driver_handle).await;
}

#[tokio::test]
async fn host_publishes_dots_client_on_connect_and_disconnect() {
    use dots_model::{DotsConnectionState, DotsClient};

    let host = HostTransceiver::new("test-host");

    // Observer guest first — subscribes to DotsClient before any other
    // guest connects, so it sees their connect/disconnect events.
    let (host_io_obs, guest_io_obs) = tokio::io::duplex(8192);
    host.accept(host_io_obs);
    let conn_obs = ConnectionBuilder::new(guest_io_obs, "observer", registry())
        .preload(false)
        .connect()
        .await
        .unwrap();
    let (gt_obs, driver_obs) =
        GuestTransceiver::from_connection("observer".to_string(), registry(), conn_obs);
    let mut sub = gt_obs.subscribe_stream::<DotsClient>();
    let driver_obs_handle = tokio::spawn(driver_obs.run());

    // Drain the observer's own connect notifications first.
    let mut observed = Vec::new();
    while let Ok(Some(event)) =
        tokio::time::timeout(Duration::from_millis(200), sub.recv()).await
    {
        observed.push(event.value);
    }
    // Observer's own DotsClient should be in the drained set.
    assert!(observed.iter().any(|c| c.name.as_deref() == Some("observer")
        && c.connection_state == Some(DotsConnectionState::Connected)));

    // Now connect a second guest. Observer should see its connect.
    let (host_io_b, guest_io_b) = tokio::io::duplex(8192);
    host.accept(host_io_b);
    let conn_b = ConnectionBuilder::new(guest_io_b, "alice", registry())
        .preload(false)
        .connect()
        .await
        .unwrap();
    let (gt_b, driver_b) =
        GuestTransceiver::from_connection("alice".to_string(), registry(), conn_b);
    let driver_b_handle = tokio::spawn(driver_b.run());

    // Wait for the alice-connect event.
    let mut alice_connected = false;
    for _ in 0..30 {
        if let Ok(Some(event)) =
            tokio::time::timeout(Duration::from_millis(100), sub.recv()).await
        {
            if event.value.name.as_deref() == Some("alice")
                && event.value.connection_state == Some(DotsConnectionState::Connected)
            {
                alice_connected = true;
                break;
            }
        }
    }
    assert!(alice_connected, "expected alice's connect event");

    // Disconnect alice; observer should see Closed.
    gt_b.exit();
    let _ = tokio::time::timeout(Duration::from_secs(1), driver_b_handle).await;

    let mut alice_closed = false;
    for _ in 0..30 {
        if let Ok(Some(event)) =
            tokio::time::timeout(Duration::from_millis(100), sub.recv()).await
        {
            if event.value.name.as_deref() == Some("alice")
                && event.value.connection_state == Some(DotsConnectionState::Closed)
                && event.value.running == Some(false)
            {
                alice_closed = true;
                break;
            }
        }
    }
    assert!(alice_closed, "expected alice's disconnect event");

    gt_obs.exit();
    let _ = tokio::time::timeout(Duration::from_secs(1), driver_obs_handle).await;
}

#[cfg(unix)]
#[tokio::test]
async fn host_serve_unix_routes_pinger_round_trip() {
    let host = HostTransceiver::new("uds-host");
    host.registry().register_struct_static(Pinger::DESCRIPTOR);

    // Allocate a unique socket path under the temp dir; clean any
    // stale leftover from a previous run.
    let sock_path = std::env::temp_dir().join(format!(
        "dots-uds-test-{}-{}.sock",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_file(&sock_path);

    let listener = UnixListener::bind(&sock_path).expect("bind UDS");
    let serve_handle = host.serve_unix(listener);

    // Client side: connect via UDS, run handshake, subscribe to Pinger.
    let stream = UnixStream::connect(&sock_path).await.expect("uds connect");
    let registry = Arc::new(registry_with_internal_types());
    registry.register_struct_static(Pinger::DESCRIPTOR);
    let conn = ConnectionBuilder::new(stream, "uds-client", registry.clone())
        .preload(false)
        .publishes::<Pinger>()
        .connect()
        .await
        .unwrap();
    let (gt, driver) = GuestTransceiver::from_connection(
        "uds-client".to_string(),
        registry.clone(),
        conn,
    );
    let mut sub = gt.subscribe_stream::<Pinger>();
    let driver_handle = tokio::spawn(driver.run());

    // Wait for the join to land.
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if host.group_size("Pinger") == 1 {
            break;
        }
    }
    assert_eq!(host.group_size("Pinger"), 1);

    // Host publishes; client receives over UDS.
    host.publish(&dots!(Pinger {
        id: 1_u32,
        message: "hi over uds",
        sequence: 7_u64,
    }));
    let event = tokio::time::timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("timed out")
        .expect("sub closed");
    assert_eq!(event.value.id, Some(1));
    assert_eq!(event.value.message.as_deref(), Some("hi over uds"));
    assert_eq!(event.value.sequence, Some(7));

    // Cleanup.
    gt.exit();
    let _ = tokio::time::timeout(Duration::from_secs(1), driver_handle).await;
    serve_handle.abort();
    let _ = std::fs::remove_file(&sock_path);
}

#[tokio::test]
async fn registering_a_struct_pulls_in_nested_enum_descriptors() {
    use dots_derive::{DotsEnum, DotsStruct};

    #[derive(DotsEnum, Default, Debug, Clone, Copy, PartialEq, Eq)]
    #[dots(name = "Mood")]
    enum Mood {
        #[default]
        #[dots(tag = 1)]
        Happy,
        #[dots(tag = 2)]
        Sad,
    }

    #[derive(DotsStruct, Default, Debug, Clone, PartialEq)]
    #[dots(name = "Greeter")]
    struct Greeter {
        #[dots(tag = 1, key)]
        id: Option<u32>,
        #[dots(tag = 2)]
        mood: Option<Mood>,
    }

    let host = HostTransceiver::new("nested-host");
    let registry = registry();

    let (host_io, guest_io) = tokio::io::duplex(8192);
    host.accept(host_io);
    let conn = ConnectionBuilder::new(guest_io, "guest", registry.clone())
        .preload(false)
        .connect()
        .await
        .unwrap();
    let (gt, driver) =
        GuestTransceiver::from_connection("guest".to_string(), registry.clone(), conn);
    // User subscribes only to `Greeter`. The `Mood` enum, embedded in
    // a Greeter field, must auto-register without an explicit
    // `register_enum` call.
    let _sub = gt.subscribe_stream::<Greeter>();
    let driver_handle = tokio::spawn(driver.run());

    // Wait for descriptor publishing + the join to land.
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if host.registry().lookup("Greeter").is_some()
            && host.registry().lookup("Mood").is_some()
        {
            break;
        }
    }
    assert!(
        host.registry().lookup("Greeter").is_some(),
        "Greeter struct should be in host registry"
    );
    assert!(
        host.registry().lookup("Mood").is_some(),
        "nested Mood enum should have been auto-registered alongside Greeter"
    );

    gt.exit();
    let _ = tokio::time::timeout(Duration::from_secs(1), driver_handle).await;
}

#[tokio::test]
async fn cleanup_flag_drops_publisher_entries_on_disconnect() {
    // A type with both `cached` and `cleanup` flags: when its
    // publisher disconnects, the host should drop matching entries
    // from the pool and fan out a removal to any subscriber.
    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "TempClient", cached, cleanup)]
    struct TempClient {
        #[dots(tag = 1, key)]
        id: Option<u32>,
        #[dots(tag = 2)]
        label: Option<String>,
    }

    let host = HostTransceiver::new("cleanup-host");
    let registry = registry();
    registry.register_struct_static(TempClient::DESCRIPTOR);
    host.registry().register_struct_static(TempClient::DESCRIPTOR);

    // Subscriber: stays connected and watches the pool.
    let (host_io_obs, guest_io_obs) = tokio::io::duplex(8192);
    host.accept(host_io_obs);
    let conn_obs = ConnectionBuilder::new(guest_io_obs, "observer", registry.clone())
        .preload(false)
        .publishes::<TempClient>()
        .connect()
        .await
        .unwrap();
    let (gt_obs, driver_obs) = GuestTransceiver::from_connection(
        "observer".to_string(),
        registry.clone(),
        conn_obs,
    );
    let mut sub = gt_obs.subscribe_stream::<TempClient>();
    let driver_obs_handle = tokio::spawn(driver_obs.run());

    // Publisher: connects, publishes one TempClient, then disconnects.
    let (host_io_pub, guest_io_pub) = tokio::io::duplex(8192);
    host.accept(host_io_pub);
    let conn_pub = ConnectionBuilder::new(guest_io_pub, "publisher", registry.clone())
        .preload(false)
        .publishes::<TempClient>()
        .connect()
        .await
        .unwrap();
    let (gt_pub, driver_pub) = GuestTransceiver::from_connection(
        "publisher".to_string(),
        registry.clone(),
        conn_pub,
    );
    let driver_pub_handle = tokio::spawn(driver_pub.run());

    gt_pub
        .publish(&dots!(TempClient {
            id: 7_u32,
            label: "hi",
        }));

    // Observer should receive the create.
    let event = tokio::time::timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("timed out for create")
        .expect("sub closed");
    assert_eq!(event.value.id, Some(7));
    assert_ne!(event.header.remove_obj, Some(true), "create should not be a remove");

    // Wait for the entry to land in the host pool.
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if host.cache_size("TempClient") == 1 {
            break;
        }
    }
    assert_eq!(host.cache_size("TempClient"), 1);

    // Publisher disconnects: aborting the driver task drops the
    // Connection<S>, closes the underlying duplex stream, and the
    // host sees EOF.
    driver_pub_handle.abort();
    drop(gt_pub);

    // Observer should receive a removal event for the publisher's entry.
    let mut got_removal = false;
    for _ in 0..30 {
        if let Ok(Some(event)) =
            tokio::time::timeout(Duration::from_millis(100), sub.recv()).await
        {
            if event.header.remove_obj == Some(true) && event.value.id == Some(7) {
                got_removal = true;
                break;
            }
        }
    }
    assert!(
        got_removal,
        "observer should have received a [cleanup] removal for the disconnected publisher's entry"
    );

    // Pool should be empty.
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if host.cache_size("TempClient") == 0 {
            break;
        }
    }
    assert_eq!(host.cache_size("TempClient"), 0);

    gt_obs.exit();
    let _ = tokio::time::timeout(Duration::from_secs(1), driver_obs_handle).await;
}

#[tokio::test]
async fn shutdown_aborts_guest_tasks_and_clears_state() {
    let host = HostTransceiver::new("shutdown-host");
    host.registry().register_struct_static(Pinger::DESCRIPTOR);

    // Connect two guests so we have something to clean up.
    let (host_io_a, _guest_io_a) = tokio::io::duplex(8192);
    let (host_io_b, _guest_io_b) = tokio::io::duplex(8192);
    host.accept(host_io_a);
    host.accept(host_io_b);

    // Give the per-guest tasks a tick to register.
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if host.guest_count() == 2 {
            break;
        }
    }
    assert_eq!(host.guest_count(), 2);

    // Shutdown should drain everything.
    host.shutdown();
    assert_eq!(host.guest_count(), 0);
    assert!(host.group_names().is_empty());
    assert_eq!(host.cache_size("Pinger"), 0);
}

#[tokio::test]
async fn host_replies_to_descriptor_request_with_known_structs() {
    use dots_model::{DotsCacheInfo, DotsDescriptorRequest, StructDescriptorData};

    let host = HostTransceiver::new("desc-host");
    let registry = registry();
    // Register a non-internal struct on the host so we can ask for it.
    registry.register_struct_static(Pinger::DESCRIPTOR);
    host.registry().register_struct_static(Pinger::DESCRIPTOR);

    let (host_io, guest_io) = tokio::io::duplex(8192);
    host.accept(host_io);
    let conn = ConnectionBuilder::new(guest_io, "asker", registry.clone())
        .preload(false)
        .connect()
        .await
        .unwrap();
    let (gt, driver) =
        GuestTransceiver::from_connection("asker".to_string(), registry.clone(), conn);
    let mut sub_descriptors = gt.subscribe_stream::<StructDescriptorData>();
    let mut sub_cache_info = gt.subscribe_stream::<DotsCacheInfo>();
    let driver_handle = tokio::spawn(driver.run());

    // Wait for subscriptions to land before sending the request.
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if host.group_size("StructDescriptorData") >= 1 {
            break;
        }
    }

    gt.publish(&DotsDescriptorRequest::default());

    // Expect at least one StructDescriptorData (Pinger) and a
    // DotsCacheInfo{end_descriptor_request:true}.
    let mut got_pinger_descriptor = false;
    for _ in 0..30 {
        if let Ok(Some(event)) =
            tokio::time::timeout(Duration::from_millis(100), sub_descriptors.recv()).await
        {
            if event.value.name.as_deref() == Some("Pinger") {
                got_pinger_descriptor = true;
                break;
            }
        }
    }
    assert!(got_pinger_descriptor, "expected Pinger descriptor in reply");

    let mut got_end = false;
    for _ in 0..30 {
        if let Ok(Some(event)) =
            tokio::time::timeout(Duration::from_millis(100), sub_cache_info.recv()).await
        {
            if event.value.end_descriptor_request == Some(true) {
                got_end = true;
                break;
            }
        }
    }
    assert!(got_end, "expected DotsCacheInfo{{end_descriptor_request: true}}");

    gt.exit();
    let _ = tokio::time::timeout(Duration::from_secs(1), driver_handle).await;
}

#[tokio::test]
async fn dots_clear_cache_drops_named_types_and_publishes_removals() {
    use dots_model::DotsClearCache;

    let host = HostTransceiver::new("clear-host");
    let registry = registry();
    registry.register_struct_static(Pinger::DESCRIPTOR);
    host.registry().register_struct_static(Pinger::DESCRIPTOR);

    // Publisher: publishes two Pingers.
    let (host_io_pub, guest_io_pub) = tokio::io::duplex(8192);
    host.accept(host_io_pub);
    let conn_pub = ConnectionBuilder::new(guest_io_pub, "publisher", registry.clone())
        .preload(false)
        .publishes::<Pinger>()
        .connect()
        .await
        .unwrap();
    let (gt_pub, driver_pub) = GuestTransceiver::from_connection(
        "publisher".to_string(),
        registry.clone(),
        conn_pub,
    );
    let driver_pub_handle = tokio::spawn(driver_pub.run());

    gt_pub
        .publish(&dots!(Pinger {
            id: 1_u32,
        }));
    gt_pub
        .publish(&dots!(Pinger {
            id: 2_u32,
        }));

    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if host.cache_size("Pinger") == 2 {
            break;
        }
    }
    assert_eq!(host.cache_size("Pinger"), 2);

    // Clearer publishes DotsClearCache for "Pinger".
    gt_pub
        .publish(&dots!(DotsClearCache {
            type_names: vec!["Pinger".into()],
        }));

    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if host.cache_size("Pinger") == 0 {
            break;
        }
    }
    assert_eq!(
        host.cache_size("Pinger"),
        0,
        "DotsClearCache should have dropped all Pinger entries"
    );

    gt_pub.exit();
    driver_pub_handle.abort();
}

#[tokio::test]
async fn gt_exit_promptly_wakes_the_driver_on_a_quiet_connection() {
    // Connect a guest, do nothing, call exit(). The driver should
    // exit promptly (under 200ms), even though no traffic has flowed.
    // Before the Notify wiring this would hang indefinitely.
    let host = HostTransceiver::new("quiet-host");
    let (host_io, guest_io) = tokio::io::duplex(8192);
    host.accept(host_io);

    let conn = ConnectionBuilder::new(guest_io, "quiet-guest", registry())
        .preload(false)
        .connect()
        .await
        .unwrap();
    let (gt, driver) =
        GuestTransceiver::from_connection("quiet-guest".to_string(), registry(), conn);
    let driver_handle = tokio::spawn(driver.run());

    // No traffic yet — call exit, expect prompt return.
    gt.exit();
    let exit_result = tokio::time::timeout(Duration::from_millis(500), driver_handle)
        .await
        .expect("driver should exit promptly when exit() is called");
    assert!(exit_result.is_ok(), "driver task ended cleanly");
}

#[tokio::test]
async fn dynamic_subscribe_receives_event_with_runtime_descriptor() {
    use std::sync::Mutex;
    use dots_core::{DynamicStructDescriptor, DynamicValue};

    let host = HostTransceiver::new("dyn-host");
    let registry = registry();
    registry.register_struct_static(Pinger::DESCRIPTOR);
    host.registry().register_struct_static(Pinger::DESCRIPTOR);

    let (host_io, guest_io) = tokio::io::duplex(8192);
    host.accept(host_io);

    // Guest connects without compiling-in `Pinger` (we register
    // a DynamicStructDescriptor built from the static one to simulate
    // a guest that learned the type at runtime).
    let guest_registry = Arc::new(registry_with_internal_types());
    let conn = ConnectionBuilder::new(guest_io, "dyn-guest", guest_registry.clone())
        .preload(false)
        .connect()
        .await
        .unwrap();
    let (gt, driver) = GuestTransceiver::from_connection(
        "dyn-guest".to_string(),
        guest_registry.clone(),
        conn,
    );
    let driver_handle = tokio::spawn(driver.run());

    let descriptor = Arc::new(DynamicStructDescriptor::from_static(Pinger::DESCRIPTOR));
    let captured: Arc<Mutex<Vec<(u32, DynamicValue)>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_clone = captured.clone();
    let _handle = gt.subscribe_dynamic(descriptor, move |event| {
        captured_clone
            .lock()
            .unwrap()
            .extend(event.value.properties.iter().cloned());
    });

    // Wait for the join to land.
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if host.group_size("Pinger") >= 1 {
            break;
        }
    }
    assert_eq!(host.group_size("Pinger"), 1);

    host.publish(&dots!(Pinger {
        id: 7_u32,
        message: "dynamic",
        sequence: 123_u64,
    }));

    // Spin until the handler captured the properties.
    let mut props = Vec::new();
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        props = captured.lock().unwrap().clone();
        if !props.is_empty() {
            break;
        }
    }
    assert!(!props.is_empty(), "dynamic handler should have fired");

    // Check the properties carry the right values via DynamicValue.
    let id = props.iter().find(|(t, _)| *t == 1).map(|(_, v)| v).unwrap();
    let msg = props.iter().find(|(t, _)| *t == 2).map(|(_, v)| v).unwrap();
    let seq = props.iter().find(|(t, _)| *t == 3).map(|(_, v)| v).unwrap();
    assert!(matches!(id, DynamicValue::U32(7)));
    assert!(matches!(msg, DynamicValue::String(s) if s == "dynamic"));
    assert!(matches!(seq, DynamicValue::U64(123)));

    gt.exit();
    let _ = tokio::time::timeout(Duration::from_secs(1), driver_handle).await;
}

#[tokio::test]
async fn dynamic_publish_routes_through_broker_to_typed_subscriber() {
    use dots_core::{DynamicStructDescriptor, DynamicStruct, DynamicValue, PropertySet};

    let host = HostTransceiver::new("dyn-pub-host");
    let registry = registry();
    registry.register_struct_static(Pinger::DESCRIPTOR);
    host.registry().register_struct_static(Pinger::DESCRIPTOR);

    // Subscriber: typed Pinger.
    let (host_io_sub, guest_io_sub) = tokio::io::duplex(8192);
    host.accept(host_io_sub);
    let conn_sub = ConnectionBuilder::new(guest_io_sub, "sub", registry.clone())
        .preload(false)
        .publishes::<Pinger>()
        .connect()
        .await
        .unwrap();
    let (gt_sub, driver_sub) =
        GuestTransceiver::from_connection("sub".to_string(), registry.clone(), conn_sub);
    let mut sub = gt_sub.subscribe_stream::<Pinger>();
    let driver_sub_handle = tokio::spawn(driver_sub.run());

    // Wait for subscriber to join.
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if host.group_size("Pinger") >= 1 {
            break;
        }
    }
    assert_eq!(host.group_size("Pinger"), 1);

    // Publisher: dynamic — knows Pinger only as a runtime descriptor.
    let pub_registry = Arc::new(registry_with_internal_types());
    let (host_io_pub, guest_io_pub) = tokio::io::duplex(8192);
    host.accept(host_io_pub);
    let conn_pub = ConnectionBuilder::new(guest_io_pub, "dyn-pub", pub_registry.clone())
        .preload(false)
        .connect()
        .await
        .unwrap();
    let (gt_pub, driver_pub) =
        GuestTransceiver::from_connection("dyn-pub".to_string(), pub_registry.clone(), conn_pub);
    let driver_pub_handle = tokio::spawn(driver_pub.run());

    let descriptor = Arc::new(DynamicStructDescriptor::from_static(Pinger::DESCRIPTOR));
    pub_registry.register_struct_dynamic(descriptor.clone());

    // Build a DynamicStruct populated with id, message, sequence.
    let valid = PropertySet::EMPTY.with_tag(1).with_tag(2).with_tag(3);
    let value = DynamicStruct {
        descriptor,
        valid,
        properties: vec![
            (1, DynamicValue::U32(11)),
            (2, DynamicValue::String("from-dyn".into())),
            (3, DynamicValue::U64(77)),
        ],
    };
    gt_pub.publish(&value.try_as_publishable().unwrap());

    let event = tokio::time::timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("timed out waiting for typed subscriber")
        .expect("subscription closed");
    assert_eq!(event.value.id, Some(11));
    assert_eq!(event.value.message.as_deref(), Some("from-dyn"));
    assert_eq!(event.value.sequence, Some(77));

    gt_sub.exit();
    gt_pub.exit();
    let _ = tokio::time::timeout(Duration::from_secs(1), driver_sub_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(1), driver_pub_handle).await;
}

#[tokio::test]
async fn subscribe_new_struct_type_catches_up_on_existing_descriptors() {
    use dots_core::DynamicStructDescriptor;
    use std::sync::Mutex;

    // The guest's registry already has Pinger registered before
    // subscribe_new_struct_type is called — sync catch-up should
    // invoke the handler for it immediately.
    let host = HostTransceiver::new("nt-host");
    let (host_io, guest_io) = tokio::io::duplex(8192);
    host.accept(host_io);
    let registry = registry();
    registry.register_struct_static(Pinger::DESCRIPTOR);

    let conn = ConnectionBuilder::new(guest_io, "nt-guest", registry.clone())
        .preload(false)
        .connect()
        .await
        .unwrap();
    let (gt, driver) =
        GuestTransceiver::from_connection("nt-guest".to_string(), registry.clone(), conn);
    let driver_handle = tokio::spawn(driver.run());

    let captured: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_clone = captured.clone();
    let _handle = gt.subscribe_new_struct_type(move |desc: &Arc<DynamicStructDescriptor>| {
        captured_clone.lock().unwrap().push(desc.name.clone());
    });

    let names = captured.lock().unwrap().clone();
    assert!(
        names.iter().any(|n| n == "Pinger"),
        "catch-up should have invoked handler for Pinger; got {names:?}"
    );

    gt.exit();
    let _ = tokio::time::timeout(Duration::from_secs(1), driver_handle).await;
}

#[tokio::test]
async fn subscribe_new_struct_type_fires_for_wire_descriptor_arrivals() {
    use dots_core::DynamicStructDescriptor;
    use dots_model::StructDescriptorData;
    use std::sync::Mutex;

    let host = HostTransceiver::new("nt-wire-host");
    let registry = registry();
    host.registry().register_struct_static(Pinger::DESCRIPTOR);

    let (host_io, guest_io) = tokio::io::duplex(8192);
    host.accept(host_io);
    let conn = ConnectionBuilder::new(guest_io, "nt-wire-guest", registry.clone())
        .preload(false)
        .connect()
        .await
        .unwrap();
    let (gt, driver) =
        GuestTransceiver::from_connection("nt-wire-guest".to_string(), registry.clone(), conn);
    let driver_handle = tokio::spawn(driver.run());

    let captured: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_clone = captured.clone();
    let _handle = gt.subscribe_new_struct_type(move |desc: &Arc<DynamicStructDescriptor>| {
        captured_clone.lock().unwrap().push(desc.name.clone());
    });
    // Catch-up sees the DOTS-internal types but not Pinger — the
    // guest hasn't learned about Pinger yet.
    assert!(
        !captured.lock().unwrap().iter().any(|n| n == "Pinger"),
        "Pinger should not be in catch-up — guest hasn't seen it yet"
    );

    // Wait for the guest's DotsMember(Join, "StructDescriptorData")
    // to land at the broker — without this, the host's publish below
    // might race the join and not get routed.
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if host.group_size("StructDescriptorData") >= 1 {
            break;
        }
    }
    assert!(host.group_size("StructDescriptorData") >= 1);

    // Host publishes Pinger's descriptor data — the broker will
    // route it to every guest subscribed to StructDescriptorData
    // (which our subscribe_new_struct_type just installed).
    host.publish(&StructDescriptorData::from_static(Pinger::DESCRIPTOR));

    let mut names: Vec<String> = Vec::new();
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        names = captured.lock().unwrap().clone();
        if names.iter().any(|n| n == "Pinger") {
            break;
        }
    }
    assert!(
        names.iter().any(|n| n == "Pinger"),
        "wire arrival should fire handler for Pinger; got {names:?}"
    );
    // Registry should now know Pinger.
    assert!(registry.lookup("Pinger").is_some());

    gt.exit();
    let _ = tokio::time::timeout(Duration::from_secs(1), driver_handle).await;
}

#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "Bonk")]
struct Bonk {
    #[dots(tag = 1, key)]
    id: Option<u32>,
    #[dots(tag = 2)]
    label: Option<String>,
}

#[tokio::test]
async fn subscribe_all_types_funnels_events_for_distinct_types() {
    use std::sync::Mutex;

    let host = HostTransceiver::new("all-types-host");
    let registry = registry();
    registry.register_struct_static(Pinger::DESCRIPTOR);
    registry.register_struct_static(Bonk::DESCRIPTOR);
    host.registry().register_struct_static(Pinger::DESCRIPTOR);
    host.registry().register_struct_static(Bonk::DESCRIPTOR);

    let (host_io, guest_io) = tokio::io::duplex(8192);
    host.accept(host_io);
    let conn = ConnectionBuilder::new(guest_io, "trace-guest", registry.clone())
        .preload(false)
        .connect()
        .await
        .unwrap();
    let (gt, driver) =
        GuestTransceiver::from_connection("trace-guest".to_string(), registry.clone(), conn);
    let driver_handle = tokio::spawn(driver.run());

    // Capture (type_name, sender) for each event.
    let captured: Arc<Mutex<Vec<(String, Option<u32>)>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_clone = captured.clone();
    let _all = gt.subscribe_all_types(move |event| {
        let type_name = event.value.descriptor.name.clone();
        captured_clone
            .lock()
            .unwrap()
            .push((type_name, event.header.sender));
    });

    // Wait for the catch-up's auto-installed dynamic subs to land at
    // the host (we know the sub for "Pinger" creates a join, so wait
    // for that group to populate).
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if host.group_size("Pinger") >= 1 && host.group_size("Bonk") >= 1 {
            break;
        }
    }
    assert!(host.group_size("Pinger") >= 1);
    assert!(host.group_size("Bonk") >= 1);

    host.publish(&dots!(Pinger {
        id: 7_u32,
        message: "hi",
        sequence: 1_u64,
    }));
    host.publish(&dots!(Bonk {
        id: 2_u32,
        label: "tagged",
    }));

    // Wait until both types have at least one captured event.
    let mut events: Vec<(String, Option<u32>)> = Vec::new();
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        events = captured.lock().unwrap().clone();
        if events.iter().any(|(t, _)| t == "Pinger")
            && events.iter().any(|(t, _)| t == "Bonk")
        {
            break;
        }
    }
    assert!(
        events.iter().any(|(t, _)| t == "Pinger"),
        "expected Pinger event; got {events:?}"
    );
    assert!(
        events.iter().any(|(t, _)| t == "Bonk"),
        "expected Bonk event; got {events:?}"
    );

    gt.exit();
    let _ = tokio::time::timeout(Duration::from_secs(1), driver_handle).await;
}
