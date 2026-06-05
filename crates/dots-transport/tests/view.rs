//! End-to-end test for [`View<T>`] — the guest-side filtered
//! subscription. Wires two guests to one host via `tokio::io::duplex`
//! and exercises the broker's four-cases dispatch (enter, in-view
//! update, leave, re-enter).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use dots_core::dots;
use dots_model::{Registry, filter::predicate, registry_with_internal_types};
use dots_transport::{ConnectionBuilder, GuestTransceiver, HostTransceiver, ViewOp};

mod model {
    use dots_derive::DotsStruct;

    #[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
    #[dots(name = "Pinger", cached)]
    pub struct Pinger {
        #[dots(tag = 1, key)]
        pub id: Option<u32>,
        #[dots(tag = 2)]
        pub message: Option<String>,
        #[dots(tag = 3)]
        pub sequence: Option<u64>,
    }
}
use model::*;

fn registry() -> Arc<Registry> {
    Arc::new(registry_with_internal_types())
}

#[tokio::test]
async fn view_four_cases_enter_update_leave_reenter() {
    let host = HostTransceiver::new("test-host");
    let registry = registry();
    registry.register_struct_static(Pinger::DESCRIPTOR);
    host.registry().register_struct_static(Pinger::DESCRIPTOR);

    // Subscriber guest opens a View<Pinger> with `sequence < 100`.
    let (host_io_a, guest_io_a) = tokio::io::duplex(16384);
    host.accept(host_io_a);
    let conn_a = ConnectionBuilder::new(guest_io_a, "subscriber", registry.clone())
        .preload(false)
        .connect()
        .await
        .unwrap();
    // Filtered subscriber: no auto-Join. Pinger is pre-registered on
    // both sides (`registry.register_struct_static` above + the same
    // on `host.registry()`), so no descriptor needs to ship. Leaving
    // both lists empty avoids a Phase 1b non-filtered Join that would
    // cause the broker to fan out every Pinger to this guest in
    // addition to the filtered view stream.
    let (gt_a, driver_a) = GuestTransceiver::from_connection(
        registry.clone(),
        conn_a,
        [],
        [],
    );
    let driver_a_handle = tokio::spawn(driver_a.run());

    let view = gt_a
        .view::<Pinger>(
            predicate(Pinger::SEQUENCE.lt(100_u64))
                .project(Pinger::PROP_ID | Pinger::PROP_SEQUENCE)
                .build(),
        )
        .expect("broker should support filtered subscriptions");

    let observed: Arc<Mutex<Vec<(ViewOp, Option<u32>, Option<u64>)>>> =
        Arc::new(Mutex::new(Vec::new()));
    let observed_for_handler = observed.clone();
    let _sub = view.subscribe(move |event| {
        observed_for_handler.lock().unwrap().push((
            event.op,
            event.value.id,
            event.value.sequence,
        ));
    });

    // Wait for the filtered subscription to register on the host.
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if host.group_size("Pinger") >= 1 {
            break;
        }
    }
    assert!(host.group_size("Pinger") >= 1);

    // Publisher guest.
    let (host_io_b, guest_io_b) = tokio::io::duplex(16384);
    host.accept(host_io_b);
    let conn_b = ConnectionBuilder::new(guest_io_b, "publisher", registry.clone())
        .preload(false)
        .connect()
        .await
        .unwrap();
    let (gt_b, driver_b) = GuestTransceiver::from_connection(
        registry.clone(),
        conn_b,
        [Pinger::DESCRIPTOR],
        [],
    );
    let driver_b_handle = tokio::spawn(driver_b.run());

    // Publish the four cases in order on a single key.
    let key = 42u32;
    let publishes = [
        (50u64, "enter"),
        (75u64, "update"),
        (150u64, "leave"),
        (42u64, "reenter"),
    ];
    for (seq, _label) in &publishes {
        gt_b.publish(&dots!(Pinger {
            id: key,
            message: "ignored by projection",
            sequence: *seq,
        }));
        tokio::time::sleep(Duration::from_millis(80)).await;
    }

    // Give the four events time to round-trip the broker.
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if observed.lock().unwrap().len() >= 4 {
            break;
        }
    }

    let got = observed.lock().unwrap().clone();
    assert_eq!(got.len(), 4, "expected 4 events, got {:?}", got);

    // 1: enter view — Create, sequence visible (50)
    assert_eq!(got[0].0, ViewOp::Create, "enter view → create");
    assert_eq!(got[0].1, Some(key));
    assert_eq!(got[0].2, Some(50));

    // 2: in-view update — Update (75)
    assert_eq!(got[1].0, ViewOp::Update, "in-view update");
    assert_eq!(got[1].1, Some(key));
    assert_eq!(got[1].2, Some(75));

    // 3: leave view — Remove. The event's value carries the *last
    //    in-view snapshot* (sequence=75), not the broker's key-only
    //    wire payload. Matches C++ Event<T>::operator()() semantics
    //    on remove: "the instance with this key, which last looked
    //    like {seq=75}, is now gone from the view."
    assert_eq!(got[2].0, ViewOp::Remove, "leave view → remove");
    assert_eq!(got[2].1, Some(key));
    assert_eq!(got[2].2, Some(75), "remove carries last cached value");

    // 4: re-enter view — Create (42)
    assert_eq!(got[3].0, ViewOp::Create, "reenter view → create");
    assert_eq!(got[3].1, Some(key));
    assert_eq!(got[3].2, Some(42));

    drop(view);
    // Give the leave to propagate.
    tokio::time::sleep(Duration::from_millis(100)).await;

    gt_a.exit();
    gt_b.exit();
    let _ = tokio::time::timeout(Duration::from_secs(1), driver_a_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(1), driver_b_handle).await;
}

#[tokio::test]
async fn view_drop_removes_filtered_sub_from_host() {
    let host = HostTransceiver::new("test-host");
    let registry = registry();
    registry.register_struct_static(Pinger::DESCRIPTOR);
    host.registry().register_struct_static(Pinger::DESCRIPTOR);

    let (host_io, guest_io) = tokio::io::duplex(8192);
    host.accept(host_io);
    let conn = ConnectionBuilder::new(guest_io, "subscriber", registry.clone())
        .preload(false)
        .connect()
        .await
        .unwrap();
    // Filtered subscriber only — no auto-Join, see the four-cases test
    // above for why both lists are empty here.
    let (gt, driver) =
        GuestTransceiver::from_connection(registry.clone(), conn, [], []);
    let driver_handle = tokio::spawn(driver.run());

    let view = gt
        .view::<Pinger>(predicate(Pinger::SEQUENCE.lt(100_u64)).build())
        .unwrap();

    // Wait for the host to record the filtered sub.
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if host.group_size("Pinger") >= 1 {
            break;
        }
    }
    assert_eq!(host.group_size("Pinger"), 1);

    drop(view);
    // Wait for the Leave to land.
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if host.group_size("Pinger") == 0 {
            break;
        }
    }
    assert_eq!(
        host.group_size("Pinger"),
        0,
        "filtered sub should be removed after View drop"
    );

    gt.exit();
    let _ = tokio::time::timeout(Duration::from_secs(1), driver_handle).await;
}

#[tokio::test]
async fn view_preload_from_existing_cache() {
    let host = HostTransceiver::new("test-host");
    let registry = registry();
    registry.register_struct_static(Pinger::DESCRIPTOR);
    host.registry().register_struct_static(Pinger::DESCRIPTOR);

    // Publisher seeds the cache before the subscriber opens its View.
    let (host_io_b, guest_io_b) = tokio::io::duplex(16384);
    host.accept(host_io_b);
    let conn_b = ConnectionBuilder::new(guest_io_b, "publisher", registry.clone())
        .preload(false)
        .connect()
        .await
        .unwrap();
    let (gt_b, driver_b) = GuestTransceiver::from_connection(
        registry.clone(),
        conn_b,
        [Pinger::DESCRIPTOR],
        [],
    );
    let driver_b_handle = tokio::spawn(driver_b.run());

    gt_b.publish(&dots!(Pinger { id: 1_u32, message: "a", sequence: 10_u64 }));
    gt_b.publish(&dots!(Pinger { id: 2_u32, message: "b", sequence: 200_u64 })); // out of view
    gt_b.publish(&dots!(Pinger { id: 3_u32, message: "c", sequence: 50_u64 }));

    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if host.cache_size("Pinger") == 3 {
            break;
        }
    }
    assert_eq!(host.cache_size("Pinger"), 3);

    // Subscriber opens View with predicate sequence < 100 — should
    // get pingers 1 and 3 in preload, not 2.
    let (host_io_a, guest_io_a) = tokio::io::duplex(16384);
    host.accept(host_io_a);
    let conn_a = ConnectionBuilder::new(guest_io_a, "subscriber", registry.clone())
        .preload(false)
        .connect()
        .await
        .unwrap();
    let (gt_a, driver_a) = GuestTransceiver::from_connection(
        registry.clone(),
        conn_a,
        [],
        [],
    );
    let driver_a_handle = tokio::spawn(driver_a.run());

    let view = gt_a
        .view::<Pinger>(predicate(Pinger::SEQUENCE.lt(100_u64)).build())
        .unwrap();

    let observed: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));
    let observed_for_handler = observed.clone();
    let _sub = view.subscribe(move |event| {
        if let Some(id) = event.value.id {
            observed_for_handler.lock().unwrap().push(id);
        }
    });

    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if observed.lock().unwrap().len() >= 2 {
            break;
        }
    }
    let mut got = observed.lock().unwrap().clone();
    got.sort();
    assert_eq!(
        got,
        vec![1, 3],
        "preload should deliver only matching cached entries"
    );

    drop(view);
    gt_a.exit();
    gt_b.exit();
    let _ = tokio::time::timeout(Duration::from_secs(1), driver_a_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(1), driver_b_handle).await;
}

/// `View::subscribe` synchronously replays the view's *current*
/// container snapshot to the new handler. `from_cache` must count down
/// `N-1 … 0` over that snapshot (matching the broker's cache-replay
/// semantics), so a handler can detect the final current-state entry
/// via `from_cache == 0` — rather than every replayed event claiming to
/// be the last.
#[tokio::test]
async fn view_subscribe_replay_counts_down_from_cache() {
    let host = HostTransceiver::new("test-host");
    let registry = registry();
    registry.register_struct_static(Pinger::DESCRIPTOR);
    host.registry().register_struct_static(Pinger::DESCRIPTOR);

    // Publisher seeds two in-view entries (sequence < 100) plus one
    // out-of-view entry.
    let (host_io_b, guest_io_b) = tokio::io::duplex(16384);
    host.accept(host_io_b);
    let conn_b = ConnectionBuilder::new(guest_io_b, "publisher", registry.clone())
        .preload(false)
        .connect()
        .await
        .unwrap();
    let (gt_b, driver_b) =
        GuestTransceiver::from_connection(registry.clone(), conn_b, [Pinger::DESCRIPTOR], []);
    let driver_b_handle = tokio::spawn(driver_b.run());

    gt_b.publish(&dots!(Pinger { id: 1_u32, message: "a", sequence: 10_u64 }));
    gt_b.publish(&dots!(Pinger { id: 2_u32, message: "b", sequence: 200_u64 })); // out of view
    gt_b.publish(&dots!(Pinger { id: 3_u32, message: "c", sequence: 50_u64 }));

    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if host.cache_size("Pinger") == 3 {
            break;
        }
    }
    assert_eq!(host.cache_size("Pinger"), 3);

    // Subscriber opens a View and waits for its local container to be
    // populated by the wire preload (2 matching entries).
    let (host_io_a, guest_io_a) = tokio::io::duplex(16384);
    host.accept(host_io_a);
    let conn_a = ConnectionBuilder::new(guest_io_a, "subscriber", registry.clone())
        .preload(false)
        .connect()
        .await
        .unwrap();
    let (gt_a, driver_a) =
        GuestTransceiver::from_connection(registry.clone(), conn_a, [], []);
    let driver_a_handle = tokio::spawn(driver_a.run());

    let view = gt_a
        .view::<Pinger>(predicate(Pinger::SEQUENCE.lt(100_u64)).build())
        .unwrap();

    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if view.container().len() >= 2 {
            break;
        }
    }
    assert_eq!(view.container().len(), 2, "wire preload should populate the container");

    // Now subscribe: the sync replay over the populated container runs
    // before `subscribe` returns. Capture the `from_cache` per event.
    let replayed: Arc<Mutex<Vec<Option<u32>>>> = Arc::new(Mutex::new(Vec::new()));
    let replayed_for_handler = replayed.clone();
    let _sub = view.subscribe(move |event| {
        replayed_for_handler
            .lock()
            .unwrap()
            .push(event.header.from_cache);
    });

    let mut got = replayed.lock().unwrap().clone();
    got.sort();
    assert_eq!(
        got,
        vec![Some(0), Some(1)],
        "two-entry snapshot replay must count from_cache down to 0, not emit all-zeros"
    );

    drop(view);
    gt_a.exit();
    gt_b.exit();
    let _ = tokio::time::timeout(Duration::from_secs(1), driver_a_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(1), driver_b_handle).await;
}
