//! Integration tests for the `dots-testing` harness itself.
//!
//! These also double as worked examples of the patterns the harness is
//! meant to support. Each test runs serialized (the harness holds the
//! process-wide global lock), so they're safe under any runtime flavor.

use std::time::Duration;

use dots_core::dots;
use dots_testing::TestHarness;
use dots_transport::global as dots;

mod model {
    use dots_derive::DotsStruct;

    #[derive(DotsStruct, Default, Debug, Clone, PartialEq)]
    #[dots(name = "HarnessGreeting", cached)]
    pub struct Greeting {
        #[dots(tag = 1, key)]
        pub id: Option<u32>,
        #[dots(tag = 2)]
        pub text: Option<String>,
    }

    #[derive(DotsStruct, Default, Debug, Clone, PartialEq)]
    #[dots(name = "HarnessCounter", cached)]
    pub struct Counter {
        #[dots(tag = 1, key)]
        pub id: Option<u32>,
        #[dots(tag = 2)]
        pub value: Option<u64>,
    }
}
use model::*;

/// The primary guest is installed as the global, so the free functions
/// (`dots::publish` / `dots::subscribe_stream`) work, and a cached
/// publish round-trips back to the publisher through the broker —
/// flagged `is_from_myself` because the sender id is our own.
#[tokio::test]
async fn global_api_pubsub_roundtrip() {
    let harness = TestHarness::new().await;

    let mut sub = dots::subscribe_stream::<Greeting>();
    dots::publish(&dots!(Greeting { id: 1_u32, text: "hello" }));

    let event = harness.recv(&mut sub).await.expect("should receive Greeting");
    assert_eq!(event.value.id, Some(1));
    assert_eq!(event.value.text.as_deref(), Some("hello"));
    assert_eq!(event.header.is_from_myself, Some(true));
}

/// A second ("spoof") guest publishes; the primary guest receives it
/// routed through the in-process broker, flagged as *not* from itself.
#[tokio::test]
async fn spoof_guest_routes_to_primary() {
    let harness = TestHarness::new().await;

    // Primary subscribes first.
    let mut sub = harness.subscribe_stream::<Greeting>();

    // A different client connects and publishes.
    let spoof = harness.add_spoof_guest().await.expect("spoof guest connects");
    spoof.publish(&dots!(Greeting { id: 7_u32, text: "from spoof" }));

    let event = harness.recv(&mut sub).await.expect("primary receives spoof publish");
    assert_eq!(event.value.id, Some(7));
    assert_eq!(event.header.is_from_myself, Some(false));
}

/// The keyed cache container mirrors published instances, keeping only
/// the latest value per key.
#[tokio::test]
async fn container_mirrors_cache() {
    let harness = TestHarness::new().await;

    let container = harness.container::<Counter>();
    let mut sub = harness.subscribe_stream::<Counter>();

    dots::publish(&dots!(Counter { id: 1_u32, value: 10_u64 }));
    harness.recv(&mut sub).await.expect("first publish");
    dots::publish(&dots!(Counter { id: 1_u32, value: 11_u64 }));
    harness.recv(&mut sub).await.expect("update");
    dots::publish(&dots!(Counter { id: 2_u32, value: 99_u64 }));
    harness.recv(&mut sub).await.expect("second key");

    assert_eq!(container.len(), 2);
    let one = container.get(&dots!(Counter { id: 1_u32 })).expect("key 1 present");
    assert_eq!(one.value, Some(11)); // latest wins
    let two = container.get(&dots!(Counter { id: 2_u32 })).expect("key 2 present");
    assert_eq!(two.value, Some(99));
}

/// `wait_for_subscribers` observes broker-side subscription state.
#[tokio::test]
async fn wait_for_subscribers_sees_the_join() {
    let harness = TestHarness::new().await;
    // The primary subscribes to Greeting at link time (this binary
    // monomorphizes subscribe_stream::<Greeting>), so the broker should
    // already see a subscriber after EarlySubscribe.
    let _sub = harness.subscribe_stream::<Greeting>();
    let joined = harness
        .wait_for_subscribers::<Greeting>(1, Duration::from_secs(2))
        .await;
    assert!(joined, "broker should report at least one Greeting subscriber");
}

/// Dropping a harness releases the global slot and the process-wide
/// lock, so a fresh harness can be built immediately afterward. (Were
/// the slot leaked, the second `TestHarness::new` would panic on
/// `global::init`.)
#[tokio::test]
async fn harness_teardown_allows_reconstruction() {
    {
        let h = TestHarness::new().await;
        h.publish(&dots!(Greeting { id: 1_u32, text: "first" }));
    } // dropped here

    let h2 = TestHarness::new().await;
    let mut sub = h2.subscribe_stream::<Greeting>();
    h2.publish(&dots!(Greeting { id: 2_u32, text: "second" }));
    let event = h2.recv(&mut sub).await.expect("second harness works");
    assert_eq!(event.value.id, Some(2));
}
