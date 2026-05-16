//! End-to-end smoke test of server-side filtered subscriptions.
//!
//! Opens a `View<Pinger>` with predicate `(sequence < 100) && (id ==
//! key)` and projection `{id, sequence}` (message is masked out),
//! then publishes four `Pinger` instances on a single key with
//! sequences `{50, 75, 150, 42}`. Each publish is designed to trigger
//! a different one of the broker's four-cases dispatch transitions:
//!
//! ```text
//! publish | seq | broker decision    | observed event
//! --------+-----+--------------------+------------------
//!   1     |  50 | enter view         | create
//!   2     |  75 | in-view update     | update
//!   3     | 150 | leave view         | remove (key-only)
//!   4     |  42 | re-enter view      | create
//! ```
//!
//! Mirrors `bin/examples/pinger-filtered/src/main.cpp` in dots-cpp.
//!
//! Verifies the observed events match expectations and exits non-zero
//! on mismatch — usable as a smoke test. Uses a random `u32` key per
//! run so reruns against a long-lived broker don't collide with stale
//! cached entries.
//!
//! ```text
//! cargo run -p dotsd                                                # in one terminal
//! cargo run -p dots-pinger-filtered                                 # in another
//! DOTS_ENDPOINT=uds:///tmp/dotsd.sock cargo run -p dots-pinger-filtered
//! ```

use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dots_derive::DotsStruct;
use dots_model::filter::predicate;
use dots_transport::App;

#[derive(DotsStruct, Default, Debug, Clone)]
#[dots(name = "Pinger", cached)]
struct Pinger {
    #[dots(tag = 1, key)]
    id: Option<u32>,
    #[dots(tag = 2)]
    message: Option<String>,
    #[dots(tag = 3)]
    sequence: Option<u64>,
}

const CLIENT_NAME: &str = "dots-pinger-filtered";

#[derive(Debug, PartialEq)]
struct Observed {
    is_remove: bool,
    sequence: Option<u64>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    dots_transport::init_tracing();

    let app = match App::new(CLIENT_NAME).await {
        Ok(a) => a,
        Err(e) => {
            eprintln!("failed to connect: {e}");
            return ExitCode::from(2);
        }
    };

    // Random key per run keeps stale cached entries from a previous
    // run (against a long-lived broker) out of view's predicate.
    let key: u32 = rand::random();
    println!("using random key = {key}");

    let view = match app.view::<Pinger>(
        predicate(Pinger::SEQUENCE.lt(100_u64) & Pinger::ID.eq(key))
            .project(Pinger::PROP_ID | Pinger::PROP_SEQUENCE)
            .build(),
    ) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("could not open View<Pinger>: {e}");
            return ExitCode::from(2);
        }
    };

    let observed: Arc<Mutex<Vec<Observed>>> = Arc::new(Mutex::new(Vec::new()));
    let observed_for_handler = observed.clone();
    let _sub = view.subscribe(move |event| {
        let is_remove = event.header.remove_obj == Some(true);
        let seq = event.value.sequence;
        let msg = event.value.message.as_deref();
        println!(
            "← event  id={:?}  seq={:?}  remove={}  message={:?} (msg should be None under projection)",
            event.value.id, seq, is_remove, msg
        );
        observed_for_handler.lock().unwrap().push(Observed {
            is_remove,
            sequence: seq,
        });
    });

    // Publisher task runs in parallel with App::run; once all four
    // publishes are out and observed, it signals the App to exit.
    let observed_for_publisher = observed.clone();
    let client = app.client();
    let app_for_exit = app.transceiver().clone();
    tokio::spawn(async move {
        let publishes = [
            (50_u64, "enter view (50 < 100)"),
            (75_u64, "in-view update (75 < 100)"),
            (150_u64, "leave view (150 >= 100)"),
            (42_u64, "re-enter view (42 < 100)"),
        ];
        for (seq, label) in publishes {
            println!("→ publish  id={key} seq={seq}  ({label})");
            client.publish(&Pinger {
                id: Some(key),
                message: Some(format!("masked: seq={seq}")),
                sequence: Some(seq),
            });
            // 120 ms gives the broker's echo time to round-trip and
            // hit our handler before we publish the next case.
            tokio::time::sleep(Duration::from_millis(120)).await;
        }

        // Wait briefly for the last event to land.
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if observed_for_publisher.lock().unwrap().len() >= 4 {
                break;
            }
        }

        // Drop view? Can't from here without ownership transfer.
        // The view is dropped when main exits; the broker tears
        // down the subscription either way (via Leave or via
        // disconnect cleanup). Signal the App to exit.
        app_for_exit.exit();
    });

    let run_result = app.run().await;

    drop(view);
    if let Err(e) = run_result {
        eprintln!("app loop ended with error: {e}");
        return ExitCode::from(2);
    }

    // Verify.
    let got = observed.lock().unwrap();
    let expected = [
        Observed { is_remove: false, sequence: Some(50) },
        Observed { is_remove: false, sequence: Some(75) },
        // Leave-view: key-only remove. The receiver sees `sequence`
        // masked off because only the key was sent.
        Observed { is_remove: true, sequence: None },
        Observed { is_remove: false, sequence: Some(42) },
    ];

    if got.len() != expected.len() {
        eprintln!(
            "MISMATCH: expected {} events, got {}: {:?}",
            expected.len(),
            got.len(),
            *got
        );
        return ExitCode::from(1);
    }
    let mut ok = true;
    for (i, (o, e)) in got.iter().zip(expected.iter()).enumerate() {
        if o != e {
            eprintln!("MISMATCH at event {i}: got {o:?}, expected {e:?}");
            ok = false;
        }
    }
    if !ok {
        return ExitCode::from(1);
    }
    println!("\nOK: all four broker decisions observed in order");
    ExitCode::SUCCESS
}
