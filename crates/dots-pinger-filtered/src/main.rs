//! End-to-end demonstration of server-side filtered subscriptions.
//! Publishes a deliberately-crafted sequence that crosses the filter
//! boundary on the same key, exercising all four cases of the
//! broker's dispatch state machine: enter view, in-view update,
//! leave view, re-enter.
//!
//! Verifies the events delivered to the View match expectations and
//! exits non-zero on mismatch — usable as a smoke test.
//!
//! ```text
//! cargo run -p dotsd                                                # in one terminal
//! cargo run -p dots-pinger-filtered                                 # in another
//! DOTS_ENDPOINT=uds:///tmp/dotsd.sock cargo run -p dots-pinger-filtered
//! ```

use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dots_core::dots;
use dots_model::filter::predicate;
use dots_transport::{App, ViewOp};

mod model {
    use dots_derive::DotsStruct;
    #[derive(DotsStruct, Default, Debug, Clone)]
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

const CLIENT_NAME: &str = "dots-pinger-filtered";

#[derive(Debug, PartialEq)]
struct ObservedEvent {
    op: ViewOp,
    id: u32,
    /// 0 if absent (the broker masked it off, etc.).
    sequence: u64,
    has_message: bool,
}

fn op_name(op: ViewOp) -> &'static str {
    match op {
        ViewOp::Create => "create",
        ViewOp::Update => "update",
        ViewOp::Remove => "remove",
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    dots_transport::init_tracing("");

    let app = match App::new(CLIENT_NAME).await {
        Ok(a) => a,
        Err(e) => {
            eprintln!("ERROR connecting to dotsd -> {e}");
            return ExitCode::from(2);
        }
    };

    let caps_ok = app
        .transceiver()
        .peer_capabilities()
        .and_then(|c| c.filtered_subscriptions)
        .unwrap_or(false);
    if !caps_ok {
        eprintln!("ERROR broker does not advertise filteredSubscriptions capability");
        return ExitCode::from(1);
    }
    println!("== pinger-filtered — server-side filter demo ==");
    println!("broker advertises filteredSubscriptions: yes\n");

    // Filter: only Pingers with sequence < 100, project to
    // {id, sequence} (drop the 'message' field on the wire).
    let view = match app.view::<Pinger>(
        predicate(Pinger::SEQUENCE.lt(100_u64))
            .project(Pinger::PROP_ID | Pinger::PROP_SEQUENCE)
            .build(),
    ) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("ERROR opening View<Pinger> -> {e}");
            return ExitCode::from(1);
        }
    };
    println!(
        "opened View (subId={}) with filter:\n  predicate: sequence < 100\n  project:   {{id, sequence}}  (message is masked out)\n",
        view.subscription_id()
    );

    let observed: Arc<Mutex<Vec<ObservedEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let observed_for_handler = observed.clone();
    let _sub = view.subscribe(move |event| {
        let o = ObservedEvent {
            op: event.op,
            id: event.value.id.unwrap_or(0),
            sequence: event.value.sequence.unwrap_or(0),
            has_message: event.value.message.is_some(),
        };
        println!(
            "  recv  op={}  id={}  seq={}  msg={}",
            op_name(o.op),
            o.id,
            o.sequence,
            if o.has_message { "<set>" } else { "<masked>" }
        );
        observed_for_handler.lock().unwrap().push(o);
    });

    // Use a process-unique key so reruns against a long-lived dotsd
    // don't collide with stale cached entries from prior runs.
    let key: u32 = 1000 + (rand::random::<u32>() % (u32::MAX - 1000));
    println!("using key id={key}");

    // Sequence designed to cross the filter boundary on a single key.
    let publishes = [
        (50_u64, "first"),   // matches → enter view → create
        (75_u64, "second"),  // matches → in-view update
        (150_u64, "outside"), // does NOT match → leave view → remove
        (42_u64, "back"),    // matches again → re-enter → create
    ];

    let client = app.client();
    let observed_for_publisher = observed.clone();
    let exit_handle = app.transceiver().clone();
    tokio::spawn(async move {
        for (sequence, msg) in publishes {
            println!("publish  id={key}  seq={sequence}  msg='{msg}'");
            client.publish(&dots!(Pinger {
                id: key,
                message: msg,
                sequence: sequence,
            }));
            // 120 ms is enough for the broker's echo to round-trip on
            // a local TCP loopback. Mirrors the C++ example's
            // `io_context.run_for(100ms)` per publish.
            tokio::time::sleep(Duration::from_millis(120)).await;
        }
        // Wait for the last event to arrive before tearing down.
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if observed_for_publisher.lock().unwrap().len() >= 4 {
                break;
            }
        }
        exit_handle.exit();
    });

    let run_result = app.run().await;

    let view_size = view.container().len();
    drop(view);
    if let Err(e) = run_result {
        eprintln!("ERROR running pinger-filtered -> {e}");
        return ExitCode::from(2);
    }

    println!("\nview container after sequence: {view_size} entry(ies)");

    // Expected events. On a remove event, the View carries the *last
    // in-view snapshot* — i.e. the value as it last appeared in the
    // view, not the key-only wire payload. The seq=75 row below is
    // that last-known value, not a key fragment. Matches dots-cpp's
    // `Event<T>::operator()()` semantics on remove.
    let expected = [
        ObservedEvent { op: ViewOp::Create, id: key, sequence: 50, has_message: false },
        ObservedEvent { op: ViewOp::Update, id: key, sequence: 75, has_message: false },
        ObservedEvent { op: ViewOp::Remove, id: key, sequence: 75, has_message: false },
        ObservedEvent { op: ViewOp::Create, id: key, sequence: 42, has_message: false },
    ];

    let got = observed.lock().unwrap();
    let ok = got.len() == expected.len()
        && got.iter().zip(expected.iter()).all(|(a, e)| a == e);

    println!(
        "\n{}: expected {} events, observed {}",
        if ok { "PASS" } else { "FAIL" },
        expected.len(),
        got.len()
    );

    if !ok {
        println!("\n--- expected ---");
        for e in &expected {
            println!("  {}  id={}  seq={}", op_name(e.op), e.id, e.sequence);
        }
        println!("--- observed ---");
        for a in got.iter() {
            println!("  {}  id={}  seq={}", op_name(a.op), a.id, a.sequence);
        }
        return ExitCode::from(1);
    }
    ExitCode::SUCCESS
}
