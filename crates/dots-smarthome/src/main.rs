//! Smart-home reactor — Rust port of
//! `dots-cpp/bin/examples/smart-home/src/main.cpp`.
//!
//! Connects to a DOTS broker (e.g. `dotsd`), wires three independent
//! components, and stays alive until Ctrl-C:
//!
//! - [`LivingRoom`] — relays a `Dimmer` to two `LightControl`s.
//! - [`Stairwell`] — toggles a single `LightControl` from either of
//!   two stateless switches.
//! - [`Basement`] — turns a `LightControl` on for motion, schedules a
//!   timed turn-off when motion clears.
//!
//! ```text
//! ./dotsd                                              # in one terminal
//! cargo run --bin smart-home                           # the reactor
//! cargo run --bin smart-home-sim                       # drives test events
//! ```
//!
//! Override the broker endpoint with `DOTS_ENDPOINT`, e.g.
//! `DOTS_ENDPOINT=uds:///tmp/dotsd.sock cargo run --bin smart-home`.

use std::time::Duration;

use dots_smarthome::basement::Basement;
use dots_smarthome::living_room::LivingRoom;
use dots_smarthome::stairwell::Stairwell;
use dots_transport::App;

const APP_NAME: &str = "smart-home";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dots_transport::init_tracing("");

    let app = App::new(APP_NAME).await?;
    tracing::info!("smart-home connected");

    // Construct each component once; they install their subscriptions
    // (and any container views) inside `new`. Holding the components
    // in scope keeps the subscription handles alive.
    let _basement = Basement::new(&app, Duration::from_secs(30));
    let _living_room = LivingRoom::new(&app);
    let _stairwell = Stairwell::new(&app);

    eprintln!("smart-home running; press Ctrl-C to exit");
    app.run_until_signal().await?;
    Ok(())
}
