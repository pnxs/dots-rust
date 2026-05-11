//! Smart-home simulator — drives the `smart-home` reactor with a
//! short sequence of fake device events.
//!
//! Each step waits ~1.5s so the reactor's reactions can be observed
//! in its log output. Connect both binaries to the same broker:
//!
//! ```text
//! ./dotsd                                              # in one terminal
//! cargo run --bin smart-home                           # the reactor
//! cargo run --bin smart-home-sim                       # this driver
//! ```

use std::time::Duration;
use dots_core::dots;
use dots_smarthome::ids::{
    BASEMENT_MOTION_SWITCH, LIVING_ROOM_MASTER_DIMMER, STAIRWELL_LOWER_SWITCH,
    STAIRWELL_UPPER_SWITCH,
};
use dots_smarthome::model::{Dimmer, StatelessSwitch, Switch};
use dots_transport::App;

const APP_NAME: &str = "smart-home-sim";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dots_transport::init_tracing();

    let app = App::new(APP_NAME).await?;
    let client = app.client();

    // Spawn the driver loop on a task so the App's read loop runs
    // concurrently — without it, no acknowledgements / dispatches.
    let driver = tokio::spawn(async move {
        let step = Duration::from_millis(1500);

        eprintln!("→ LivingRoom: master dimmer to 42%");
        client.publish(&dots!(Dimmer {
            id: LIVING_ROOM_MASTER_DIMMER,
            brightness: 42_u32,
        }));
        tokio::time::sleep(step).await;

        eprintln!("→ LivingRoom: master dimmer to 75%");
        client.publish(&dots!(Dimmer {
            id: LIVING_ROOM_MASTER_DIMMER,
            brightness: 75_u32,
        }));
        tokio::time::sleep(step).await;

        eprintln!("→ Stairwell: lower switch (toggle on)");
        client.publish(&StatelessSwitch::default().with_id(STAIRWELL_LOWER_SWITCH));
        tokio::time::sleep(step).await;

        eprintln!("→ Stairwell: upper switch (toggle off)");
        client.publish(&StatelessSwitch::default().with_id(STAIRWELL_UPPER_SWITCH));
        tokio::time::sleep(step).await;

        eprintln!("→ Basement: motion detected");
        client.publish(&dots!(Switch {
            id: BASEMENT_MOTION_SWITCH,
            enabled: true,
        }));
        tokio::time::sleep(step).await;

        eprintln!("→ Basement: motion cleared (reactor schedules turn-off)");
        client.publish(&dots!(Switch {
            id: BASEMENT_MOTION_SWITCH,
            enabled: false,
        }));
        tokio::time::sleep(step).await;

        eprintln!("simulator done; reactor's basement timer is still pending.");
    });

    // Run the App's read loop until the simulator task finishes.
    tokio::select! {
        res = app.run_until_signal() => res?,
        _ = driver => {
            eprintln!("simulator finished; exiting.");
        }
    }
    Ok(())
}
