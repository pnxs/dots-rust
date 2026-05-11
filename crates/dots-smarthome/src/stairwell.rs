//! Stairwell: toggle a single light from two stateless switches.
//!
//! Each `StatelessSwitch` event for either end of the stairwell flips
//! the stairwell light: if no entry exists yet (or it's at `0`), turn
//! to `100`; otherwise turn off (`0`).

use dots_core::dots;
use dots_transport::{App, SubscriptionHandle};

use crate::ids::{STAIRWELL_LIGHT, STAIRWELL_LOWER_SWITCH, STAIRWELL_UPPER_SWITCH};
use crate::model::{LightControl, StatelessSwitch};

pub struct Stairwell {
    _sub: SubscriptionHandle,
}

impl Stairwell {
    pub fn new(app: &App) -> Self {
        let client = app.client();
        let lights = app.container::<LightControl>();

        let sub = app.subscribe::<StatelessSwitch>(move |event| {
            let id = event.value.id.as_deref();
            if id != Some(STAIRWELL_LOWER_SWITCH) && id != Some(STAIRWELL_UPPER_SWITCH) {
                return;
            }

            // Note: `get` matches by `#[dots(key)]` fields only —
            // `query`'s `brightness == None` is irrelevant.
            let next_brightness: u32 = match lights.get(&LightControl::new(STAIRWELL_LIGHT)) {
                Some(entry) => {
                    // `None` brightness reads as "0" per the model
                    // contract (invalid property == 0).
                    if entry.value.brightness.unwrap_or(0) == 0 {
                        100
                    } else {
                        0
                    }
                }
                None => 100,
            };
            tracing::info!(from = %id.unwrap_or("?"), next_brightness, "Stairwell: toggling light");
            client.publish(&dots!(LightControl {
                id: STAIRWELL_LIGHT,
                brightness: next_brightness,
            }));
        });
        Self { _sub: sub }
    }
}
