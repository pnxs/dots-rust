//! Basement: motion-sensor light with a hold-on timeout.
//!
//! When motion is detected (`Switch.enabled == true`) and the light
//! isn't already on, turn it on. When motion clears
//! (`Switch.enabled == false`) and the light is on, schedule a turn-off
//! after `light_timeout`. Re-triggering motion cancels the pending
//! turn-off.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use dots_core::dots;
use dots_transport::{App, SubscriptionHandle};
use tokio::task::JoinHandle;

use crate::ids::{BASEMENT_LIGHT, BASEMENT_MOTION_SWITCH};
use crate::model::{LightControl, Switch};

pub struct Basement {
    _sub: SubscriptionHandle,
}

impl Basement {
    pub fn new(app: &App, light_timeout: Duration) -> Self {
        let client = app.client();
        let lights = app.container::<LightControl>();

        // Single pending timer slot. The closure may schedule a
        // turn-off and re-triggering motion (on→off→on) must cancel
        // the previous timer rather than queue another. `Mutex<Option<…>>`
        // gives the closure interior mutability and a place to stash
        // / cancel the JoinHandle. The handler is Send + 'static, so
        // the slot has to be `Send + Sync` — Arc<Mutex<…>> fits.
        let pending: Arc<Mutex<Option<JoinHandle<()>>>> = Arc::new(Mutex::new(None));

        let sub = app.subscribe::<Switch>(move |event| {
            let switch = &event.value;
            if switch.id.as_deref() != Some(BASEMENT_MOTION_SWITCH) {
                return;
            }
            
            let existing = lights.get(&LightControl::new(BASEMENT_LIGHT));
            let existing_brightness =
                existing.as_deref().and_then(|e| e.brightness).unwrap_or(0);

            if switch.enabled == Some(true) {
                // Motion detected — turn the light on if it isn't
                // already, and cancel any pending turn-off.
                if let Some(handle) = pending.lock().expect("pending mutex").take() {
                    handle.abort();
                    tracing::debug!("Basement: motion cancelled pending turn-off");
                }
                if existing_brightness == 0 {
                    tracing::info!("Basement: motion → light on");
                    client.publish(&dots!(LightControl {
                        id: BASEMENT_LIGHT,
                        brightness: 100_u32,
                    }));
                }
            } else if existing_brightness != 0 {
                // Motion cleared while the light is on — schedule
                // turn-off after the timeout, replacing any earlier
                // pending timer.
                let publisher = client.clone();
                let new_handle = tokio::spawn(async move {
                    tokio::time::sleep(light_timeout).await;
                    tracing::info!("Basement: timeout elapsed → light off");
                    publisher.publish(&dots!(LightControl {
                        id: BASEMENT_LIGHT,
                        brightness: 0_u32,
                    }));
                });
                if let Some(prev) = pending
                    .lock()
                    .expect("pending mutex")
                    .replace(new_handle)
                {
                    prev.abort();
                }
                tracing::info!(?light_timeout, "Basement: motion cleared → turn-off scheduled");
            }
        });
        Self { _sub: sub }
    }
}
