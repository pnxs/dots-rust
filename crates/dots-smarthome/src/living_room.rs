//! Living room: control two lights with one master dimmer.
//!
//! When a `Dimmer` event for `LIVING_ROOM_MASTER_DIMMER` arrives,
//! publish the same `brightness` to both `LIVING_ROOM_COUCH_LIGHT`
//! and `LIVING_ROOM_CEILING_LIGHT`.

use dots_rs::{App, SubscriptionHandle};

use dots_rs::dots;

use crate::ids::{LIVING_ROOM_CEILING_LIGHT, LIVING_ROOM_COUCH_LIGHT, LIVING_ROOM_MASTER_DIMMER};
use crate::model::*;

pub struct LivingRoom {
    /// Hold the subscription so dropping the component removes the
    /// handler from the dispatch table.
    _sub: SubscriptionHandle,
}

impl LivingRoom {
    pub fn new(app: &App) -> Self {
        let client = app.client();
        let sub = app.subscribe::<Dimmer>(move |event| {
            let dimmer = event.updated();
            // `id` is now a bare-`String` key: use the infallible getter.
            if dimmer.id != LIVING_ROOM_MASTER_DIMMER {
                return;
            }

            let brightness = dimmer.brightness;
            tracing::info!(?brightness, "LivingRoom: master dimmer changed");
            client.publish(&dots!(LightControl {
                id: LIVING_ROOM_COUCH_LIGHT,
                brightness: brightness,
            }));
            client.publish(&dots!(LightControl {
                id: LIVING_ROOM_CEILING_LIGHT,
                brightness: brightness,
            }));
        });
        Self { _sub: sub }
    }
}
