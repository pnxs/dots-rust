//! Smart-home example — Rust port of
//! `dots-cpp/bin/examples/smart-home`.
//!
//! Exposes the model types and the three reactor components
//! (LivingRoom, Stairwell, Basement) as library items so the
//! `smart-home` reactor binary and the `smart-home-sim` driver
//! binary can share them.
//!
//! The DOTS types live in `proto/model.dots`; `build.rs` parses that
//! file via `dots-build` and writes the generated Rust source to
//! `$OUT_DIR/dots_generated.rs`. Including it here brings a
//! `pub mod model { ... }` namespace into scope with `Switch`,
//! `StatelessSwitch`, `Dimmer`, and `LightControl`.

include!(concat!(env!("OUT_DIR"), "/dots_generated.rs"));

pub mod basement;
pub mod ids;
pub mod living_room;
pub mod stairwell;
