//! Device identifier constants used by the smart-home components.
//!
//! The IDs are pure application-level naming conventions — they're
//! not part of the DOTS model, so they live in plain Rust rather
//! than in `proto/model.dots`.

pub const LIVING_ROOM_MASTER_DIMMER: &str = "LivingRoom_MasterDimmer";
pub const LIVING_ROOM_COUCH_LIGHT: &str = "LivingRoom_CouchLight";
pub const LIVING_ROOM_CEILING_LIGHT: &str = "LivingRoom_CeilingLight";

pub const STAIRWELL_LOWER_SWITCH: &str = "Stairwell_LowerSwitch";
pub const STAIRWELL_UPPER_SWITCH: &str = "Stairwell_UpperSwitch";
pub const STAIRWELL_LIGHT: &str = "Stairwell_Light";

pub const BASEMENT_MOTION_SWITCH: &str = "Basement_MotionSwitch";
pub const BASEMENT_LIGHT: &str = "Basement_Light";
