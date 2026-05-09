//! Compile-only fixture for `dots-build`.
//!
//! `build.rs` runs the parser/codegen on `proto/types.dots` and writes
//! the result to `$OUT_DIR/dots_generated.rs`. Including it here means
//! a successful `cargo build -p dots-build-test` confirms the
//! generated source is syntactically valid AND that the
//! `#[derive(DotsStruct)]` / `#[derive(DotsEnum)]` macros accept it.

include!(concat!(env!("OUT_DIR"), "/dots_generated.rs"));

#[cfg(test)]
mod tests {
    use super::types::{DotsClient, DotsConnectionState, Pinger, StatusReport};
    use dots_core::StructValue;

    #[test]
    fn generated_pinger_has_expected_descriptor_name() {
        let p = Pinger::default();
        assert_eq!(p.descriptor().name, "Pinger");
    }

    #[test]
    fn generated_dots_client_has_connection_state_field() {
        let c = DotsClient {
            connection_state: Some(DotsConnectionState::Connected),
            ..Default::default()
        };
        assert_eq!(c.connection_state, Some(DotsConnectionState::Connected));
    }

    #[test]
    fn generated_status_uses_temporal_newtypes() {
        let s = StatusReport {
            server_name: Some("srv".into()),
            start_time: Some(dots_core::Timepoint(123.0)),
            uptime: Some(dots_core::Duration(45.0)),
        };
        assert_eq!(s.start_time, Some(dots_core::Timepoint(123.0)));
        assert_eq!(s.uptime, Some(dots_core::Duration(45.0)));
    }
}
