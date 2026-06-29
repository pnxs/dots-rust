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
    use dots_rs::StructValue;

    #[test]
    fn generated_pinger_has_expected_descriptor_name() {
        let p = Pinger::new(1u32);
        assert_eq!(p.descriptor().name, "Pinger");
    }

    #[test]
    fn generated_dots_client_has_connection_state_field() {
        let mut c = DotsClient::new(1u32);
        c.connection_state = Some(DotsConnectionState::Connected);
        assert_eq!(c.connection_state, Some(DotsConnectionState::Connected));
    }

    #[test]
    fn generated_status_uses_temporal_newtypes() {
        let s = StatusReport {
            server_name: "srv".into(),
            start_time: Some(dots_rs::Timepoint(123.0)),
            uptime: Some(dots_rs::Duration(45.0)),
        };
        assert_eq!(s.start_time, Some(dots_rs::Timepoint(123.0)));
        assert_eq!(s.uptime, Some(dots_rs::Duration(45.0)));
    }
}
