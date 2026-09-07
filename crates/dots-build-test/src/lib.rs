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
    use super::types::{
        DotsClient, DotsConnectionState, Pinger, SetPointStatus, StatusReport, TestResult,
    };
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
    fn generated_enum_values_match_dots_source() {
        // Explicit `= N` values from the .dots source must survive
        // codegen → derive; without one the derive defaults to tag - 1.
        let d = TestResult::DESCRIPTOR;
        assert_eq!(d.elements[0].value, 0); // 1: passed = 0
        assert_eq!(d.elements[1].value, -1); // 2: failed = -1
        assert_eq!(d.elements[2].value, 2); // 3: skipped (default tag - 1)
        // Default enum descriptor values match the dots-cpp generator:
        // connecting..closed advertise 0..4, not their tags 1..5.
        let c = DotsConnectionState::DESCRIPTOR;
        assert_eq!(c.elements[0].value, 0);
        assert_eq!(c.elements[4].value, 4);
    }

    #[test]
    fn generated_instance_refs_round_trip() {
        let pinger = Pinger::new(7u32);
        let status = SetPointStatus {
            subject: dots_rs::instance_ref(&pinger),
            parent: Some(dots_rs::InstanceRef::of(&pinger)),
            applied: Some(true),
        };
        // Generic ref (key) carries the type name; typed ref does not.
        assert_eq!(status.subject.type_name(), "Pinger");
        assert_eq!(status.parent.as_ref().unwrap().type_name(), "Pinger");
        assert_eq!(status.subject.key_bytes(), [0x81, 0x07]); // [7]
        assert_eq!(status.parent.as_ref().unwrap().key_bytes(), [0x81, 0x07]);

        let bytes = dots_rs::encode_to_vec(&status);
        let decoded: SetPointStatus = dots_rs::decode_typed_from_slice(&bytes).unwrap();
        assert_eq!(decoded, status);
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
