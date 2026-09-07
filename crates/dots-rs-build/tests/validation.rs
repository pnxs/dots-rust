//! Semantic validation of `.dots` sources (wire-format spec §6.3 and
//! the `instance_ref` target rule of §6.5), exercised through
//! `compile_to_dir` — the pass runs between parsing and codegen.

use dots_rs_build::{BuildError, compile_to_dir};

/// Compile one `.dots` source and return the validation message, if
/// the build was rejected.
fn validate(source: &str) -> Option<String> {
    let dir = std::env::temp_dir().join(format!(
        "dots-rs-build-validation-{}-{:p}",
        std::process::id(),
        &source
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let input = dir.join("model.dots");
    std::fs::write(&input, source).unwrap();
    let result = compile_to_dir(&[&input], &dir);
    let message = match result {
        Ok(()) => None,
        Err(BuildError::Validate { message, .. }) => Some(message),
        Err(other) => panic!("expected Validate error, got: {other}"),
    };
    let _ = std::fs::remove_dir_all(&dir);
    message
}

#[test]
fn accepts_compliant_sources() {
    assert_eq!(
        validate(
            "struct Route {\n    1: [key] string parent;\n    2: [key] string name;\n    3: string via;\n}\n\
             struct Status {\n    1: [key] instance_ref subject;\n    2: instance_ref<Route> route;\n}\n"
        ),
        None
    );
}

#[test]
fn key_declaration_order_is_free() {
    // Identity artifacts sort by tag (spec §6.3), so declaring key
    // properties out of tag order is legal.
    assert_eq!(
        validate("struct Route {\n    2: [key] string name;\n    1: [key] string parent;\n}\n"),
        None
    );
}

#[test]
fn rejects_non_scalar_key_types() {
    for (source, expected) in [
        ("struct S { 1: [key] float64 value; }", "not byte-stable"),
        ("struct S { 1: [key] timepoint at; }", "not byte-stable"),
        (
            "struct S { 1: [key] vector<string> names; }",
            "vectors cannot be keys",
        ),
        (
            "struct Inner { 1: string x; }\nstruct S { 1: [key] Inner inner; }",
            "nested structs cannot be keys",
        ),
    ] {
        let message = validate(source).expect(source);
        assert!(message.contains(expected), "{source}: {message}");
    }
}

#[test]
fn accepts_enum_and_ref_keys() {
    assert_eq!(
        validate(
            "enum Mood { 1: happy, 2: grumpy }\n\
             struct S {\n    1: [key] Mood mood;\n    2: [key] instance_ref subject;\n}\n"
        ),
        None
    );
}

#[test]
fn rejects_non_struct_ref_targets() {
    let message = validate("enum Mood { 1: happy }\nstruct S { 1: instance_ref<Mood> mood_ref; }")
        .expect("enum ref target must be rejected");
    assert!(
        message.contains("targets must be struct types"),
        "{message}"
    );

    let message = validate("struct S { 1: instance_ref<uint32> id_ref; }")
        .expect("primitive ref target must be rejected");
    assert!(message.contains("primitive"), "{message}");

    // Unknown targets pass (cross-package imports; the derive's const
    // assertion is the backstop).
    assert_eq!(
        validate("struct S { 1: instance_ref<SomewhereElse> other; }"),
        None
    );
}

#[test]
fn checks_ref_targets_inside_vectors() {
    let message =
        validate("enum Mood { 1: happy }\nstruct S { 1: vector<instance_ref<Mood>> refs; }")
            .expect("vector element ref target must be validated");
    assert!(
        message.contains("targets must be struct types"),
        "{message}"
    );
}
