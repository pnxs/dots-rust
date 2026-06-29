//! End-to-end test: parse a real `.dots` file from dots-cpp and
//! verify the generated source has the expected shape.
//!
//! We don't try to actually compile the generated code in-process
//! (that would require trampolining through `cargo build`), but we
//! do exercise the full lexer + parser + codegen pipeline.

use dots_rs_build::{compile_to_dir, parse_str};

const DAEMON_DOTS: &str = "\
struct DotsClient [internal] {
    1: [key] uint32 id;
    2: string name;
    3: bool running;
    4: vector<string> publishedTypes;
    5: vector<string> subscribedTypes;
    6: DotsConnectionState connectionState;
}

struct DotsStatistics [internal] {
    1: uint64 bytes;
    2: uint64 packages;
}

struct DotsResourceUsage [internal] {
    1: int32 minorFaults; /// number of minor page-faults
    10: duration userCpuTime; /// used 'user' CPU-time in seconds
    11: duration systemCpuTime; /// used 'system' CPU-time in seconds
}

struct DotsDaemonStatus [internal] {
    1: [key] string serverName;
    2: timepoint startTime;
    3: DotsStatistics received;
}
";

#[test]
fn parses_daemon_dots_full_file() {
    let file = parse_str(DAEMON_DOTS).expect("parse should succeed");
    assert_eq!(file.items.len(), 4);
}

#[test]
fn generated_source_contains_expected_items() {
    let file = parse_str(DAEMON_DOTS).unwrap();
    let out = dots_rs_build::generate(&file);

    // DotsClient checks
    assert!(out.contains("pub struct DotsClient"));
    assert!(out.contains("#[dots(name = \"DotsClient\", cached, internal)]"));
    // Key field is bare `T`, not `Option<T>`.
    assert!(out.contains("pub id: u32,"));
    assert!(out.contains("pub published_types: Option<Vec<String>>,"));
    // DotsConnectionState reference passes through unchanged.
    assert!(out.contains("pub connection_state: Option<DotsConnectionState>,"));

    // A keyed struct does NOT derive `Default` (a bare-`T` key has no
    // meaningful default); a keyless one still does.
    assert!(out.contains("#[derive(DotsStruct, Debug, Clone, PartialEq)]\n#[dots(name = \"DotsClient\""));
    assert!(out.contains("#[derive(DotsStruct, Default, Debug, Clone, PartialEq)]\n#[dots(name = \"DotsStatistics\""));

    // DotsResourceUsage uses Duration newtype for user/system CPU time.
    assert!(out.contains("pub user_cpu_time: Option<dots_rs::Duration>,"));
    assert!(out.contains("pub system_cpu_time: Option<dots_rs::Duration>,"));

    // DotsDaemonStatus references DotsStatistics + Timepoint.
    assert!(out.contains("pub start_time: Option<dots_rs::Timepoint>,"));
    assert!(out.contains("pub received: Option<DotsStatistics>,"));

    // Trailing comments are preserved as Rust doc comments.
    assert!(out.contains("/// number of minor page-faults"));
}

#[test]
fn cross_file_import_emits_use_super_path() {
    let tmp = tempdir();
    let a_path = tmp.join("colors.dots");
    let b_path = tmp.join("paint.dots");
    std::fs::write(
        &a_path,
        "enum Color { 1: red, 2: green, 3: blue }",
    )
    .unwrap();
    std::fs::write(
        &b_path,
        "import Color\nstruct Paint { 1: [key] uint32 id; 2: Color hue; }",
    )
    .unwrap();

    let out_dir = tmp.join("out");
    compile_to_dir(&[&a_path, &b_path], &out_dir).unwrap();

    let combined = std::fs::read_to_string(out_dir.join("dots_generated.rs")).unwrap();
    // The paint module should use Color via the colors module.
    assert!(
        combined.contains("use super::colors::Color;"),
        "expected `use super::colors::Color;` in generated source; got:\n{combined}"
    );
}

#[test]
fn compile_to_dir_writes_combined_file_with_module_per_input() {
    // Two input files in a temp dir; expect two `pub mod` blocks.
    let tmp = tempdir();
    let a_path = tmp.join("a.dots");
    let b_path = tmp.join("b.dots");
    std::fs::write(&a_path, "struct A { 1: [key] uint32 id; }").unwrap();
    std::fs::write(&b_path, "enum B { 1: foo, 2: bar }").unwrap();

    let out_dir = tmp.join("out");
    compile_to_dir(&[&a_path, &b_path], &out_dir).unwrap();

    let combined = std::fs::read_to_string(out_dir.join("dots_generated.rs")).unwrap();
    assert!(combined.contains("pub mod a {"));
    assert!(combined.contains("pub mod b {"));
    assert!(combined.contains("pub struct A"));
    assert!(combined.contains("pub enum B"));
}

#[test]
fn any_field_maps_to_any_object() {
    // An `any` property maps to `Option<dots_rs::AnyObject>`; the
    // contained type is not part of the static schema.
    let src = "\
struct Envelope {
    1: [key] uint32 id;
    2: any payload;
}
";
    let file = parse_str(src).expect("parse should succeed");
    let out = dots_rs_build::generate(&file);
    assert!(out.contains("pub payload: Option<dots_rs::AnyObject>,"));
}

fn tempdir() -> std::path::PathBuf {
    let base = std::env::temp_dir().join(format!(
        "dots-build-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&base).unwrap();
    base
}
