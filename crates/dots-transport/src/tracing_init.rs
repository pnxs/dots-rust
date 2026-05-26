//! Opt-in helper that installs a `tracing_subscriber` global with the
//! defaults every dots-rust binary in this workspace uses. Gated by
//! the `tracing-init` feature so library consumers don't pull
//! `tracing-subscriber` transitively.

use tracing_subscriber::EnvFilter;

/// Library crates whose log output is pinned at `warn` so dots-rust
/// internals stay quiet by default. These caps are crate-specific
/// directives, so they always win over a bare level directive in the
/// user-supplied filter string (per `EnvFilter` precedence rules).
const LIB_CAPS: &str =
    "dots_core=warn,dots_derive=warn,dots_model=warn,dots_transport=warn";

/// Install a [`tracing_subscriber::fmt`] subscriber as the global
/// default with dots-rust library crates capped at `warn` and
/// everything else at `info`.
///
/// `app_directives` is appended to the default filter so binaries can
/// adjust verbosity for their own crate (or any other target) without
/// touching the library caps. Pass `""` to accept the defaults — the
/// caller's crate is already at `info` and doesn't need to opt in.
///
/// Filter resolution order:
///
/// 1. If `RUST_LOG` is set and valid, it replaces the entire filter
///    (standard [`EnvFilter`] behaviour). Use this to override the
///    library caps at runtime.
/// 2. Otherwise the filter is `info` globally, with `dots_*` crates
///    pinned at `warn`, plus `app_directives`.
///
/// Calling more than once panics — same semantics as
/// [`tracing_subscriber::fmt::SubscriberBuilder::init`].
///
/// # Example
///
/// ```ignore
/// #[tokio::main]
/// async fn main() {
///     // Defaults: caller's crate at info, dots-rust at warn.
///     dots_transport::init_tracing("");
///     // Or raise verbosity for a specific module:
///     // dots_transport::init_tracing("my_app::sync=debug");
/// }
/// ```
///
/// Override at runtime:
///
/// ```text
/// RUST_LOG=dots_transport=debug,my_app=trace cargo run ...
/// ```
pub fn init_tracing(app_directives: &str) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        let mut directives = String::from("info,");
        directives.push_str(LIB_CAPS);
        if !app_directives.is_empty() {
            directives.push(',');
            directives.push_str(app_directives);
        }
        EnvFilter::new(directives)
    });

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .compact()
        .init();
}
