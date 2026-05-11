//! Opt-in helper that installs a `tracing_subscriber` global with the
//! defaults every dots-rust binary in this workspace uses. Gated by
//! the `tracing-init` feature so library consumers don't pull
//! `tracing-subscriber` transitively.

use tracing_subscriber::EnvFilter;

/// Install a [`tracing_subscriber::fmt`] subscriber as the global
/// default, with:
///
/// - Filter taken from the `RUST_LOG` environment variable, falling
///   back to `info` if unset or malformed.
/// - Compact event format with module-target column enabled.
///
/// Intended for binary `main` functions. Calling more than once
/// panics — same semantics as
/// [`tracing_subscriber::fmt::SubscriberBuilder::init`].
///
/// # Example
///
/// ```ignore
/// #[tokio::main]
/// async fn main() {
///     dots_transport::init_tracing();
///     // ...
/// }
/// ```
///
/// Override the level at runtime:
///
/// ```text
/// RUST_LOG=dots_transport=debug,dots_demo_client=info cargo run ...
/// ```
pub fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(true)
        .compact()
        .init();
}
