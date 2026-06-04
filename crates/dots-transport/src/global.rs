//! Process-wide global API mirroring the [`crate::App`] surface.
//!
//! After [`crate::App::new`] / [`crate::App::connect_tcp`] (etc.)
//! succeeds, the constructed transceiver is also installed in a
//! process-wide singleton. The free functions in this module forward
//! to it, mirroring dots-cpp's `dots::publish` / `dots::subscribe` /
//! `dots::container` namespace.
//!
//! ```ignore
//! use dots_transport::{App, global as dots};
//!
//! let app = App::new("client").await?;
//! dots::publish(&Pinger { id: Some(1) });
//! let _sub = dots::subscribe::<Pinger>(|event| { /* ... */ });
//! app.run().await?;
//! // App drops here, global is cleared.
//! ```
//!
//! ## Lifecycle
//!
//! The global is installed by [`crate::App`]'s constructors and
//! cleared by [`crate::App`]'s `Drop` — so an [`crate::App`] value's
//! lifetime defines the global's lifetime. Constructing a second
//! [`crate::App`] while another is still alive panics: only one
//! global at a time is supported, matching dots-cpp's
//! `dots::Application` singleton.
//!
//! Tests that need to spin up multiple [`crate::App`] instances (one
//! after the other) can either rely on `Drop` ordering or call
//! [`destroy`] explicitly. Tests that need to run *concurrently* with
//! one [`crate::App`] each must serialize App construction —
//! parallel `App::new` calls will collide. See `tests/app.rs` for
//! the in-tree pattern (a static mutex guarding each test).
//!
//! Tests / library code that wants neither the global nor a
//! singleton constraint can skip [`crate::App`] entirely and drive
//! [`crate::GuestTransceiver::from_connection`] directly — that path
//! does not touch the global.

use std::sync::{Arc, Mutex};

use dots_core::{
    DynamicStruct, DynamicStructDescriptor, GlobalRegistration, PropertySet, Publishable,
    StructValue,
};
use dots_model::filter::DotsFilter;

use crate::Subscription;
use crate::connection::Event;
use crate::container::Container;
use crate::guest::{AllTypesSubscription, GuestTransceiver, SubscriptionHandle};
use crate::view::{View, ViewError};

static GLOBAL: Mutex<Option<Arc<GuestTransceiver>>> = Mutex::new(None);

/// Install the process-wide transceiver. Called from
/// [`crate::App`]'s constructors; not part of the public API.
///
/// Panics if a transceiver is already installed — only one
/// [`crate::App`] per process is supported. Drop the existing
/// [`crate::App`] (or call [`destroy`]) before constructing another.
pub(crate) fn init(transceiver: Arc<GuestTransceiver>) {
    let mut slot = GLOBAL.lock().expect("global mutex poisoned");
    if slot.is_some() {
        panic!(
            "dots_transport::global: an App is already active in this process. \
             Drop the existing App (or call dots_transport::global::destroy) \
             before constructing another."
        );
    }
    *slot = Some(transceiver);
}

/// Clear the process-wide transceiver slot.
///
/// Called automatically by [`crate::App`]'s `Drop` so the typical
/// `let app = App::new(...).await?; app.run().await?;` flow needs no
/// explicit teardown. Exposed publicly for tests and applications
/// with non-standard lifecycles that want to release the slot before
/// the [`crate::App`] value goes out of scope.
///
/// Idempotent. Existing [`crate::Client`] handles obtained from
/// [`client`] / [`try_client`] before destruction stay valid — only
/// the global lookup is cleared.
pub fn destroy() {
    *GLOBAL.lock().expect("global mutex poisoned") = None;
}

/// `Some(transceiver)` if an [`crate::App`] is currently constructed,
/// `None` otherwise. Useful for libraries that want to gracefully
/// fall back to non-global behavior when run outside an
/// [`crate::App`] context.
pub fn try_client() -> Option<Arc<GuestTransceiver>> {
    GLOBAL.lock().expect("global mutex poisoned").clone()
}

/// The process-wide transceiver. Panics if no [`crate::App`] is
/// currently constructed. Use [`try_client`] for a non-panicking
/// variant.
pub fn client() -> Arc<GuestTransceiver> {
    try_client().expect(
        "dots_transport::global: no App is currently constructed. \
         Construct one with App::new (or any App::connect_* variant) first.",
    )
}

/// See [`GuestTransceiver::publish`](crate::GuestTransceiver::publish).
pub fn publish<P: Publishable>(value: &P) {
    client().publish(value)
}

/// See [`GuestTransceiver::publish_with_mask`](crate::GuestTransceiver::publish_with_mask).
pub fn publish_with_mask<P: Publishable>(value: &P, included: PropertySet) {
    client().publish_with_mask(value, included)
}

/// See [`GuestTransceiver::remove`](crate::GuestTransceiver::remove).
pub fn remove<P: Publishable>(value: &P) {
    client().remove(value)
}

/// See [`GuestTransceiver::subscribe`](crate::GuestTransceiver::subscribe).
pub fn subscribe<T>(handler: impl FnMut(&Event<T>) + Send + 'static) -> SubscriptionHandle
where
    T: StructValue + Send + 'static + GlobalRegistration,
{
    client().subscribe(handler)
}

/// See [`GuestTransceiver::subscribe_stream`](crate::GuestTransceiver::subscribe_stream).
pub fn subscribe_stream<T>() -> Subscription<T>
where
    T: StructValue + Send + 'static + GlobalRegistration,
{
    client().subscribe_stream::<T>()
}

/// See [`GuestTransceiver::subscribe_dynamic`](crate::GuestTransceiver::subscribe_dynamic).
pub fn subscribe_dynamic(
    descriptor: Arc<DynamicStructDescriptor>,
    handler: impl FnMut(&Event<DynamicStruct>) + Send + 'static,
) -> SubscriptionHandle {
    client().subscribe_dynamic(descriptor, handler)
}

/// See [`GuestTransceiver::subscribe_new_struct_type`](crate::GuestTransceiver::subscribe_new_struct_type).
pub fn subscribe_new_struct_type<F>(handler: F) -> SubscriptionHandle
where
    F: FnMut(&Arc<DynamicStructDescriptor>) + Send + 'static,
{
    client().subscribe_new_struct_type(handler)
}

/// See [`GuestTransceiver::subscribe_all_types`](crate::GuestTransceiver::subscribe_all_types).
pub fn subscribe_all_types<F>(handler: F) -> AllTypesSubscription
where
    F: FnMut(&Event<DynamicStruct>) + Send + 'static,
{
    client().subscribe_all_types(handler)
}

/// See [`GuestTransceiver::container`](crate::GuestTransceiver::container).
pub fn container<T>() -> Container<T>
where
    T: StructValue + Send + 'static + GlobalRegistration,
{
    client().container::<T>()
}

/// See [`GuestTransceiver::view`](crate::GuestTransceiver::view).
pub fn view<T>(filter: DotsFilter) -> Result<View<T>, ViewError>
where
    T: StructValue + Send + Clone + 'static + GlobalRegistration,
{
    client().view::<T>(filter)
}

/// See [`GuestTransceiver::exit`](crate::GuestTransceiver::exit).
pub fn exit() {
    client().exit()
}
