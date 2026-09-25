# dots-rust agent guidance

When implementing or reviewing dots-rust clients, models, or tests, read
[the general AI usage guide](crates/dots-rs/AI_USAGE_GUIDE.md).

When changing container access, subscriptions, or replay, also read
[the container usage guide](crates/dots-rs/CONTAINER_GUIDE.md).
For broader client behavior, consult [the user guide](docs/USER_GUIDE.md).

Prefer indexed `get_ref()` for known keys, `find_ref()` for borrowed predicate
searches, and `with_iter()` for projections or counting. Keep borrowed lock
lifetimes short and invoke replay callbacks only after releasing cache locks.
Preserve full snapshots when callers need historical or complete owned state.
Check the actual dependency revision before recommending newly added methods
to downstream clients.

For startup-state checks, account for early subscription: normal `App`
connections finish preload for statically registered unfiltered types before
returning. Enable application release LTO for pruning unused registrations;
do not add sleeps merely to wait for that initial cache.

Prefer `dots!` for fresh client publications and key queries: unspecified
optional fields become `None`, allowing additive optional model fields without
rewriting call sites. Required bare keys must still be supplied; omission is
not a request to clear a cached property.

For explicit invalidation, use `publish_with_mask` with the absent property's
`PROP_*` bit. Ordinary publishing of `None` leaves the cached field unchanged.
Review masks alongside payloads: broad masks on sparse values clear every
addressed absent field, and republishing full cached copies can overwrite
unrelated changes.

Normally publish complete keys plus only changed fields. Multiple clients can
maintain one cached instance by addressing disjoint properties. Avoid sending
full cached copies that could overwrite another client's newer values; same-
property writes and whole-instance deletion need application coordination.

Use `PropertySet` and generated `PROP_*` constants to check valid fields and
shape publish masks: `|` unions, `&` intersects, `-` excludes, and
`required.is_subset_of(value.valid_set())` checks all required fields.
Keep validity separate from addressed update fields; intersecting a mask with
`valid_set()` suppresses clears. See the general guide's PropertySet section.
