Linking Strategies
==================

Time-series instances rarely operate in isolation. A node's input
borrows another node's output; a ``REF`` value resolves into a target
output; a nested graph's output is presented as a parent's output
without copying the data. The runtime distinguishes three flavours of
"this position delegates to another position" — each with its own
state type, lifetime contract, and notification shape — and uses them
to support binding, unbinding, and rebinding across all of these
cases:

================  ===================  =================================================
Strategy          Direction            Used for
================  ===================  =================================================
TargetLink        input → output       Ordinary input-to-output binding (the common case)
RefLink           output → output      Dereferencing a REF inside an output alternative
ForwardingLink    output → output      Aliasing a child output as a parent output
                                       (nested-graph boundary export)
================  ===================  =================================================

Each link points at a memory-stable target — see *Time-Series Plans
and Ops > Memory Stability Invariant*. The runtime keeps a reverse-
subscription token registered on the target's state so that, if the
target is destroyed first, the link can be safely severed before its
dangling pointer is read.

TargetLink
----------

TargetLink is the input-side binding state for a peered terminal inside
a ``TSInput``. Peered terminals are declared by the generic
``TSEndpointSchema`` annotation tree; the ``TSInputPlanFactory``
validates that the root is a non-peered input bundle and that every
peered terminal matches the schema position it annotates. The input
terminal carries:

- the bound output view, including the borrowed output-owned
  ``TSDataView``;
- input-local ``last_modified_time`` used to bubble changes through
  non-peered input prefixes;
- an internal target observer registered with the bound ``TSData``
  observer set, so target destruction invalidates the binding before a
  stale pointer is read.

A subtle point: a peered input terminal carries **two** notification paths:

- **Target-modified path.** Subscribed to the target's modification
  notifier. When the target ticks, this path marks the link state
  modified and propagates the tick up through the parent chain
  (non-peered collections above the link).
- **Scheduling path.** Forwards scheduling
  notifications to the owning node, but does *not* mark the link
  state itself modified. Each active input path owns a forwarding
  ``Notifiable`` identity. This matters because multiple peered terminals
  under a non-peered collection may schedule the same node — using the
  node pointer directly would collapse them into one entry in an
  observer set and make independent subscribe/unsubscribe impossible.

Under a peered boundary, scheduling and modification flow through
different identities even though both ultimately reach the same owning
node.

RefLink
-------

RefLink is the output-side dereference of a ``REF`` value. A
``RefLinkState`` follows two moving pieces simultaneously:

- the **REF source** position — where the ``TimeSeriesReference`` value
  lives. Subscribed via ``source_notifiable``; when the reference
  retargets, the link rebinds.
- the **dereferenced target** — the time-series the reference currently
  points at. Subscribed via ``target_notifiable``; when the target
  ticks, the link state propagates that tick.

Internally, ``RefLinkState`` composes the same target-link binding
mechanics for the current dereferenced target, applied to a target
chosen by the source's value rather than at wiring time.

To support multiple consumers downstream of one ``RefLink``, the state
holds a per-upstream ``boundary_attachments`` map keyed by the
upstream notifier, each entry carrying its own forwarding
``SchedulingNotifier`` and an active-subtree handle. This lets several
consumers coexist below a single dereference boundary without their
subscription identities collapsing onto each other.

Sampled semantics: when the reference retargets at tick *t*, the
runtime keeps a ``previous_target_value`` snapshot so a consumer that
reads ``delta_value`` at *t* sees the rebinding event coherently. (The
``retain_transition_value`` flag turns this off for internal
alternative-replay paths that resync structurally.)

RefLink is **only** used inside output alternatives, never on the
plain ``TSInput`` binding path. A REF input goes through TargetLink
binding to a REF output; the dereference happens on the output side
when an alternative needs to expose the referenced TS value.

ForwardingLink
--------------

ForwardingLink redirects one output position to another output
position. The state type is the output-only ``OutputLinkState``. It
carries:

- a stable handle to the forwarded-to output;
- a ``TargetNotifiable`` subscribed to that output so ticks propagate
  to the redirected position's subscribers;
- the same kind of previous-target handling as TargetLink so
  switch ticks read coherently.

Unlike TargetLink, ForwardingLink does **not** participate in input
scheduling. There is no scheduling notifier and no active input
subtree to manage — all that's needed is for an output's subscribers
to receive ticks from the forwarded-to output as if they had been
notified directly.

The canonical use case is the nested-graph boundary plan: the
``alias_child_output`` binding mode exposes a child node's output
directly as one of the parent node's outputs, by installing a
ForwardingLink at the parent output that points at the child's. No
value is copied; subscribers of the parent see the child's ticks.
Other nested-graph modes (``bind_bundle_member_output``,
``alias_parent_input``) compose ForwardingLink with structural
navigation through bundles or input fields.

Why three link states, not one
------------------------------

The three link types could in principle be unified into a single
"redirect" state with conditional logic. The runtime keeps them
separate because:

- TargetLink's two-notifier (target-modified + scheduling) split is
  load-bearing for the input scheduling contract; a uniform link
  doesn't need the scheduling-notifier-per-link identity machinery
  for output-only paths.
- RefLink owns *two* moving pieces (source + target) and a
  per-upstream attachment map; that machinery is wasted if hardwired
  into every link.
- ForwardingLink is the simplest of the three (no input scheduling,
  no source tracking) and benefits from a smaller state footprint.

The three types share the same lifetime invariants — borrowed
target pointers backed by reverse-subscription invalidators — but
each one's notification surface is shaped to its job.
