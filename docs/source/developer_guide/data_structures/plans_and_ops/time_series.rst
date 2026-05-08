Time-Series Plans and Ops
=========================

The time-series layer owns full runtime ``TSValue`` objects. A
``TSValue`` is not just a scalar ``Value`` with a timestamp attached:
it is split into separate components so the hot value payload, delta
payload, and graph-evaluation state can be stored and accessed in the
shape each one needs.

This page focuses on the layout strategies — memory stability, the
slot-store family, the storage shapes for TSS and TSD, and buffer
exposure. The per-kind tick contract and the value/delta schema
mappings are described in *Schemas > Time-Series Schemas*; the
binding/redirect machinery is described in *Linking Strategies*.

Terminology: TSValue and TSData
-------------------------------

The implementation uses the following names consistently:

``TSValue``
    The full runtime time-series object owned by a node input or
    output. It combines the current payload/delta component with
    evaluation state and child time-series objects. This is the object
    that participates in graph binding, notification, and traversal.

``TSState``
    The graph-evaluation state associated with a ``TSValue``:
    validity, ``last_modified_time``, parent/child relationships,
    subscribers, path identity, and kind-specific notification state.
    ``TSState`` does not own the hot payload bytes.

``TSData``
    The payload/delta component inside a ``TSValue``. It owns the
    current value storage and the per-tick delta information, laid out
    so those two views stay aligned and can expose useful buffer /
    NumPy representations. ``TSData`` does not own subscribers,
    parent links, or scheduling state.

``TSDataOps``
    The type-erased operations over a ``TSData`` memory region:
    lifecycle, read access, mutation opening/closing, delta access,
    and buffer exposure. These ops operate on the payload/delta
    component only; notification is coordinated by the surrounding
    ``TSState``.

``TSDataBinding``
    The interned binding for a ``TSData`` implementation: the
    ``TSValueTypeMetaData`` schema, the data ``StoragePlan``, and the
    ``TSDataOps`` table. The schema is the time-series schema rather
    than the scalar value schema because delta shape and mutation
    behaviour depend on the time-series kind.

``TSDataPlanFactory``
    The schema → data-plan resolver for ``TSData``. It chooses the
    compact mutable implementation for atomic time-series data and the
    slot-oriented implementation for collection-shaped time-series
    data. The factory does not plan the whole ``TSValue`` object, only
    its payload/delta data component.

``TSValueBuilder``
    The reusable builder for full ``TSValue`` instances. It composes a
    ``TSDataBinding`` / data plan with the separate ``TSState`` layout
    and the reusable child-builder graph needed for nested
    time-series. ``TSValueBuilder`` is the object cached by node and
    graph construction code.

TSData implementation families
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``CompactTSDataStorage``
    Used for atomic time-series data. The current payload uses the
    compact scalar ``StoragePlan`` for the value type, but the bound
    ``TSDataOps`` allow mutation because a time-series output updates
    in place during evaluation. Delta information is stored separately
    from the current payload while remaining directly associated with
    it.

``SlotTSDataStorage``
    Used for collection-shaped time-series data. The data store is
    slot-oriented: every child or element has a stable slot id and the
    current payload, validity, and delta information are aligned by
    that slot id. Fixed-shape collections such as bundles and fixed
    lists use static ordinal slots; dynamic and keyed collections use
    non-relocating slot stores. The point is the same in both cases:
    collection mutation changes slot state instead of compacting or
    relocating already-published child addresses.

The terms above keep three layers distinct: scalar ``Value`` storage,
``TSData`` payload/delta storage, and the full ``TSValue`` runtime
object. Code and docs should avoid using "TS value plan" unless they
mean the complete object; payload/delta plans are ``TSData`` plans.

Builder Lifetime
----------------

Time-series value builders are reusable builders. A
``TSValueBuilder`` resolves a ``TSValueTypeMetaData`` schema to the
``TSDataBinding`` / data plan, state layout, and child-builder graph
needed to construct a full ``TSValue`` runtime object. Once resolved,
it should be cached and reused to construct multiple time-series
instances with the same schema. This is the opposite of the value-
layer ``ListBuilder`` / ``MapBuilder`` family, which is local scratch
storage for one immutable ``Value``.

This distinction matters most for nested structures. A ``TSB`` builder owns
the reusable builders for its fields; a fixed ``TSL`` builder owns the
reusable builder for each element position; a ``TSD`` builder owns the
reusable value-side time-series builder used whenever a new key appears. The
builder graph is shared construction metadata, while each ``TSValue``
instance owns its ``TSState``, ``TSData``, and child storage
independently.

Memory Stability Invariant
--------------------------

Every time-series value in the runtime must be memory-stable. Once an
output's value and its ops table are published, their addresses must
not move for the lifetime of the owning node. Output-to-input binding
is implemented by recording pointers — to the value, to the ops, to
per-element state — and those pointers must remain valid across
ticks, rebinds, and structural mutation of containers.

For fixed-shape time-series (TS, TSB, fixed-size TSL, TSW), stability
is trivial: the value lives in node-owned storage and survives until
the owning node is destroyed.

For TSD and dynamic TSL, stability is harder. Elements are added and
removed during evaluation, but a consumer that bound to one of them
on the previous tick must still be able to dereference it on the
current tick. Compacting storage cannot be used. The runtime instead
uses chained, non-relocating slot blocks: new capacity is appended
without moving previously published slot addresses.

.. _ts-path-construction:

Path Construction and the Slot Concept
--------------------------------------

Every binding carries a path that locates the target value within its
owning graph. For indexable kinds — TSB, TSL — the path is a sequence
of ``size_t`` indices (field index, element index) and addressing is
direct.

TSD breaks this because keys can be of any scalar type. Carrying
arbitrary keys in paths would be expensive (variable-width encoding,
type-aware comparison at every step) and would couple every path
traversal to key semantics.

The runtime resolves this by introducing the **slot**: a stable
non-negative integer that names an element of a keyed container
without referencing the key itself. A slot is allocated when a key is
first inserted, remains allocated while the key is live, and persists
through delayed-erase windows so consumers can still inspect a removed
element on the tick of its removal. With slots, paths into TSD become
``(slot_id)`` — exactly the shape of paths into TSL — and the runtime
can treat all keyed containers uniformly.

Slots originate in TSS rather than TSD. The reason is the TSS/TSD
relationship described below: a TSD exposes its keys as a TSS, and
that TSS must use the same slot ids as the parent TSD. Putting slots
at the TSS layer first lets TSD's value side reuse them directly.

.. _ts-slot-store-family:

The Slot Store Family
---------------------

Three primitives in ``hgraph/types/utils/`` express the slot machinery.
The ``SlotTSDataStorage`` implementations for collection-shaped
time-series data build on these primitives so they can support
per-element insert / remove / replace with stable addresses and the
per-slot ``updated`` bit needed to surface deltas. The value-layer
(scalar) container shapes are different — they are compact and atomic
by design (see *Scalar Plans and Ops > Container Storage Shapes*) and
do not use the slot stores.

``StableSlotStorage``
    Non-relocating, double-indexed slot storage. ``slots`` is a
    logical slot-id → payload-pointer table; ``blocks`` owns the
    chained heap allocations behind those pointers. Growth appends a
    block; previously issued slot pointers never move.

``KeySlotStore``
    Stable slot-backed key storage with delayed-erase semantics. Owns
    homogeneous keys keyed off a ``StoragePlan`` and a small ops vtable
    (``hash``, ``equal``). Maintains two parallel bitmaps:

    - ``constructed[slot]`` — payload object exists in slot memory
    - ``live[slot]`` — payload is currently a member of the set

    A slot in ``constructed && !live`` is *pending erase*: still
    addressable and inspectable until the next outermost
    ``begin_mutation()`` or an explicit ``erase_pending()`` flush. This
    is what lets a consumer that bound on the previous tick inspect
    the slot's last value during the tick of its removal.

``ValueSlotStore``
    Parallel value memory keyed off the same slot ids as a
    ``KeySlotStore``. The owning store decides which slot ids are live;
    ``ValueSlotStore`` owns the value payload, the per-slot
    ``constructed`` bit, and a per-slot ``updated`` bit used to drive
    the current mutation epoch.

Both stores expose a ``SlotObserver`` notification protocol —
``on_capacity``, ``on_insert``, ``on_remove``, ``on_erase``,
``on_clear`` — so parallel structures over the same slot ids stay
synchronised without any of them needing to know about the others.
The TS layer uses this for two purposes: a ``MapValueObserver``
mirrors a key store's slot lifecycle onto a paired value store
(``TSD`` keys → values); and a delta-recording observer captures
``added`` / ``removed`` slot ids per cycle so the layer can publish
``delta_value``.

The set and map ``SlotTSDataStorage`` shapes are layered on these
primitives. A TS set data store owns one ``KeySlotStore``. A TS map
data store owns one ``KeySlotStore`` for keys plus one
``ValueSlotStore`` for values, with the value store registered as a
slot observer on the key store.

Slot stores are deliberately **not** used for scalar values. The
delayed-erase, per-slot-bit, and observer machinery exists to support
delta tracking across ticks; for non-time-series payloads that
machinery is overkill and a plain ``StorageHandle`` suffices (see
*Scalar Plans and Ops*).

TSS Storage
-----------

TSS is the time-series wrapper around a delta-tracking Set. It owns:

- a ``KeySlotStore`` for the keys, providing stable per-slot addresses
  and delayed-erase semantics;
- the time-series state common to every kind (modification time,
  subscribers, validity);
- per-slot insertion and removal records that drive ``delta_value``.

The slot ids assigned by the ``KeySlotStore`` are the path identifiers
used throughout the rest of the time-series layer.

A TSS instance can be **owning** — a TSS output written by a node — or
**read-only** — a TSS view exposed by another container (see TSD).
Both modes share the same TSS surface: ``value``, ``delta_value``,
subscription, and slot-based path access. The read-only mode rejects
write operations because the underlying storage belongs to the parent
container.

TSD Storage
-----------

TSD is the time-series wrapper around a delta-tracking Map. It owns:

- a ``KeySlotStore`` for the keys, identical in shape to a TSS's;
- a ``ValueSlotStore`` whose slot ids match the key store's, holding
  the per-key time-series values;
- the time-series state common to every kind.

The value side is itself a recursive time-series layer: each value-
slot holds a complete time-series value (most often a ``TS``, but
``TSB``, ``TSL``, or further nested ``TSD`` are all permitted by the
schema). Memory stability is preserved by the underlying
``StableSlotStorage`` so consumers can bind to a specific slot's value
without worrying about future structural changes.

Key-Set Exposure
~~~~~~~~~~~~~~~~

A TSD exposes its keys as a TSS through ``key_set()``. The returned
TSS is **read-only**:

- it shares the parent TSD's ``KeySlotStore`` directly, so slot ids
  match one-to-one with the parent's value side;
- it can be subscribed to and exposes ``value`` and ``delta_value``
  like any other TSS;
- it rejects write operations — keys are owned by the TSD and only
  change through the TSD's mutable view.

This is the value-layer Map → read-only Set view (described in
*Erased Types*) lifted into the time-series layer. The difference is
that the time-series version carries modification time and a delta
stream, not just structural read access.

Buffer Exposure
~~~~~~~~~~~~~~~

Because keys and values live in slot stores backed by stable
contiguous blocks, both TSS and the value side of TSD can expose
buffer views the same way the value layer does — over live keys, over
live values, and over the per-slot ``updated`` mask for the current
tick. This matters for adaptors and analytics paths that consume
large keyed time-series in bulk.
