Time-Series Plans and Ops
=========================

The time-series layer wraps value-layer storage with the additional
state every output and input needs to participate in graph evaluation:
modification time, subscribers, binding, delta tracking, and the path
identity that makes output-to-input wiring possible.

This page focuses on the layout strategies — memory stability, the
slot-store family, the storage shapes for TSS and TSD, and buffer
exposure. The per-kind tick contract and the value/delta schema
mappings are described in *Schemas > Time-Series Schemas*; the
binding/redirect machinery is described in *Linking Strategies*.

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

The Slot Store Family
---------------------

Three primitives in ``v2/types/utils/`` express the slot machinery:

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

The Set and Map shapes used by the value layer's delta-tracking
implementations are layered on these primitives. A delta-tracking Set
owns one ``KeySlotStore``. A delta-tracking Map owns one
``KeySlotStore`` for keys plus one ``ValueSlotStore`` for values, with
the value store registered as a slot observer on the key store.

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
slot holds a complete time-series value (most often a ``TS[T]``, but
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
