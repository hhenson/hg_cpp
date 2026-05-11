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
    The type-erased operation table over a ``TSData`` memory region:
    the literal ``allows_mutation`` property, layout access, read/write
    memory access, delta reset, copy, and the per-kind hook used when a
    child time-series value reports that it modified. The table is
    deliberately passive; generic mutation sequencing and propagation
    rules live on ``TSDataView`` / ``TSDataMutationView``. For a real
    bound ``TSData`` implementation the table is total: required entries
    are never null, empty optional behaviours use no-op thunks, and
    unsupported operations are explicit throwing thunks rather than
    missing pointers.

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
    ``TSDataView`` allows mutation because a time-series output updates
    in place during evaluation. Current value bytes, delta value bytes,
    and tracking stamps are separate memory regions in one TSData plan,
    so the current value can still be exposed as a compact buffer when
    the scalar type supports that.

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

TSData Memory Layout and Delta Tracking
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Every ``TSData`` plan makes current-value access and delta-management
rules explicit. For compact atomic ``TS<T>``-style data, the delta
value is the current value and is valid only when
``last_modified_time == evaluation_time``. Collection storage adds
separate delta masks or payloads where the delta shape is not the
current value. The layout context carried by ``TSDataOps`` records the
bindings and memory offsets needed by the concrete implementation:

- ``value`` — the current payload in the value-layer representation;
- ``delta`` — optional per-tick delta payload or masks, using
  ``delta_value_schema`` when the delta is not an alias of the current
  value;
- ``tracking`` — modification stamps and other transient delta
  metadata.

The diagrams below are conceptual. ``StoragePlan`` still owns the
exact byte offsets, padding, and alignment decisions for the target
platform. The invariant is the shape and ownership of each region, not
the literal byte numbers shown in a diagram.

Compact Atomic TSData
^^^^^^^^^^^^^^^^^^^^^

``TS<T>``, ``REF<T>``, and ``SIGNAL`` currently use a compact TSData
plan. The owning memory is one storage object with two separately
addressable regions:

.. code-block:: text

   TSData storage allocation
   +---------------------------------------------------------------+
   | value region                                                  |
   | Value storage for T, also used as delta(T) when modified      |
   | layout.value_offset                                           |
   +---------------------------------------------------------------+
   | tracking region                                                |
   | TSDataTracking {                                                |
   |   last_modified_time                                           |
   | }                                                              |
   | layout.tracking_offset                                         |
   +---------------------------------------------------------------+

The current-value read path points directly at the ``value`` region.
The delta read path returns an empty typed view unless
``tracking.last_modified_time == evaluation_time``; when it matches,
the delta view points directly at the current ``value`` region:

.. mermaid::

   flowchart LR
      View["TSDataView"]
      ValueCall["value()"]
      DeltaCall["delta_value(t)"]
      Current["value region<br/>current T bytes"]
      Tracking["tracking region<br/>last_modified_time"]
      Check{"last_modified_time == t"}
      Null["typed null ValueView"]

      View --> ValueCall --> Current
      View --> DeltaCall --> Check
      Tracking -.read.-> Check
      Check -->|yes| Current
      Check -->|no| Null

During mutation, the first write for an engine time copies the source
payload into the current value region, then updates
``last_modified_time``. A same-time overwrite writes the value region again
but does not advance ``last_modified_time`` and does not produce a
second parent notification:

.. mermaid::

   flowchart TD
      Write["write source at engine time t"]
      Check{"already modified at t?"}
      FirstCopy["copy source into<br/>current value"]
      FirstMark["last_modified_time = t<br/>parent notification may bubble"]
      OverwriteCopy["overwrite<br/>current value"]
      NoNotify["last_modified_time unchanged<br/>no second parent notification"]

      Write --> Check
      Check -->|no| FirstCopy --> FirstMark
      Check -->|yes| OverwriteCopy --> NoNotify

.. code-block:: text

   before write at t
   +----------+   +----------------------------+
   | current  |   | last_modified_time = t - 1 |
   +----------+   +----------------------------+

   first write at t
   +----------+   +----------------------------+
   | source   |   | last_modified_time = t     |
   +----------+   +----------------------------+

   overwrite again at t
   +----------+   +----------------------------+
   | source2  |   | last_modified_time = t     |
   +----------+   +----------------------------+

View Handles
^^^^^^^^^^^^

View objects are handles over TSData memory; they are not embedded
inside the TSData allocation. A plain data view needs the binding and
data pointer. A child view additionally carries a ``TSDataParentLink``:
the parent view reference plus the parent-owned child id used for
bubble-up:

.. mermaid::

   flowchart LR
      View["TSDataView handle<br/>binding_<br/>data_<br/>parent_link_"]
      Link["TSDataParentLink<br/>parent<br/>child_id"]
      Binding["TSDataBinding<br/>schema + plan + ops"]
      Data["TSData storage allocation<br/>value + optional delta + tracking"]
      Parent["parent TSDataView<br/>for bubble-up"]

      View -->|binding_| Binding
      View -->|data_| Data
      View -->|parent_link_| Link
      Link -->|parent| Parent
      Link -.child_id belongs to parent.-> Parent

``TSDataMutationView`` is the mutation-only handle. It carries a view
copy plus the current engine time and validates that the bound
``TSDataOps::allows_mutation`` property is true. Mutation depth is
tracked by the owning root ``TSValue`` / state object, not by each
TSData element:

.. mermaid::

   flowchart TD
      Mutation["TSDataMutationView<br/>view_<br/>mutation_time_"]
      Tracking["view_.tracking()<br/>TSDataTracking"]
      EngineTime["active engine time"]
      Root["root TSValue mutation state<br/>tracks mutation depth"]

      Mutation -->|references through view_| Tracking
      Mutation -->|carries| EngineTime
      Root -.coordinates mutation lifetime.-> Mutation

The active mutation time is deliberately not stored in
``TSDataTracking``. The tracking state records what happened to the
data; the mutation view records the engine time for the in-flight
operation.

Modification handling is deliberately split into three responsibilities.
``TSData`` tracks local modification state; child data views are
constructed with a ``TSDataParentLink`` that owns the parent reference
and parent-relative child id; and
the parent data ops record child-level modification details through
``record_child_modified(parent_data, child_id)`` before the parent marks
itself modified. The later processing of completed modified elements
belongs to the surrounding ``TSValue`` / state layer. TSData does not
own external subscriber lists or graph scheduling fan-out.

The implemented atomic plan follows the compact layout above.
Collection-shaped TSData will use the slot-oriented layout below:
``TSS`` tracks membership deltas with per-slot bitsets for ``added`` and
``removed``, while ``TSD`` reuses the same key side and adds a per-slot
``modified`` bitset for child values that changed in the current engine
time. The collection owns only its collection-level
``last_modified_time``; keyed value modification times are read from
the child time-series values. When the root mutation coordinator starts
an outermost mutation for an engine time later than the collection's
``last_modified_time``, the collection resets any retained delta
bitsets before accepting new changes.

Slot-Oriented Collection TSData
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Collection TSData uses stable slot ids so child addresses and binding
paths do not change when the collection grows or when other keys are
removed. A slot id indexes parallel structures: key or child payload,
live/constructed state, and delta masks.

.. mermaid::

   flowchart TD
      Slot["stable slot id"]
      KeyPayload["key payload<br/>or positional child"]
      ChildPayload["child TSData / TSValue<br/>where the shape has children"]
      LiveState["key constructed/live state"]
      DeltaMasks["delta masks<br/>added / removed / modified"]
      Path["binding path component"]

      Slot --> KeyPayload
      Slot --> ChildPayload
      Slot --> LiveState
      Slot --> DeltaMasks
      Slot --> Path

For a TSS, the key store owns the scalar key payload and membership
state. Added and removed deltas are bitsets indexed by the same slot
ids:

.. code-block:: text

   TSS SlotTSDataStorage
   +--------------------------------------------------------------+
   | collection tracking                                          |
   | last_modified_time                                           |
   +--------------------------------------------------------------+
   | KeySlotStore                                                 |
   |                                                              |
   | slot id        0        1        2        3        ...       |
   | key bytes    [K0]     [K1]     [K2]     [K3]       ...       |
   | constructed   1        1        1        0         ...       |
   | live          1        0        1        0         ...       |
   +--------------------------------------------------------------+
   | delta bitsets for current evaluation time                    |
   | added       [bit0]   [bit1]   [bit2]   [bit3]     ...       |
   | removed     [bit0]   [bit1]   [bit2]   [bit3]     ...       |
   +--------------------------------------------------------------+

``constructed && !live`` represents a pending-erase slot. The payload
is still inspectable for the tick in which it was removed, and the
slot can be erased later without invalidating any other slot address.

For a TSD, the key side is TSS-shaped and the value side is parallel
payload storage indexed by the same slot ids. The key store's
``constructed`` bit is authoritative for both key and value payload
lifetime: a TSD must construct the child time-series value with the key
and destroy it when the key slot is physically erased. There is no
independent value-side constructed state in the TSData layout:

.. code-block:: text

   TSD SlotTSDataStorage
   +--------------------------------------------------------------+
   | collection tracking                                          |
   | last_modified_time                                           |
   +--------------------------------------------------------------+
   | KeySlotStore                                                 |
   | slot id        0        1        2        3        ...       |
   | key bytes    [K0]     [K1]     [K2]     [K3]       ...       |
   | constructed   1        1        1        0         ...       |
   | live          1        0        1        0         ...       |
   +--------------------------------------------------------------+
   | Value payload slots                                          |
   | slot id        0        1        2        3        ...       |
   | child TS     [V0]     [V1]     [V2]     [--]       ...       |
   | lifetime      follows KeySlotStore.constructed               |
   +--------------------------------------------------------------+
   | delta bitsets                                                |
   | added       [bit0]   [bit1]   [bit2]   [bit3]     ...       |
   | removed     [bit0]   [bit1]   [bit2]   [bit3]     ...       |
   | modified    [bit0]   [bit1]   [bit2]   [bit3]     ...       |
   +--------------------------------------------------------------+

The ``modified`` bitset records which child slots reported a
modification during the current engine time. The child's own
``last_modified_time`` still lives with the child time-series value;
the parent bitset is the parent's delta surface, not a duplicate
timestamp store.

The bubble-up path uses the slot id carried by the child view:

.. mermaid::

   flowchart TD
      ChildMutation["child TSDataMutationView<br/>slot s"]
      Mark["mark_modified() / copy_value_from()"]
      ParentHook["parent ops<br/>record_child_modified(parent_data, s)"]
      ModifiedBit["parent modified bitset[s] = 1"]
      ParentTime["parent last_modified_time<br/>= current engine time"]
      Bubble["repeat with parent's parent"]

      ChildMutation --> Mark
      Mark --> ParentHook
      ParentHook --> ModifiedBit
      ModifiedBit --> ParentTime
      ParentTime --> Bubble

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
per-slot bitsets needed to surface deltas. The value-layer
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
    Standalone parallel value memory keyed off externally supplied slot
    ids. As a reusable utility it owns a per-slot ``constructed`` bit
    so it can be used independently and still destroy its payloads
    correctly. A TSD-specific value side should not treat that bit as a
    second source of truth: TSD key construction and value construction
    happen together, so ``KeySlotStore.constructed`` is authoritative
    and any reused value-store constructed bit is only a derived mirror.

``KeyMirroredValueSlotStore``
    Wrapper for keyed value storage that enforces the TSD-style
    lifetime rule in code. It registers as a ``KeySlotStore`` observer,
    constructs value payloads when key slots are constructed, keeps
    pending-erase value payloads alive while the key slot remains
    constructed, and destroys value payloads only when the key slot is
    physically erased or cleared. Its public ``has_slot`` answer is
    derived from ``KeySlotStore.constructed``.

Both stores expose a ``SlotObserver`` notification protocol —
``on_capacity``, ``on_insert``, ``on_remove``, ``on_erase``,
``on_clear`` — so parallel structures over the same slot ids stay
synchronised without any of them needing to know about the others.
The TS layer uses this for two purposes: a ``MapValueObserver``
mirrors a key store's slot lifecycle onto a paired value store
(``TSD`` keys → values); and delta-recording observers capture
``TSS`` ``added`` / ``removed`` slot ids plus ``TSD`` ``modified``
slot ids for the current engine time so the layer can publish
``delta_value``.

These structural slot observers are internal synchronisation hooks, not
the public change-notification surface. Per-level change propagation
uses the ``TSDataParentLink`` constructed into child views plus the
``record_child_modified`` hook on the parent ops table. The parent link
owns the child id because that id is a parent-local slot/path
identifier. The slot hooks may update bitsets immediately during
mutation; processing of modified elements and external notification
fan-out belongs to the surrounding state/value layer after the
value-level mutation count returns to zero.

The set and map ``SlotTSDataStorage`` shapes are layered on these
primitives. A TS set data store owns one ``KeySlotStore``. A TS map
data store owns one ``KeySlotStore`` for keys plus value payload
storage indexed by the key slots. If the generic ``ValueSlotStore`` is
reused for that value side, use ``KeyMirroredValueSlotStore`` or the
same rule internally: its constructed bitmap must be kept as a strict
mirror of the key store's constructed bitmap rather than a separate
state surface.

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
- collection-level tracking, including ``last_modified_time``;
- per-slot ``added`` and ``removed`` bitsets that drive
  ``delta_value``.

The slot ids assigned by the ``KeySlotStore`` are the path identifiers
used throughout the rest of the time-series layer. A TSS has no child
time-series values, so key-level modification time is not a concept on
this storage shape.

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
- value payload storage whose slot ids match the key store's, holding
  the per-key time-series values and deriving payload lifetime from
  ``KeySlotStore.constructed``;
- collection-level tracking, including ``last_modified_time``;
- the TSS-shaped per-slot ``added`` and ``removed`` key bitsets;
- a per-slot ``modified`` bitset for keys whose child time-series
  value modified in the current engine time.

The value side is itself a recursive time-series layer: each value-
slot holds a complete time-series value (most often a ``TS``, but
``TSB``, ``TSL``, or further nested ``TSD`` are all permitted by the
schema). Memory stability is preserved by the underlying
``StableSlotStorage`` so consumers can bind to a specific slot's value
without worrying about future structural changes.

Per-key modification time is read from the child value stored in the
matching value slot. The TSD-level ``modified`` bitset is the current
delta membership surface; it is not the source of the child's
``last_modified_time``.

Key-Set Exposure
~~~~~~~~~~~~~~~~

A TSD exposes its keys as a TSS through ``key_set()``. The returned
TSS is **read-only**:

- it shares the parent TSD's ``KeySlotStore`` directly, so slot ids
  match one-to-one with the parent's value side;
- it shares the parent TSD's key ``added`` / ``removed`` tracking;
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
live values, and over the per-slot delta masks for the current tick:
``added`` / ``removed`` for TSS-shaped key storage and ``modified`` for
TSD value changes. This matters for adaptors and analytics paths that
consume large keyed time-series in bulk.
