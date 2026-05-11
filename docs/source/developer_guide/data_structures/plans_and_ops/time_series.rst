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
    the literal ``allows_mutation`` property, common layout access,
    read/write memory access, delta reset, copy, and the per-kind hook
    used when a child time-series value reports that it modified. The
    table is deliberately passive; generic mutation sequencing and
    propagation rules live on ``TSDataView`` /
    ``TSDataMutationView``. For a real bound ``TSData``
    implementation the table is total: required entries are never null,
    empty optional behaviours use no-op thunks, and unsupported
    operations are explicit throwing thunks rather than missing
    pointers.

``TSDataLayout``
    The common layout prefix for every TSData kind. It records only the
    offsets/bindings required at the "this is a time-series payload"
    layer: current value, delta value, and local tracking. It does not
    describe every possible collection shape.

``FixedTSBDataLayout`` / ``FixedTSLDataLayout``
    Specialised layouts for fixed structured TSData. ``TSB`` keeps
    per-field entries because each field can have its own schema and
    offset. Fixed ``TSL`` keeps the element count and value/auxiliary
    strides because every element has the same schema and fixed span.
    Other major TSData families use their own specialised layouts rather
    than adding more shape fields to ``TSDataLayout``.

``TSWDataLayout`` / ``SizeTSWDataLayout`` / ``TimeTSWDataLayout``
    Specialised layouts for window TSData. ``TSWDataLayout`` is only
    the common window prefix: payload element binding, timestamp
    element binding, plus the normal value, delta, and tracking offsets
    inherited from ``TSDataLayout``.
    ``SizeTSWDataLayout`` carries ``period`` and ``min_period`` for
    tick-count windows. ``TimeTSWDataLayout`` carries ``time_range`` and
    ``min_time_range`` for duration windows. A concrete ``TSW`` schema
    resolves to exactly one of those layouts; it never switches between
    the two models at runtime.

``IndexedTSDataOps``
    The shared view-facing indexed access surface for TSData
    shapes. ``TSB`` and ``TSL`` can expose common indexed operations
    such as ``size()``, ``at(index)``, and value/item iteration while
    still using different concrete storage ops. Fixed ``TSL`` and
    dynamic ``TSL`` therefore share ``TSLDataView`` but do not have to
    share the same layout or mutation implementation.

``TSDataBinding``
    The interned binding for a ``TSData`` implementation: the
    ``TSValueTypeMetaData`` schema, the data ``StoragePlan``, and the
    ``TSDataOps`` table. The schema is the time-series schema rather
    than the scalar value schema because delta shape and mutation
    behaviour depend on the time-series kind.

``TSDataPlanFactory``
    The schema → data-plan resolver for ``TSData``. It chooses the
    compact mutable implementation for atomic time-series data, fixed
    structured plans for ``TSB`` / fixed ``TSL``, window plans for
    ``TSW``, and slot-oriented plans for keyed or dynamically-sized
    collection-shaped time-series data. The factory does not plan the
    whole ``TSValue`` object, only its payload/delta data component.

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
    in place during evaluation. Current value bytes and tracking stamps
    are separate memory regions in one TSData plan. The atomic delta
    view aliases the current value when ``last_modified_time`` matches
    the evaluation time, so no duplicate scalar delta payload is
    allocated.

``FixedStructuredTSDataStorage``
    Used for fixed-shape structured time-series data: ``TSB`` and
    fixed-size ``TSL``. The parent plan allocates the complete current
    value as one canonical value-layer region, followed by an auxiliary
    tree containing child and parent tracking. The full memory footprint
    of the parent and all fixed children is known before allocation, and
    child views use embedded TSData bindings whose offsets point into the
    shared value and auxiliary regions.

``WindowTSDataStorage``
    Used for ``TSW``. It exposes one common ``TSWDataView`` surface but
    has two concrete storage models: a fixed-capacity cyclic buffer for
    tick-count windows and a timestamped queue for duration windows.
    ``SizeTSWindowStorage`` and ``TimeTSWindowStorage`` share only the
    low-level timestamp/payload buffer management; push, pruning, and
    capacity policy are selected by the concrete storage type. Both
    models store two aligned value-element buffers: an ``engine_time_t``
    timestamp buffer and a payload ``T`` buffer. The current-value
    surface is list-shaped over the payload buffer, but the bound value
    ops project directly over the window storage instead of
    materialising a compact immutable value-layer ``ListStorage``.

``SlotTSDataStorage``
    Used for keyed or dynamically-sized collection time-series data
    such as ``TSS`` and ``TSD``. Dynamic ``TSL`` will use the same
    family when its dynamic storage is implemented. The data store is
    slot-oriented: every child or element has a stable slot id and the
    current payload, validity, and delta information are aligned by
    that slot id. Collection mutation changes slot state instead of
    compacting or relocating already-published child addresses.

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

Fixed Structured TSData
^^^^^^^^^^^^^^^^^^^^^^^

``TSB`` and fixed-size ``TSL`` use recursive fixed layouts. The parent
storage plan starts with one canonical value-layer region for the full
current value, followed by an auxiliary tracking tree. This keeps all
current value bytes collected together under the same recursive layout
that the value layer uses. For example ``TSL[TS[int], Size[2]]`` stores
its current values as the exact fixed ``List[int, 2]`` value plan, so
the value region is suitable for buffer-oriented access.

.. code-block:: text

   Fixed structured TSData allocation
   +--------------------------------------------------------------+
   | value region                                                 |
   | canonical ValuePlanFactory plan for TSData.value_schema      |
   | e.g. TSL[TS[int], Size[2]] -> fixed List[int, 2]             |
   +--------------------------------------------------------------+
   | auxiliary region                                             |
   | TSB: field_0/field_1/... auxiliary tracking trees            |
   | TSL: elements auxiliary array with fixed element stride       |
   | parent TSDataTracking { last_modified_time }                 |
   +--------------------------------------------------------------+

For ``TSL[TS[int], Size[2]]`` the current-value part is therefore:

.. code-block:: text

   value region
   +-------------------------+
   | int[0] | int[1]         |
   +-------------------------+
   | stride == sizeof(int)   |
   +-------------------------+

and the tracking side is separate:

.. code-block:: text

   auxiliary region
   +--------------------------------------------------------------+
   | elements[0] tracking | elements[1] tracking                  |
   | parent tracking                                              |
   +--------------------------------------------------------------+

The concrete layouts reflect that difference. ``FixedTSBDataLayout``
keeps per-field entries because each field may have a different TSData
schema and offset. ``FixedTSLDataLayout`` stores the element count plus
the current-value and auxiliary strides; element offsets are computed as
``base + index * stride``.

For nested fixed structures the same rule is applied recursively inside
the value region. A ``TSB`` field that is a fixed ``TSL`` points into a
bundle field whose payload is the fixed list value plan; the child and
grandchild TSData views use embedded bindings with offsets into the
shared value and auxiliary regions.

.. mermaid::

   flowchart LR
      Root["root TSData allocation"]
      Value["value region<br/>Bundle/List value plan"]
      Aux["auxiliary region<br/>child tracking trees + parent tracking"]
      ChildView["child TSDataView<br/>data = root base<br/>binding offsets select child value/tracking"]

      Root --> Value
      Root --> Aux
      ChildView --> Value
      ChildView --> Aux

Child storage is therefore prepared and default-constructed as part of
parent construction; no child region is allocated lazily on first
access.

The parent ``value()`` view is the canonical value-layer view over the
value region. ``TSB.value()`` exposes the bundle binding for the full
current value. Fixed ``TSL.value()`` exposes the fixed list binding for
the full current value. Fixed elements are reached through specialised
views using the standard time-series API names:
``as_bundle()`` for ``TSB`` and ``as_list()`` for ``TSL``. Generic
``TSDataView`` does not expose indexed traversal. Fixed and dynamic
``TSL`` may use different data ops and layouts, but callers use the
same ``TSLDataView`` surface for indexed list semantics.

.. code-block:: text

   fixed TSL current value memory
   +--------------------------------------------------------------+
   | root.value()                                                 |
   | ValueView{List[int, 2], value_offset}                        |
   +--------------------------------------------------------------+
   | as_list().at(0).value() -> element 0                         |
   | as_list().at(1).value() -> element 1                         |
   +--------------------------------------------------------------+

The parent ``delta_value(t)`` view is valid when the parent
``last_modified_time == t``. For ``TSB`` it exposes a bundle-shaped
delta where each field projects to the matching child's delta if that
child also modified at ``t``; unmodified fields are typed-null. For
fixed ``TSL`` it exposes the documented map-shaped delta
``Map<int64, child.delta>`` and iterates only child indices modified at
``t``.

Child views are constructed with a ``TSDataParentLink`` that records
the parent view and the field/index id. When a child modification is
first recorded for an engine time, the child bubbles that id to the
parent; the parent then records its own ``last_modified_time`` for the
same engine time.

Window TSData
^^^^^^^^^^^^^

``TSW<T>`` stores the current rolling window and exposes a scalar delta:
the element pushed at the current evaluation time. The value schema is
still list-shaped:

- tick-count windows expose ``List<T, period>``;
- duration windows expose ``List<T, 0>`` because the number of elements
  in the time range depends on tick rate.

The TSData plan has a window storage component plus the common tracking
stamp. There is no separate delta region. ``delta_value(t)`` returns the
latest element in the window when ``last_modified_time == t``; otherwise
it returns a typed-null scalar view.

.. code-block:: text

   TSW TSData allocation
   +--------------------------------------------------------------+
   | window region                                                |
   | SizeTSWindowStorage or TimeTSWindowStorage                   |
   | - fixed tick: cyclic timestamp/value buffers                 |
   | - duration: timestamp/value queue buffers                    |
   | - timestamps are engine_time_t value elements                |
   | - payload values are T value elements                        |
   +--------------------------------------------------------------+
   | tracking region                                              |
   | TSDataTracking { last_modified_time }                        |
   +--------------------------------------------------------------+

Both models share ``TSWDataView``. The view reports elements in logical
oldest-to-newest order and exposes the same operations for size, indexed
value access, timestamps, timestamp value access, first/last element,
readiness, and mutation. ``time_at(index)`` returns the raw
``engine_time_t`` and ``time_value_at(index)`` returns the same stored
timestamp as a ``ValueView`` backed by the timestamp element buffer.
The layout and ops behind the view differ by schema:

.. mermaid::

   flowchart LR
      View["TSWDataView"]
      FixedOps["tick-count ops<br/>SizeTSWDataLayout<br/>fixed cyclic buffer"]
      DurationOps["duration ops<br/>TimeTSWDataLayout<br/>timestamped queue"]
      Value["value()<br/>list-shaped ValueView"]
      Delta["delta_value(t)<br/>latest element if modified at t"]

      View --> FixedOps
      View --> DurationOps
      View --> Value
      View --> Delta

For a tick-count ``TSW<T, period, min_period>``, the window is a
fixed-capacity cyclic buffer. Pushing appends while there is free
capacity and overwrites the oldest element after the period is reached.
The exposed order remains oldest-to-newest:

.. code-block:: text

   tick TSW, period = 3
   push 1 @ t1        [1]
   push 2 @ t2        [1, 2]
   push 3 @ t3        [1, 2, 3]
   push 4 @ t4        [2, 3, 4]

``all_valid()`` for this model is ``size() >= min_period``. The
``value()`` view is bound to custom list ops over the window component,
so ``value().as_list()`` has the documented list schema while reading
directly from the cyclic storage.

For a duration ``TSW<T, time_range, min_time_range>``, the window is a
queue paired with per-element timestamps. Before each push, elements
older than ``evaluation_time - time_range`` are removed. The queue may
grow to match the number of ticks observed inside the time range:

.. code-block:: text

   duration TSW, time_range = 10us
   push 1 @ 1us       [1 @ 1us]
   push 2 @ 6us       [1 @ 1us, 2 @ 6us]
   push 3 @ 16us      [2 @ 6us, 3 @ 16us]

``all_valid()`` for this model is false while empty; once non-empty, a
zero ``min_time_range`` is immediately valid and a positive
``min_time_range`` requires ``last_element_time - first_element_time``
to cover that duration.

View Handles
^^^^^^^^^^^^

View objects are handles over TSData memory; they are not embedded
inside the TSData allocation. A plain data view needs the binding and
data pointer, and exposes only the common time-series operations.
Specialised views, for example ``TSBDataView`` and ``TSLDataView``,
wrap the plain view and provide kind-specific child access. A child
view additionally carries a
``TSDataParentLink``: the parent view reference plus the parent-owned
child id used for bubble-up:

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

The implemented atomic plan follows the compact layout above. ``TSB``
and fixed-size ``TSL`` use the fixed structured layout above. ``TSW``
uses the window layout above. ``TSS`` and ``TSD`` use the
slot-oriented layout below: ``TSS`` tracks membership deltas with
per-slot bitsets for ``added`` and ``removed``, while ``TSD`` reuses
the same key side and adds a per-slot ``modified`` bitset for child
values that changed in the current engine time. The collection owns
only its collection-level ``last_modified_time``; keyed value
modification times are read from the child time-series values. The
bitset delta surface is reset at the first collection mutation for a
new engine time. The implementation keeps a small internal
``delta_time`` marker for that reset decision; the public modification
answer still comes only from ``last_modified_time == evaluation_time``.

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

For fixed-shape time-series (``TS``, ``TSB``, and fixed-size ``TSL``),
stability is trivial: the value lives in node-owned storage and
survives until the owning node is destroyed. Tick-count ``TSW`` also
has a fixed-capacity window. Duration ``TSW`` keeps the owning TSData
object stable, but its internal queue may grow; callers should treat
element ``ValueView`` handles as short-lived projections rather than
stable child time-series addresses.

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

The C++ TSData API uses the standard set-view names:
``TSDataView::as_set()``
returns ``TSSDataView`` with ``size()``, ``empty()``, ``contains()``,
``find_slot()``, ``values()``, ``added_values()``, ``removed_values()``,
``slot_added()``, and ``slot_removed()``. ``TSSDataMutationView`` adds
``add()``, ``remove()``, ``clear()``, and ``reserve()``.

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

The C++ TSData API uses the standard dictionary-view names:
``TSDataView::as_dict()`` returns ``TSDDataView`` with keyed
``contains()``, ``find_slot()``, ``at(key)``, ``keys()``, ``values()``,
``items()``, ``valid_items()``, ``modified_items(evaluation_time)``,
and ``key_set()``. ``TSDDataMutationView`` adds ``set(key, value)``,
``erase(key)``, ``clear()``, and ``reserve()``. Child mutation bubbles
through the child view's parent link and records the parent's
``modified`` bit for the child slot.

Buffer Exposure
~~~~~~~~~~~~~~~

Because keys and values live in slot stores backed by stable
contiguous blocks, both TSS and the value side of TSD can expose
buffer views the same way the value layer does — over live keys, over
live values, and over the per-slot delta masks for the current tick:
``added`` / ``removed`` for TSS-shaped key storage and ``modified`` for
TSD value changes. This matters for adaptors and analytics paths that
consume large keyed time-series in bulk.
