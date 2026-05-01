Data Structures
===============

This page starts with the logical data structures used by the runtime. The goal is to name the layers and the elements that move through them before fixing the exact memory layout.

The first implementation should keep these structures simple and explicit. Once the relationships are stable, each element can be refined into arena storage, packed arrays, intrusive handles, or type-specialized storage where that provides measurable value.

Core Concepts
-------------

Every data structure in the runtime is built from the same six-element
vocabulary. Defining it once lets each layer below describe itself by naming
the concrete Plan, Schema, Ops, and Builder it uses, instead of restating the
pattern.

The vocabulary is grouped into three roles: a *concept* group that describes
what something is, a *resolution* group that picks an implementation for it,
and a *data* group that holds and exposes the actual instance.

Concept Group
~~~~~~~~~~~~~

``Plan``
    Memory-layout primitive. A Plan describes how to allocate, construct,
    destruct, and deallocate the memory for one data structure. It does not
    carry semantic meaning beyond memory lifecycle.

``Schema``
    Independent, generic description of a concept. A Schema describes what a
    time-series, node, or higher-level structure *is*. It is not tied to any
    particular memory layout or behaviour, so multiple implementations of the
    same concept share one Schema.

``Ops``
    Struct of function pointers that exposes behaviour for a data structure.
    The first parameter of every operation is always a pointer to the memory
    representing the structure; remaining parameters follow as needed. An
    Ops table is the type-erasure vehicle for one implementation of a
    concept.

Plan, Schema, and Ops are plain C++ structs and are immutable once
constructed.

Resolution Group
~~~~~~~~~~~~~~~~

``Builder``
    The only place a Schema is bound to a specific Plan and Ops instance.
    A Builder resolves a Schema into a concrete implementation, then
    constructs Value instances. A Builder may itself be type-erased so that
    generic graph-construction code can hold builders for unrelated concepts
    uniformly.

Anything that needs a Value for a given Schema goes through a Builder.

Data Group
~~~~~~~~~~

``Value``
    Holds the data described by a Plan. By definition, Values are
    type-erased storage. A Value carries only the minimum behaviour needed
    to live in a container: deallocation, copy and move construction and
    assignment, equality, and hash. Reads, writes, comparisons over content,
    and iteration are exposed through a View, never directly on the Value.

``View``
    Constructed from an existing Value. A View references the Value's
    memory and pairs it with the corresponding Ops table so that behaviour
    can be invoked. A View carries no payload of its own; it is a
    lightweight reference.

Interning
~~~~~~~~~

Plans, Schemas, Ops tables, and Builders are *interned*: there is only ever
one true instance of each, kept alive by an intern table that returns stable
references for structurally equivalent inputs. Any consumer—a View, a
Builder, a node, a graph—holds a borrowed pointer to its Plan, Schema, Ops,
or Builder for the artifact's whole lifetime without managing ownership.

The generic vehicle is ``InternTable<Key, Value>``, which guarantees stable
addresses for the values it owns.

Values are *not* interned. Each instance is independent.

The Universal Pattern
~~~~~~~~~~~~~~~~~~~~~

Every layer below instantiates the same shape:

.. mermaid::

   flowchart LR
      Schema[Schema<br/>concept]
      Plan[Plan<br/>memory layout]
      Ops[Ops<br/>function pointers]
      Builder[Builder<br/>resolves Schema with chosen Plan + Ops]
      Value[Value<br/>holds data]
      View[View<br/>exposes behaviour]

      Schema --> Builder
      Plan --> Builder
      Ops --> Builder
      Builder -->|constructs| Value
      Value -.referenced by.-> View
      Ops -.bound into.-> View

A Builder is fed by a Schema (what it is), a Plan (how its memory is laid
out), and an Ops table (how it behaves). The Builder constructs Values.
A View is built from a Value and pulls in the same Ops table so that
callers can act on the Value's data.

Subsequent sections name the concrete Schema, Plan, Ops, and Builder used
for each layer rather than re-introduce the pattern.

Structural Layers
-----------------

The runtime has five major structural layers:

``GraphExecutor``
    Owns or coordinates a complete graph run. It constructs the runtime graph, installs observers, starts and stops lifecycle processing, and drives the evaluation loop.

``EvaluationEngine``
    Provides the clock, run mode, lifecycle observer dispatch, one-shot evaluation callbacks, stop state, and optional push-message receiver state. Conceptually, this is the engine that evaluates a graph.

``Graph``
    Holds the flattened runtime node set, the graph schedule table, graph identity, boundary bindings, push-source partition, and links to parent or nested graph context.

``Node``
    Holds node identity, runtime operations, lifecycle state, time-series inputs and outputs, optional node-local scheduler, and optional node-local state.

``TimeSeries``
    Holds value validity, modification time, parent/child relationships, subscribers, structural delta state, and type-specific value storage.

The high-level relationship is:

.. mermaid::

   flowchart TD
      Executor[GraphExecutor]
      Engine[EvaluationEngine]
      Clock[EvaluationClock]
      Observers[Lifecycle Observers]
      Callbacks[Before/After Callback Queues]
      Receiver[Push Message Receiver]
      Graph[Graph]
      Schedule[Graph Schedule Table]
      Nodes[Flattened Node Array]
      Boundaries[Graph Boundaries]
      Node[Node]
      NodeScheduler[Optional Node Scheduler]
      Inputs[Inputs]
      Outputs[Outputs]
      State[Node State]
      TS[TimeSeries Tree]
      Value[Value Storage]

      Executor --> Engine
      Executor --> Graph
      Engine --> Clock
      Engine --> Observers
      Engine --> Callbacks
      Engine --> Receiver
      Graph --> Schedule
      Graph --> Nodes
      Graph --> Boundaries
      Nodes --> Node
      Node --> NodeScheduler
      Node --> Inputs
      Node --> Outputs
      Node --> State
      Inputs --> TS
      Outputs --> TS
      TS --> Value
      Receiver -.push source node index + message.-> Graph
      NodeScheduler -.earliest event.-> Schedule
      Schedule -.scheduled time + node index.-> Nodes

Execution Layer
---------------

The execution layer contains run-level state. It is not the graph topology itself, but it controls how graph topology is evaluated.

Core elements:

``EvaluationEngine``
    Stores ``start_time``, ``end_time``, run mode, stop-request state, the engine clock, lifecycle observers, and one-shot callback queues.

``EvaluationClock``
    Stores ``evaluation_time`` and the next scheduled evaluation time. Real-time clocks additionally store wall-clock alarm state and push-source wake-up state.

``LifecycleObserverList``
    Ordered list of observer handles. Observers receive lifecycle events but do not own graph runtime state.

``EvaluationCallbackQueues``
    One-shot before-evaluation and after-evaluation callback queues. These queues are drained until complete.

``PushMessageReceiver``
    Queue of external messages addressed to push-source nodes. Each queued item is logically ``(node_index, message)``.

Graph Layer
-----------

The graph layer is the hot path for evaluation. It should be compact and directly indexed.

Core elements:

``GraphIdentity``
    Stable graph id, optional label, and optional parent-node link for nested graphs.

``FlattenedNodeArray``
    The fully ordered node set. The array order is rank order and is the evaluation order.

``GraphScheduleTable``
    Parallel array indexed by node index. Each entry is the next visible scheduled time for that node.

``PushSourcePartition``
    Index boundary that separates push-source nodes from the normal rank-ordered scan. Push-source nodes occupy the prefix ``[0, push_source_nodes_end)``.

``BoundaryBindings``
    Runtime bindings for graph inputs, outputs, nested graph boundaries, and parent/child graph communication.

``GraphStorage``
    Memory owned by the graph instance. This may eventually be an arena or packed allocation that contains nodes, node-local schedulers, node-local state, and time-series objects.

The graph's most important paired structure is:

.. code-block:: text

   FlattenedNodeArray[index] -> Node
   GraphScheduleTable[index] -> scheduled_time

Together these form the scheduled time-node relationship used by evaluation. The schedule table does not own scheduler events; it only exposes the next time each node may need to run.

Node Layer
----------

Each node is a runtime object in the flattened graph. The node object should be small enough to scan efficiently and should point to heavier state only when needed.

Core elements:

``NodeHeader``
    Node index, rank position, owning graph pointer, lifecycle state, and schema/runtime metadata needed for dispatch.

``RuntimeOps``
    Function table or equivalent dispatch object for start, stop, evaluate, dispose, and error handling.

``InputSet``
    Ordered inputs used by readiness checks and evaluation dispatch. Inputs may be active subscribers, passive samples, or context inputs.

``OutputSet``
    Ordered outputs produced by the node. Outputs propagate modification notifications to subscribers.

``NodeState``
    Optional node-local state. This is present only for nodes whose schema or implementation requires state.

``NodeScheduler``
    Optional node-local scheduler. This is present only when the schema declares that the node requires scheduling.

``ErrorOutput``
    Optional output used to capture evaluation errors when the schema provides one.

The optional elements are important. A node that does not require a scheduler must not allocate scheduler state. A stateless node must not pay for state storage.

Scheduling Structures
---------------------

Scheduling uses two layers:

``GraphScheduleEntry``
    A single ``scheduled_time`` stored in the graph schedule table at ``node_index``.

``NodeScheduledEvent``
    A node-local event containing ``scheduled_time`` and optional ``tag``. Real-time scheduling may also be backed by a wall-clock alarm handle.

``NodeScheduler``
    A per-node structure that stores multiple ``NodeScheduledEvent`` values, supports tag replacement and cancellation, and exposes the earliest pending event to the graph schedule table.

The intended relationship is:

.. mermaid::

   flowchart LR
      Event1[NodeScheduledEvent time=t1 tag=a]
      Event2[NodeScheduledEvent time=t2 tag=b]
      Event3[NodeScheduledEvent time=t3]
      NodeScheduler[NodeScheduler]
      GraphEntry[GraphScheduleTable node_index = min time]
      Clock[EvaluationClock next scheduled time]

      Event1 --> NodeScheduler
      Event2 --> NodeScheduler
      Event3 --> NodeScheduler
      NodeScheduler --> GraphEntry
      GraphEntry --> Clock

The node scheduler owns the complete set of pending events. The graph schedule table owns only the next visible time for the node.

Time-Series Layer
-----------------

The time-series layer wraps value-layer storage with the additional state
every output and input needs to participate in graph evaluation: modification
time, subscribers, binding, delta tracking, and the path identity that makes
output-to-input wiring possible.

This section currently focuses on the foundational concepts that make TSS
and TSD work — memory stability, path-from-root addressing, and the slot
abstraction. Other kinds (TS, TSB, TSL, TSW, REF) reuse the same
primitives and will be elaborated as the implementation settles.

Time-Series Kinds
~~~~~~~~~~~~~~~~~

``TS[T]``
    Scalar time-series of one atomic type.

``TSB``
    Named bundle of time-series fields.

``TSL``
    Ordered list of one time-series type, fixed-size or dynamic.

``TSS``
    Unordered set of one scalar type.

``TSD``
    Keyed dictionary with scalar keys and time-series values.

``TSW``
    Sliding window of one scalar type.

``REF``
    Reference to a time-series target. The target is resolved through
    binding state rather than carried inline.

Tick Semantics
~~~~~~~~~~~~~~

Every time-series, regardless of kind, exposes the same five tick-level
properties. These are the universal time-series contract. Inputs and
outputs both expose them; on an input, the values reflect the bound
output. The kind-specific view APIs described later add
container-specific access on top of this base, but the base is always
present.

``value``
    The cumulative current state of the time-series. For a ``TS[T]`` this
    is the scalar that has been written; for a ``TSB`` it is the bundle
    of all currently valid fields; for a ``TSD`` it is the map of all
    live keys to their current values. ``value`` persists across ticks
    until it is replaced or extended.

``delta_value``
    What changed in the current tick. ``delta_value`` is ``None`` when
    the time-series did not tick in the current engine cycle. When it
    did tick, the shape depends on the kind:

    - ``TS[T]`` — ticks are atomic value replacements with no diffing,
      so ``delta_value`` is the new scalar (the same object as
      ``value``). Setting a ``TS[T]`` to its current value still
      produces a delta.
    - ``TSB`` — modified fields only.
    - ``TSL`` — modified elements with their indices.
    - ``TSS`` — added and removed sets.
    - ``TSD`` — added entries, removed keys, and per-key modified values.
    - ``TSW`` — the element added in the current tick (if any).

    ``delta_value`` is logically present only for the engine cycle of
    the change. The runtime is not required to physically reset it at
    the end of the cycle, and in fact prefers lazy cleanup: removed
    ``TSS`` and ``TSD`` slot entries live past their logical destruction
    so the current cycle's delta can be read, and physical erase happens
    later at the next outermost ``begin_mutation()``. A subsequent
    engine cycle querying ``delta_value`` returns ``None`` if no new
    change has occurred, or the new change if one has.

``modified``
    Whether the time-series ticked in the current evaluation cycle. For
    container kinds this is recursive: a container is modified if any
    of its elements is modified. Equivalent to
    ``last_modified_time == evaluation_time``. Must be an O(1) query —
    consumers test it on every active input on every tick, so it is
    read directly from per-TS state, never derived by scanning children.

``valid``
    Whether the time-series has ever been ticked. Equivalent to
    ``last_modified_time != MIN_DT`` (``MIN_DT`` is the sentinel for
    never-modified). Note that ``valid`` does *not* imply the
    time-series holds any content: a ``TSS`` or ``TSD`` that ticked
    with no members is valid but empty. Also an O(1) query.

``last_modified_time``
    The engine time at which the time-series was last modified. Stored
    once per time-series instance in the per-TS runtime state tree.
    Defaults to ``MIN_DT`` until the first tick.

Per-Element Modification
^^^^^^^^^^^^^^^^^^^^^^^^

For composite kinds, a consumer often needs to ask not just *did this
container tick?* but *which children ticked?*. The answer comes from
each child's own time-series state: every time-series instance — at
every level of nesting — carries its own ``last_modified_time`` as
part of the per-TS runtime state tree. A collection like TSD holds a
vector of slots where each slot contains the full time-series state of
that element, including the child's ``last_modified_time``. Walking
the slots and inspecting each child's ``last_modified_time`` is how
the container reports which elements changed in the current cycle —
there is no separate side structure tracking this.

The container's own ``last_modified_time`` is updated whenever any
child ticks, which is what makes the recursive container-level
``modified`` query a single field read rather than a tree walk. A
slot id is therefore both a path identifier and the key into the
child's full time-series state.

Memory Stability Invariant
~~~~~~~~~~~~~~~~~~~~~~~~~~

Every time-series value in the runtime must be memory-stable. Once an
output's value and its ops table are published, their addresses must not
move for the lifetime of the owning node. Output-to-input binding is
implemented by recording pointers — to the value, to the ops, to
per-element state — and those pointers must remain valid across ticks,
rebinds, and structural mutation of containers.

For fixed-shape time-series (TS, TSB, fixed-size TSL, TSW), stability is
trivial: the value lives in node-owned storage and survives until the
owning node is destroyed.

For TSD and dynamic TSL, stability is harder. Elements are added and
removed during evaluation, but a consumer that bound to one of them on
the previous tick must still be able to dereference it on the current
tick. Compacting storage cannot be used. The runtime instead uses
chained, non-relocating slot blocks: new capacity is appended without
moving previously published slot addresses.

Path Construction and the Slot Concept
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Every binding carries a path that locates the target value within its
owning graph. For indexable kinds — TSB, TSL — the path is a sequence
of ``size_t`` indices (field index, element index) and addressing is
direct.

TSD breaks this because keys can be of any scalar type. Carrying
arbitrary keys in paths would be expensive (variable-width encoding,
type-aware comparison at every step) and would couple every path
traversal to key semantics.

The runtime resolves this by introducing the **slot**: a stable
non-negative integer that names an element of a keyed container without
referencing the key itself. A slot is allocated when a key is first
inserted, remains allocated while the key is live, and persists through
delayed-erase windows so consumers can still inspect a removed element
on the tick of its removal. With slots, paths into TSD become
``(slot_id)`` — exactly the shape of paths into TSL — and the runtime
can treat all keyed containers uniformly.

Slots originate in TSS rather than TSD. The reason is the TSS/TSD
relationship described below: a TSD exposes its keys as a TSS, and that
TSS must use the same slot ids as the parent TSD. Putting slots at the
TSS layer first lets TSD's value side reuse them directly.

The Slot Store Family
~~~~~~~~~~~~~~~~~~~~~

Three primitives in ``v2/types/utils/`` express the slot machinery:

``StableSlotStorage``
    Non-relocating, double-indexed slot storage. ``slots`` is a logical
    slot-id → payload-pointer table; ``blocks`` owns the chained heap
    allocations behind those pointers. Growth appends a block; previously
    issued slot pointers never move.

``KeySlotStore``
    Stable slot-backed key storage with delayed-erase semantics. Owns
    homogeneous keys keyed off a ``StoragePlan`` and a small ops vtable
    (``hash``, ``equal``). Maintains two parallel bitmaps:

    - ``constructed[slot]`` — payload object exists in slot memory
    - ``live[slot]`` — payload is currently a member of the set

    A slot in ``constructed && !live`` is *pending erase*: still
    addressable and inspectable until the next outermost
    ``begin_mutation()`` or an explicit ``erase_pending()`` flush. This
    is what lets a consumer that bound on the previous tick inspect the
    slot's last value during the tick of its removal.

``ValueSlotStore``
    Parallel value memory keyed off the same slot ids as a
    ``KeySlotStore``. The owning store decides which slot ids are live;
    ``ValueSlotStore`` owns the value payload, the per-slot
    ``constructed`` bit, and a per-slot ``updated`` bit used to drive the
    current mutation epoch.

Both stores expose a ``SlotObserver`` notification protocol —
``on_capacity``, ``on_insert``, ``on_remove``, ``on_erase``,
``on_clear`` — so parallel structures over the same slot ids stay
synchronised without any of them needing to know about the others.

The Set and Map shapes used by the value layer's delta-tracking
implementations are layered on these primitives. A delta-tracking Set
owns one ``KeySlotStore``. A delta-tracking Map owns one ``KeySlotStore``
for keys plus one ``ValueSlotStore`` for values, with the value store
registered as a slot observer on the key store.

TSS
~~~

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

TSD
~~~

TSD is the time-series wrapper around a delta-tracking Map. It owns:

- a ``KeySlotStore`` for the keys, identical in shape to a TSS's;
- a ``ValueSlotStore`` whose slot ids match the key store's, holding
  the per-key time-series values;
- the time-series state common to every kind.

The value side is itself a recursive time-series layer: each value-slot
holds a complete time-series value (most often a ``TS[T]``, but ``TSB``,
``TSL``, or further nested ``TSD`` are all permitted by the schema).
Memory stability is preserved by the underlying ``StableSlotStorage`` so
consumers can bind to a specific slot's value without worrying about
future structural changes.

Key-Set Exposure
^^^^^^^^^^^^^^^^

A TSD exposes its keys as a TSS through ``key_set()``. The returned TSS
is **read-only**:

- it shares the parent TSD's ``KeySlotStore`` directly, so slot ids
  match one-to-one with the parent's value side;
- it can be subscribed to and exposes ``value`` and ``delta_value`` like
  any other TSS;
- it rejects write operations — keys are owned by the TSD and only
  change through the TSD's mutable view.

This is the value-layer Map → read-only Set view (described in the
Value Layer) lifted into the time-series layer. The difference is that
the time-series version carries modification time and a delta stream,
not just structural read access.

Buffer Exposure
^^^^^^^^^^^^^^^

Because keys and values live in slot stores backed by stable contiguous
blocks, both TSS and the value side of TSD can expose buffer views the
same way the value layer does — over live keys, over live values, and
over the per-slot ``updated`` mask for the current tick. This matters
for adaptors and analytics paths that consume large keyed time-series
in bulk.

Value Layer
-----------

The value layer stores non-time-series payloads. It is the simplest concrete
instantiation of the Core Concepts pattern: each role in the Plan / Schema /
Ops / Builder / Value / View vocabulary maps onto one named runtime element.

Mapping to Core Concepts
~~~~~~~~~~~~~~~~~~~~~~~~

================  ============================================================
Concept role      Value-layer name
================  ============================================================
Schema            ``ValueTypeMetaData`` — kind, size, alignment, fields,
                  element/key types, capability flags
Plan              ``MemoryUtils::StoragePlan`` — memory layout plus a
                  ``LifecycleOps`` table (alloc, construct, destruct, dealloc)
Ops               ``ValueOps`` — ``hash``, ``equals``, ``compare``,
                  ``to_string`` function pointers
Binding           ``ValueTypeBinding`` — interned ``(ValueTypeMetaData,
                  StoragePlan, ValueOps)`` triple; the canonical handle the
                  rest of the layer shares
Builder           ``ValueBuilder`` — wraps a binding; constructs ``Value``
                  instances; cached in a ``ValueBuilderRegistry`` keyed by
                  schema
Value             ``Value`` — owning handle over storage + binding + allocator
View              ``ValueView`` — two-word reference: ``(binding, data)``.
                  Specialized adapters extend it for composite kinds.
================  ============================================================

Value Kinds
~~~~~~~~~~~

A Value has one of eight kinds, recorded on its ``ValueTypeMetaData``:

``Atomic``
    A single scalar: integer, floating-point, boolean, string, or one of the
    engine time/date scalars.

``Tuple``
    Fixed-arity ordered fields, accessed by index. Field types may differ.

``Bundle``
    Named tuple. Fields are accessed by name; field-name metadata is
    preserved in the storage plan.

``List``
    Ordered sequence of one element type. May be ``fixed_size`` or dynamic.

``Set``
    Unordered collection of unique elements of one type.

``Map``
    Key/value mapping with one key type and one value type.

``CyclicBuffer``
    Fixed-capacity ring buffer of one element type.

``Queue``
    FIFO queue with capacity and ordering.

Each kind has a corresponding specialized view: ``TupleView``, ``BundleView``,
``ListView``, ``SetView``, ``MapView``, ``CyclicBufferView``, ``QueueView``.
Specialized views are thin wrappers over the same two-word ``ValueView``
context; they expose kind-specific read access (``at``, ``contains``,
iteration). Mutation goes through the corresponding mutable view, described
under Read-Only and Mutable Views.

Multiple Implementations per Kind
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The reason every kind is exposed through a type-erased view rather than a
concrete C++ template is that one kind can have several implementations,
chosen by the caller's needs and bound at the value layer's resolution
step.

Pure-data callers — those that only need to store, hash, compare, and
serialise values — use **compact** implementations. A pure-data Set is a
hash-backed key store; a pure-data Map is a paired key/value slot store.
These minimise memory and skip bookkeeping the caller does not need.

Time-series callers — those that need to observe insertions, removals, and
modifications across ticks — use **delta-tracking** implementations. The
delta-tracking Set is the storage substrate for ``TSS``; the delta-tracking
Map is the substrate for ``TSD``. They carry per-slot observers and
pending-erase state so the time-series layer can produce coherent deltas.

Both implementations expose the same ``SetView`` / ``MapView`` shape. Code
that walks a Set or a Map through its view does not need to know which
implementation it is looking at. The choice is made by the schema's bound
``StoragePlan`` and ``ValueOps``; the view contract is unchanged.

This is the load-bearing reason the value layer is type-erased rather than
generic-templated: it lets the time-series layer reuse the value-layer
container vocabulary without forking the surface.

Binding and Interning
~~~~~~~~~~~~~~~~~~~~~

The binding is the shared anchor that lets the rest of the layer stay
lightweight:

.. code-block:: cpp

   template <typename TypeMeta, typename Ops>
   struct TypeBinding {
       const TypeMeta*                 type_meta;
       const MemoryUtils::StoragePlan* storage_plan;
       const Ops*                      ops;
   };

   using ValueTypeBinding = TypeBinding<ValueTypeMetaData, ValueOps>;

A ``Value``, ``ValueView``, or ``ValueBuilder`` stores one borrowed pointer
to its binding and reaches schema, plan, lifecycle, and ops uniformly
through it.

All binding and builder registries are thin wrappers over the generic
``InternTable<Key, Value>`` primitive. ``ValueTypeBinding`` is interned by
``(type, plan, ops)`` triple; ``ValueBuilder`` is interned by
``ValueTypeMetaData``. Each registry exists only to expose a typed
convenience API on top of one ``InternTable`` instance.

Composite schemas are populated lazily. The first request for a tuple,
list, map, or other container schema synthesises its plan from the
element/key bindings, interns the resulting binding, and stores the
builder.

Owning Value
~~~~~~~~~~~~

``Value`` is the owning handle:

- ``Value(const ValueTypeMetaData&)`` — constructs an owning value for the
  given schema, in a typed-null state. Schema is preserved even when the
  payload is absent.
- ``Value(T&&)`` — convenience for scalars; resolves to the canonical
  scalar binding via ``value::scalar_type_meta<T>()``.
- ``has_value()`` / ``reset()`` — manage top-level payload presence.
- ``view()`` — produces a ``ValueView`` over the live payload.
- ``hash()`` / ``equals()`` / ``compare()`` / ``to_string()`` — route
  through ``view()`` and the bound ``ValueOps``. ``Value`` itself only
  carries the minimum behaviour needed to live in a container.

Anything richer than container-membership operations is exposed through
``ValueView`` or one of its specialized adapters, never directly on
``Value``.

Storage and Allocation
~~~~~~~~~~~~~~~~~~~~~~

Memory is owned by ``MemoryUtils::StorageHandle``, parameterised by an
inline-storage policy and the binding type. Lifecycle hooks are delegated
to ``LifecycleOps`` on the binding's ``StoragePlan``:

- ``construct``, ``copy_construct``, ``move_construct``
- ``copy_assign``, ``move_assign``
- ``destroy``
- ``allocate``, ``deallocate``

Small payloads use inline (SBO) storage; larger payloads heap-allocate
with schema alignment. Container kinds have their own internal storage
shapes that keep element addresses stable across growth and
reconciliation; those shapes feed directly into the time-series
representation and are described in the next layer.

Non-Owning View
~~~~~~~~~~~~~~~

``ValueView`` is the type-erased non-owning handle:

.. code-block:: cpp

   struct ValueViewContext {
       const ValueTypeBinding* binding;
       void*                   data;
   };

The binding gives the view its schema, plan, lifecycle, and ops; the data
pointer addresses the live payload. Because the context is two pointers,
a view is cheap to copy and pass through internal traversal code.

A ``ValueView`` exposes:

- type interrogation: ``is_atomic()``, ``is_tuple()``, ``is_list()``, etc.
- typed access: ``as<T>()``, ``try_as<T>()``, ``checked_as<T>()`` for
  atomic kinds.
- generic ops: ``hash()``, ``equals()``, ``compare()``, ``to_string()`` —
  always routed through the bound ``ValueOps``.
- read access for composite kinds via the specialized adapters described
  under View Casting.

Mutation, including atomic ``set<T>`` and structural assignment, requires a
mutable view obtained via ``begin_mutation()`` (see Read-Only and Mutable
Views).

View Casting
~~~~~~~~~~~~

The value layer supports direct casting from one view shape to another so
callers do not need to chain calls to reach a typed handle. Two cast
families exist:

- **Kind-specialised view casts**: ``as_tuple()``, ``as_bundle()``,
  ``as_list()``, ``as_set()``, ``as_map()``, ``as_cyclic_buffer()``,
  ``as_queue()``, with ``try_as_*`` counterparts that return
  ``std::optional`` and throw nothing.
- **Atomic typed casts**: ``as<T>()``, ``try_as<T>()``, ``checked_as<T>()``
  reach the underlying scalar in one call.

Both families are mirrored on ``Value`` itself (``Value::as_list()``,
``Value::as<T>()``, etc.) so callers holding a ``Value`` do not need to
dereference into a ``ValueView`` and then cast again.

These casts only re-interpret the existing binding's view shape. They do
not change the underlying schema or copy the payload. Cross-schema
adaptation — exposing one schema's value through a different schema —
is a time-series concern, not a value-layer concern.

Read-Only and Mutable Views
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Views come in read-only and mutable variants. The distinction is part of
the public contract, not just C++ ``const`` discipline:

- A **read-only view** exposes inspection and iteration: typed access,
  ``hash``, ``equals``, ``compare``, ``to_string``, buffer exposure, and
  structural reads.
- A **mutable view** adds the kind-specific mutation operations: ``set``
  for atomics, field mutation on bundles and tuples, ``push_back`` /
  ``resize`` on lists, ``add`` / ``remove`` on sets, key insertion and
  value updates on maps, and so on.

A mutable view is obtained from an existing view by calling
``begin_mutation()``. The transition is explicit so that consumers can
reason about when mutation is in scope — the time-series layer in
particular needs to know precisely when changes start and end so its
delta accounting stays coherent.

The mutation is closed by calling ``end_mutation()`` on the mutable
view. If the caller does not call it, the mutable view's destructor
closes the mutation when the view goes out of scope. Explicit
``end_mutation()`` gives the caller deterministic control over when
the runtime sees the mutation complete; RAII closure is the safety net
that guarantees no mutation is left dangling if a caller forgets or an
exception unwinds the stack.

One specific cross-kind read-only view is worth calling out: ``MapView``
exposes ``key_set()``, which returns a read-only ``SetView`` over the
map's keys. Callers can iterate or query keys with the same surface they
would use for a standalone set, without copying or materialising a second
container. The view is read-only because the keys belong to the map;
structural changes go through the map's mutable view.

Nullability
~~~~~~~~~~~

Null is a *state*, not a schema or type. There is no null
``ValueTypeMetaData``.

- **Top-level**: ``Value::has_value()`` distinguishes a typed-null Value
  (schema known, payload absent) from a populated one.
- **Nested**: composite kinds track per-child validity — per-field for
  tuples and bundles, per-element for fixed and dynamic lists, per-slot
  for map values. Map keys and set elements are non-null by design.

Top-level null Values map to Python ``None`` at the bridge boundary;
``from_python(None)`` calls ``reset()``.

Buffer and Arrow Interop
~~~~~~~~~~~~~~~~~~~~~~~~

Exposing value data as a buffer is a first-class concern of the value
layer. Internal storage is not required to be Arrow-native, but every
kind whose payload can be sensibly viewed as a contiguous buffer must
expose a path to do so:

- atomic and fixed-size composite kinds expose a direct buffer view over
  their payload memory.
- dynamic list and queue kinds expose buffer views over their element
  storage, with a separate validity buffer where nested nullability
  applies.
- map and set kinds expose key, value, and validity buffers in parallel.

Validity bits follow Arrow conventions (``1 = valid``, ``0 = null``) so a
buffer view is interchangeable with an Arrow-compatible consumer without a
translation step. This applies whether the buffer is exported for
analytics, sent over an external bridge, or consumed by an Arrow-aware
adaptor.

Buffer access is read-only at the value layer. Mutation continues to go
through the typed view APIs.

Memory Layout Refinement Topics
-------------------------------

The next refinement pass should decide the physical layout for each structure:

- whether graph nodes live in one arena allocation or separate stable vectors,
- whether ``GraphScheduleTable`` is a plain ``std::vector<engine_time_t>`` or packed with node metadata,
- whether optional node schedulers are colocated with nodes or held in a side table,
- how input and output arrays are laid out for fast readiness checks,
- how subscriber lists avoid invalidation during graph construction and dynamic graph changes,
- how time-series child structures maintain stable identity while supporting sparse dictionaries and dynamic lists,
- which value containers need stable slots, tombstones, or compacting storage,
- where Python bridge objects are attached without infecting C++ core structures.

Open Questions
--------------

- Should graph storage be a single arena per graph instance, or a set of stable typed arenas?
- Should node-local scheduler storage be inline in a node allocation when present, or stored in a scheduler side table keyed by node index?
- Should graph schedule entries carry only time, or time plus debug/status flags for trace tooling?
- Which time-series containers require stable child addresses across mutation?
- Which structures must survive graph hot-swap or dynamic subgraph extension?
