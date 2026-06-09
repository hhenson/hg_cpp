Node Schemas
============

A node schema describes the contract of a single graph node — what it
exposes to the wiring layer, what lifecycle hooks it supports, and what
runtime state it requires. The schema itself owns no executable code
and no memory layout: it is a pure description that the
*Allocation, Plans and Ops* layer turns into a concrete runtime node.

A node, viewed through the Plan / Schema / Ops / Builder / Value / View
vocabulary, is the next layer up from a time-series:

================  ==========================================================
Concept role      Node-layer name
================  ==========================================================
Schema            ``NodeTypeMetaData`` — kind, input/output/error/state
                  schemas, lifecycle flags, injection requirements,
                  generic-resolution metadata.
Plan              ``NodePlan`` — memory layout for the node's runtime
                  state: input slots, output slots, state buffer,
                  optional scheduler, optional error output.
Ops               ``NodeOps`` — the behaviour vtable: ``start``,
                  ``eval``, ``stop``, plus an error-handling hook
                  that is wired in only when the schema turns on
                  exception capture (otherwise exceptions propagate
                  out of ``eval`` normally). The first parameter of
                  every op is a pointer to the node's runtime memory.
                  Construction and destruction are handled by the
                  plan's ``LifecycleOps``, not by ``NodeOps``.
Binding           ``NodeTypeBinding`` — interned ``(schema, plan, ops)``
                  triple.
Builder           ``NodeBuilder`` — reusable builder that wraps a
                  binding and constructs runtime node instances from
                  graph contexts.
Value             ``NodeValue`` — the owning runtime node instance that
                  lives in the graph's flattened node array. It owns the
                  node storage through the same binding + storage-handle
                  pattern as value and time-series data.
View              ``NodeView`` — a borrowed type-erased cursor over a
                  node allocation. It carries the node binding, data
                  pointer, and no cycle-local state. Evaluation time is
                  passed explicitly to lifecycle operations and
                  input/output projection methods. Active input
                  notification targets are recovered from the node
                  runtime storage header.
================  ==========================================================

``NodeBuilder`` is a reusable builder, not a value-builder-style scratch
object. Once the node schema has been resolved to a binding, the builder can
be cached and used to construct many node instances. The instance-specific
inputs are the graph context, node position, boundary bindings, and any
per-node configuration captured by the graph schema.

What a Node Schema Records
--------------------------

A ``NodeTypeMetaData`` carries:

``input_schema``
    Always a TSB. The top-level input of a node is invariably a
    bundle whose fields are the named arguments. A single-argument
    node carries a single-field TSB; a multi-argument node carries a
    multi-field TSB; a source node records an empty bundle. There is
    no non-TSB top-level input. The schema lives in the time-series
    registry; the node schema only holds a borrowed pointer.

``output_schema``
    The time-series schema for the node's primary output (named
    ``output`` at runtime — see *Overview > Node Layer*). It is a
    single TS-shaped schema (``TS<Int>``, ``TSD<Str, TS<Float>>``,
    …) — at most one main output per node. A sink node records
    ``nullptr`` for ``output_schema``. Auxiliary outputs (error,
    recordable state) are recorded as separate properties below, not
    folded into ``output_schema``.

``error_output_schema``
    Optional. The time-series schema for the node's ``error_output``,
    set only when the schema turns on exception capture. With capture
    off, this property is ``nullptr`` and exceptions thrown by
    ``eval`` propagate normally; with capture on, the runtime catches
    them and writes a ``NodeError`` value to ``error_output``. The
    default schema is ``TS<NodeError>``; the ``NodeError`` shape is
    itself an interned scalar schema, so node implementations are
    free to use a richer error type when needed.

``state_schema``
    Optional. The scalar (value-layer) schema for the node's local
    state. Records the field layout and types but no actual values.
    Runtime construction allocates a read-write ``Value`` for this schema,
    default-constructs it through the matching ``StoragePlan`` and
    ``LifecycleOps``, and exposes it through ``NodeView::state``. State
    mutation uses the normal value-layer ``begin_mutation`` path.

``recordable_state_schema``
    Optional. The time-series schema for the node's recordable state —
    state that participates in graph evaluation as a TS rather than as
    plain scalar storage. Used by replay-aware operators.

``injection_flags``
    Bit set recording which injectable resources the node consumes:
    ``CLOCK``, ``SCHEDULER``, ``STATE``, ``RECORDABLE_STATE``. The
    runtime uses this to decide whether to allocate an evaluation clock
    handle, a per-node scheduler, and so on. A stateless, scheduler-
    less compute node carries an empty injection set and pays no
    storage cost for the resources it does not need.

``node_kind``
    Enum: ``COMPUTE``, ``PUSH_SOURCE``, ``PULL_SOURCE``, ``SINK``,
    ``NESTED``. Drives evaluation order (push sources occupy the
    push-source partition described in *Graph Layer*) and which
    boundary contract applies for nested graphs.

``nested_graph_schema``
    Optional. Set only when ``node_kind == NESTED``. Borrowed pointer
    to the ``GraphTypeMetaData`` of the nested graph this node hosts.
    Containment between graphs flows through the hosting node: the
    parent graph schema sees a node entry; the node schema records
    which graph schema lives inside it. The graph schema itself never
    tracks its children.

``lifecycle_flags``
    Records which of ``start`` / ``eval`` / ``stop`` the node's ops
    table actually implements. Most compute nodes only need ``eval``;
    nodes with internal resources (open files, network sockets,
    accumulators) implement ``start`` and ``stop``.

``unresolved_args``
    For generic node schemas, the input names whose schema contains
    unresolved type variables. ``wire<>`` resolves these against the
    connected input bindings (via ``ts_unifier``) before constructing the
    node — see *Generic Resolution* below.

Runtime Value/View
------------------

The runtime node follows the same plan / ops / binding / value / view
shape as the lower layers:

``NodeTypeBinding``
    Interned ``(NodeTypeMetaData, NodePlan, NodeOps)`` identity. The
    binding is the only object a generic node view needs in order to
    recover schema, storage lifecycle, and runtime behaviour.

``NodeValue``
    Owns one node allocation. Moving a node value transfers ownership of
    the allocation; copying is not part of the runtime contract. The node
    storage plan is a composite slab. The
    fixed ``NodeRuntimeStorage`` header is the first component and contains
    only common wrapper state such as graph attachment, node index, lifecycle
    flags, label, and the stable ``Notifiable`` implementation used by active
    input subscriptions. Optional components such as ``TSInput``, ``TSOutput``,
    local ``Value`` state, error output, and recordable-state output are
    present only when declared by the node schema and are laid out after the
    header in the same allocation.

``NodeView``
    Borrowed, type-erased access to a node. Public node operations such as
    ``start``, ``evaluate``, ``stop``, ``input``, ``output``, ``state``,
    and ``cleanup_delta`` dispatch through ``NodeOps``. This keeps graph
    evaluation independent of concrete node implementation classes. The
    state view is writable-capable when the node declares ``state_schema``;
    callers still open mutation explicitly with ``ValueView::begin_mutation``.
    ``NodeView`` itself is timeless: callers pass ``DateTime`` to
    lifecycle operations and to time-series endpoint projections such as
    ``input(evaluation_time)`` and ``output(evaluation_time)``.

``NodeBuilder``
    Cached construction recipe. It resolves the node schema and endpoint
    annotations to a binding, then constructs ``NodeValue`` instances on
    demand.

The first C++ runtime pass supplies a native callback-backed node family
using this structure. Static C++ nodes, Python-backed nodes, nested graph
nodes, and richer scheduler-aware node families should become additional
``NodeOps`` implementations rather than branches in the graph loop.

The Node Lifecycle
------------------

A node's runtime behaviour is exposed through the ``NodeOps`` vtable.
The hooks are intentionally minimal:

``start(node*, evaluation_time)``
    Called once per node, after construction and before the first
    ``eval``. Used for any node-local resource acquisition that cannot
    be expressed as plain construction.

``eval(node*, evaluation_time)``
    Called whenever the node is scheduled. The node reads its inputs
    via the bound input views, computes, and writes its output through
    the bound output view. ``eval`` is the only op every node must
    implement.

``stop(node*, evaluation_time)``
    Called once per node, after the last ``eval`` and before
    destruction. Symmetric counterpart to ``start``: releases anything
    ``start`` acquired. Most compute nodes do not need it.

``handle_error(node*, error_value*)``
    Optional, and **only present when the schema turns on exception
    capture**. With capture off, an exception thrown by ``eval``
    propagates out as a normal C++ exception and is handled by
    whatever frame above the node catches it; the runtime does not
    intercept it. With capture on, the runtime catches the exception,
    constructs a ``NodeError``, and routes it through ``handle_error``
    if the node implementation provides one (otherwise the default
    behaviour is to write the ``NodeError`` to ``error_output``).

The first parameter of every op is a pointer to the node's runtime
memory — the same pattern the value-layer ops table uses. This keeps
nodes type-erased: the runtime evaluation loop dispatches against the
ops vtable without knowing the concrete C++ type behind it.

Construction and destruction of a node's runtime memory are not part
of ``NodeOps``: they are the responsibility of the bound ``NodePlan``'s
``LifecycleOps`` (``construct`` / ``destroy``), invoked by the
allocator that owns the node's storage. Keeping them on the plan
preserves the plan/ops separation described in *Allocation, Plans
and Ops*: ``NodeOps`` describes runtime behaviour over already-
constructed memory, ``LifecycleOps`` describes how that memory is
brought into and out of existence.

Generic Resolution
------------------

A node schema may carry **type variables** (``TsVar`` / ``ScalarVar``) —
placeholder schemas that must be resolved at wiring time before the node
can be instantiated. For example, a generic ``add`` node has inputs
``In<"lhs", TsVar<"T">>`` / ``In<"rhs", TsVar<"T">>`` and output
``Out<TsVar<"T">>``; ``T`` is unresolved on the abstract schema and is
bound to a concrete time-series schema at the point the node is wired.

Resolution is **implemented at the wiring layer**
(``include/hgraph/types/type_resolution.h``):

- a ``ResolutionMap`` binds each variable name to concrete metadata;
- ``wire<>`` populates it — ``ts_unifier`` matches each input selector's
  pattern against the connected port's runtime schema (binding input
  variables), a scalar variable is inferred from the configured value's
  type, and a source-side output variable is supplied explicitly
  (``ts_type<>()`` / an explicit output schema);
- ``StaticNodeSignature::is_generic()`` plus its resolution-map schema
  overloads, driven through ``NodeBuilder::implementation<T>(const
  ResolutionMap&)``, then build the node's concrete ``NodeTypeMetaData``.

The node's *callbacks* never see ``T`` — they read the resolved endpoints
from the ``NodeView`` at eval time — so one closure over ``&eval`` serves
every resolution. Two wirings of the same generic definition with the
same resolved schemas and inputs intern to one node (the resolved
input/output/scalar/state schema pointers are part of the wiring key, so
distinct resolutions do **not** collide). This is the C++ counterpart of
Python type-variable resolution and the seed for generic ``map_`` /
``reduce`` / ``switch_``.

Interning Key
-------------

The ``NodeRegistry`` interns node schemas by a structural key
combining:

- the input schema pointer,
- the output schema pointer (or ``nullptr`` for sinks),
- the error-output schema pointer (or ``nullptr``),
- the state schema pointer (or ``nullptr``),
- the recordable-state schema pointer (or ``nullptr``),
- ``injection_flags``,
- ``node_kind``,
- ``lifecycle_flags``.

Two structurally identical nodes — same I/O shapes, same injection,
same lifecycle, same kind — always resolve to the same interned
``NodeTypeMetaData *``. This is what lets a graph schema reference its
member nodes by pointer without worrying about identity drift.

Status
------

The node-schema vocabulary separates compile-time C++ descriptors from
runtime schema metadata. The compile-time layer is a *generator* that
produces ``NodeTypeMetaData`` entries in the registry, and the runtime
evaluation path consults the registry directly. Several details —
exact ``NodeError`` schema, exact representation of
``unresolved_args``, error-handling hook signature — are still in
flight and will be filled in alongside the implementation.
