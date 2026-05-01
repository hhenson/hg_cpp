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
Ops               ``NodeOps`` — the lifecycle vtable: ``start``, ``eval``,
                  ``stop``, plus error-handling and disposal hooks. The
                  first parameter of every op is a pointer to the node's
                  runtime memory.
Binding           ``NodeTypeBinding`` — interned ``(schema, plan, ops)``
                  triple.
Builder           ``NodeBuilder`` — wraps a binding and constructs a
                  runtime node from a graph context.
Value             ``Node`` — the runtime node instance that lives in the
                  graph's flattened node array (see *Overview >
                  Structural Layers*).
View              Node access happens through the structural layer, not
                  through a type-erased view. Nodes are not values; they
                  are runtime participants.
================  ==========================================================

What a Node Schema Records
--------------------------

A ``NodeTypeMetaData`` carries:

``input_schema``
    The TSB-shaped time-series schema describing the named input ports.
    A node with no inputs (a source node) records an empty bundle. The
    schema lives in the time-series registry; the node schema only
    holds a borrowed pointer.

``output_schema``
    The time-series schema for the node's output. Single-output nodes
    point at a single TS schema (``TS<int>``, ``TSD<string, double>``,
    …); multi-output nodes point at a TSB whose fields are the named
    outputs. A sink node records ``nullptr`` for ``output_schema``.

``error_output_schema``
    Optional. When present, the schema of the per-node error output
    used to capture evaluation errors. Default is a ``TS<NodeError>``
    or similar — but the exact ``NodeError`` shape is itself an
    interned scalar schema, so node implementations are free to use a
    richer error type when they need to.

``state_schema``
    Optional. The scalar (value-layer) schema for the node's local
    state. Records the field layout and types but no actual values; the
    runtime allocates and constructs the state via the matching
    ``StoragePlan`` and ``LifecycleOps`` at node construction time.

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

``lifecycle_flags``
    Records which of ``start`` / ``eval`` / ``stop`` the node's ops
    table actually implements. Most compute nodes only need ``eval``;
    nodes with internal resources (open files, network sockets,
    accumulators) implement ``start`` and ``stop``.

``unresolved_args``
    For generic node schemas, the list of input names whose schema
    contains unresolved type variables. Wiring resolves these against
    actual input bindings before constructing the node.

The Node Lifecycle
------------------

A node's runtime behaviour is exposed through the ``NodeOps`` vtable.
The hooks are intentionally minimal:

``start(node*)``
    Called once per node, after construction and before the first
    ``eval``. Used for any node-local resource acquisition that cannot
    be expressed as plain construction.

``eval(node*)``
    Called whenever the node is scheduled. The node reads its inputs
    via the bound input views, computes, and writes its output through
    the bound output view. ``eval`` is the only op every node must
    implement.

``stop(node*)``
    Called once per node, after the last ``eval`` and before
    destruction. Symmetric counterpart to ``start``: releases anything
    ``start`` acquired. Most compute nodes do not need it.

``dispose(node*)``
    Final teardown hook, called by the destructor through the bound
    plan's ``LifecycleOps``. Distinct from ``stop`` in that it runs
    even if ``start`` failed.

``handle_error(node*, error_value*)``
    Optional. Called when an exception thrown by ``eval`` is captured
    by the runtime. Default behaviour is to surface the error on the
    node's error output if one is declared; the hook lets node
    implementations override that.

The first parameter of every op is a pointer to the node's runtime
memory — the same pattern the value-layer ops table uses. This keeps
nodes type-erased: the runtime evaluation loop dispatches against the
ops vtable without knowing the concrete C++ type behind it.

Generic Resolution
------------------

A node schema may carry **type variables** — placeholder schemas that
must be resolved at wiring time before the node can be instantiated.
For example, a generic ``add`` node has input schemas
``In<"lhs", TS<T>>`` and ``In<"rhs", TS<T>>`` and output schema
``Out<TS<T>>``; ``T`` is unresolved on the abstract schema and gets
bound to a concrete scalar schema (``int``, ``double``, …) at the
point the node is wired into a graph.

Resolution is a registry operation: ``NodeRegistry::resolve(generic,
{T -> int_meta})`` returns the interned concrete node schema. Two
generic nodes with the same structural shape and the same resolution
substitution always resolve to the same concrete schema pointer.

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

The node-schema vocabulary above is being refined from the prior
``StaticNodeSignature`` / ``static_schema.h`` work in the 2603 branch.
The 2603 design conflated compile-time C++ reflection with runtime
schema metadata; the rewrite separates them: the compile-time layer
becomes a *generator* that produces ``NodeTypeMetaData`` entries in
the registry, and the runtime evaluation path consults the registry
directly. Several details — exact ``NodeError`` schema, exact
representation of ``unresolved_args``, error-handling hook signature —
are still in flight and will be filled in alongside the implementation.
