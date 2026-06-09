Graph Schemas
=============

A graph schema describes a graph topology — the set of nodes that make
up the graph, the directed edges between them, and (for nested graphs) the
boundary contract that connects the graph to its parent. As with node
schemas, a graph schema records identity only: it carries no
allocations, no runtime nodes, and no link state. The runtime graph
that gets evaluated is built from a graph schema by the corresponding
plan, ops, and builder.

================  ==========================================================
Concept role      Graph-layer name
================  ==========================================================
Schema            ``GraphTypeMetaData`` — node entries, edges,
                  boundary descriptors, and identity.
Plan              ``GraphPlan`` — memory layout for the runtime graph:
                  flattened node array, schedule table, per-node state
                  storage, boundary-binding storage.
Ops               ``GraphOps`` — graph-level behaviour vtable:
                  ``start``, ``evaluate``, ``stop``. Construction and
                  destruction are handled by the plan's
                  ``LifecycleOps``, not by ``GraphOps``.
Binding           ``GraphTypeBinding`` — interned ``(schema, plan,
                  ops)`` triple.
Builder           ``GraphBuilder`` — reusable builder that turns a
                  schema into runtime ``Graph`` instances, recursively
                  building member nodes and (for nested graphs) child
                  graph templates.
Value             ``GraphValue`` — the owning runtime graph instance. It
                  owns the flattened ``NodeValue`` array, schedule table,
                  link state, and cycle-local evaluation state.
View              ``GraphView`` — a borrowed type-erased cursor over graph
                  storage. Graph lifecycle and evaluation dispatch through
                  ``GraphOps``.
================  ==========================================================

``GraphBuilder`` is cached as a reusable construction recipe. It can build a
top-level graph instance, and when a graph appears as a nested node it also
acts as the child graph template retained by the parent builder. Runtime
instances still own their node storage, link state, schedulers, and boundary
bindings independently; the builder is the shared recipe, not the runtime
state.

Runtime Value/View
------------------

Graphs use the same type-erased runtime shape as values, time-series data,
and nodes:

``GraphTypeBinding``
    Interned ``(GraphTypeMetaData, GraphPlan, GraphOps)`` identity.

``GraphValue``
    Owns graph storage. The first implementation stores the flattened
    ``NodeValue`` array and a schedule entry per node. When graph storage is
    moved, child node parent links are reattached so input notifications
    continue to schedule through the owning graph.

``GraphView``
    Borrowed graph cursor. It exposes ``start``, ``evaluate``, ``stop``,
    ``schedule_node``, ``node_at``, and schedule inspection by delegating to
    ``GraphOps``. The graph runtime owns the current evaluation-time cache
    for scheduling and graph-level inspection. Node views remain timeless;
    the graph passes the current ``DateTime`` into node lifecycle calls
    and callers pass time explicitly when projecting node input/output views.

``GraphExecutorValue`` / ``GraphExecutorView``
    The executor layer is also type-erased. The first pass provides a
    simulation executor that starts the graph, advances through scheduled
    times, evaluates graph cycles, and stops the graph when no work remains
    or the configured end time is reached.

The first graph implementation intentionally keeps edge binding in the
builder path. Runtime evaluation does not switch on node kinds; it walks the
schedule table and dispatches node work through ``NodeView``.

What a Graph Schema Records
---------------------------

A ``GraphTypeMetaData`` carries:

``identity``
    Stable graph id and optional human-readable label. For nested
    graphs this also includes a logical path component used for
    diagnostics.

``nodes``
    Ordered list of ``NodeEntry`` records. Each entry pairs a
    ``NodeTypeMetaData *`` with the node's position-in-graph
    information: rank ordering, push-source flag, an optional generic-
    resolution substitution map, and any per-node configuration the
    node schema supports (e.g. window length on a TSW source).

``edges``
    List of ``GraphEdge`` records, each describing one link from an
    output position on one node to an input position on another. The
    entry names the source ``(node_index, output_path)``, the target
    ``(node_index, input_path)``, and the link strategy
    (TargetLink / RefLink / ForwardingLink — see *Linking
    Strategies*). The edge schema is purely topological; the runtime
    link states are constructed from these entries when the graph is
    built.

``boundary``
    Optional ``GraphBoundarySchema`` describing the graph's external
    interface. Top-level graphs may carry a boundary that exposes
    external inputs and outputs (e.g. for an embedded runtime). Nested
    graphs always carry a boundary — that is what makes them nestable.

``source_partition``
    Pre-computed boundary index that separates push-source nodes from
    rank-ordered nodes. The runtime uses this to drive its evaluation
    loop without rescanning the node list each cycle.

Edges
-----

Each graph edge records the topological connection between an
output position and an input position. The position references are
*paths* — sequences of indices and slot ids — not pointers, so the
schema stays free of runtime addresses:

.. code-block:: cpp

   struct GraphEdge {
       std::size_t          source_node;
       std::vector<size_t>  source_path;  // empty = single output
       std::size_t          target_node;
       std::vector<size_t>  target_path;  // index into the input bundle
   };

The graph builder turns each entry into the matching link state at
construction time, threading the pointers through ``BoundaryBindings``
and node input/output positions. The path representation matches the
slot/index addressing scheme described in
:ref:`ts-path-construction`.

The Graph Boundary
------------------

A graph boundary is the schema's description of which time-series
positions cross out of the graph and how. The boundary schema records:

``input_ports``
    Named external inputs the graph accepts. Each port carries a
    time-series schema and the path inside the graph it routes to.
    External inputs are how a parent graph injects ticks into a nested
    child.

``output_ports``
    Named external outputs the graph publishes. Each port carries a
    time-series schema and the source path inside the graph that
    backs it.

``binding_modes``
    For each port, the binding mode that should apply when the graph
    is instantiated as a child:

    - ``alias_child_output`` — expose a child node's output directly
      as a parent output via a ForwardingLink (zero-copy).
    - ``alias_parent_input`` — expose a parent's input through the
      child boundary as if it were a child output. Used by
      ``try_except`` and ``component`` patterns where child edge binding
      passes an input through unchanged.
    - ``bind_bundle_member_output`` — expose one field of a child's
      output bundle as a parent output (TSB navigation composed with
      ForwardingLink).
    - ``detach_restore_blank`` — detach a child input and restore an
      inert local input when the child is removed; used for two-phase
      removal during dynamic graph mutation.

The mode set is the schema-side counterpart of the link types
described in *Linking Strategies*.

Nested Graphs
-------------

A graph schema does **not** record its children. Containment runs the
other way: a graph that wishes to embed a nested graph does so
through one of its **nodes** — specifically, a node whose
``node_kind`` is ``NESTED``. The hosting node schema records the
nested graph's schema pointer; the graph schema itself sees only the
node entry and its edges, exactly like any other node. This keeps
the graph-schema vocabulary uniform: graph schemas describe a flat
set of node entries plus the edges between them, with no
hierarchical "child graphs" list.

What distinguishes a nested-graph schema from a top-level graph
schema is therefore not its content but its *use*: a nested-graph
schema must declare a ``GraphBoundarySchema`` so that the hosting
node can wire its ports onto the surrounding graph. Top-level graphs
optionally carry a boundary; nested graphs always do.

The schema carries no parent reference. A *runtime* nested-graph
instance may carry a back-pointer to the parent node that hosts it
(useful for clock delegation and diagnostics), but that is runtime
state, not schema content.

The runtime instantiation of a nested graph is the *child graph
template* (``ChildGraphTemplate`` / ``ChildGraphInstance``). The
schema layer described here owns only the template's identity; the
actual runtime graph storage and the boundary-binding plan that maps
ports to parent positions live in *Allocation, Plans and Ops*.

Dynamic Nested Graphs
~~~~~~~~~~~~~~~~~~~~~

Some operators — ``map_``, ``switch_``, ``try_except``, ``mesh`` —
instantiate child graphs dynamically during evaluation. Their
schemas carry an additional flag (``dynamic = true``) and the
runtime uses a ``ChildGraphTemplateRegistry`` to keep already-built
templates around for re-instantiation. The schema itself does not
record any of the dynamic state; it only records the template's
identity. The runtime side of this — pooling, two-phase removal,
clock delegation — is described under the nested-graph notes in
*Linking Strategies* and will be expanded in *Allocation, Plans and
Ops* when that layer fills out.

Generic Graphs
--------------

Graph schemas inherit the generic-resolution mechanism from node
schemas. A graph that contains generic nodes is itself generic: the
unresolved type variables of its members are aggregated into a single
graph-level substitution map, and resolving the graph against
concrete input types resolves all of its members in one pass.

This is what makes a function like ``map_(f, ts_in)`` describable as
a single graph schema: the inner ``f`` is a generic node, the outer
graph schema embeds it, and the edge-binding layer resolves both at the
same time.

Status
------

The graph-schema vocabulary is in active design. The
``ChildGraphTemplate``, ``BoundaryBindingPlan``, and boundary mode list
above are part of the intended design, with schema identity separated
from allocation and boundary-binding plans. Several details (exact
edge shape, exact form of the nested generic-resolution map,
exact registry key for graph schemas) are not yet fixed. Subsequent
passes will fill these in and update this page.
