Graph Schemas
=============

A graph schema describes a graph topology — the set of nodes that make
up the graph, the wiring between them, and (for nested graphs) the
boundary contract that connects the graph to its parent. As with node
schemas, a graph schema records identity only: it carries no
allocations, no runtime nodes, and no link state. The runtime graph
that gets evaluated is built from a graph schema by the corresponding
plan, ops, and builder.

================  ==========================================================
Concept role      Graph-layer name
================  ==========================================================
Schema            ``GraphTypeMetaData`` — node entries, wiring entries,
                  boundary descriptors, identity, optional parent-link
                  metadata for nested graphs.
Plan              ``GraphPlan`` — memory layout for the runtime graph:
                  flattened node array, schedule table, per-node state
                  storage, boundary-binding storage.
Ops               ``GraphOps`` — graph-level lifecycle vtable:
                  ``construct``, ``start``, ``evaluate``, ``stop``,
                  ``dispose``.
Binding           ``GraphTypeBinding`` — interned ``(schema, plan,
                  ops)`` triple.
Builder           ``GraphBuilder`` — turns a schema into a runtime
                  ``Graph`` instance, recursively building member nodes
                  and (for nested graphs) child graph templates.
Value             ``Graph`` — the runtime graph object described in
                  *Overview > Graph Layer*.
View              Graph access happens through the structural layer.
================  ==========================================================

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

``wiring``
    List of ``WiringEntry`` records, each describing one link from an
    output position on one node to an input position on another. The
    entry names the source ``(node_index, output_path)``, the target
    ``(node_index, input_path)``, and the link strategy
    (TargetLink / RefLink / ForwardingLink — see *Linking
    Strategies*). The wiring schema is purely topological; the runtime
    link states are constructed from these entries when the graph is
    built.

``boundary``
    Optional ``GraphBoundarySchema`` describing the graph's external
    interface. Top-level graphs may carry a boundary that exposes
    external inputs and outputs (e.g. for an embedded runtime). Nested
    graphs always carry a boundary — that is what makes them nestable.

``nested_children``
    For graphs that themselves embed nested graphs, the list of child
    graph schema pointers and how their boundaries bind to positions
    in this graph. Nested-child resolution is recursive: a nested
    graph can embed further nested graphs without bound.

``source_partition``
    Pre-computed boundary index that separates push-source nodes from
    rank-ordered nodes. The runtime uses this to drive its evaluation
    loop without rescanning the node list each cycle.

Wiring Entries
--------------

Each wiring entry records the topological connection between an
output position and an input position. The position references are
*paths* — sequences of indices and slot ids — not pointers, so the
schema stays free of runtime addresses:

.. code-block:: cpp

   struct WiringEntry {
       size_t                source_node_index;
       std::vector<size_t>   source_output_path;  // empty = single output
       size_t                target_node_index;
       std::vector<size_t>   target_input_path;   // index into the input bundle
       LinkStrategy          strategy;            // TargetLink / RefLink / ForwardingLink
   };

The graph builder turns each entry into the matching link state at
construction time, threading the pointers through ``BoundaryBindings``
and node input/output positions. The path representation matches the
slot/index addressing scheme described in *Time-Series Plans and Ops >
Path Construction*.

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
      ``try_except`` and ``component`` patterns where the child
      wiring passes an input through unchanged.
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

A nested graph schema is a graph schema whose boundary is meant to
bind into a parent rather than to an external runtime. Two
characteristics distinguish nested-graph schemas from top-level
schemas:

1. **Mandatory boundary.** A nested graph must declare its
   ``GraphBoundarySchema`` so the parent can wire its ports.
2. **Parent-link metadata.** The nested schema records which parent
   node owns it (``parent_node_index``) and which positions on that
   parent provide the bound inputs / receive the bound outputs.

The runtime instantiation of a nested graph is the *child graph
template* that the 2603 branch already implemented (``ChildGraphTemplate``
/ ``ChildGraphInstance``). The schema layer described here owns only
the template's identity description; the actual runtime graph storage
and the boundary-binding plan that maps ports to parent positions
live in *Allocation, Plans and Ops*.

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
graph schema embeds it, and the wiring layer resolves both at the
same time.

Status
------

The graph-schema vocabulary is in active design. Pieces already
prototyped in the 2603 branch — the ``ChildGraphTemplate``,
``BoundaryBindingPlan``, and the boundary mode list above — feed
into this rewrite, but the schema-versus-plan separation here is
new and several details (exact wiring-entry shape, exact form of the
nested generic-resolution map, exact registry key for graph schemas)
are not yet fixed. Subsequent passes will fill these in and update
this page.
