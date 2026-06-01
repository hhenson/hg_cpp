Graph Wiring
============

This page describes how C++ graph wiring is intended to work, and how the same
engine will back Python graph wiring. The goals are: wiring that reads like node
wiring (see *Wiring*), runs **at wiring time** (not during evaluation), and shares
its core with the Python bridge so a Python ``@graph`` and a C++ graph build the
same runtime graph the same way.

.. note::

   **Planned.** The runtime ``GraphBuilder`` / ``GraphEdge`` and the static node
   authoring layer (``NodeBuilder::implementation<T>()``) exist today. The
   ``Wiring`` core and the ``wire<>`` / ``build_graph<>`` facade described here
   are **not yet implemented**; this is the design record. The user-facing view
   is *User Guide > Wiring Graphs in C++*.


Two tiers
---------

Because Python cannot instantiate C++ templates, the engine is split so that the
part Python shares is an ordinary runtime object, and the C++ ergonomics sit on
top of it:

.. code-block:: text

   C++ graph-struct  ─┐                          ┌─ StaticGraphSignature<G>  (reflect wire())
                      ├─►  shared runtime core  ──┤
   Python @graph     ─┘   (Wiring → GraphBuilder)  └─ Python signature introspection
                                   │
                                   ▼
                           topo-sort + rank → flattened nodes + edges → runtime graph

- **Runtime core** (``Wiring``) — a small, language-agnostic object that
  accumulates a graph and, on finish, topologically sorts and ranks it. Both C++
  and Python call into this.
- **Typed C++ facade** — ``Port<Schema>``, ``wire<T>(...)``, ``wire<G>(...)`` and
  the graph-as-struct form. Adds compile-time checking and *lowers to* core
  calls.

A graph in either language is the same thing: a named **signature**
``(time-series inputs) -> time-series output(s)`` whose body wires sub-nodes and
sub-graphs, lowering to identical core calls. The signature is the shared
contract; only how it is recovered differs (C++ reflection vs Python
introspection).


Identity at wiring time, rank at build time
-------------------------------------------

A wired node cannot be given its final ordering index while wiring, because the
final index *is* its rank, and rank is only known once the whole graph exists. So
identification and ranking are separate steps:

- **Wiring identifies — and interns.** Each ``add_node`` forms a **wiring
  instance** — the node *definition* plus its inputs (the time-series input ports
  **and** the scalar input values) — and **dedups** it: an equivalent instance
  returns the existing one; otherwise the new instance is added. Because instances
  are interned with a **stable address**, the ``WiringInstance`` pointer *is* the
  node's wiring-time identity — ports and edges reference that pointer, never a
  final index; insertion order is irrelevant. Two instances are equivalent when
  the node definition matches **and all inputs are equal** — time-series inputs
  (ports) are equal when they reference the same producing ``WiringInstance`` and
  path; scalar inputs are equal by value. Because producing instances are
  themselves interned, the keys are canonical and the dedup is transitive across
  the whole graph: this is wiring-time common-subexpression elimination, the
  node-level analogue of the structural interning of plans/schemas/ops (see *Data
  Structures > Core Concepts*).

  A wiring instance is the closest analogue of a Python node *instance*. The node
  **definition** (its type / signature) is shared and carries no per-instance
  scalars or ports; the **instance** adds the scalar values and input ports that
  distinguish it. Neither is a runtime ``NodeBuilder`` yet — the builder is
  created later, at build time (see *The shared core*), from the definition plus
  the instance's scalar values.
- **Build ranks.** Finishing runs the rank pass: place source nodes in the
  prefix (and set ``push_source_nodes_end``), topologically order the rest so
  ``rank(parent) < rank(child)``, assign ``final_index = rank``, and remap every
  edge's ``WiringInstance`` endpoints to final indices (via a ``WiringInstance* →
  index`` map).

The **runtime is unchanged** by this: it still evaluates in index order and relies
on ``rank(parent) < rank(child)``; that invariant is now *produced* by the rank
pass rather than being the caller's responsibility (which is the fragility behind
add-order-equals-rank-order assumptions). Feedback edges are deferred: they are
delayed edges that must not constrain rank, and the first cut is DAG-only.


The shared core
---------------

.. code-block:: cpp

   // Planned — provisional shapes.
   struct WiringInstance;                     // interned wiring identity (defined below)

   struct Port {                              // a handle to a wiring instance's output
       const WiringInstance      *node;       // the producing (interned) instance — its identity
       std::vector<std::size_t>   path;       // output path within that node
       const TSValueTypeMetaData *schema;
   };

   // The interned wiring identity: a node definition plus the inputs that
   // distinguish this instance — scalar values and input ports. Interned with a
   // stable address, so the WiringInstance* IS the node's identity. There is no
   // runtime NodeBuilder yet, and no edges (ports become edges at build time).
   struct WiringInstance {
       const NodeTypeBinding *def;            // node definition (type/schema/ops); shared, scalar/port-free
       std::vector<Value>     scalars;        // scalar input values (value-equality drives interning)
       std::vector<Port>      inputs;         // time-series input ports
   };

   class Wiring {
     public:
       // Intern a wiring instance (def + scalars + ports); return its output port.
       Port         add_node(const NodeTypeBinding *def,
                             std::span<const Value> scalars,
                             std::span<const Port>  inputs);
       // Build: per instance create a NodeBuilder from (def + scalars) and apply
       // edges from its ports; topo-sort + rank → a rank-ordered GraphBuilder.
       GraphBuilder finish() &&;
   };

- ``add_node`` forms the wiring instance's key (``def`` + scalar values + input
  ports) and looks it up in the wiring intern table. On a hit it returns the
  existing instance's port and adds nothing. On a miss it interns the new instance
  and returns a ``Port`` referencing it (the instance's stable address is its
  identity). It does **not** build a
  runtime ``NodeBuilder`` and records **no** edges yet.
- ``finish`` runs the rank pass, then for each instance in rank order creates the
  runtime ``NodeBuilder`` from its ``def`` **with its scalar values** (scalars are
  a construction input — the node stores them so ``eval`` reads them through its
  ``Scalar`` selectors) and applies an **edge per input port** (port ``i`` → this
  node's input field ``i``). The ``NodeBuilder`` itself has no knowledge of ports;
  edges are applied by the graph builder. It emits a rank-ordered ``GraphBuilder``
  ready for ``make_graph()``.
- The core is language-agnostic: it consumes node definitions, scalar values and
  ports. The interning, rank pass and per-instance builder construction living
  here means **Python gets all of it for free**.


The typed C++ facade
--------------------

- ``Port<Schema>`` wraps the erased ``Port`` with a compile-time schema marker.
- ``wire<T>(w, args...)`` — splits its arguments into **scalar values** and
  **input ports** by selector type, checks arity and schemas at **compile time**
  (via a small ``StaticNodeSignature<T>`` extension that exposes the ``In`` schema
  types and the output type), lowers to ``w.add_node(<T's binding>, scalars,
  ports)``, and returns ``Port<output>`` (or ``void`` for a sink).
- ``wire<G>(w, ports...)`` — inlines ``G::wire(w, ports...)`` (graphs flatten) and
  returns ``G``'s output port.
- ``StaticGraphSignature<G>`` — reflects ``&G::wire`` **skipping the leading
  ``Wiring&``**: ``Port`` parameters are the graph's time-series inputs, any
  non-``Port`` parameters are its scalar inputs, and the return type is its
  time-series output(s). This is the graph-level mirror of ``StaticNodeSignature``.
- ``build_graph<G>(...)`` — constructs a ``Wiring``, supplies the time-series
  boundary input ports (for a sub-graph; a top-level graph has **no** time-series
  inputs or outputs), forwards any scalar inputs, calls ``G::wire(w, …)``, and
  returns ``w.finish()``.

The compile-time checks are the C++ advantage over Python; the core re-validates
schemas at wiring time as a safety net (and as the only check Python relies on).


Graphs flatten
--------------

Graph composition is **inlining**: ``wire<G>`` expands ``G``'s body into the
current ``Wiring``; only nodes become runtime objects. This matches Python, where
``@graph`` functions are wiring-time only and disappear into nodes.

Nested graphs that must **not** flatten — ``map_`` / ``reduce`` / ``switch_`` and
other higher-order operators — need boundary binding (the child-graph template /
boundary substrate) rather than inlining. That is the extension point for those
operators and is out of scope for the first cut.


Extending to Python
-------------------

Python wiring drives the **same core**:

- At wiring time a Python ``@graph`` runs its body; calling a node builds a
  ``NodeBuilder`` through the Python→C++ bridge and calls ``Wiring::add_node``;
  calling a sub-graph inlines its wiring; finishing runs the same rank pass.
- Ranking, flattening and graph construction are therefore single-sourced in C++;
  Python never sees node indices.
- The graph **signature** is the shared contract: C++ recovers it with
  ``StaticGraphSignature`` reflection, Python with function-signature
  introspection; both yield the same ``(TS inputs) -> TS output`` shape and the
  same sequence of core calls.
- The explicit ``Wiring&`` first parameter in C++ corresponds to Python's
  implicit *current graph* context; it is an implementation detail that the C++
  signature reflection ignores, so the logical signatures line up.


Status and roadmap
------------------

Not implemented yet. Intended slices:

1. The shared ``Wiring`` core + erased ``Port`` (thin over ``GraphBuilder``) with
   the topo-sort/rank pass; the ``StaticNodeSignature`` extension exposing
   ``In`` schema types and the output type; ``Port<Schema>`` and
   ``wire<T>(w, …)`` (nodes); ``build_graph<G>()`` for a **top-level** graph.
   Validate by rebuilding the ``source → add_one`` graph without manual indices
   or edges.
2. Sub-graph composition — ``wire<G>(w, …)`` and ``StaticGraphSignature<G>``.

Deferred: multiple outputs (``TSB`` ports, optionally returned as an array as
sugar), generic graphs (``TsVar`` /
``ScalarVar`` in signatures), higher-order operators and feedback, and the Python
bridge that drives the core.
