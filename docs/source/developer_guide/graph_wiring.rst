Graph Wiring
============

This page describes how C++ graph wiring is intended to work, and how the same
engine will back Python graph wiring. The goals are: wiring that reads like node
wiring (see *Wiring*), runs **at wiring time** (not during evaluation), and shares
its core with the Python bridge so a Python ``@graph`` and a C++ graph build the
same runtime graph the same way.

.. note::

   **Status.** Slices 1–3b are **implemented**: the ``Wiring`` core (interning +
   topo-sort/rank), the typed ``Port<Schema>`` + ``wire<T>`` facade for nodes,
   ``build_graph<G>(…)`` for a top-level graph, **sub-graph composition**
   (``wire<G>`` inlines a graph, with the same compile-time argument checking and
   scalar-literal auto-wrapping as ``wire<T>``), **scalar inputs** (``Scalar<>``
   arguments folded into the intern key and recorded on the node builder), and
   ``StaticGraphSignature<G>`` with **graph-level scalar parameters** (a top-level
   graph's ``compose`` may take ``Scalar<>`` parameters, supplied through
   ``build_graph<G>(values…)``) — in ``include/hgraph/types/graph_wiring.h`` +
   ``src/hgraph/types/graph_wiring.cpp``, with ``tests/cpp/test_graph_wiring.cpp``.
   Still **not yet implemented**: standalone sub-graph building / time-series
   boundary binding (deferred until there is a consumer — the non-flattening nested
   graphs), generics and higher-order operators; those parts of this page are the
   design record. The user-facing view is *User Guide > Wiring Graphs in C++*.


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
  **definition** is identified by its *type* — ``typeid(T)`` for a C++ static
  node, the node class in Python — and **not** by the runtime ``NodeBuilder``,
  which is a per-construction build artifact (two builds of the same node type are
  not the same object). The definition is shared and carries no per-instance
  scalars or ports; the **instance** adds the scalar values and input ports that
  distinguish it. In the scalar-aware design the runtime ``NodeBuilder`` is built
  at ``finish`` from the definition plus the instance's scalar values; the current
  slice has no scalars yet, so it builds the (scalar-free) ``NodeBuilder`` eagerly
  and stores it on the instance.
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

   // Implemented shapes. The typed facade Port<Schema> is below.
   struct WiringInstance;

   struct WiringPortRef {                     // erased handle to a wiring instance's output
       const WiringInstance      *node;       // the producing (interned) instance
       std::vector<std::size_t>   path;       // output path within that node
       const TSValueTypeMetaData *schema;
   };

   // The interned wiring identity (stable address, so WiringInstance* IS the
   // identity). The NodeBuilder carries any per-instance scalar configuration
   // (set by add_node). Edges are derived from `inputs` at finish.
   struct WiringInstance {
       NodeBuilder                builder;    // build artifact (carries scalars)
       std::vector<WiringPortRef> inputs;     // time-series input ports
   };

   class Wiring {
     public:
       // `def` is the node definition's identity (typeid(T) for a C++ static node):
       // two calls with the same def + equal inputs + equal scalars dedup. `builder`
       // is stored for finish; the `scalars` value is recorded on it (empty if none).
       WiringPortRef add_node(std::type_index def, NodeBuilder builder,
                              std::span<const WiringPortRef> inputs, Value scalars);
       // Topo-sort + rank → a rank-ordered GraphBuilder (edges from each instance's ports).
       GraphBuilder  finish() &&;
   };

- ``add_node`` keys the instance on ``(def, input ports, scalars)`` — ``def`` is
  the node type's identity (``typeid(T)``) and ``scalars`` is the compound scalar
  configuration value (empty when the node has no scalar inputs) — and looks it up
  in the wiring intern table. On a hit it returns the existing instance's port; on
  a miss it records the ``scalars`` on the ``NodeBuilder``, interns the new
  instance (storing that builder), and returns a ``WiringPortRef`` to its output.
  No edges are recorded yet — the input ports carry the dependencies.
- ``finish`` runs the rank pass, then walks the instances in rank order, adding
  each instance's ``NodeBuilder`` to a ``GraphBuilder`` and emitting an **edge per
  input port** (port ``i`` → this node's input field ``i``), remapping
  ``WiringInstance*`` endpoints to final indices. The ``NodeBuilder`` has no
  knowledge of ports; edges are applied by the graph builder.
- The core is language-agnostic, so the Python wiring bridge reuses the interning
  and rank pass — including the scalar values in the key.


The typed C++ facade
--------------------

- ``Port<Schema>`` is the typed handle; ``.erased()`` lowers it to a
  ``WiringPortRef`` (the runtime schema comes from ``Schema``). A sub-graph
  input parameter declared as ``Port<SIGNAL>`` is treated as a tick subscription:
  any upstream time-series output port may be supplied, regardless of its value
  schema.
- ``wire<T>(w, args...)`` — takes the node's wiring arguments **in eval-parameter
  order**: a ``Port`` for each ``In`` and a scalar argument for each ``Scalar``. It
  checks **arity and, per position, the port schema or scalar convertibility** at
  compile time (via ``StaticNodeSignature<T>``, which exposes the output schema
  type, the input schema types and the ordered list of wiring parameters), splits
  the arguments into the time-series input ports and a compound scalar value, lowers
  to ``w.add_node(typeid(T), builder, ports, scalars)``, and returns
  ``Port<output_schema_type>`` (or ``void`` for a sink). The scalar value is built
  as the un-named bundle ``StaticNodeSignature<T>::scalar_schema()`` describes.
- ``wire<G>(w, args...)`` — inlines ``G::compose(w, …)`` (graphs flatten) and
  returns ``G``'s output port. The arguments follow the **same rule as for a node**
  (via ``StaticGraphSignature<G>``): in compose-parameter order, a ``Port`` for each
  ``Port`` parameter (schema-checked, then passed to ``compose`` as the declared
  port type) and a scalar argument for each ``Scalar`` parameter (wrapped into it,
  convertibility-checked) — so a sub-graph and a node are wired identically at the
  call site. An erased generic-source port is checked against the declared
  sub-graph input schema before it is retyped for ``compose``.
- **Scalar arguments unpack uniformly.** Every scalar wiring argument (in
  ``wire<T>``, ``wire<G>`` and ``build_graph``) passes through one helper,
  ``graph_wiring_detail::coerce_scalar_value<V>``: it accepts either a plain value or
  a ``Scalar<Name, T>`` **selector** (whose value it unpacks), so a scalar received
  as a node/graph parameter can be forwarded straight on — ``wire<Shift>(w, x, by)``
  rather than ``…, by.value()``. Only the value type must be convertible; the
  ``Name`` need not match (the producer's and consumer's scalar names are
  independent).
- ``StaticGraphSignature<G>`` — **implemented.** Reflects ``&G::compose``
  **skipping the leading ``Wiring&``**: ``Port`` parameters are the graph's
  time-series inputs, ``Scalar`` parameters are its scalar inputs, and the return
  type is its time-series output(s). It exposes ``param_types`` (the ordered
  parameter selector list), ``output_type``, and ``param_count`` / ``input_count``
  / ``scalar_count``. This is the graph-level mirror of ``StaticNodeSignature``.
- ``build_graph<G>(values...)`` — **implemented for a top-level graph.** Constructs
  a ``Wiring``, wraps each supplied plain value into the corresponding
  ``Scalar<>`` ``compose`` parameter (checked against ``StaticGraphSignature<G>``:
  ``input_count() == 0`` — a top-level graph has **no** time-series inputs or
  outputs — and the argument count matches ``scalar_count()``), calls
  ``G::compose(w, scalars…)``, and returns ``w.finish()``. Supplying time-series
  boundary input ports (for standalone sub-graph building) is a later slice.

The compile-time checks are the C++ advantage over Python; the core re-validates
schemas at wiring time as a safety net (and as the only check Python relies on).

.. note::

   The graph's body method is named ``compose`` and the wiring verb is ``wire`` —
   distinct names — so inside a ``compose`` body you call ``wire<…>`` directly,
   without qualification.


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

Slices:

1. **Done.** The shared ``Wiring`` core (``WiringInstance`` interning by
   ``(typeid(T), input ports)`` + Kahn topo-sort/rank), the ``StaticNodeSignature``
   extension exposing the output schema type, the typed ``Port<Schema>`` +
   ``wire<X>(w, …)`` facade (compile-time arity **and per-port schema matching**),
   ``build_graph<G>()`` for a top-level graph, and **sub-graph composition** —
   ``wire<X>`` dispatches on
   whether ``X`` is a node (``eval``; adds a runtime node) or a graph (``wire``;
   inlined and flattened). Tests: ``tests/cpp/test_graph_wiring.cpp``.
2. **Done.** Scalar inputs: ``wire<T>`` takes ``Scalar<>`` arguments in
   eval-parameter order, builds the compound scalar value, **folds the scalar
   values into the intern key** (so equal scalars dedup, distinct scalars do not),
   and records them on the ``NodeBuilder``. Node-layer scalar storage and the
   ``Scalar<>`` authoring selector back it (see *Authoring Nodes in C++*).
3. **3a — Done.** ``StaticGraphSignature<G>`` (reflects ``compose`` skipping
   ``Wiring&``; classifies ``Port`` vs ``Scalar`` parameters; exposes the output
   type) and **graph-level scalar parameters**: ``build_graph<G>(values…)`` wraps
   the supplied values into the graph's ``Scalar<>`` ``compose`` parameters and
   forwards them. Tests: ``tests/cpp/test_graph_wiring.cpp``.
4. **3b — Done.** ``wire<G>`` parity with ``wire<T>``: the sub-graph branch now
   reflects ``compose`` via ``StaticGraphSignature<G>``, checks argument arity and
   per-position port schema / scalar convertibility at compile time, and **auto-wraps
   scalar literals** into the sub-graph's ``Scalar<>`` parameters. A sub-graph and a
   node are now wired identically at the call site. Tests:
   ``tests/cpp/test_graph_wiring.cpp``.
5. **Generic (type-variable) nodes — done.** A node authored over ``TsVar`` /
   ``ScalarVar`` (one implementation, no per-type instantiation) is resolved to a
   concrete node at the ``wire<>`` call. ``wire<>`` builds a ``ResolutionMap``
   (``include/hgraph/types/type_resolution.h``): each input selector's pattern is
   **unified** against the connected port's runtime schema, a scalar variable is
   inferred from the configured value's type, and a source-side output variable is
   supplied **explicitly** — either ``wire<replay, TS<Int>>(w, key)`` (an
   explicit output schema, which also makes the returned port the typed
   ``Port<TS<Int>>``) or
   via ``ts_type<...>()`` / ``scalar_type<...>()`` helpers. The resolved schemas
   build the node through ``NodeBuilder::implementation<T>(map)``. When a generic
   output type is only known at wiring (resolved from inputs/values, not supplied),
   ``wire<>`` returns the **erased** ``Port<void>`` carrying the runtime schema;
   downstream ``wire<>`` accepts it (typed or erased). The resolved schema pointers
   are part of the node's interning key, so distinct resolutions of one definition do
   not collide. The framework's own ``replay`` / ``record`` / ``const_`` /
   ``debug_print`` / ``null_sink`` are authored this way. Tests:
   ``tests/cpp/test_type_resolution.cpp``.
6. **Next.** Standalone sub-graph building / time-series **boundary binding**
   (supplying ``Port`` inputs to ``build_graph`` / ``wire<G>``) — **deferred until
   it has a consumer**, the non-flattening nested graphs (``map_`` / ``reduce`` /
   ``switch_``), which is where the boundary substrate is actually needed.

Deferred: **by-name graph/node scalar arguments and parameter defaults** (today
arguments are positional and all required — a compile-time ``arg<"name">(value)``
matched to the ``Scalar<Name, T>`` parameter, plus defaults for omitted arguments,
are the planned additions to ``build_graph<G>`` / ``StaticGraphSignature``);
multiple outputs (``TSB`` ports, optionally returned as an array as sugar);
**graph-level** generic resolution (``TsVar`` / ``ScalarVar`` in a *graph*
``compose`` signature — node-level resolution above is done); higher-order operators
and feedback; dead-node pruning; and the Python bridge that drives the core.
