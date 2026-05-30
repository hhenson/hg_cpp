Wiring
======

Wiring is graph construction: it turns user-authored node and graph
descriptions into a runtime graph definition. For hg_cpp the **C++-first
static wiring** path is primary — a node is authored as a small C++ type and
assembled into a graph with the runtime builders. Python wiring remains a
compatibility goal that *lowers* to the same runtime primitives (deferred; see
*Project Boundary*).

Authoring a node
----------------

A node implementation is a **stateless struct** with a static ``eval`` hook
(and optional static ``start`` / ``stop``). Its parameters are *selector*
markers (see *Schemas > Static Schema*) that pair a compile-time schema with a
runtime view:

.. code-block:: cpp

   #include <hgraph/types/static_node.h>

   struct AddOne
   {
       static constexpr auto name = "add_one";

       static void eval(In<"in", TS<int>> in, Out<TS<int>> out)
       {
           out.set(in.value() + 1);
       }
   };

   struct Counter            // a source with node-local state
   {
       static constexpr auto name      = "counter";
       static constexpr NodeKind node_kind = NodeKind::PullSource;

       static void start(State<int> state) { state.set(0); }
       static void eval(State<int> state, Out<TS<int>> out)
       {
           const int next = state.get() + 1;
           state.set(next);
           out.set(next);
       }
   };

The selectors used at this layer are:

- ``In<Name, TS<T>>`` — a typed input. ``value()`` reads the current value,
  ``modified()`` / ``valid()`` report tick state.
- ``Out<TS<T>>`` — a typed output. ``set(v)`` writes the value and ticks the
  output at the current evaluation time.
- ``State<T>`` — a typed handle into node-local (value-layer) state.

Parameters are injected **by type, not position**: a hook lists only the
selectors it needs and the runtime synthesises each from the node and the
current evaluation time. ``eval`` (and ``start`` / ``stop``) must be ``static``
and return ``void``, and the implementation type must be empty/stateless.

Signature extraction
---------------------

``StaticNodeSignature<TImplementation>`` reflects the ``eval`` (and
``start`` / ``stop``) parameter lists at compile time and derives the runtime
node contract:

- **input schema** — the ``In<>`` parameters become the fields of a non-peered
  input ``TSB`` (in argument order, by their ``Name``);
- **output schema** — the single ``Out<>`` parameter's schema (at most one);
- **state schema** — the ``State<T>`` parameter's value-layer schema;
- **node kind** — inferred from which selectors are present: ``In`` and ``Out``
  → ``Compute``; ``Out`` only → ``PullSource``; ``In`` only → ``Sink``. A
  ``static constexpr NodeKind node_kind`` member overrides the inference;
- **input endpoint** — a ``TSEndpointSchema`` of ``non_peered(input_tsb, {
  peered(field)… })``, one peered terminal per input.

Mapping onto the runtime (no parallel mechanism)
------------------------------------------------

``NodeBuilder::implementation<TImplementation>()`` is a **typed front-end over
the existing** ``NodeBuilder::native(...)``. It assembles exactly the triple
``native`` already consumes and delegates to it:

.. code-block:: text

   StaticNodeSignature<T>  ->  ( NodeTypeMetaData,        // kind + input/output/state schema
                                 NodeCallbacks,           // eval/start/stop thunks
                                 TSEndpointSchema )       // input peering annotation
                            ->  NodeBuilder::native(...)

The ``NodeCallbacks`` thunks are stateless lambdas that call
``static_node_detail::invoke<&T::eval>(view, evaluation_time)``. ``invoke``
walks the hook's parameter types and, for each, asks an ``arg_provider`` to
build the selector from the type-erased ``NodeView``:

- ``In<Name, TS<T>>`` ← ``view.input(t).as_bundle().field(Name)``
- ``Out<TS<T>>`` ← ``view.output(t)`` (carrying ``t`` for ``set``)
- ``State<T>`` ← ``view.state()``

Because the static layer produces the same ``native`` inputs the hand-written
path uses, there is **one** runtime node model; static authoring is sugar over
it, not a second mechanism.

Assembling a graph
------------------

Nodes are assembled with ``GraphBuilder`` and connected with ``GraphEdge``
(``source_node`` / ``source_path`` → ``target_node`` / ``target_path``). Paths
index into ``TSB`` / ``TSL`` structure; an input ``target_path`` of ``{0}``
selects the first field of the node's input bundle.

.. code-block:: cpp

   GraphBuilder builder;
   builder.add_node(NodeBuilder{}.label("src").implementation<ConstantSource>())
          .add_node(NodeBuilder{}.label("inc").implementation<AddOne>())
          .add_edge(GraphEdge{.source_node = 0, .source_path = {},
                              .target_node = 1, .target_path = {0}});

   GraphExecutorBuilder executor;
   executor.graph_builder(std::move(builder)).start_time(MIN_ST).end_time(end);
   executor.make_executor().view().run();

Project Boundary
----------------

Python wiring remains a compatibility goal for the existing hgraph ecosystem.
When built, it must *lower* into the same runtime construction data the static
path produces (``NodeTypeMetaData`` + ``NodeCallbacks`` + ``TSEndpointSchema``
via ``native``), so the runtime never depends on Python wiring for
correctness. The runtime should validate the invariants that protect memory
safety, and trust the type-checking done at the wiring layer.

Status
------

Implemented (the scalar time-series slice): ``In<Name, TS<T>>`` /
``Out<TS<T>>`` / ``State<T>`` selectors, ``StaticNodeSignature``,
``NodeBuilder::implementation<T>()``, ``start`` / ``stop`` / ``eval`` hooks,
node-kind inference (Compute / PullSource / Sink) with a ``node_kind``
override, and ``GraphBuilder`` / ``GraphEdge`` assembly. A static
source → compute graph builds and runs in simulation mode.

Deferred (land with the relevant runtime layer):

- container-shaped selectors over ``TSB`` / ``TSL`` / ``TSS`` / ``TSD`` /
  ``TSW`` inputs and outputs (non-peered prefixes, nested endpoint
  annotations);
- ``RecordableState``, ``EvaluationClock`` / ``NodeScheduler`` injection;
- push-source ``apply_message`` hooks and ``ScalarArg`` injection;
- input ``InputActivity`` / ``InputValidity`` policy flags, named state;
- generic resolution (``TsVar`` / ``ScalarVar``) and the Python lowering.
