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

       static void eval(In<"in", TS<Int>> in, Out<TS<Int>> out)
       {
           out.set(in.value() + 1);
       }
   };

   struct Counter            // a source with node-local state
   {
       static constexpr auto name = "counter";

       static void start(State<Int> state) { state.set(0); }
       static void eval(State<Int> state, Out<TS<Int>> out)
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
- ``Scalar<"name", T>`` *(planned)* — a named scalar value (a wiring argument or
  a push-source message); the scalar analog of ``In``.

Each parameter's **selector type** identifies its role, so the ``In`` / ``Out``
/ ``State`` / service parameters may appear in **any position** — the runtime
classifies them by type, not by where they sit. The node's *argument schema*,
however, is **ordered**: the relative order of ``eval``'s inputs defines the
node's arguments and is wired positionally or by keyword (the ``In<>`` name),
matching the equivalent Python signature (the two schemas must agree).
``start`` / ``stop`` differ — each takes only the subset it needs, matched by
name and validated by type. All hooks must be ``static`` and return ``void``,
and the implementation type must be empty/stateless.

Signature extraction
---------------------

``StaticNodeSignature<TImplementation>`` reflects the ``eval`` (and
``start`` / ``stop``) parameter lists at compile time and derives the runtime
node contract:

- **input schema** — the ``In<>`` parameters become the fields of a non-peered
  input ``TSB`` (in argument order, by their ``Name``);
- **output schema** — the single ``Out<>`` parameter's schema (at most one);
- **state schema** — the ``State<T>`` parameter's value-layer schema;
- **node kind** — determined entirely from the node's shape: ``eval`` with
  ``In`` and ``Out`` → ``Compute``; ``Out`` only → ``PullSource``; ``In`` only
  → ``Sink``; an ``apply_message`` hook → ``PushSource`` (planned). There is no
  override — the kind has a single source of truth in the code;
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
node-kind inference (Compute / PullSource / Sink) from shape with no override,
and ``GraphBuilder`` / ``GraphEdge`` assembly. A static source → compute graph
builds and runs in simulation mode.

Deferred (land with the relevant runtime layer):

- container-shaped selectors over ``TSB`` / ``TSL`` / ``TSS`` / ``TSD`` /
  ``TSW`` inputs and outputs (non-peered prefixes, nested endpoint
  annotations);
- ``RecordableState``, ``EvaluationClock`` / ``NodeScheduler`` injection;
- push-source nodes — a required ``start(Sender<T>)`` plus an
  ``apply_message(Scalar<"name", T>, …)`` hook — and ``Scalar<"name", T>``
  arguments;
- input ``InputActivity`` / ``InputValidity`` policy flags, named state;
- the Python lowering, and **graph-level** generic resolution (node-level
  ``TsVar`` / ``ScalarVar`` resolution is now implemented — see *Graph Wiring*).
