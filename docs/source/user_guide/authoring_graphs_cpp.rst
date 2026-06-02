Wiring Graphs in C++
====================

A *graph* composes nodes — and other graphs — into a runnable program. In C++ a
graph is a small ``struct`` with a static ``compose`` method: the graph counterpart
of authoring a node (see *Authoring Nodes in C++*). It mirrors a Python
``@graph``: a signature of time-series inputs and outputs whose body wires the
sub-components together.

Wiring runs **at wiring time, not at run time** — ``compose`` executes once to build
the graph, never during evaluation. Graphs also **flatten**: composing graphs
inlines their nodes, and only nodes exist at run time.

.. note::

   **Status.** The core is **implemented**: ``Wiring``, typed ``Port<Schema>``,
   ``wire<T>`` for nodes (including **scalar arguments** to a wired node),
   ``wire<G>`` sub-graph composition (graphs flatten), and ``build_graph<G>(…)`` for
   a top-level graph — including **graph-level scalar parameters** (a top-level
   graph's ``compose`` may take ``Scalar<>`` parameters, supplied through
   ``build_graph<G>(values…)``). Still **not yet implemented** (see *What's
   planned*): standalone sub-graph building / time-series boundary inputs, generic
   graphs and higher-order operators. A graph's body method is named ``compose`` and
   the wiring verb is ``wire`` — distinct names, so inside a ``compose`` body you
   call ``wire<…>`` directly. How it works internally, and how it is shared with
   Python wiring, is in *Developer Guide > Graph Wiring*.


A first graph
-------------

A top-level graph has **no time-series inputs or outputs** — its sources generate
data and its sinks consume it. It *may*, however, take **scalar** inputs: plain
(non-time-series) configuration values that parameterise the graph at wiring time.
Graph-level scalar inputs appear as extra ``compose`` parameters and are supplied
to ``build_graph`` — e.g.
``static void compose(Wiring &w, Scalar<"window", int> window)`` built with
``build_graph<MyGraph>(20)`` (the plain value is wrapped into the ``window``
parameter). The simplest graph takes no scalars; its ``compose`` body just adds
nodes:

.. code-block:: cpp

   struct PriceGraph
   {
       static constexpr auto name = "price_graph";

       static void compose(Wiring &w)
       {
           auto a = wire<ConstantSource>(w);
           auto b = wire<ConstantSource>(w);
           wire<Print>(w, wire<Sum>(w, a, b));
       }
   };

   GraphBuilder g = build_graph<PriceGraph>();   // runs compose() once, at wiring time

A graph that takes a scalar parameter is built by supplying the value:

.. code-block:: cpp

   struct ScaledPriceGraph
   {
       static constexpr auto name = "scaled_price_graph";

       static void compose(Wiring &w, Scalar<"factor", double> factor)
       {
           auto px = wire<ConstantSource>(w);
           wire<Print>(w, wire<Scale>(w, px, factor));   // graph scalar forwarded to node scalar
       }
   };

   GraphBuilder g = build_graph<ScaledPriceGraph>(2.0);   // factor = 2.0

A scalar received as a parameter (here ``factor``) can be passed **straight on** to
a node's or sub-graph's scalar — the wiring layer unpacks it for you, so an explicit
``factor.value()`` is not required (it still works if you prefer it). The parameter
names need not match; only the value type must.

.. code-block:: python

   @graph
   def price_graph():
       a, b = constant_source(), constant_source()
       print_(sum_(a, b))

``wire<T>(w, ports...)`` adds node ``T`` to the graph ``w`` and returns a handle
(a *port*) to its output; passing ports as the inputs of another ``wire<T>``
connects them. You never write node indices or edges by hand, and you never have
to add nodes in any particular order — see *Ordering is automatic*.


Graph scalar parameters
-----------------------

A top-level graph's ``Scalar<>`` parameters are its **build parameters** — supplied
when you build the graph, alongside (conceptually) the run window you later give the
executor. They are consumed at **wiring time**: ``build_graph<G>(values…)`` runs
``compose`` with the supplied values, so the values must be known before the
``GraphBuilder`` is produced (you cannot defer them to the executor, which receives
an already-built graph).

Pass one value per ``Scalar<>`` parameter, **in ``compose``-parameter order**:

.. code-block:: cpp

   struct ReportGraph
   {
       static constexpr auto name = "report_graph";
       //                                   two scalar build parameters
       static void compose(Wiring &w, Scalar<"window", int> window, Scalar<"factor", double> factor)
       {
           auto px = wire<ConstantSource>(w);
           wire<Print>(w, wire<RollingMean>(w, px, window, factor));
       }
   };

   // window = 20, factor = 1.5 — positional, in compose order.
   GraphBuilder g = build_graph<ReportGraph>(20, 1.5);

The arguments are checked at compile time: the count must match the graph's
``Scalar<>`` parameters and each value must be convertible to its parameter's type.
A graph with no scalar parameters is simply ``build_graph<G>()``. The resulting
``GraphBuilder`` is handed to a ``GraphExecutor`` exactly as for a scalar-free graph
(see *Running a graph*).

.. note::

   **Positional only, for now.** Arguments are matched by position. Supplying them
   **by name** (so order does not matter, e.g. ``arg<"factor">(1.5)``) and
   **default values** for omitted parameters are planned but not yet implemented.


Ports
-----

A ``Port<TS<int>>`` is a **wiring-time handle** to a node's output — not a value.
You obtain one from ``wire<T>(...)`` and pass it as an input to another node:

.. code-block:: cpp

   Port<TS<int>> a = wire<ConstantSource>(w);   // handle to the source's output
   Port<TS<int>> s = wire<Sum>(w, a, a);        // a is fed into both inputs of Sum

Ports are typed, so passing the wrong port type — or the wrong number of inputs —
to ``wire<T>`` is a **compile error**. (Python catches the same mistakes, but only
when the graph is wired at run time.)


Configuring a node with scalars
-------------------------------

A node can take **scalar** parameters — read-only, non-time-series configuration
fixed at wiring time (see *Authoring Nodes in C++ > Scalar values and arguments*).
You pass them to ``wire<T>`` **in eval-parameter order**, interleaved with the
input ports exactly as the node's ``eval`` lists them: a ``Port`` for each ``In``
and a plain value for each ``Scalar``.

.. code-block:: cpp

   // node: eval(In<"in", TS<int>> in, Scalar<"delta", int> delta, Out<TS<int>> out)
   auto src = wire<ConstantSource>(w);   // 41
   auto out = wire<Shift>(w, src, 5);    // port for `in`, then 5 for `delta` -> 46

The scalar value is checked against the node's ``Scalar<>`` type at compile time,
and it becomes part of the node's wiring identity: two ``wire`` calls dedup only
when their scalars are **also** equal (see *Identical nodes are shared*).


Identical nodes are shared
--------------------------

Wiring the **same node with the same inputs** gives you back the **same** node —
the wiring layer interns nodes (wiring-time common-subexpression elimination). Two
calls are equivalent when the node type matches and every input is equal: ports
are equal when they come from the same producing node, scalar inputs when their
values are equal. So you can re-derive a value freely without creating duplicates:

.. code-block:: cpp

   auto x  = wire<Source>(w);
   auto y1 = wire<AddOne>(w, x);
   auto y2 = wire<AddOne>(w, x);   // same inputs as y1 -> the SAME node; y2 == y1

To get two *distinct* nodes, give them distinct inputs — for a source, a
distinguishing scalar input. (In the first graph above, both
``wire<ConstantSource>(w)`` calls likewise refer to one shared node, since
``ConstantSource`` has no distinguishing inputs.)


Graphs with a signature
------------------------

A sub-graph declares inputs and an output, and composes like a node. The ``compose``
parameters after the context are the graph's inputs; the return is its output:

.. code-block:: cpp

   struct Mid
   {
       static constexpr auto name = "mid";
       // logical signature: (TS<double>, TS<double>) -> TS<double>
       static Port<TS<double>> compose(Wiring &w, Port<TS<double>> bid, Port<TS<double>> ask)
       {
           return wire<Average>(w, bid, ask);
       }
   };

.. code-block:: python

   @graph
   def mid(bid: TS[float], ask: TS[float]) -> TS[float]:
       return average(bid, ask)

The logical signature ``(TS<double>, TS<double>) -> TS<double>`` is the same one
the Python ``@graph`` declares. The leading ``Wiring &w`` is plumbing (always the
first parameter, by convention); it is not part of the logical signature.


Composing graphs
----------------

``wire<G>(w, args...)`` wires a sub-graph. Because graphs flatten, this **inlines**
the sub-graph's nodes into the current graph and returns its output port — there
is no runtime "graph node". A call site treats a node and a graph **the same way**:
you pass a ``Port`` for each of the sub-graph's ``Port`` parameters and a plain
value for each of its ``Scalar`` parameters, in ``compose`` order, and the
arguments are checked at compile time exactly as for a node. The only difference is
whether a runtime node is produced.

.. code-block:: cpp

   static Port<TS<double>> compose(Wiring &w, Port<TS<double>> bid, Port<TS<double>> ask)
   {
       auto m = wire<Mid>(w, bid, ask);   // inline the Mid sub-graph
       return wire<Scale>(w, m);
   }

A sub-graph may take scalar parameters too; pass the value just like a node's
scalar (it is wrapped into the sub-graph's ``Scalar<>`` parameter):

.. code-block:: cpp

   // sub-graph: compose(Wiring &, Port<TS<int>> x, Scalar<"by", int> by) -> TS<int>
   auto shifted = wire<ShiftBy>(w, src, 5);   // port for `x`, 5 wrapped into `by`


Ordering is automatic
---------------------

You wire nodes in whatever order is convenient. When the graph is built it is
**topologically sorted and ranked** so that each node evaluates after the nodes
it depends on — you never assign or reason about evaluation order yourself. This
is the main difference from assembling a ``GraphBuilder`` by hand, where node
order is the caller's responsibility.


Running a graph
---------------

``build_graph<G>()`` produces a ``GraphBuilder``; run it through a
``GraphExecutor`` in simulation mode (see *Authoring Nodes in C++ > Assembling
and running a graph*):

.. code-block:: cpp

   GraphExecutorBuilder ex;
   ex.graph_builder(build_graph<PriceGraph>()).start_time(MIN_ST).end_time(end);
   ex.make_executor().view().run();

.. code-block:: python

   run_graph(price_graph, run_mode=EvaluationMode.SIMULATION, start_time=start, end_time=end)


Graph global state
------------------

A graph carries a shared, mutable ``string -> value`` store — the
``GlobalState`` — created on the ``GraphBuilder`` at wiring time and copied onto
each ``GraphValue`` it builds. Seed it before building, and read it back after a
run; a node sees the same store via the ``GlobalStateView`` injectable (see
*Authoring Nodes in C++ > Injected services*):

.. code-block:: cpp

   GraphBuilder gb = build_graph<MyGraph>();
   gb.global_state().set("seed", Value{20});   // seed at wiring time

   GraphExecutorBuilder ex;
   ex.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(end);
   GraphExecutorValue executor = ex.make_executor();
   executor.view().run();

   // read results the nodes wrote into the store
   const int total = executor.view().graph().global_state().get_as<int>("total");

A ``compose`` body can also seed the store **during wiring**, via
``Wiring::global_state()`` — ``finish`` carries those entries onto the graph, so a
value set in ``compose`` is visible to nodes at run time (and can be modified by
their ``eval``):

.. code-block:: cpp

   struct CounterGraph
   {
       static constexpr auto name = "counter_graph";
       static void compose(Wiring &w)
       {
           w.global_state().set("counter", Value{100});   // set at wiring time
           wire<BumpCounter>(w);                           // a node whose eval bumps "counter"
       }
   };
   // after running: global_state().get_as<int>("counter") == 101

Each built graph gets its own copy seeded with the wiring-time entries, so the
builder stays reusable. Values are heterogeneous (a mutable ``Map<string, Any>``
under the hood). This is the store the testing toolkit's replay/record use.


What's planned
--------------

Node wiring, sub-graph composition (with the same scalar-literal ergonomics as
nodes), node-level scalar arguments and top-level graph-level scalar parameters work
today; beyond the basics above the following are deferred (and map onto Python
features):

- **standalone sub-graph building / boundary binding** — supplying time-series
  **input ports** to ``build_graph`` / ``wire<G>`` so a sub-graph can be built on
  its own; this is the precondition for non-flattening nested graphs and is deferred
  until those operators need it;
- **multiple outputs** — a graph returning a ``TSB`` becomes a bundle ``Port``
  with ``.field<"x">()`` to take a sub-port; as syntactic sugar a graph's outputs
  may instead be returned as an array;
- **generic graphs** — ``TsVar`` / ``ScalarVar`` in the ``compose`` signature
  (``TIME_SERIES_TYPE`` / ``SCALAR`` / ``K`` / ``V`` in Python);
- **higher-order operators** — ``map_`` / ``reduce`` / ``switch_`` / feedback,
  which take graphs as arguments; these are where C++ is furthest from Python's
  dynamism.


C++ ↔ Python cheat sheet
------------------------

.. list-table::
   :header-rows: 1
   :widths: 50 50

   * - C++
     - Python
   * - ``struct G { static Out compose(Wiring&, In...){} };``
     - ``@graph def g(in...) -> Out``
   * - ``wire<T>(w, ports...)`` (node)
     - calling a node — ``t(ports...)``
   * - ``wire<G>(w, ports...)`` (sub-graph, inlined)
     - calling a sub-graph — ``g(ports...)``
   * - ``Port<TS<int>>``
     - a wiring-time time-series handle
   * - ``build_graph<G>()`` → ``GraphExecutor``
     - wiring the ``@graph`` + ``run_graph(...)``
