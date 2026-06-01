Wiring Graphs in C++
====================

A *graph* composes nodes — and other graphs — into a runnable program. In C++ a
graph is a small ``struct`` with a static ``wire`` method: the graph counterpart
of authoring a node (see *Authoring Nodes in C++*). It mirrors a Python
``@graph``: a signature of time-series inputs and outputs whose body wires the
sub-components together.

Wiring runs **at wiring time, not at run time** — ``wire`` executes once to build
the graph, never during evaluation. Graphs also **flatten**: composing graphs
inlines their nodes, and only nodes exist at run time.

.. note::

   **Status.** The core is **implemented**: ``Wiring``, typed ``Port<Schema>``,
   ``wire<T>`` for nodes, ``wire<G>`` sub-graph composition (graphs flatten), and
   ``build_graph<G>()`` for a top-level graph. **Scalar inputs**, generic graphs
   and higher-order operators are **not yet implemented** (those examples below are
   provisional). One ergonomic note: inside a graph's own ``wire`` body, call the
   free function **qualified** — ``hgraph::wire<...>`` — because the graph method is
   also named ``wire``; the examples below omit the qualifier for brevity. How it
   works internally, and how it is shared with Python wiring, is in
   *Developer Guide > Graph Wiring*.


A first graph
-------------

A top-level graph has **no time-series inputs or outputs** — its sources generate
data and its sinks consume it. It *may*, however, take **scalar** inputs: plain
(non-time-series) configuration values that parameterise the graph at wiring
time. Scalar inputs appear as extra ``wire`` parameters and are passed to
``build_graph`` — e.g.
``static void wire(Wiring &w, Scalar<"window", int> window)`` built with
``build_graph<MyGraph>(Scalar<"window", int>{20})``. The simplest graph is a
``struct`` whose ``wire`` body just adds nodes:

.. code-block:: cpp

   // Planned — provisional syntax
   struct PriceGraph
   {
       static constexpr auto name = "price_graph";

       static void wire(Wiring &w)
       {
           auto a = wire<ConstantSource>(w);
           auto b = wire<ConstantSource>(w);
           wire<Print>(w, wire<Sum>(w, a, b));
       }
   };

   GraphBuilder g = build_graph<PriceGraph>();   // runs wire() once, at wiring time

.. code-block:: python

   @graph
   def price_graph():
       a, b = constant_source(), constant_source()
       print_(sum_(a, b))

``wire<T>(w, ports...)`` adds node ``T`` to the graph ``w`` and returns a handle
(a *port*) to its output; passing ports as the inputs of another ``wire<T>``
connects them. You never write node indices or edges by hand, and you never have
to add nodes in any particular order — see *Ordering is automatic*.


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

A sub-graph declares inputs and an output, and composes like a node. The ``wire``
parameters after the context are the graph's inputs; the return is its output:

.. code-block:: cpp

   // Planned — provisional syntax
   struct Mid
   {
       static constexpr auto name = "mid";
       // logical signature: (TS<double>, TS<double>) -> TS<double>
       static Port<TS<double>> wire(Wiring &w, Port<TS<double>> bid, Port<TS<double>> ask)
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

``wire<G>(w, ports...)`` wires a sub-graph. Because graphs flatten, this **inlines**
the sub-graph's nodes into the current graph and returns its output port — there
is no runtime "graph node". A call site treats a node and a graph the same way;
the only difference is whether a runtime node is produced.

.. code-block:: cpp

   // Planned — provisional syntax
   static Port<TS<double>> wire(Wiring &w, Port<TS<double>> bid, Port<TS<double>> ask)
   {
       auto m = wire<Mid>(w, bid, ask);   // inline the Mid sub-graph
       return wire<Scale>(w, m);
   }


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


What's planned
--------------

The graph-wiring layer is planned; beyond the basics above the following are
deferred (and map onto Python features):

- **multiple outputs** — a graph returning a ``TSB`` becomes a bundle ``Port``
  with ``.field<"x">()`` to take a sub-port; as syntactic sugar a graph's outputs
  may instead be returned as an array;
- **generic graphs** — ``TsVar`` / ``ScalarVar`` in the ``wire`` signature
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
   * - ``struct G { static Out wire(Wiring&, In...){} };``
     - ``@graph def g(in...) -> Out``
   * - ``wire<T>(w, ports...)`` (node)
     - calling a node — ``t(ports...)``
   * - ``wire<G>(w, ports...)`` (sub-graph, inlined)
     - calling a sub-graph — ``g(ports...)``
   * - ``Port<TS<int>>``
     - a wiring-time time-series handle
   * - ``build_graph<G>()`` → ``GraphExecutor``
     - wiring the ``@graph`` + ``run_graph(...)``
