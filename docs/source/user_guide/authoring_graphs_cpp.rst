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
``static void compose(Wiring &w, Scalar<"window", Int> window)`` built with
``build_graph<MyGraph>(Int{20})`` (the typed value is wrapped into the
``window`` parameter). The simplest graph takes no scalars; its ``compose`` body
just adds nodes:

The standard aliases ``Bool``/``Int``/``Float``/``Str`` map to the hgraph/Python
scalar vocabulary. For shorter wiring call sites, opt into
``hgraph::literals`` and use literals such as ``20_i``, ``2.0_f`` and
``"out"_str``; they are not imported by default.

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

       static void compose(Wiring &w, Scalar<"factor", Float> factor)
       {
           auto px = wire<ConstantSource>(w);
           wire<Print>(w, wire<Scale>(w, px, factor));   // graph scalar forwarded to node scalar
       }
   };

   using namespace hgraph::literals;

   GraphBuilder g = build_graph<ScaledPriceGraph>(2.0_f);   // factor = 2.0

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
       static void compose(Wiring &w, Scalar<"window", Int> window, Scalar<"factor", Float> factor)
       {
           auto px = wire<ConstantSource>(w);
           wire<Print>(w, wire<RollingMean>(w, px, window, factor));
       }
   };

   // window = 20, factor = 1.5 — positional, in compose order.
   using namespace hgraph::literals;

   GraphBuilder g = build_graph<ReportGraph>(20_i, 1.5_f);

The arguments are checked at compile time: the count must match the graph's
``Scalar<>`` parameters and each value must be convertible to its parameter's type.
A graph with no scalar parameters is simply ``build_graph<G>()``. The resulting
``GraphBuilder`` is handed to a ``GraphExecutor`` exactly as for a scalar-free graph
(see *Running a graph*).

.. note::

   **Positional only, for now.** Arguments are matched by position. Supplying them
   **by name** (so order does not matter, e.g. ``arg<"factor">(Float{1.5})``) and
   **default values** for omitted parameters are planned but not yet implemented.


Ports
-----

A ``Port<TS<Int>>`` is a **wiring-time handle** to a node's output — not a value.
You obtain one from ``wire<T>(...)`` and pass it as an input to another node:

.. code-block:: cpp

   Port<TS<Int>> a = wire<ConstantSource>(w);   // handle to the source's output
   Port<TS<Int>> s = wire<Sum>(w, a, a);        // a is fed into both inputs of Sum

Ports are typed, so passing the wrong port type — or the wrong number of inputs —
to ``wire<T>`` is a **compile error**. (Python catches the same mistakes, but only
when the graph is wired at run time.)

``SIGNAL`` is the exception on the input side: a ``Port<TS<Int>>``,
``Port<TSD<...>>`` or any other time-series output port may be passed to a node or
sub-graph input declared as ``SIGNAL``. The input observes the upstream tick rather
than the upstream value.

For standard operators, a compose body may opt into expression syntax:

.. code-block:: cpp

   using namespace hgraph::stdlib::syntax;

   auto a = wire<testing::replay, TS<Int>>(w, "a");
   auto b = wire<testing::replay, TS<Int>>(w, "b");
   auto c = (a + b * Int{2}).as<TS<Int>>();

The C++ operators are only syntax for the standard operator registry
(``+`` -> ``stdlib::add_``, ``*`` -> ``stdlib::mul_``, comparisons -> ``TS<Bool>``, and so on).
They return an erased ``Port<void>`` because overload resolution can change the result
type, such as ``int / int -> float``. Use ``.as<Schema>()`` when a graph return or
downstream API needs a typed ``Port<Schema>``.


Supported standard operator overloads
-------------------------------------

Include ``<hgraph/lib/std/std_operators.h>`` and call
``stdlib::register_standard_operators()`` before wiring graphs that use these operators.
The tables below list the overloads currently registered by that call. Operator markers
outside this table may be declared for catalogue completeness, but they are not wired
until a concrete implementation is registered.

Notation:

- ``Int`` is the standard hgraph integer scalar (``std::int64_t``), ``Float`` is
  ``double``, ``Str`` is ``std::string``.
- ``Date`` aliases ``engine_date_t``, ``DateTime`` aliases ``engine_time_t``, and
  ``TimeDelta`` aliases ``engine_time_delta_t``.
- Every operand and result below is a time-series unless stated otherwise:
  ``Int + Float -> Float`` means ``TS<Int> + TS<Float> -> TS<Float>``.
- A plain scalar argument may be supplied where an operator expects ``TS<T>``; wiring
  promotes it through an internal const source, so ``a + Int{2}`` is valid.

Arithmetic
~~~~~~~~~~

.. list-table::
   :header-rows: 1

   * - Operator
     - Registered overloads
     - Result
     - Notes
   * - ``add_`` / ``+``
     - ``Int + Int``; ``Float + Float``; ``Int + Float``; ``Float + Int``;
       ``Str + Str``; ``TimeDelta + TimeDelta``; ``DateTime + TimeDelta``;
       ``TimeDelta + DateTime``; ``Date + TimeDelta``
     - same as operands for homogeneous cases; ``Float`` for mixed numeric;
       ``DateTime`` for ``DateTime``/``TimeDelta``; ``Date`` for ``Date``/``TimeDelta``
     - ``Date + TimeDelta`` advances by whole days.
   * - ``sub_`` / ``-``
     - ``Int - Int``; ``Float - Float``; ``Int - Float``; ``Float - Int``;
       ``TimeDelta - TimeDelta``; ``DateTime - TimeDelta``;
       ``DateTime - DateTime``; ``Date - Date``
     - same as operands for homogeneous numeric/``TimeDelta``; ``Float`` for mixed numeric;
       ``DateTime`` for ``DateTime`` minus ``TimeDelta``; ``TimeDelta`` for ``DateTime``/``Date``
       differences
     - ``Date - Date`` returns a whole-day ``TimeDelta``.
   * - ``mul_`` / ``*``
     - ``Int * Int``; ``Float * Float``; ``Int * Float``; ``Float * Int``;
       ``Str * Int``; ``Int * Str``
     - ``Int``; ``Float`` for mixed/float numeric; ``Str`` for string repetition
     - Repeating a string zero or fewer times returns an empty string.
   * - ``div_`` / ``/``
     - ``Int / Int``; ``Float / Float``; ``Int / Float``; ``Float / Int``;
       ``TimeDelta / TimeDelta``
     - ``Float``
     - Numeric overloads have an optional ``DivideByZero`` scalar policy. Duration
       division does not currently take that policy.
   * - ``floordiv_`` / ``floordiv(lhs, rhs)``
     - ``Int // Int``; ``Float // Float``; ``Int // Float``; ``Float // Int``
     - ``Int`` for ``Int // Int``; otherwise ``Float``
     - Uses Python-style floor semantics, not C++ truncation. Optional
       ``DivideByZero`` policy.
   * - ``mod_`` / ``%``
     - ``Int % Int``; ``Float % Float``; ``Int % Float``; ``Float % Int``
     - ``Int`` for ``Int % Int``; otherwise ``Float``
     - Uses Python-style modulo semantics. Optional ``DivideByZero`` policy.
   * - ``pow_`` / ``pow(lhs, rhs)``
     - ``Int ** Int``; ``Float ** Float``; ``Int ** Float``; ``Float ** Int``
     - ``Float``
     - Numeric power is explicitly float-valued in C++. Optional ``DivideByZero``
       policy for ``0 ** negative``.
   * - ``neg_`` / unary ``-``
     - ``Int``; ``Float``; ``TimeDelta``
     - same type
     -
   * - ``pos_`` / unary ``+``
     - ``Int``; ``Float``; ``TimeDelta``
     - same type
     -
   * - ``abs_`` / ``abs(ts)``
     - ``Int``; ``Float``; ``TimeDelta``
     - same type
     -
   * - ``sign`` / ``sign(ts)``
     - ``Int``; ``Float``
     - same type
     - Returns ``-1``, ``0`` or ``1``.
   * - ``ln`` / ``ln(ts)``
     - ``Float``
     - ``Float``
     -

``DivideByZero`` policies are wiring-time scalar arguments. The two-argument form of
``div_`` / ``floordiv_`` / ``mod_`` / ``pow_`` defaults to ``DivideByZero::Error``; the
three-argument form accepts a policy value. Numeric ``div_`` accepts all policies
(``Error``, ``Nan``, ``Inf``, ``NoTick``, ``Zero``, ``One``). ``floordiv_`` accepts the
same policies for float/mixed numeric overloads; ``Int // Int`` accepts ``Error``,
``NoTick``, ``Zero`` and ``One``. ``mod_`` accepts ``Nan`` / ``Inf`` / ``NoTick`` for
float/mixed numeric overloads, while ``Int % Int`` only treats ``NoTick`` specially;
the other zero-divisor cases throw. ``pow_`` applies the policy only for
``0 ** negative``.

Comparison
~~~~~~~~~~

.. list-table::
   :header-rows: 1

   * - Operator
     - Registered overloads
     - Result
     - Notes
   * - ``eq_`` / ``==``
     - same-type ``Bool``, ``Int``, ``Float``, ``Str``, ``Date``, ``DateTime``,
       ``TimeDelta``; mixed ``Int``/``Float``
     - ``Bool``
     -
   * - ``ne_`` / ``!=``
     - same-type ``Bool``, ``Int``, ``Float``, ``Str``, ``Date``, ``DateTime``,
       ``TimeDelta``; mixed ``Int``/``Float``
     - ``Bool``
     -
   * - ``lt_`` / ``<``; ``le_`` / ``<=``; ``gt_`` / ``>``; ``ge_`` / ``>=``
     - same-type ``Int``, ``Float``, ``Str``, ``Date``, ``DateTime``, ``TimeDelta``;
       mixed ``Int``/``Float``
     - ``Bool``
     - ``Bool`` ordering is not registered.
   * - ``cmp_`` / ``cmp(lhs, rhs)``
     - same-type ``Int``, ``Float``, ``Str``, ``Date``, ``DateTime``, ``TimeDelta``;
       mixed ``Int``/``Float``
     - ``CmpResult``
     - Returns ``CmpResult::LT``, ``CmpResult::EQ`` or ``CmpResult::GT``.

Logical and bitwise
~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1

   * - Operator
     - Registered overloads
     - Result
     - Notes
   * - ``not_`` / ``!``
     - ``Bool``; ``Int``; ``Float``; ``Str``
     - ``Bool``
     - Empty string is false; non-empty string is true.
   * - ``and_`` / ``&&``; ``or_`` / ``||``
     - ``Bool``/``Bool``; ``Int``/``Int``; ``Float``/``Float``; ``Str``/``Str``;
       mixed ``Int``/``Float``
     - ``Bool``
     - These are graph operators, so C++ ``&&`` / ``||`` syntax does not short-circuit.
   * - ``invert_`` / ``~``
     - ``Int``
     - ``Int``
     -
   * - ``bit_and`` / ``&``; ``bit_or`` / ``|``; ``bit_xor`` / ``^``
     - ``Int``/``Int``; ``Bool``/``Bool``
     - same type
     - ``Bool`` bitwise overloads use logical bool semantics.
   * - ``lshift_`` / ``<<``; ``rshift_`` / ``>>``
     - ``Int`` shifted by ``Int``
     - ``Int``
     - Shift counts must be non-negative and smaller than the number of value bits.

Sources, conversions and sinks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1

   * - Operator
     - Registered overloads
     - Result
     - Notes
   * - ``const_``
     - ``const_(value)``; ``const_(value, delay)``
     - Defaults to ``TS<T>`` for scalar value type ``T``; may be explicitly resolved to
       any output whose current-value schema matches the supplied value.
     - Emits one tick at graph start, or at ``start + delay`` for the delayed overload.
   * - ``zero_``
     - explicit output ``TS<Int>``; ``TS<Float>``; ``TS<Str>``
     - selected output schema
     - Emits ``0``, ``0.0`` or empty string once at graph start.
   * - ``debug_print``
     - ``debug_print(label: Str, ts: any time-series)``
     - sink
     - Prints ``label: value`` on each tick.
   * - ``null_sink``
     - ``null_sink(ts: any time-series)``
     - sink
     - Consumes the input and does nothing.


Configuring a node with scalars
-------------------------------

A node can take **scalar** parameters — read-only, non-time-series configuration
fixed at wiring time (see *Authoring Nodes in C++ > Scalar values and arguments*).
You pass them to ``wire<T>`` **in eval-parameter order**, interleaved with the
input ports exactly as the node's ``eval`` lists them: a ``Port`` for each ``In``
and a plain value for each ``Scalar``.

.. code-block:: cpp

   using namespace hgraph::literals;

   // node: eval(In<"in", TS<Int>> in, Scalar<"delta", Int> delta, Out<TS<Int>> out)
   auto src = wire<ConstantSource>(w);   // 41
   auto out = wire<Shift>(w, src, 5_i);   // port for `in`, then 5 for `delta` -> 46

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
       // logical signature: (TS<Float>, TS<Float>) -> TS<Float>
       static Port<TS<Float>> compose(Wiring &w, Port<TS<Float>> bid, Port<TS<Float>> ask)
       {
           return wire<Average>(w, bid, ask);
       }
   };

.. code-block:: python

   @graph
   def mid(bid: TS[float], ask: TS[float]) -> TS[float]:
       return average(bid, ask)

The logical signature ``(TS<Float>, TS<Float>) -> TS<Float>`` is the same one
the Python ``@graph`` declares. The leading ``Wiring &w`` is plumbing (always the
first parameter, by convention); it is not part of the logical signature.


Composing graphs
----------------

``wire<G>(w, args...)`` wires a sub-graph. Because graphs flatten, this **inlines**
the sub-graph's nodes into the current graph and returns its output port — there
is no runtime "graph node". A call site treats a node and a graph **the same way**:
you pass a ``Port`` for each of the sub-graph's ``Port`` parameters and a plain
value for each of its ``Scalar`` parameters, in ``compose`` order, and the
arguments are checked exactly as for a node. Typed ports are checked at compile
time; erased generic-source ports are checked at wiring time and then passed to
``compose`` as the declared port type. The only difference is whether a runtime
node is produced.

.. code-block:: cpp

   static Port<TS<Float>> compose(Wiring &w, Port<TS<Float>> bid, Port<TS<Float>> ask)
   {
       auto m = wire<Mid>(w, bid, ask);   // inline the Mid sub-graph
       return wire<Scale>(w, m);
   }

A sub-graph may take scalar parameters too; pass the value just like a node's
scalar (it is wrapped into the sub-graph's ``Scalar<>`` parameter):

.. code-block:: cpp

   using namespace hgraph::literals;

   // sub-graph: compose(Wiring &, Port<TS<Int>> x, Scalar<"by", Int> by) -> TS<Int>
   auto shifted = wire<ShiftBy>(w, src, 5_i);   // port for `x`, 5 wrapped into `by`


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
   gb.global_state().set("seed", Value{Int{20}});   // seed at wiring time

   GraphExecutorBuilder ex;
   ex.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(end);
   GraphExecutorValue executor = ex.make_executor();
   executor.view().run();

   // read results the nodes wrote into the store
   const Int total = executor.view().graph().global_state().get_as<Int>("total");

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
           w.global_state().set("counter", Value{Int{100}});   // set at wiring time
           wire<BumpCounter>(w);                           // a node whose eval bumps "counter"
       }
   };
   // after running: global_state().get_as<Int>("counter") == 101

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
   * - ``Port<TS<Int>>``
     - a wiring-time time-series handle
   * - ``build_graph<G>()`` → ``GraphExecutor``
     - wiring the ``@graph`` + ``run_graph(...)``
