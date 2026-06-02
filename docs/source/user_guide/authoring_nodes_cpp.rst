Authoring Nodes in C++
======================

HGraph lets you write nodes and graphs directly in C++. A node is an ordinary,
stateless C++ ``struct`` with a static ``eval`` function; its parameters declare
what the node consumes and produces. The runtime reflects that signature at
compile time and builds the node for you — there is no base class to inherit and
no virtual dispatch in your code.

This page is the C++ counterpart of writing ``@compute_node`` / ``@generator`` /
``@sink_node`` functions in the Python ``hgraph`` package. Every section shows
the C++ form next to the equivalent Python so you can carry your mental model
across.

.. note::

   **Feature status.** This project is C++-first and still being built out. Each
   feature below is tagged:

   - **Available** — compiles and runs in the current build.
   - **Planned** — shown for completeness so the full model is visible. The
     syntax is provisional and **does not compile yet**; planned C++ snippets
     are marked with a ``// Planned`` comment.

   A complete matrix is in `Feature status`_ at the end.

To use the node-authoring API, include one header:

.. code-block:: cpp

   #include <hgraph/types/static_node.h>   // selectors + NodeBuilder::implementation<T>()
   #include <hgraph/runtime/runtime.h>      // GraphBuilder, GraphExecutor


Your first node
---------------

**Available.** A compute node that adds two integer time-series:

.. code-block:: cpp

   struct Add
   {
       static constexpr auto name = "add";

       static void eval(In<"lhs", TS<int>> lhs, In<"rhs", TS<int>> rhs, Out<TS<int>> out)
       {
           out.set(lhs.value() + rhs.value());
       }
   };

The Python equivalent:

.. code-block:: python

   from hgraph import compute_node, TS

   @compute_node
   def add(lhs: TS[int], rhs: TS[int]) -> TS[int]:
       return lhs.value + rhs.value

The correspondence is direct:

- the C++ ``struct`` ↔ the decorated Python function,
- each ``In<Name, TS<T>>`` parameter ↔ a Python time-series parameter,
- the single ``Out<TS<T>>`` parameter ↔ the Python ``-> TS[int]`` return
  annotation (C++ writes the result with ``out.set(...)`` instead of
  ``return``).


The shape of a C++ node
-----------------------

A node implementation must be:

- a **stateless** ``struct`` / ``class`` (empty — it holds no members; all state
  lives in ``State`` selectors, see `Node-local state`_),
- with a ``static void eval(...)`` function,
- whose parameters are **selectors** (``In`` / ``Out`` / ``State`` / …).

The selector **type** of each parameter identifies its role, so the ``In`` /
``Out`` / ``State`` / service parameters may sit in **any position** in the C++
signature — the runtime classifies each by its type, not by where it sits. (This
type-matching of the *annotations* is the only thing that is position-free, and
is what distinguishes a marker from an argument.)

The node's **argument schema is ordered**, exactly like the equivalent Python
node. The relative order of the ``In<>`` (and scalar) parameters defines the
node's arguments, and a caller wires them **positionally, or by keyword** using
the ``In<>`` name — the same calling convention Python uses, and the two schemas
must match. Inputs and scalar arguments are supplied by the caller / wiring;
outputs, state and injected services are supplied by the runtime. By convention,
place the ``Out`` parameter **last**, so a signature reads its inputs first and
its output last.

.. code-block:: cpp

   // 'Out' is identified by its type so it may sit anywhere, but by convention it
   // goes last. The input order (lhs, rhs) is the argument schema.
   static void eval(In<"lhs", TS<int>> lhs, In<"rhs", TS<int>> rhs, Out<TS<int>> out);

``start`` and ``stop`` are **not** the argument schema. Each lists only the
parameters it needs — matched **by name** (the ``In<>`` name, or the selector for
state and services) and validated by type — so a hook can request just the state,
or just a service, in any order.

All hooks (``eval`` / ``start`` / ``stop``) return ``void``.

A ``static constexpr auto name`` member is optional and sets the node's display
name. The **node kind** (compute, source, sink) is inferred from which selectors
are present; see `Node kinds`_.


Reading inputs — ``In<>``
-------------------------

**Available (scalar ``TS<T>``).** An input selector exposes the tick contract of
a time-series:

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Concept
     - C++ (``In<"x", TS<T>>``)
     - Python (``x: TS[T]``)
   * - current value
     - ``x.value()``
     - ``x.value``
   * - was modified this cycle
     - ``x.modified()``
     - ``x.modified``
   * - has a value yet
     - ``x.valid()``
     - ``x.valid``
   * - delta of this tick
     - ``x.delta_value()`` *(planned typed accessor)*
     - ``x.delta_value``

.. code-block:: cpp

   struct GateOnChange
   {
       static constexpr auto name = "gate_on_change";

       static void eval(In<"in", TS<int>> in, Out<TS<int>> out)
       {
           if (in.modified()) { out.set(in.value()); }
       }
   };

.. code-block:: python

   @compute_node
   def gate_on_change(in_: TS[int]) -> TS[int]:
       if in_.modified:
           return in_.value


Producing output — ``Out<>``
----------------------------

**Available (scalar ``TS<T>``).** ``Out<TS<T>>::set(v)`` writes the value and
ticks the output at the current evaluation time. A node has **at most one**
``Out`` parameter; emit multiple values by making the output a bundle (see
`Bundles and compound shapes`_).

.. code-block:: cpp

   struct Increment
   {
       static void eval(In<"in", TS<int>> in, Out<TS<int>> out) { out.set(in.value() + 1); }
   };

.. code-block:: python

   @compute_node
   def increment(in_: TS[int]) -> TS[int]:
       return in_.value + 1

In Python you ``return`` the value; in C++ you call ``out.set(...)``. Not calling
``set`` (Python: returning ``None``) leaves the output unticked for that cycle.


.. _node-local state:

Node-local state — ``State<>``
------------------------------

**Available (scalar state).** ``State<T>`` is a typed handle to per-node state
that persists across evaluations. Use ``start`` to initialise it.

.. code-block:: cpp

   struct Counter
   {
       static constexpr auto name = "counter";

       static void start(State<int> state) { state.set(0); }

       static void eval(State<int> state, Out<TS<int>> out)
       {
           const int next = state.get() + 1;
           state.set(next);
           out.set(next);
       }
   };

.. code-block:: python

   from hgraph import compute_node, generator, TS, STATE
   from dataclasses import dataclass

   @dataclass
   class CounterState:
       n: int = 0

   @compute_node
   def counter(_state: STATE[CounterState] = None) -> TS[int]:
       _state.n += 1
       return _state.n

**Planned:** named state ``State<TSchema, "name">`` and bundle/compound state
(today ``State<T>`` is a single scalar slot).


Node kinds
----------

The kind is **always determined from the node's shape** — which selectors and
hooks are present. There is deliberately no override: the kind has a single
source of truth, the code.

.. list-table::
   :header-rows: 1
   :widths: 22 26 28 24

   * - Kind
     - Inferred when
     - C++
     - Python
   * - **Compute** *(available)*
     - ``eval`` has ``In`` and ``Out``
     - ``eval(In<…>, Out<…>)``
     - ``@compute_node``
   * - **Pull source** *(available)*
     - ``eval`` has ``Out``, no ``In``
     - ``eval(Out<…>)``
     - ``@generator``
   * - **Sink** *(available)*
     - ``eval`` has ``In``, no ``Out``
     - ``eval(In<…>)``
     - ``@sink_node``
   * - **Push source** *(planned)*
     - has an ``apply_message`` hook
     - ``start(Sender<T>)`` + ``apply_message(Scalar<"msg", T>, Out<…>)``
     - ``@push_queue``

A sink node (side effect, no output):

.. code-block:: cpp

   struct Print
   {
       static constexpr auto name = "print";
       static void eval(In<"in", TS<int>> in) { std::printf("%d\n", in.value()); }
   };

.. code-block:: python

   @sink_node
   def print_(in_: TS[int]) -> None:
       print(in_.value)

A push source receives messages from outside the graph (**planned**). It is
identified by its ``apply_message`` hook, and is **required** to define
``start(Sender<T> sender)``: the runtime calls ``start`` once to hand the node
the ``Sender`` it uses to enqueue messages (for example from another thread or
an I/O callback). Each enqueued ``Scalar`` message is then turned into an
output tick by ``apply_message``:

.. code-block:: cpp

   // Planned — provisional syntax
   struct FromQueue
   {
       static constexpr auto name = "from_queue";

       // Required for a push source: receive the Sender used to enqueue messages.
       static void start(Sender<int> sender) { /* register sender with a producer */ }

       // Convert each scalar message into an output tick.
       static bool apply_message(Scalar<"message", int> message, Out<TS<int>> out)
       {
           out.set(message.value());
           return true;
       }
   };

.. code-block:: python

   @push_queue(TS[int])
   def from_queue(sender: Callable[[int], None]):
       ...  # register `sender`; call sender(value) from another thread to inject ticks

The message is a named ``Scalar<"message", int>``, not a bare ``int`` — like
``In``, a ``Scalar`` carries a name and type, which makes explicit that a push
source consumes *scalar messages* to produce a *time-series* response. (A
lightweight implicit ``int`` → ``Scalar`` conversion may be offered as a
shortcut, but the named wrapper is the canonical form.)


Lifecycle: ``start`` / ``eval`` / ``stop``
------------------------------------------

**Available.** Besides ``eval``, a node may define ``static void start(...)`` and
``static void stop(...)``, taking the same kind of selectors. ``start`` runs once
when the graph starts (good for initialising ``State``), ``stop`` once at
teardown.

.. code-block:: cpp

   struct WithLifecycle
   {
       static void start(State<int> s) { s.set(0); }
       static void eval(In<"in", TS<int>> in, State<int> s, Out<TS<int>> out)
       {
           s.set(s.get() + in.value());
           out.set(s.get());
       }
       static void stop(State<int> s) { /* flush / release */ (void) s; }
   };

.. code-block:: python

   @compute_node
   def with_lifecycle(in_: TS[int], _state: STATE = None) -> TS[int]:
       _state.total += in_.value
       return _state.total

   @with_lifecycle.start
   def _start(_state: STATE):
       _state.total = 0

   @with_lifecycle.stop
   def _stop(_state: STATE):
       ...  # flush / release


Time-series type vocabulary
---------------------------

Schemas are expressed as compile-time **marker types** that mirror the Python
time-series types. All markers exist today; the **selectors** that read/write
each kind through ``In`` / ``Out`` are arriving kind by kind.

.. list-table::
   :header-rows: 1
   :widths: 26 30 26 18

   * - Kind
     - C++ marker
     - Python
     - ``In``/``Out`` selector
   * - scalar time-series
     - ``TS<T>``
     - ``TS[T]``
     - **available**
   * - signal (valueless tick)
     - ``SIGNAL``
     - ``SIGNAL``
     - planned
   * - bundle (named fields)
     - ``TSB<"Name", Field<…>…>``
     - ``TSB[Schema]``
     - planned
   * - list (fixed / dynamic)
     - ``TSL<T, N>``
     - ``TSL[T, Size[N]]``
     - planned
   * - set
     - ``TSS<T>``
     - ``TSS[T]``
     - planned
   * - dict (keyed)
     - ``TSD<K, V>``
     - ``TSD[K, V]``
     - planned
   * - rolling window
     - ``TSW<T, Period>``
     - ``TSW[T, ...]``
     - planned
   * - reference
     - ``REF<TSchema>``
     - ``REF[...]``
     - planned

The marker types compose exactly like the Python generics — for example a dict
of bundles keyed by string:

.. code-block:: cpp

   using PriceTick = TSB<"PriceTick",
                         Field<"bid", TS<double>>,
                         Field<"ask", TS<double>>>;

   using QuoteFeed = TSD<std::string, PriceTick>;

.. code-block:: python

   from hgraph import TimeSeriesSchema, TS, TSB, TSD

   class PriceTick(TimeSeriesSchema):
       bid: TS[float]
       ask: TS[float]

   QuoteFeed = TSD[str, TSB[PriceTick]]


Bundles and compound shapes
---------------------------

**Planned selectors.** A bundle groups named time-series. It is how a node takes
several related inputs as one parameter, or returns several outputs.

.. code-block:: cpp

   // Planned — provisional syntax
   using Quote = TSB<"Quote", Field<"bid", TS<double>>, Field<"ask", TS<double>>>;

   struct MidPrice
   {
       static void eval(In<"q", Quote> q, Out<TS<double>> out)
       {
           out.set((q.bid().value() + q.ask().value()) / 2.0);
       }
   };

.. code-block:: python

   @compute_node
   def mid_price(q: TSB[Quote]) -> TS[float]:
       return (q.bid.value + q.ask.value) / 2.0

Internally the top-level input of every node is already a bundle — each ``In``
parameter becomes one field of it — which is why a node's inputs and a ``TSB``
share the same structure.


Collections — ``TSS`` / ``TSD`` / ``TSL`` / ``TSW``
---------------------------------------------------

**Planned selectors.** Collection time-series carry per-element tick state and a
delta (added / removed / modified). The Python iteration API maps onto C++ view
methods:

.. code-block:: cpp

   // Planned — provisional syntax
   struct SumValues
   {
       static void eval(In<"d", TSD<std::string, TS<int>>> d, Out<TS<int>> out)
       {
           int total = 0;
           for (auto &&[key, v] : d.items()) { total += v.value(); }
           out.set(total);
       }
   };

.. code-block:: python

   @compute_node
   def sum_values(d: TSD[str, TS[int]]) -> TS[int]:
       return sum(v.value for v in d.values())

The delta surfaces (``d.added()`` / ``d.removed()`` / ``d.modified()`` in C++,
``d.added_items()`` / ``d.removed_items()`` / ``d.modified_items()`` in Python)
will follow the same naming as the Python collection time-series.


References and signals
----------------------

**Planned selectors.**

- ``REF<TSchema>`` passes a *reference* to a time-series rather than its value —
  used to rebind what an input points at without copying data. Mirrors Python
  ``REF[...]``.
- ``SIGNAL`` is a valueless tick: a node depends only on *that something
  changed*, not on a value. Mirrors Python ``SIGNAL``.

.. code-block:: cpp

   // Planned — provisional syntax
   struct CountTicks
   {
       static void eval(In<"trigger", SIGNAL> trigger, State<int> n, Out<TS<int>> out)
       {
           (void) trigger;
           n.set(n.get() + 1);
           out.set(n.get());
       }
   };

.. code-block:: python

   @compute_node
   def count_ticks(trigger: SIGNAL, _state: STATE = None) -> TS[int]:
       _state.n += 1
       return _state.n


Injected services — global state, clock and scheduler
-----------------------------------------------------

A node can ask for runtime services by listing them as parameters, exactly as
Python injects ``_clock`` / ``_scheduler``. Injectables are **not** part of the
node's data contract — they add no input, output, scalar or state, and do not
affect node-kind inference; the node simply receives them at evaluation. The
first one, ``GlobalStateView``, is implemented:

.. list-table::
   :header-rows: 1
   :widths: 34 33 33

   * - Service
     - C++ selector
     - Python injectable
   * - global state
     - ``GlobalStateView`` *(available)*
     - ``GLOBAL_STATE``
   * - evaluation clock
     - ``EvaluationClock`` *(planned)*
     - ``_clock: EvaluationClock``
   * - node scheduler
     - ``NodeScheduler &`` *(planned)*
     - ``_scheduler: SCHEDULER``
   * - current time
     - ``engine_time_t`` *(available)*
     - ``_clock.evaluation_time``

``GlobalStateView`` is a borrowing **view** over the graph's shared, mutable
``string -> value`` store — the owning ``GlobalState`` lives on the graph (created
at wiring time, carried onto the graph, the same instance seen at run time). A
node that declares it can read and write the store during evaluation:

.. code-block:: cpp

   struct EmitSeed
   {
       static void eval(GlobalStateView gs, Out<TS<int>> out)
       {
           out.set(gs.get_as<int>("seed"));   // read a value seeded at wiring time
           gs.set("emitted", Value{1});       // ...and write back into the store
       }
   };

The store is seeded at wiring time through ``GraphBuilder::global_state()`` (and
read back after a run via ``GraphView::global_state()``); see *Wiring Graphs in
C++*. Values are heterogeneous — each key may hold a differently-typed value
(it is a mutable ``Map<string, Any>`` under the hood).

.. code-block:: cpp

   // Planned — provisional syntax
   struct Heartbeat
   {
       static void start(NodeScheduler &sched) { sched.schedule(std::chrono::seconds{1}); }
       static void eval(NodeScheduler &sched, Out<TS<int>> out)
       {
           out.set(1);
           sched.schedule(std::chrono::seconds{1});   // re-arm
       }
   };

.. code-block:: python

   @generator
   def heartbeat(_scheduler: SCHEDULER = None) -> TS[int]:
       while True:
           yield _scheduler.next_tick(), 1


Scalar values and arguments
---------------------------

**Available as ``eval`` arguments.** Not every value in HGraph is a time-series.
A *scalar* is a plain value (an ``int``, ``double``, string, …).
``Scalar<"name", T>`` is the scalar analog of ``In`` — like ``In`` it carries a
**name and type** (read it with ``.value()``) — and it is a first-class node
parameter alongside ``In`` / ``Out`` / ``State``. Scalars are **read-only**
per-instance configuration: they are fixed when the node is built and do not
change during evaluation.

It covers two roles with one marker: a scalar **argument** fixed at wiring time
(the C++ counterpart of an ordinary, non-time-series Python function argument),
and the scalar **message** a push source consumes (see `Node kinds`_). The
argument role is implemented for ``eval``; the push-source message role is
planned.

.. code-block:: cpp

   struct Scale
   {
       static void eval(In<"in", TS<double>> in, Scalar<"factor", double> factor, Out<TS<double>> out)
       {
           out.set(in.value() * factor.value());
       }
   };

Each ``Scalar<>`` parameter becomes a field of the node's compound scalar
configuration; the values are supplied when the node is built. The scalars are
**not** part of the input TSB, so they never affect node-kind inference (a node
with only ``Scalar`` inputs and an ``Out`` is still a pull source). You supply the
values when wiring the node — ``wire<T>(w, ports…, scalars…)`` (see *Wiring Graphs
in C++ > Configuring a node with scalars*) — and equal scalars fold into the wiring
intern key, so nodes that differ only in a scalar value stay distinct.

.. code-block:: python

   @compute_node
   def scale(in_: TS[float], factor: float) -> TS[float]:
       return in_.value * factor

For a scalar that is a **Python object** (for Python user nodes), the sibling
``PythonScalar<"name", Type<"my.module.MyType">>`` carries the raw Python value
and names its expected Python type as a string, so the wiring layer can
type-check it — the Python-typed counterpart of ``Scalar<"name", T>``. If the
type is omitted (``PythonScalar<"name">``), it defaults to ``object`` — any
Python object, i.e. un-typed / generic.


Recordable state
----------------

**Planned (``RecordableState``).** Recordable state is node state that the
runtime can record and replay (for deterministic record/replay runs). It is
exposed as an output-backed handle. An optional ``Id<"...">`` template argument
names the recordable — the C++ counterpart of Python's optional
``recordable_id`` — and may be omitted:

.. code-block:: cpp

   // Planned — provisional syntax
   using LastSeen = TSB<"LastSeen", Field<"last", TS<int>>>;

   struct PreviousValue
   {
       static void eval(In<"in", TS<int>> in,
                        RecordableState<LastSeen, Id<"previous_value">> state,   // Id<> optional
                        Out<TS<int>> out)
       {
           auto last = state.last();
           out.set(last.valid() ? last.value() : -1);
           last.set(in.value());
       }
   };

.. code-block:: python

   @compute_node(recordable_id="previous_value")   # recordable_id is optional
   def previous_value(in_: TS[int], _state: RECORDABLE_STATE[LastSeen] = None) -> TS[int]:
       out = _state.last if _state.last is not None else -1
       _state.last = in_.value
       return out


Activity and validity policies
------------------------------

**Planned (policy flags on ``In``).** By default a compute node evaluates when an
*active* input ticks, and only once its *valid* inputs have values. These
policies can be tuned per input — the C++ form attaches policy flags to ``In``,
the Python form lists them on the decorator:

.. code-block:: cpp

   // Planned — provisional syntax
   struct Sample
   {
       // 'signal' drives evaluation; 'value' is read but does not by itself trigger.
       static void eval(In<"signal", SIGNAL> signal,
                        In<"value", TS<int>, InputActivity::Passive> value,
                        Out<TS<int>> out)
       {
           (void) signal;
           if (value.valid()) { out.set(value.value()); }
       }
   };

.. code-block:: python

   @compute_node(active=("signal",), valid=("value",))
   def sample(signal: SIGNAL, value: TS[int]) -> TS[int]:
       if value.valid:
           return value.value


Generics and type variables
---------------------------

HGraph nodes can be generic over a scalar type or a time-series type, resolved at
wiring time. The Python package uses the type variables ``SCALAR``,
``TIME_SERIES_TYPE``, ``K``, ``V`` (and friends). The C++ markers are
``ScalarVar<"Name">`` and ``TsVar<"Name">``.

**Available:** the ``ScalarVar`` / ``TsVar`` markers and their descriptors — a
schema that contains one reports as *not concrete* until resolved.
**Planned:** using them in a node ``eval`` signature and resolving them during
wiring.

A passthrough generic over any time-series type:

.. code-block:: cpp

   // Planned — provisional syntax
   struct Passthrough
   {
       static void eval(In<"in", TsVar<"T">> in, Out<TsVar<"T">> out)
       {
           out.set(in.value());           // 'T' resolved to a concrete schema at wiring time
       }
   };

.. code-block:: python

   from hgraph import compute_node, TIME_SERIES_TYPE

   @compute_node
   def passthrough(in_: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
       return in_.delta_value

A node generic over a scalar type:

.. code-block:: cpp

   // Planned — provisional syntax
   struct Const
   {
       static void eval(Scalar<"value", ScalarVar<"T">> value, Out<TS<ScalarVar<"T">>> out)
       {
           out.set(value.value());
       }
   };

.. code-block:: python

   from hgraph import generator, TS, SCALAR

   @generator
   def const(value: SCALAR) -> TS[SCALAR]:
       yield MIN_ST, value

A dict-keyed generic uses ``ScalarVar`` / ``TsVar`` in C++ and ``K`` / ``V`` in
Python:

.. code-block:: cpp

   // Planned — provisional syntax
   struct KeysOf
   {
       static void eval(In<"d", TSD<ScalarVar<"K">, TsVar<"V">>> d, Out<TSS<ScalarVar<"K">>> out)
       {
           out.apply_delta(d.key_set().delta_value());   // tick added / removed keys
       }
   };

.. code-block:: python

   @compute_node
   def keys_of(d: TSD[K, V]) -> TSS[K]:
       return d.key_set.delta_value


Assembling and running a graph
------------------------------

**Available.** Nodes are assembled with ``GraphBuilder`` and connected with
``GraphEdge`` (an edge runs from a source node's output to a field of a target
node's input). The graph runs under a ``GraphExecutor`` in simulation mode.

.. code-block:: cpp

   #include <hgraph/runtime/runtime.h>
   #include <hgraph/types/static_node.h>

   using namespace hgraph;

   GraphBuilder builder;
   builder.add_node(NodeBuilder{}.label("src").implementation<ConstantSource>())   // -> TS<int>
          .add_node(NodeBuilder{}.label("inc").implementation<Increment>())
          .add_edge(GraphEdge{.source_node = 0, .source_path = {},
                              .target_node = 1, .target_path = {0}});   // src -> inc."in"

   GraphExecutorBuilder executor;
   executor.graph_builder(std::move(builder))
           .start_time(MIN_ST)
           .end_time(MIN_ST + engine_time_delta_t{10});

   executor.make_executor().view().run();

.. code-block:: python

   from hgraph import graph, run_graph, EvaluationMode

   @graph
   def my_graph():
       inc = increment(constant_source())
       # ... sinks, etc.

   run_graph(my_graph, run_mode=EvaluationMode.SIMULATION,
             start_time=start, end_time=end)

In Python, wiring is implicit: calling ``increment(constant_source())`` *is* the
edge. In C++ today you name nodes and edges explicitly; a higher-level fluent
wiring layer that infers edges from call structure is planned.

.. note::

   **Source ticks over time.** Today a simple source writes a value once at
   start. A source that injects ticks across simulated time (the C++ counterpart
   of a Python ``@generator`` that yields a stream) is the current milestone and
   is being added next.


Feature status
--------------

.. list-table::
   :header-rows: 1
   :widths: 50 25 25

   * - Feature
     - C++
     - Python
   * - Compute / pull-source / sink nodes
     - available
     - available
   * - ``In<TS<T>>`` (value / modified / valid)
     - available
     - available
   * - ``Out<TS<T>>`` (set / tick)
     - available
     - available
   * - ``State<T>`` (scalar) + ``start`` / ``stop``
     - available
     - available
   * - Node-kind inference (from shape, no override)
     - available
     - available
   * - ``GraphBuilder`` / ``GraphEdge`` / ``GraphExecutor`` (simulation)
     - available
     - available
   * - Schema markers (``TS``/``TSS``/``TSD``/``TSL``/``TSW``/``TSB``/``REF``/``SIGNAL``)
     - available
     - available
   * - ``ScalarVar`` / ``TsVar`` markers + descriptors
     - available
     - available
   * - Source tick injection over time (``@generator`` stream)
     - planned
     - available
   * - Container selectors (``In``/``Out`` over ``TSB``/``TSL``/``TSS``/``TSD``/``TSW``)
     - planned
     - available
   * - ``SIGNAL`` / ``REF`` selectors
     - planned
     - available
   * - ``GlobalStateView`` injectable (shared ``string -> value`` store)
     - available
     - available
   * - ``EvaluationClock`` / ``NodeScheduler`` injection
     - planned
     - available
   * - ``Scalar<"name", T>`` (named scalar arguments)
     - available
     - available
   * - ``RecordableState``
     - planned
     - available
   * - Activity / validity policy flags
     - planned
     - available
   * - Generic node resolution (``TsVar`` / ``ScalarVar`` in signatures)
     - planned
     - available
   * - Push-source ``apply_message`` (``Scalar<"name", T>``) + required ``start(Sender<T>)``
     - planned
     - available
   * - Named state ``State<S, "name">``
     - planned
     - available
   * - Fluent / implicit edge wiring
     - planned
     - available


C++ ↔ Python cheat sheet
------------------------

.. list-table::
   :header-rows: 1
   :widths: 50 50

   * - C++
     - Python
   * - ``struct N { static void eval(...){} };``
     - ``@compute_node`` / ``@generator`` / ``@sink_node`` function
   * - ``In<"x", TS<int>> x`` → ``x.value()``
     - ``x: TS[int]`` → ``x.value``
   * - ``x.modified()`` / ``x.valid()``
     - ``x.modified`` / ``x.valid``
   * - ``Out<TS<int>> out`` → ``out.set(v)``
     - ``-> TS[int]`` → ``return v``
   * - ``State<int> s`` → ``s.get()`` / ``s.set(v)``
     - ``_state: STATE[...]`` → ``_state.field``
   * - ``EvaluationClock`` *(planned)*
     - ``_clock: EvaluationClock``
   * - ``NodeScheduler &`` *(planned)*
     - ``_scheduler: SCHEDULER``
   * - ``Scalar<"f", double>``
     - plain arg ``f: float``
   * - ``TsVar<"T">`` / ``ScalarVar<"T">`` *(planned in signatures)*
     - ``TIME_SERIES_TYPE`` / ``SCALAR``
   * - ``eval`` with ``Out`` and no ``In`` (kind inferred)
     - ``@generator``
   * - ``Scalar<"msg", T>`` message + ``Sender<T>`` *(planned)*
     - ``@push_queue`` message + ``sender`` callable
   * - ``NodeBuilder{}.implementation<N>()``
     - the decorator applied to the function
   * - ``GraphEdge{...}`` / ``GraphBuilder::add_edge``
     - calling one node's output into another (implicit)
   * - ``GraphExecutor`` + ``run()`` (simulation)
     - ``run_graph(..., run_mode=EvaluationMode.SIMULATION)``
