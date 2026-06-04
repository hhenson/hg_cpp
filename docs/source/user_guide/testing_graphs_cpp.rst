Testing Graphs in C++
=====================

The C++ testing toolkit mirrors the Python ``eval_node`` model: feed a node (or
graph) a fixed sequence of inputs, run it in simulation, and read back the
sequence of outputs. It is built from two reusable static nodes — ``replay`` (a
source that emits a recorded sequence) and ``record`` (a sink that captures one)
— both layered on the :doc:`GlobalState <authoring_nodes_cpp>` injectable, plus
the :doc:`NodeScheduler <authoring_nodes_cpp>` for multi-cycle ticking.

Most tests use the high-level :ref:`eval_node <eval-node>` harness (below), which
wires ``replay → node-under-test → record`` for you. This page documents that
harness and the ``replay`` / ``record`` building blocks it is made of, including
their shared buffer representation.

.. _eval-node:

``eval_node``
-------------

``eval_node<NodeT>(inputs)`` runs a node over a sequence of per-cycle inputs and
returns its per-cycle outputs — the one call most node tests need:

.. code-block:: cpp

   struct AddOne
   {
       static constexpr auto name = "add_one";
       static void           eval(In<"in", TS<int>> in, Out<TS<int>> out) { out.set(in.value() + 1); }
   };

   // One engine cycle per element; `none` (= std::nullopt) means no tick that cycle.
   CHECK_OUTPUT(testing::eval_node<AddOne>({1, none, 3}), {2, none, 4});

Each input element is fed at successive engine cycles from ``MIN_ST`` and the graph
runs until it is **quiescent**; the result is the recorded output, cycle-aligned
(``output[i]`` is the node's tick at cycle ``i``, or ``none`` if it did not tick).
It is padded to **at least** the input length, but is **not truncated** — a node
that emits beyond the input window (e.g. a scheduled follow-up tick) produces a
longer result, and those ticks are kept. Node ``State`` persists across cycles, so
a stateful node accumulates as expected:

.. code-block:: cpp

   CHECK_OUTPUT(testing::eval_node<RunningSum>({1, 2, 3, 4}), {1, 3, 6, 10});

Arguments are given in the node's **eval-parameter order**. A time-series input is
a ``std::vector<std::optional<T>>``; a scalar input is the value itself. Value types
are inferred from the node's signature, and **every scalar value type** is supported
(``bool``, the integer widths, ``float``/``double``, ``std::string``, …) — the
harness plumbs values type-erased, so it is generic over the value type.

A node configured by a **scalar** takes the scalar after its inputs:

.. code-block:: cpp

   struct Shift { static void eval(In<"in", TS<int>> in, Scalar<"delta", int> d, Out<TS<int>> o)
                  { o.set(in.value() + d.value()); } };

   CHECK_OUTPUT(testing::eval_node<Shift>({1, 2, 3}, 5), {6, 7, 8});

A node with **multiple time-series inputs** takes one sequence per input. The first
input may be a braced list (its type is inferred); later inputs are passed as
``std::vector<std::optional<T>>``:

.. code-block:: cpp

   struct Sum { static void eval(In<"lhs", TS<int>> a, In<"rhs", TS<int>> b, Out<TS<int>> o)
                { o.set(a.value() + b.value()); } };

   std::vector<std::optional<int>> rhs{10, 20, 30};
   CHECK_OUTPUT(testing::eval_node<Sum>({1, none, 3}, rhs), {11, 21, 33});  // lhs persists at cycle 1

The first parameter must be a time-series input, and the node must have exactly one
output. ``eval_node`` drives **scalar** ``TS<T>``, **set** ``TSS<T>``, and fixed-size
**list** ``TSL<C, N>`` inputs and outputs, the last **recursively** in its child
``C`` (``TS`` / ``TSS`` / ``TSL``). A scalar input/output exchanges a bare ``T`` per
cycle; a **container** input/output exchanges the per-cycle **delta as a canonical
type-erased ``Value``** built by ``set_delta`` / ``list_delta`` (see *Set time-series*
and *List time-series* below). The remaining container types (``TSB`` / ``TSD`` /
``TSW``) are a future extension — each is one new ``ts_harness`` specialisation plus its
``delta_schema`` / ``apply`` entry in the recursive ``ts_delta`` trait.

.. _ts-harness:

How the harness dispatches per schema
.....................................

``eval_node`` itself is schema-agnostic: for each input and the output it consults a
``ts_harness<S>`` trait (in ``<hgraph/lib/testing/eval_node.h>``) that names the
per-cycle **harness element** — ``T`` for a scalar ``TS<T>``, and the canonical delta
``Value`` for a container (``TSS`` / ``TSL`` / …) — and how to wire the ``replay``
source / ``record`` sink and seed/read the cycle-aligned buffer. Containers flow as
canonical ``Value``\ s end to end: ``record`` captures ``in.delta_value()``, ``replay``
re-creates ticks from a delta ``Value`` via the recursive ``ts_delta<S>::apply``, and
``CHECK_OUTPUT`` compares with ``Value::equals`` (order-independent) and renders with
``to_string``. The first input's element type is what the (braced) first argument
holds; the output's element type is what the returned ``std::vector<std::optional<…>>``
holds. Supporting a new time-series kind is a localised change: a ``ts_harness`` /
``ts_delta`` specialisation; ``eval_node`` does not change.

Comparing results: ``CHECK_OUTPUT``
....................................

``CHECK_OUTPUT(actual, {expected...})`` (from
``<hgraph/lib/testing/check_output.h>``) compares an ``eval_node`` result against an
expected per-cycle sequence and, on mismatch, reports a readable delta — Catch2's
default ``==`` stringifies ``std::optional`` as the unhelpful ``{?}``. ``none`` is
the shorthand for "no tick" (bring it in with ``using namespace hgraph::testing;``).

.. code-block:: text

   output mismatch (4 elements):
     actual:   [1, 2, none, 3]
     expected: [1, 5, none, 9]
     > index 1: actual = 2, expected = 5
     > index 3: actual = 3, expected = 9

``CHECK_OUTPUT`` is non-fatal (continues the test); ``REQUIRE_OUTPUT`` aborts on
mismatch. The expected argument is a braced list (element type inferred from
``actual``, so values and ``none`` mix freely) or any existing
``std::vector<std::optional<T>>``. (This is a test-only header — it depends on
Catch2 and is not part of the ``hgraph_core`` library.)

The cycle-aligned buffer
------------------------

A replay/record buffer is a value-layer **mutable** ``List<Any>`` stored in the
``GlobalState`` under a string key. The list is **cycle-aligned**:

* index ``i`` corresponds to engine time ``MIN_ST + i * MIN_TD`` (the simulation
  starts at ``MIN_ST`` and steps one tick at a time, matching the Python
  ``SimpleArrayReplaySource``);
* element ``i`` is an ``Any``: an **empty** ``Any`` means *no tick* on that cycle,
  and a **non-empty** ``Any`` wrapping a scalar means *tick with that value*.

Using ``Any`` for the element type is what lets a single list express both "no
value this cycle" and "a value of type ``T``", the same role ``None`` plays in the
Python list-of-values. Because both the input and the output use this one shape,
comparing them is an element-wise list compare.

.. note::

   The buffer convention is anchored at ``MIN_ST`` with a ``MIN_TD`` step. This is
   the ``eval_node`` convention (Python anchors at ``start_time``, defaulting to
   ``MIN_ST``); a configurable start time is a future extension.

``replay<S>``
-------------

``replay<S>`` is the source node — a **single template keyed on the time-series
schema** ``S``, with a specialisation per kind; there are no named variants. Wire
``replay<TS<int>>`` for a scalar source (``Out<TS<T>>``), and ``replay<TSS<int>>`` /
``replay<TSL<TS<int>, N>>`` to replay set / list deltas (see below). Each initiates
itself at start via ``schedule_on_start`` (sources are not scheduled by default),
reads its buffer from the ``GlobalState`` under the ``key`` scalar, ticks once per
cycle that has a value, and reschedules itself (via ``NodeScheduler``) until the
buffer is exhausted.

.. code-block:: cpp

   // emits the value at the current cycle, then re-arms for the next
   auto src = wire<testing::replay<TS<int>>>(w, std::string{"in"});

The ``key`` names the ``GlobalState`` entry holding the input buffer; seed it
before running (``gs.set("in", buffer)``). A cycle whose element is an empty
``Any`` is skipped (the output does not tick that cycle).

``record<S>``
-------------

``record<S>`` is the sink node — the dual of ``replay<S>``, likewise one template
keyed on the schema (``record<TS<T>>`` / ``record<TSS<T>>`` / ``record<TSL<TS<T>,
N>>``). On ``start`` it creates a fresh cycle-aligned ``List<Any>`` in the
``GlobalState`` under its ``key``; on each evaluation where the input ticks it writes
the input's ``delta_value`` (the per-tick event, not the cumulative ``value`` — they
coincide for scalar time-series but differ for compound types) at the current cycle
offset (padding any skipped cycles with empty ``Any`` entries). After the run the
buffer is the recorded output, readable from the ``GlobalState``.

.. code-block:: cpp

   wire<testing::record<TS<int>>>(w, inc, std::string{"out"});  // (input port, key)

Worked example
--------------

Wiring ``replay → add_one → record`` and reading the result back:

.. code-block:: cpp

   struct AddOne
   {
       static constexpr auto name = "add_one";
       static void           eval(In<"in", TS<int>> in, Out<TS<int>> out) { out.set(in.value() + 1); }
   };

   struct ReplayRecordGraph
   {
       static constexpr auto name = "replay_record_graph";
       static void           compose(Wiring &w)
       {
           auto src = wire<testing::replay<TS<int>>>(w, std::string{"in"});
           auto inc = wire<AddOne>(w, src);
           wire<testing::record<TS<int>>>(w, inc, std::string{"out"});
       }
   };

   // Seed the input [1, <skip>, 3], run, and read back [2, <skip>, 4].
   GraphBuilder gb = build_graph<ReplayRecordGraph>();
   testing::set_replay_values<int>(gb.global_state(), "in", {1, std::nullopt, 3});
   /* ... run an executor over [MIN_ST, MIN_ST + n*MIN_TD) ... */
   auto out = testing::get_recorded_values<int>(executor.view().graph().global_state(), "out");
   // out == { 2, std::nullopt, 4 }

``set_replay_values`` / ``get_recorded_values`` are convenience helpers that build
a cycle-aligned ``List<Any>`` from a ``std::vector<std::optional<T>>`` and read one
back, so tests deal in ordinary C++ vectors rather than value-layer containers.

Standard helper nodes (``lib/std``)
-----------------------------------

A small set of reusable nodes lives in ``<hgraph/lib/std/std_nodes.h>`` (namespace
``hgraph::stdlib``) — the building blocks graphs and tests reach for most:

* ``const_<T>`` — a constant source: emits its ``value`` scalar once at start
  (named with a trailing underscore because ``const`` is a C++ keyword; the Python
  operator is ``const``).
* ``debug_print<T>`` — a sink that prints ``label: value`` on each tick (a
  diagnostic aid; ``T`` must be ``fmt``-formattable).
* ``null_sink<T>`` — a sink that consumes its input and does nothing (gives a graph
  a terminal sink without side effects).

.. code-block:: cpp

   auto c = wire<stdlib::const_<int>>(w, 7);        // source emitting 7 at start
   wire<stdlib::debug_print<int>>(w, c, "value");   // prints "value: 7"

Deltas are canonical ``Value``\ s
---------------------------------

A container time-series ticks a **delta** each cycle, and that delta is the **canonical
type-erased** ``Value`` whose schema is the runtime ``delta_value_schema`` —
``Bundle{added: Set<T>, removed: Set<T>}`` for ``TSS<T>``, ``Map<int64, delta(C)>`` for
``TSL<C, N>`` (recursive in ``C``). Tests build these with **recursive builder
functions** that *produce a standard* ``Value`` *matching the type-erased signature*
(there is no parallel wrapper type — comparison is ``Value::equals``, display is
``to_string``):

.. code-block:: cpp

   set_delta<int>({1, 2}, {})                            // -> Bundle{added:{1,2}, removed:{}}
   list_delta<TS<int>>({{0, 10}, {2, 30}})               // -> Map<int64, int>   (sparse: idx 0->10, 2->30)
   list_delta<TS<int>>({10, none, 30})                   // positional: position is the index, none = skip
   list_delta<TSS<int>>({{0, set_delta<int>({1}, {})}})  // -> Map<int64, Bundle>   (TSL of sets)
   list_delta<TSL<TS<int>,2>>({{0, list_delta<TS<int>>({{0,1}})}})   // nested, recursive

Set time-series (``TSS``)
-------------------------

Author with ``In<Name, TSS<T>>`` (``size``/``contains``/``values``/``added``/
``removed``; ``delta()`` is the canonical ``Bundle`` ``ValueView``) and ``Out<TSS<T>>``
(``add``/``remove``/``clear``; the delta accumulates within a cycle). The simplest test
path is :ref:`eval_node <eval-node>`: a ``TSS`` input/output exchanges the per-cycle
delta ``Value`` from ``set_delta``.

.. code-block:: cpp

   struct MirrorSet { static void eval(In<"s", TSS<int>> s, Out<TSS<int>> out)
                      { for (int r : s.removed()) out.remove(r); for (int a : s.added()) out.add(a); } };

   const std::vector<std::optional<Value>> deltas{
       set_delta<int>({1, 2}, {}),   // add 1,2
       set_delta<int>({3}, {1}),      // add 3, remove 1
       set_delta<int>({}, {2, 3}),    // remove 2,3
   };
   CHECK_OUTPUT(testing::eval_node<MirrorSet>(deltas), deltas);   // round-trips the deltas

By hand: ``replay<TSS<T>>`` re-creates ticks from a delta ``Value`` (remove then add);
``record<TSS<T>>`` captures ``ts.delta_value()``; ``CHECK_OUTPUT`` renders each delta as
``{added: {…}, removed: {…}}`` on mismatch. A ``TSS`` ``const_`` (a constant set source)
is still future work — it needs a set-valued wiring scalar.

List time-series (``TSL``) — recursive
--------------------------------------

A fixed-size ``TSL<C, N>`` is ``N`` child time-series whose schema ``C`` is **any**
supported time-series — ``TS`` / ``TSS`` / ``TSL`` — nested arbitrarily. Author with
``In<Name, TSL<C, N>>`` (``size``, ``operator[](i) -> In<"", C>``, ``modified_items``;
``delta()`` is the canonical ``Map<int64, delta(C)>`` ``ValueView``) and
``Out<TSL<C, N>>`` (``operator[](i) -> Out<C>``; ``set(i, v)`` is a scalar-child
convenience). Children compose recursively — ``out[i]`` is an ``Out<C>``, so a TSL of
sets writes via ``out[i].add(...)`` and a TSL of TSL via ``out[i][j].set(...)``.

The simplest test path is :ref:`eval_node <eval-node>`: a ``TSL`` input/output exchanges
the per-cycle delta ``Value`` from ``list_delta`` (recursive in ``C``).

.. code-block:: cpp

   // TSL of scalars
   struct MirrorList { static void eval(In<"l", TSL<TS<int>, 2>> l, Out<TSL<TS<int>, 2>> out)
                       { for (std::size_t i = 0; i < l.size(); ++i) if (l[i].modified()) out.set(i, l[i].value()); } };

   const std::vector<std::optional<Value>> deltas{
       list_delta<TS<int>>({{0, 1}, {1, 2}}),   // both children tick
       list_delta<TS<int>>({{0, 5}}),            // only child 0 ticks
   };
   CHECK_OUTPUT(testing::eval_node<MirrorList>(deltas), deltas);

   // TSL of sets — same harness, child delta Values are themselves set_delta(...)
   const std::vector<std::optional<Value>> set_deltas{
       list_delta<TSS<int>>({{0, set_delta<int>({1, 2}, {})}, {1, set_delta<int>({9}, {})}}),
   };
   CHECK_OUTPUT(testing::eval_node<MirrorSetList>(set_deltas), set_deltas);

By hand: ``replay<TSL<C, N>>`` re-creates ticks by recursively applying each
``index -> child_delta`` of the buffered delta to ``out[index]`` (an ``Out<C>``);
``record<TSL<C, N>>`` captures ``ts.delta_value()`` (only the children that ticked);
``CHECK_OUTPUT`` renders each delta as the map ``{0: 1, 1: 10}`` on mismatch.

Dynamic (resizable) ``TSL`` (``N == 0``) and ``TSD``/``TSW`` children are future
extensions; fixed-size ``TSL`` over ``TS``/``TSS``/``TSL`` children works today.
