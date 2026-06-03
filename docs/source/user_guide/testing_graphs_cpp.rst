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
**list** ``TSL<TS<T>, N>`` inputs and outputs: a ``TSS`` input/output exchanges a
per-cycle ``SetDelta<T>`` and a ``TSL`` one a per-cycle ``ListDelta<T>``, rather than a
bare value (see *Set time-series* and *List time-series* below). The remaining
container types (``TSB``/``TSD``/``TSW``, and dynamic/nested ``TSL``) are a future
extension — each is one new ``ts_harness`` specialisation (see below) plus its
``replay``/``record`` pair.

.. _ts-harness:

How the harness dispatches per schema
.....................................

``eval_node`` itself is schema-agnostic: for each input and the output it consults a
``ts_harness<S>`` trait (in ``<hgraph/lib/testing/eval_node.h>``) that names the
per-cycle **harness element** (``T`` for ``TS<T>``, ``SetDelta<T>`` for ``TSS<T>``,
``ListDelta<T>`` for ``TSL<TS<T>, N>``) and the four operations it needs — wire the
``replay`` source, seed its buffer, wire the ``record`` sink, and read the captured
buffer back. The first input's element type
is what the (braced) first argument holds, and the output's element type is what the
returned ``std::vector<std::optional<…>>`` holds. Supporting a new time-series kind is
therefore a localised change: add a ``ts_harness`` specialisation alongside that kind's
``replay``/``record`` nodes; ``eval_node`` does not change.

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

Set time-series (``TSS``)
-------------------------

A set time-series ticks a **delta** each cycle — the elements *added* and *removed*.
Nodes author it with the ``In<Name, TSS<T>>`` selector (``size``/``contains``/
``values``/``added``/``removed``, and ``delta()``) and the ``Out<TSS<T>>`` selector
(``add``/``remove``/``clear``); the delta accumulates across mutations within a cycle.

A delta is a **``SetDelta<T>``** (in ``<hgraph/types/static_node.h>``, alongside
``In``/``Out``) — a lightweight wrapper over the delta *value*
(``Bundle{added, removed}``) that reads its elements on demand, rather than a
materialised ``std::set``. It is order-independent for equality, and it is the
``delta_value`` type a node reads via ``In<Name, TSS<T>>::delta()``. Build one with
``set_delta(added, removed)``.

The testing toolkit captures **correct deltas**, not cumulative values. The simplest
path is :ref:`eval_node <eval-node>`, which dispatches ``TSS`` inputs/outputs through
the ``SetDelta``-valued harness (see :ref:`ts-harness`): a ``TSS`` input is a
``std::vector<std::optional<SetDelta<T>>>`` and a ``TSS`` output comes back the same
way.

.. code-block:: cpp

   struct MirrorSet { static void eval(In<"s", TSS<int>> s, Out<TSS<int>> out)
                      { for (int r : s.removed()) out.remove(r); for (int a : s.added()) out.add(a); } };

   const std::vector<std::optional<SetDelta<int>>> deltas{
       set_delta<int>({1, 2}, {}),   // add 1,2
       set_delta<int>({3}, {1}),      // add 3, remove 1
       set_delta<int>({}, {2, 3}),    // remove 2,3
   };
   CHECK_OUTPUT(testing::eval_node<MirrorSet>(deltas), deltas);   // round-trips the deltas

The building blocks underneath are also usable directly when you need to wire a graph
by hand: ``replay<TSS<T>>`` applies a recorded delta sequence to a ``TSS<T>`` output
(remove then add); ``record<TSS<T>>`` captures each tick's delta; ``set_replay_deltas``
/ ``get_recorded_deltas`` convert to/from ``std::vector<std::optional<SetDelta<T>>>``;
and ``CHECK_OUTPUT`` compares them (rendering each delta as ``{added: {…}, removed:
{…}}`` on mismatch).

A delta bundle is ``Bundle{added: Set<T>, removed: Set<T>}`` (the fields are
value-layer **mutable sets**, matching a live ``TSS`` delta), so ``SetDelta``
equality is order-independent and hash-based — no per-comparison materialisation. A
``TSS`` ``const_`` (a constant set source) is still future work: it needs a
set-valued wiring scalar, which is its own design step.

List time-series (``TSL``)
--------------------------

A fixed-size list time-series ``TSL<TS<T>, N>`` is ``N`` child scalar time-series. It
ticks a **delta** each cycle — the children that ticked, as a ``{index -> value}``
map. Nodes author it with the ``In<Name, TSL<TS<T>, N>>`` selector (``size``, ``at(i)``,
``values``, and ``delta()``) and the ``Out<TSL<TS<T>, N>>`` selector, which ticks a
child either flat (``out.set(i, v)``) or through a per-child sub-selector
(``out[i].set(v)``, reusing the scalar ``Out<TS<T>>`` surface).

A delta is a **``ListDelta<T>``** (in ``<hgraph/types/static_node.h>``, alongside
``In``/``Out``) — a wrapper over an **immutable** value-layer ``Map<index, value>``
that reads its entries on demand. It is the ``delta_value`` type a node reads via
``In<...>::delta()``, order-independent for equality. Build one either as a sparse map
or positionally:

.. code-block:: cpp

   list_delta<int>({{0, 10}, {2, 30}})   // sparse: index 0 -> 10, index 2 -> 30
   list_delta<int>({10, none, 30})        // positional: position is the index, none = no tick

(The delta value is *immutable* — a compact ``Map``, built once — unlike the ``TSS``
delta whose set fields are mutable. A no-tick *cycle* is ``none`` at that cycle, not an
empty ``list_delta``.)

The simplest test path is :ref:`eval_node <eval-node>`, which dispatches ``TSL``
inputs/outputs through the ``ListDelta``-valued harness (see :ref:`ts-harness`): a
``TSL`` input is a ``std::vector<std::optional<ListDelta<T>>>`` and a ``TSL`` output
comes back the same way.

.. code-block:: cpp

   struct MirrorList { static void eval(In<"l", TSL<TS<int>, 2>> l, Out<TSL<TS<int>, 2>> out)
                       { for (auto &[i, v] : l.delta().items()) out.set(i, v); } };

   const std::vector<std::optional<ListDelta<int>>> deltas{
       list_delta<int>({{0, 1}, {1, 2}}),   // both children tick
       list_delta<int>({{0, 5}}),            // only child 0 ticks
   };
   CHECK_OUTPUT(testing::eval_node<MirrorList>(deltas), deltas);   // round-trips the deltas

The building blocks are also usable directly: ``replay<TSL<TS<T>, N>>`` sets each
``index -> value`` of the buffered delta on a ``TSL<TS<T>, N>`` output;
``record<TSL<TS<T>, N>>`` captures each tick's delta (only the children modified that
cycle); ``set_replay_list_deltas`` / ``get_recorded_list_deltas`` convert to/from
``std::vector<std::optional<ListDelta<T>>>``;
and ``CHECK_OUTPUT`` renders each delta as the map ``{0: 1, 1: 10}`` on mismatch.

This first slice covers fixed-size ``TSL`` with **scalar** children (``TS<T>``);
dynamic (resizable) ``TSL`` and nested element schemas (a ``TSL`` of ``TSS``/``TSB``/…)
are future extensions.
