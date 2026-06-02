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

   // One engine cycle per element; std::nullopt = no tick that cycle.
   auto out = testing::eval_node<AddOne>({1, std::nullopt, 3});
   // out == { 2, std::nullopt, 4 }   (a skipped input cycle stays skipped)

Each input element is fed at successive engine cycles from ``MIN_ST``; the result
is the recorded output aligned the same way. Node ``State`` persists across cycles,
so a stateful node accumulates as expected:

.. code-block:: cpp

   auto sums = testing::eval_node<RunningSum>({1, 2, 3, 4});   // { 1, 3, 6, 10 }

The input/output value types (``TIn`` / ``TOut``) are inferred from the node's
signature. **First slice:** ``NodeT`` must have exactly one ``In<TS<TIn>>``, one
``Out<TS<TOut>>`` and no scalar inputs; multi-input / scalar-configured nodes are a
planned extension.

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

``replay<T>``
-------------

``replay<T>`` is a source node (``Out<TS<T>>`` only). It initiates itself at start
via ``schedule_on_start`` (sources are not scheduled by default), then reads its
buffer from the ``GlobalState`` under the ``key`` scalar and ticks the output once
per cycle that has a value, rescheduling itself (via ``NodeScheduler``) until the
buffer is exhausted. It is the first genuine multi-cycle simulation source.

.. code-block:: cpp

   // emits the value at the current cycle, then re-arms for the next
   auto src = wire<testing::replay<int>>(w, std::string{"in"});

The ``key`` names the ``GlobalState`` entry holding the input buffer; seed it
before running (``gs.set("in", buffer)``). A cycle whose element is an empty
``Any`` is skipped (the output does not tick that cycle).

``record<T>``
-------------

``record<T>`` is a sink node (``In<"ts", TS<T>>``). On ``start`` it creates a fresh
cycle-aligned ``List<Any>`` in the ``GlobalState`` under its ``key``; on each
evaluation where the input ticks it writes the value at the current cycle offset
(padding any skipped cycles with empty ``Any`` entries). After the run the buffer
is the recorded output, readable from the ``GlobalState``.

.. code-block:: cpp

   wire<testing::record<int>>(w, inc, std::string{"out"});  // (input port, key)

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
           auto src = wire<testing::replay<int>>(w, std::string{"in"});
           auto inc = wire<AddOne>(w, src);
           wire<testing::record<int>>(w, inc, std::string{"out"});
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
