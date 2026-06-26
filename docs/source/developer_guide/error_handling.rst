Error handling
==============

This page is the **authoritative design record** for runtime error handling:
per-node exception capture, the ``NodeError`` value, ``exception_time_series``
(the per-node extractor) and ``try_except`` (wrapping a whole sub-graph). It
mirrors the Python reference (``ext/main/hgraph/_wiring/_exception_handling.py``,
``_types/_error_type.py``) and records the deliberate C++ adaptations.

The substrate this builds on (an error-output port already plumbed into the
node storage, the ``ErrorOutput`` graph-edge source kind, and the
``single_nested_graph_node`` policy-wrapper view) is described in
:doc:`nested_graphs`.


Model
-----

A node evaluation can throw. The framework's contract:

- A node that **captures errors** runs its evaluation under a try/catch. On an
  exception it tries to build a ``NodeError``, writes it to its **error output**
  (a ``TS[NodeError]``) for that cycle, and the graph **keeps running**. This is
  best-effort stability, not a transaction: if the node wrote ordinary output
  before throwing, that output is unspecified and may remain observable. A node
  that does not capture errors lets the exception propagate (up to a
  ``try_except`` boundary, else out of the graph).
- ``exception_time_series(port)`` is the **per-node** extractor: it activates
  error capture on the producing node and returns its error-output time series.
- ``try_except(func, …)`` wraps a **whole sub-graph** in one node that runs the
  child graph under try/catch and produces ``TSB[{exception, out}]`` — the
  ``out`` field forwards the wrapped graph's output, the ``exception`` field
  ticks a ``NodeError`` when the child raises.

This matches Python: ``exception_time_series`` is the light per-node path and
``try_except`` the graph path (Python's ``_try_except_node`` short-circuits a
single node through its error output, while ``try_except`` over a graph builds a
catching wrapper node).


``NodeError``
-------------

``NodeError`` is a value-layer **compound scalar** (Python's
``CompoundScalar``) — a named ``bundle`` registered as ``"NodeError"`` with the
same fields as the reference:

==========================  =======  ============================================
field                       type     content
==========================  =======  ============================================
``signature_name``          ``str``  the node's display/signature name
``label``                   ``str``  the node label (empty if unset)
``wiring_path``             ``str``  best-effort path to the node in the graph
``error_msg``               ``str``  ``exception.what()``
``stack_trace``             ``str``  best-effort (see adaptation below)
``activation_back_trace``   ``str``  best-effort (see adaptation below)
==========================  =======  ============================================

It is a first-class scalar type (``scalar_descriptor<NodeError>``), so
``TS<NodeError>`` resolves like any other time series and nodes may declare
``In<"err", TS<NodeError>>`` / ``Out<TS<NodeError>>``.

**Deliberate C++ adaptation — trace fidelity.** Python populates
``stack_trace`` / ``activation_back_trace`` from Python tracebacks and an
input-value back trace. C++ has no cheap equivalent; the first cut captures the
structurally meaningful fields (node identity + ``error_msg``) faithfully and
leaves the two trace fields best-effort (the node's runtime path; richer
back traces are a recorded refinement). The *structure* matches Python so
downstream code reads the same field names.


Per-node capture and activation
-------------------------------

The node storage already reserves an error-output ``TSOutput`` when
``NodeTypeMetaData::error_output_schema`` is set, and the runtime ops serve it
(``error_output_view``); the missing pieces are capture and activation.

- **Capture (runtime).** ``evaluate_impl`` (``src/hgraph/runtime/node.cpp``)
  runs the user evaluate callback under a try/catch **only when**
  ``captures_errors`` is set. On ``std::exception`` it builds a ``NodeError``
  from the node identity + ``what()``; on a non-standard exception it builds one
  with ``error_msg = "unknown error"``. It writes the error to the error output
  for the cycle and returns normally. Ordinary output is deliberately
  unspecified on an error cycle because the framework does not roll back writes
  that happened before the throw. Scheduler re-arming still runs. A node without
  ``captures_errors`` pays nothing and propagates as before.
- **Activation (wiring).** Node bindings are *interned* (schema + ops + storage
  plan are immutable and shared), so a node cannot be mutated in place to gain
  an error output. ``exception_time_series(port)`` therefore **re-binds** the
  producing node: ``NodeBuilder::with_error_capture(error_schema)`` clones the
  node's binding with ``error_output_schema = TS[NodeError]`` and
  ``captures_errors = true`` (reusing the original native callbacks; only
  supported for native nodes — a custom-ops node such as ``map_`` raises a clear
  error), and ``Wiring`` swaps the instance's builder before ``finish``. The
  later-built storage plan then includes the error output. The returned port is
  the node's error output, addressed by the existing ``ErrorOutput`` source
  kind.

Because activation happens during wiring (before ``finish``) the storage plan
is built from the amended schema. The error-output schema joins the interning
identity; in the rare case where a structurally identical node without an error
reference deduped to one that has it, the unused error port is harmless.


``try_except`` over a sub-graph
-------------------------------

``try_except<G>(w, args…)`` compiles ``G`` into a child graph (the same
``compile_subgraph`` path as ``nested_<G>``) and adds one **try/except node**
built on the ``single_nested_graph_node`` substrate — the header advertises this
exact use ("policy wrappers such as try/catch … supply their own callbacks while
reusing the same storage and child-graph binding model").

- **Output shape.** The node output is a ``TSB`` with fields ``exception``
  (``TS[NodeError]``) and ``out`` (the wrapped graph's output) — Python's
  ``TryExceptResult``. A **sink** sub-graph (no output) yields just
  ``TS[NodeError]``. The ``exception`` field is owned by the wrapper; ``out``
  is a forwarding endpoint bound to the child terminal.
- **Catch.** The node's evaluate binds inputs, then runs
  ``child_graph().evaluate(t)`` under a try/catch. On a normal cycle the
  recorded ``NestedGraphOutputBinding`` keeps ``out`` forwarded to the child
  terminal; on an exception it moves a freshly built ``NodeError`` into
  ``exception`` instead. The child graph is **kept** across an exception
  (Python parity) and the graph continues.
- **Scheduling / lifecycle** delegate to the shared nested helpers
  (``single_nested_graph_bind_inputs`` / ``…_propagate_schedule``); start/stop
  reuse the substrate (custom start/evaluate skip the forwarding bind).
- **Interning.** Like ``nested_``: same ``G`` + equal inputs dedup; the child
  builder and its program-lifetime context are created on an intern miss.


Files
-----

- ``include/hgraph/runtime/node_error.{h}`` / ``src/…/node_error.cpp`` —
  ``NodeError`` scalar type, its interned ``bundle`` meta + ``TS`` meta, and
  ``make_node_error_value(...)``.
- ``src/hgraph/runtime/node.cpp`` — capture in ``evaluate_impl``;
  ``NodeBuilder::with_error_capture``.
- ``include/hgraph/runtime/try_except_node.{h}`` / ``src/…/try_except_node.cpp``
  — the try/except node on the nested substrate.
- ``include/hgraph/types/subgraph_wiring.h`` — ``try_except<G>`` wiring entry;
  ``exception_time_series`` lives beside the existing ``error_output(port)``
  helper in ``graph_wiring.h``.
- Tests: ``tests/cpp/test_error_handling.cpp``.


Roadmap / deferrals
-------------------

Done in this increment: ``NodeError``, per-node capture + ``exception_time_series``,
``try_except`` over a sub-graph (value + sink), ASAN/UBSAN-verified.

Deferred (recorded, not yet built):

- the ``map_`` error variant — ``exception_time_series`` over a ``map_``
  yielding ``TSD[K, TS[NodeError]]`` (Python's ``TryExceptTsdMapResult``);
- ``__trace_back_depth__`` / ``__capture_values__`` knobs and richer
  ``stack_trace`` / ``activation_back_trace`` content;
- error capture on custom-ops nodes (nested/map/switch) via their own evaluate;
- child-graph reset/rebuild policy after an exception (currently kept as-is).
