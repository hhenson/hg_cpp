Roadmap
=======

This page records the remaining work needed to bring the C++ implementation in
line with the Python graph feature set. The order matters: graph capability
comes before catalogue completeness, and the C++ runtime remains the source of
truth.

Implementation Standards
------------------------

- Keep code compact and readable. Prefer direct, local logic over long call
  chains with little behaviour in each hop.
- Review each implementation after writing it: the code should match the
  objective, avoid convenience-only state expansion, and avoid unnecessary work.
- Treat memory size and runtime cost as first-class design constraints.
- Prefer operation tables and typed dispatch over ``switch`` statements when
  behaviour belongs to a type or runtime operation. ``switch`` is acceptable in
  factory or builder code that selects a runtime builder.
- Do not add helper functions unless they have at least two real call sites, or
  the local complexity is high enough to justify the separation.
- External interfaces need examples that compile or are otherwise directly
  usable.
- User-facing wiring APIs require explicit approval before implementation. They
  should preserve the simplicity of the Python model while taking advantage of
  C++ type information where that improves safety or ergonomics.

Priority 1: Adaptors, Services, and Contexts
--------------------------------------------

These features define the full graph boundary model and therefore come first.

Planned work:

- Add runtime support for graph services and service implementations.
- Add runtime support for context capture, context lookup, and nested graph
  context import/export.
- Define adaptor foundations for source, sink, request/reply, and subscription
  flows.
- Integrate external events with the scheduler and real-time executor.
- Define lifecycle ownership for external resources.
- Support data-catalogue-style publish/subscribe as a graph feature.
- Build concrete adaptor families only after the common runtime model is
  established.

Boundary design decisions:

- Runtime boundary outputs follow the normal graph rule: every non-sink node
  owns its output, and nested or adaptor-specific machinery forwards into that
  output instead of copying values between outputs.
- Shared outputs use a feedback-style source/capture pair. The source is a
  pull-source node that owns a ``REF<T>`` output and graph-local REF state. The
  capture node is a sink that captures the producer reference and writes that
  reference into the paired source node state.
- Shared outputs do not use global-state subscribers or shared ownership for
  graph-local state. Consumers bind to the source node output and are woken by
  ordinary output notification.
- Capture during ``start`` schedules the paired source for the current engine
  time. Capture during graph evaluation schedules the source for
  ``evaluation_time + MIN_TD`` so a source that has already passed in rank order
  is observed on the next engine cycle.
- The source clears its captured reference on ``stop``. A restarted graph must
  republish through capture before the source can produce a live shared output.

Priority 2: Graph and Higher-Order Completeness
-----------------------------------------------

Once graph boundaries are in place, complete the internal graph model against
the Python semantics.

Planned work:

- Sink maps.
- All-sink switches.
- Dynamic ``TSL`` map, reduce, and mesh.
- Non-associative reduce.
- Dynamic ``TSD`` reduce pass-through combiner outputs.
- Graph-level generic ``TsVar`` subgraphs.
- Remaining nested graph boundary modes, including REF adaptation and
  recordable-state pass-through.
- Structural reference alternatives that are still unsupported.
- Push sources inside nested graphs, if the service/adaptor model requires them.

Priority 3: Parity Matrix and Operators
---------------------------------------

After the graph model is stable, build and maintain a Python-to-C++ parity
matrix. Then complete operator families against that inventory.

Planned work:

- IO, record, replay, and compare operators.
- Stream, window, and analytical operators.
- ``TSD`` and ``TSL`` convenience operators.
- Conversion and serialization operators.
- JSON, table, dataframe, numpy, and related scalar value types.
- Scalar and compound type coverage required by the Python tests.

Priority 4: Python Support
--------------------------

Python support is built on top of the C++ runtime, not the other way around.

Planned work:

- Python graph wiring bridge.
- Python user-node execution inside the C++ runtime.
- Python/C++ type metadata conversion and identity.
- Python object lifetime, GIL handling, callback scheduling, and exception
  translation.
- Python operator registration.
- Compatibility tests that exercise the Python surface through the C++ runtime.
