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

Landed (design record: :doc:`services`):

- Graph services and service implementations: **reference**, **subscription**,
  and **request/reply** services execute end-to-end with path-aware addressing
  (``types/service_wiring.h``, ``runtime/service_node.*``,
  ``tests/cpp/test_service_wiring.cpp``); implementation registration
  (``register_*_service``) is separate from client wiring.
- Request/reply services use feedback-style request dictionaries exactly as
  designed below: capture sinks update a source-owned request-delta state for
  ``TSD<int, request_schema>`` and the source emits the cumulative delta on its
  next scheduled tick before resetting the delta state.
- Shared outputs (``runtime/shared_output_node.*``) and the context
  source/capture **runtime primitive** (``runtime/context_node.*``).
- Adaptor foundations for source, sink, and duplex flows:
  ``adaptor::interface`` descriptors, ``register_adaptor``/``register_adaptors``,
  client ``wire<Interface>``, implementation-side ``from_graph``/``to_graph``
  (``types/adaptor_wiring.h``, ``tests/cpp/test_adaptor_wiring.cpp``).
- **Service adaptors** (the request/reply adaptor flow): per-client keyed
  exchange via ``service_adaptor::interface`` + ``from_graph``/``to_graph``
  over ``TSD<Int, schema>``.
- Multi-interface implementations: ``register_services<Impl, Services…>`` with
  ``impl_input``/``impl_output``.
- Scalar-qualified paths (``path("p", arg<"k">(v))``), per-descriptor
  ``default_path``, template service descriptors, and build-time rejection of
  duplicate registrations.
- Real-time wall-clock scheduler alarms
  (``NodeScheduler(..., on_wall_clock=true)``).

Planned work:

- The user-facing context wiring surface: graph-level context capture/lookup
  and nested graph context import/export (the runtime primitive above exists;
  the C++ API still needs approval).
- Subscription adaptor flows (request/reply landed as service adaptors).
- Integrate remaining adaptor external events with the scheduler and real-time
  executor.
- Define lifecycle ownership for external resources.
- Support data-catalogue-style publish/subscribe as a graph feature.
- Build concrete adaptor families only after the common runtime model is
  established.

Boundary design decisions (implemented; the authoritative record is
:doc:`services`):

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
- Context capture/lookup uses the same source/capture runtime primitive. Context
  keys remain wiring-time identifiers for scope resolution; they are not runtime
  ``GlobalState`` storage locations for copied reference values.
- Subscription services collect keys through a source/capture pair. The service
  source owns a ``TSS<K>`` output and graph-local reference counts; capture sinks
  enqueue add/remove intents and schedule the source. The source mutates only
  its own output, so client key changes do not copy values between outputs.
- Request/reply services collect client request values through a
  feedback-style source/capture pair. The source owns
  ``TSD<int, request_schema>``; each client has a stable wiring-time request id;
  capture sinks update source-local mutable delta state; the source applies
  that delta in one mutation when scheduled and then resets it. Multiple
  captures can update before the source emits, so the final request delta is
  cumulative.
- Real-time wall-clock scheduler alarms use the normal graph schedule queue:
  ``NodeScheduler(..., on_wall_clock=true)`` is enabled only for real-time graph
  executors, where engine time is wall-clock-aligned. Simulation rejects
  wall-clock alarms because simulated time cannot be advanced by host time.

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
