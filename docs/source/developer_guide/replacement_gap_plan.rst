Replacement Gap Plan
====================

This document is the current audit and execution plan for replacing the
Python hgraph engine with ``hg_cpp``.  It complements :doc:`roadmap`, which
also preserves the history and validation evidence for completed work.

Audit Baseline
--------------

The audit was performed on 2026-07-20 against:

- ``hg_cpp`` at ``f1f4b4ba``;
- the adjacent Python hgraph checkout at ``a0deb32e``; and
- application evidence from ``hg_oap`` and ``hg_systematic``, which both run
  against ``hg_cpp``.

The comparison is behavioural and public-API based.  Private Python runtime
objects are not counted as missing when the supported behaviour has a native
C++ route.  In particular, the ``Hg*TypeMetaData`` hierarchy, Python
``GraphBuilder`` / ``WiringNodeInstance`` layouts, implementation-injection
hooks, and generic Python ``nested_graph`` construction remain intentional
non-targets.

The package is close to application replacement, but it is not yet
operationally complete.  The largest missing area is diagnostics rather than
execution: native evaluation tracing and profiling exist, while wiring
tracing and the inspector do not.  The authoring mismatches found by this
audit were completed in milestones R0 and R1 on 2026-07-20.

Completed Authoring Work
------------------------

Milestone R0: upstream canaries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Public regression cases now freeze the recent upstream behaviours that were
previously supported only by ad-hoc probes:

- falsey scalar and Arrow ``Frame`` values pass through ``throttle``;
- TSD remove/add/remove and reference invalidation retain the correct
  lifetime;
- ``publish_output(delta=False)`` sends full values instead of deltas;
- mapped invalid bundle fields do not materialize an empty child value; and
- the existing Kafka adaptor cases continue to cover message headers.

The ``Frame`` assertion observes the documented Arrow boundary rather than
requiring a Polars runtime value.  Private Python builder layouts remain
excluded from the compatibility numerator.

Milestone R1: context adaptation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Python context syntax now lowers consistently onto the native name-based
context model:

- optional named node contexts retain their declared default when no value is
  published, while required contexts still fail at wiring;
- generic time-series contexts resolve through the native resolution scope,
  including multiple independently named generic contexts;
- graph context parameters are resolved before composition, and an absent
  optional time-series context produces a typed invalid output;
- context lookup covers nested map/switch graphs, service calls, and nominal
  CompoundScalar base types; and
- service and adaptor registration normalize ``None`` to the declaration's
  default path before native binding.

These are adapter changes only.  Context publication, binding, nested graph
execution, and service endpoints remain C++ runtime concepts.

Milestone R1: higher-order overload identity
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Native C++ ``map_`` and ``mesh_`` overload selection already used semantic
``WiredFn`` identity correctly.  The mismatch was at the Python bridge: it
retained the generated registration wrapper as the callable inspected by
``requires_``, and ``mesh_`` also exposed its private scope-name argument to a
user overload.

The bridge now stores the registration identity, callable wrapper, and
semantic user callable separately.  Private higher-order control arguments
are filtered before a Python overload is invoked.  Paired native and Python
``eval_node`` tests prove both selection for the matching callable and fallback
for a different callable; no Python-owned dispatch path was introduced.

Milestone R1: scalar formatting
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``ValueOps`` now distinguishes user formatting from diagnostic rendering.
Boolean diagnostics and JSON remain lower-case ``true`` / ``false``, while
``format_`` produces Python-compatible ``True`` / ``False``.  The formatting
operation was appended to the erased operation table and its ABI version was
incremented, with native layout, generic-value, and operator coverage plus a
Python ``eval_node`` case.

Validation Record
-----------------

The completed R0/R1 source was validated on 2026-07-20 with clean builds:

- macOS ARM64 native Release build: 1,164 of 1,164 C++ tests passed;
- macOS ``cp312-abi3`` wheel installed under Python 3.14.6: 1,431 passed and
  16 skipped;
- Ubuntu 24.04 x86_64, GCC 13.3, warnings-as-errors: 1,164 of 1,164 C++ tests
  passed, including the public-header and ABI-boundary targets; and
- Linux ``cp312-abi3`` Release wheel installed under Python 3.14.6: 1,431
  passed and 16 skipped.

Both wheel builds used ``-O3``.  The Linux Python run used
``polars[rtcompat]`` because the x86_64 OrbStack VM does not expose AVX/AVX2.

Operational Gaps
----------------

Milestone R2: profiler and run logging
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The native ``EvaluationProfiler`` now records graph cycles, wall/evaluation
time, real-time scheduling lag and load, plus per-path start/evaluation/stop
count, failures, total, maximum, and recent-window time. Snapshots own paths
and labels; copies of a profiler share its state so the Python handle remains
readable while the run owns its observer. Explicit lifecycle failure events
finalize start/stop timers without changing the propagated exception.

Graph and node identities are cached during start. Steady evaluation performs
pointer lookups rather than rebuilding paths, and the recent window is a
pre-grown circular vector. With no profiler registered, the observer list is
empty and the runtime performs no profiling clock reads or Python calls.
Process RSS sampling was intentionally not added: it is process-wide and too
expensive at node granularity. Planned graph storage and tracked slot capacity
remain the better input for the later inspector memory view.

``hgraph.test.EvaluationProfiler`` is the bound native observer;
``GraphConfiguration(profile=...)`` accepts an instance, ``True``, or the
upstream options dictionary. Human-readable profile logs format the immutable
snapshot rather than serving as its storage model.

The executor now owns the run's shared spdlog logger. Root and nested graphs
cache the same borrowed pointer, and ``LoggerView`` no longer reads a process
global on the tick path. The Python bridge sink forwards native node, trace,
and runner messages to the configured Python ``graph_logger``; Python-authored
``LOGGER`` parameters use the same object from copied-in ``GlobalState``.
``default_log_level`` and ``logger_formatter`` apply to mixed graphs, with
node paths supplied through the optional native ``ContextualLogger`` contract.

The allocation/timing guard lives in ``hgraph_type_erasure_perf`` as the
``evaluation_profiler_disabled_cycle`` and
``evaluation_profiler_enabled_cycle`` workloads. Both require zero
steady-state allocations. Exact macOS and Linux measurements are recorded in
the R2 validation record below.

R2 Validation Record
~~~~~~~~~~~~~~~~~~~~

The completed R2 source was validated on 2026-07-20 with clean Release
builds:

- macOS ARM64 native build: 1,170 of 1,170 C++ tests passed;
- macOS ``cp312-abi3`` wheel installed under Python 3.14.6: 1,434 passed and
  16 skipped;
- Ubuntu 24.04 x86_64, GCC 13.3, warnings-as-errors: 1,170 of 1,170 C++ tests
  passed, including the public-header and ABI-boundary targets; and
- Linux ``cp312-abi3`` wheel installed under Python 3.14.6: 1,434 passed and
  16 skipped.

The profiler timing guard used seven samples of 20,000 cycles.  The median
macOS ARM64 cost was 40.094 ns per disabled cycle and 165.331 ns per enabled
cycle.  The median Linux x86_64 cost was 78.764 ns per disabled cycle and
243.382 ns per enabled cycle.  All four measurements reported zero
steady-state allocations.  These are regression baselines, not a
cross-platform speed comparison.

Lifecycle observers and error policy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The executor now owns run-wide ``ErrorCaptureOptions`` and cleanup policy.
Uncaught root errors use the same bounded native activation trace as captured
``NodeError`` values. Immediate stop is optional during propagation, but C++
executor destruction remains the mandatory final teardown boundary.

``GraphConfiguration.life_cycle_observers`` and
``eval_node(__observers__=...)`` install observers on the executor's single
native list. Built-in trace/profiler instances stay native; arbitrary Python
observers use an owned adapter and guarded callback-scoped graph/node views.
Root and keyed nested graph lifetimes, push-phase gating, callback expiry,
reentrant native removal, and start/evaluate/stop failure paths have explicit
coverage.

R3 Validation Record
~~~~~~~~~~~~~~~~~~~~

The completed R3 source was validated on 2026-07-20 with clean Release
builds:

- macOS ARM64 native build: 1,173 of 1,173 C++ tests passed;
- macOS ``cp312-abi3`` wheel installed under Python 3.14.6: 1,440 passed and
  16 skipped;
- Ubuntu 24.04 x86_64, GCC 13.3, warnings-as-errors: 1,173 of 1,173 C++ tests
  passed; and
- Linux ``cp312-abi3`` wheel installed under Python 3.14.6: 1,448 passed and
  17 skipped.

Both wheel builds used Release optimisation. The Linux Python environment used
``polars[rtcompat]`` because the x86_64 OrbStack VM does not expose AVX/AVX2.
The six Python lifecycle/error ownership cases also passed against a Linux
Debug extension instrumented with AddressSanitizer; leak detection was disabled
to exclude process-wide Python shutdown state.

Wiring trace
~~~~~~~~~~~~

There is no native equivalent of ``WiringObserver`` / ``WiringTracer`` or
``GraphConfiguration(trace_wiring=...)``.  Add a C++ wiring-observer protocol
at the graph composer and operator registry.  Events should carry stable
graph paths, labels, schema/type handles, selected candidates, ranks, and
rejection reasons.  They must not expose the rejected Python
``WiringNodeInstance`` representation.

The built-in tracer should consume those events natively.  Python observer
objects may consume immutable bridge records because wiring is not a runtime
hot path.  This protocol will also make overload and generic-resolution bugs
substantially easier to diagnose.

Inspector
~~~~~~~~~

The public ``hgraph.debug`` inspector is absent.  Implement it on the same
native measurements as the profiler rather than porting the Python runtime
object walker.  The native inspection snapshot should include graph and node
identity, parent/child paths, schema labels, evaluation statistics, scheduled
time, and planned/dynamic storage metrics.  Snapshots must own or intern their
diagnostic data; they must not retain borrowed node pointers across graph
teardown or keyed-slot erase.

The Perspective/HTTP presentation can remain Python and optional.  It should
consume native snapshots, which keeps inspection correct for pure C++ and
mixed graphs and avoids a second model of nested graph lifetime.

Catalogue Gaps
--------------

These are useful compatibility additions, but they are lower priority than
the correctness and operational work above.

Arrow dataframe operations
~~~~~~~~~~~~~~~~~~~~~~~~~~

The data-frame adaptor lacks the upstream public ``join``, ``concat``, filter,
sort, ``ungroup``, and ``with_columns`` helpers.  The old implementations are
Polars-oriented.  Implement common operations over the native Arrow ``Frame``
boundary and classify expression-only Polars forms explicitly instead of
introducing Polars as a second runtime substrate.  The already recorded
``Series`` to tuple convenience conversion belongs in this slice.

NumPy and analytical facades
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The public ``hgraph.numpy_`` module is absent.  The upstream surface is small:
array conversion/indexing, ``cumsum``, ``corrcoef``, and ``quantile``.  Decide
whether these operate on the existing tuple/Series scalar forms or justify a
native array scalar before implementation.  NumPy is already a package
dependency, but Python must not own time-series execution semantics.

The compatibility ``hgraph.nodes`` namespace also lacks several public
rolling-window/statistics conveniences.  Port useful graph/operator facades
over native operators.  Do not port internal request/reply writer helpers or
old node-builder implementation hooks merely to match an export list.

Deferred And Accepted Restrictions
----------------------------------

The following are not scheduled replacement blockers:

- ``Hg*TypeMetaData`` and the old Python reflection/resolution hierarchy;
- private Python graph-builder, peering, binding, and node-instance layouts;
- a generic Python nested-graph constructor;
- Python ``node_impl`` source decorators where generators and concrete native
  adaptors provide the public authoring route;
- dynamic-TSL mesh, which is an ``hg_cpp`` extension without upstream parity;
- notebook presentation helpers;
- general process checkpoint/recovery beyond component and record/replay
  semantics; and
- durable stores, authentication, deployment, or scheduling policies without
  a concrete application requirement.

Implementation Sequence
-----------------------

Milestone R0: freeze the audit
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Completed 2026-07-20.**  The public behavioural canaries identified above
are part of the compatibility suite.  Private-runtime collection failures stay
out of the compatibility numerator.  ``hg_oap`` and ``hg_systematic`` remain
release-level application checks rather than repository unit tests.

Milestone R1: authoring correctness
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Completed 2026-07-20.**  Context adaptation, default service-path
normalization, semantic higher-order callable identity, and
Python-compatible scalar formatting have paired public native/Python coverage.
The implementation details and validation counts are recorded above.

Milestone R2: native profiler and unified run logging
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Completed 2026-07-20.** Aggregate snapshots, runner phase logging,
run-owned logging, ``profile``, ``graph_logger``, ``default_log_level``,
``logger_formatter``, and the native benchmark guard have paired native and
Python coverage.  The clean macOS/Linux acceptance and performance evidence
is recorded above.

Milestone R3: lifecycle and error configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Completed 2026-07-20.** Native lifecycle adapters now back
``GraphConfiguration`` and ``eval_node``; executor-owned error detail and
cleanup policy cover root and nested execution. Callback views expire at the
callback boundary, and failed Python runs with cleanup disabled are retained
by the raised exception until safe final teardown.

Milestone R4: wiring diagnostics
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Add the native wiring observer event model and ``WiringTracer``.  Use it in
operator-resolution tests so selected and rejected overloads can be inspected
without debugger knowledge of erased implementation objects.

Milestone R5: native inspector
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Build inspection snapshots on the profiler and graph-view infrastructure,
then port the optional Python presentation.  Validate nested switch/map/mesh
creation, stop/delete/erase, and repeated inspector queries under ASan on
Linux.

Milestone R6: data and analytics catalogue
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Add the high-value Arrow dataframe operations and Series conversion, followed
by the NumPy and rolling-statistics facades chosen from real application use.
Keep every Python facade paired with a C++ wiring route.

Milestone R7: release hardening
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the full macOS and Linux native/Python gates, Linux ASan, install/consumer
tests, stable-ABI wheel audit, and the two application suites.  Refresh
:doc:`roadmap`, :doc:`parity_matrix`, public compatibility documentation, and
the release checklist with exact results.  Windows remains a wheel-focused,
best-effort CI gate.

Completion Rules
----------------

Each milestone follows the repository definition of done in ``AGENTS.md``.
Focused tests are iteration evidence only.  Runtime and bridge milestones need
fresh full native and Python 3.14 suites; ownership, observer, inspection, or
cross-language lifetime changes also need the Linux and sanitizer gates.  A
milestone is not complete while an option is accepted and silently ignored:
unsupported options must continue to fail explicitly until their native path
lands.
