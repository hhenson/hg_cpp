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
execution: native evaluation tracing exists, while profiling, wiring tracing,
and the inspector do not.  The authoring mismatches found by this audit were
completed in milestones R0 and R1 on 2026-07-20.

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

Evaluation profiler
~~~~~~~~~~~~~~~~~~~

``EvaluationTrace`` and the native lifecycle-observer protocol have landed,
but ``EvaluationProfiler`` and ``GraphConfiguration(profile=...)`` have not.
The profiler must be native and should expose structured measurements rather
than make log text its only result.

The first implementation should collect:

- graph cycles, wall time, scheduling lag, and runtime load;
- per-graph and per-node evaluation count, total time, maximum time, and
  recent-window time;
- start, evaluation, and stop failures without losing the original exception;
  and
- optional process-level memory samples at graph or sample-window boundaries.

Use a monotonic clock for elapsed time.  Per-node RSS sampling should not be
the default: it is expensive, process-wide, and ``ru_maxrss`` is a peak rather
than current memory on Linux.  Planned static storage and tracked dynamic slot
capacity are more useful native memory metrics where available.  Disabled
profiling must not call Python, allocate per evaluation, or read clocks.

The Python bridge should provide ``hgraph.test.EvaluationProfiler`` as a view
of the native profiler and accept the upstream boolean/dictionary profile
configuration.  Human-readable logging is a formatter over the snapshot, not
the storage model.

Run logging and logger ownership
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Python runner currently configures a Python logger for Python-authored
nodes, while native ``log_`` uses the process spdlog logger.  It also omits the
upstream wiring duration, run start/finish, and failure messages.  This split
becomes visible in mixed graphs.

Introduce a run-owned native logger interface configured from the copied-in
``GlobalState`` / runner configuration.  The normal C++ implementation should
use spdlog; the optional bridge may attach a Python logging sink.  Native
nodes, Python nodes, the executor, trace, and profiler must all resolve the
same run logger.  ``default_log_level`` and formatting policy belong to this
run configuration rather than an unrelated process global.

Lifecycle observers and error policy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The native executor can already host lifecycle observers, but
``GraphConfiguration.life_cycle_observers`` and
``eval_node(__observers__=...)`` cannot install them from Python.  Add a
bridge adapter with callback-scoped graph/node views and explicit lifetime
guards.  Built-in trace and profiler observers remain entirely native; a
custom Python observer is the slower compatibility path.

Run-wide ``trace_back_depth`` and ``capture_values`` should configure the
existing native error-capture path.  ``cleanup_on_error=False`` needs an
explicit executor policy and tests for node, nested graph, and observer
failures.  It must not be approximated by swallowing stop exceptions in
Python.

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

Implement the aggregate profiler, immutable snapshots, runner phase logging,
and run-owned logger.  Wire ``profile``, ``graph_logger``,
``default_log_level``, and ``logger_formatter`` through the C++ runner.  Add a
benchmark guard for disabled and enabled profiler overhead and record macOS
and Linux results.

Milestone R3: lifecycle and error configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Expose native lifecycle observers through ``GraphConfiguration`` and
``eval_node``; then connect run-wide error capture and cleanup policy.  Test
root and keyed nested graph lifetimes, observer removal/reentrancy, callback
view expiry, and failure during start/evaluate/stop.

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
