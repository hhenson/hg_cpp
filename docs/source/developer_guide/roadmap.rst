Roadmap
=======

This page records the remaining work needed to replace the Python hgraph
engine for the supported user base while keeping the C++ runtime as the source
of truth.  It distinguishes four states deliberately:

``Landed``
   Implemented through the C++ runtime and covered by native and/or bridge
   tests.

``In progress``
   The design is accepted and implementation is under way, but it has not yet
   passed the repository acceptance gates.

``Remaining``
   A genuine compatibility or product capability still to implement.

``Accepted deviation``
   Behaviour intentionally differs from Python hgraph and must not quietly
   return as a second Python runtime implementation.

Review Snapshot: 2026-07-15
---------------------------

This review is current through ``65bc05ec`` and the follow-ons described
below, including the completed mutable-output, structural-REF,
constrained-generic, nested explicit-key map, logging, compiled-context,
dynamic-reduce, bridge-residue, and node-self work.
Evidence came from the public implementation and tests, the commit history,
:doc:`parity_matrix`, :doc:`python_integration`, :doc:`nested_graphs`, and
:doc:`services`.

The important corrections to the previous roadmap are:

- The Python graph bridge, Python user nodes, type conversion, services,
  adaptors, contexts, real-time execution, push sources, components, dispatch,
  and stable-ABI packaging have **landed**.  They are not future bridge work.
- Runtime-defined Python ``Enum`` classes have **landed** as nominal C++ enum
  schemas (``enum_vt`` / ``TypeRegistry::enum_type``) with conversion back to
  the registered Python class.  They are not a bridge blocker.
- Closed CompoundScalar hierarchies have landed as C++ Bundle unions, including
  namespace-qualified identity, graph-realization closure, dispatch, largest-
  leaf planned storage, and owner-backed recursive fields.
- ``@component`` and all record/replay modes, including Recover seeding and
  Compare, have landed in both languages.  General process checkpointing is a
  different feature and is not required to preserve this compatibility
  surface.
- All 48 upstream operator-test files are present under
  ``python/tests/ported/_operators``.  The wiring tier is only a selected port:
  24 files are present under ``python/tests/ported/_wiring``.  It must not be
  described as the entire upstream wiring suite.
- The current operator inventory is **135 registered**, **0 declared-only**,
  **1 missing**, and **29 equivalent APIs** out of the 165 upstream public
  definitions.  Use :doc:`parity_matrix` for the per-module inventory.  The
  bridge registry also contains internal and compatibility operators, so its
  raw ``operator_names()`` count is intentionally larger and is not the parity
  numerator.

The A3, compiled-boundary REF, constrained-generic, and nested-map baseline
passed the full acceptance gates on both local platforms:

- macOS arm64, AppleClang 21, Release with warnings as errors: 1018/1018 native
  tests; a ``cp312-abi3`` wheel installed under Python 3.14.6 produced 986
  passed, 22 skipped, 4 xfailed, and 6 deselected;
- Ubuntu 24.04 x86_64, GCC 13.3, Release with warnings as errors: 1018/1018 native
  tests; the Linux ``cp312-abi3`` wheel under Python 3.14.6 produced the same
  Python result; and
- the same working tree passed Ubuntu 24.04 x86_64, GCC 13.3, Debug with
  AddressSanitizer and the Python bridge enabled: the full non-WIP suite under
  Python 3.12.3 produced 986 passed, 22 skipped, 4 xfailed, and 6 deselected
  with no sanitizer report.

The subsequent C++-first ``log_`` compatibility work passed the clean macOS
Release gate with 1021/1021 native tests.  Its rebuilt ``cp312-abi3`` wheel,
installed under Python 3.14.6, produced 990 passed, 22 skipped, and 6
deselected, with no remaining xfails.  This follow-on did not change nested
storage, ownership, or cross-language lifetime behaviour, so the Linux and
sanitizer evidence above remains the applicable baseline for those areas.

The C++-first ``downcast_ref`` follow-on passed clean macOS arm64/AppleClang 21
and Ubuntu 24.04 x86_64/GCC 13.3 Release gates with 1026/1026 native tests on
each platform, with warnings as errors.  Rebuilt ``cp312-abi3`` wheels installed
under Python 3.14.6 produced 991 passed, 22 skipped, and 6 deselected on each
platform.  The tests cover native wiring, concrete closed-union views,
zero-storage leaves, and both dense and elided Python recording.  The same
working tree also passed the Ubuntu GCC 13.3 Debug bridge build with
AddressSanitizer enabled: the full suite under Python 3.12.3 produced 991
passed, 22 skipped, and 6 deselected with no sanitizer report.

The structural-REF wiring follow-on passed clean macOS arm64/AppleClang 21 and
Ubuntu 24.04 x86_64/GCC 13.3 Release gates with warnings as errors.  The native
results were 1030/1030 on macOS after the logging follow-on landed and
1029/1029 on Linux immediately before it landed.  Rebuilt ``cp312-abi3`` wheels
installed in fresh Python 3.14.6 environments produced 1007 passed, 22 skipped,
and 6 deselected on each platform.  This increment adds the pinned upstream REF
wiring pack, exact nested REF-shape adaptation for Python user nodes, and direct
structural TSB/TSL branch binding through the C++ switch runtime.

The compiled-context boundary follow-on passed clean macOS
arm64/AppleClang 21 and Ubuntu 24.04 x86_64/GCC 13.3 Release gates with
warnings as errors: 1056/1056 native tests passed on each platform.  Rebuilt
``cp312-abi3`` wheels installed in fresh Python 3.14.6 environments produced
1025 passed, 22 skipped, and 6 deselected on each platform.  The coverage
includes direct, fixed-structural, two-level, branch, keyed-child, mesh,
dispatch, and stacked error-boundary context imports through public
``eval_node`` APIs.

The Python service-adaptor follow-on passed clean macOS arm64/AppleClang 21 and
Ubuntu 24.04 x86_64/GCC 13.3 Release gates with warnings as errors: 1057/1057
native tests passed on each platform.  Rebuilt ``cp312-abi3`` wheels installed
in fresh Python 3.14.6 environments produced 1026 passed, 22 skipped, and 6
deselected on each platform.  The native test proves an erased registration
serves typed C++ clients; the Python tests cover multiple clients, independent
paths with scalar implementation configuration, explicit multi-interface
stubs, definition validation, and missing implementations through public
``eval_node`` graphs.

The catalogue-residue follow-on passed clean macOS arm64/AppleClang 21 and
Ubuntu 24.04 x86_64/GCC 13.3 Release gates with warnings as errors: 1065/1065
native tests passed on each platform. Rebuilt ``cp312-abi3`` wheels passed the
strict stable-ABI audit and, when installed in fresh Python 3.14.6 environments,
produced 1040 passed, 19 skipped, and 6 deselected on each platform. The Linux
wheel was also repaired to ``manylinux_2_39_x86_64`` before its test run. This
increment adds field-wise TSB ``dedup``, lifted-output deduplication, the
``drop_dups`` compatibility name, and table-shape projections over the native
table layout.

The parity-closure follow-on through ``65bc05ec`` passed clean macOS and Linux
Release gates with warnings as errors: 1071/1071 native tests passed on each
platform. A rebuilt macOS ``cp312-abi3`` wheel installed under Python 3.14.6,
and a fresh Linux stable-ABI bridge build, each produced 1046 passed, 19
skipped, and 6 deselected. The same committed baseline then passed Linux
Debug/AddressSanitizer validation: 1071/1071 native tests and the complete
non-WIP Python suite (1046 passed, 19 skipped, 6 deselected), with no sanitizer
report. This increment closes registered ``Context<>`` injection,
recordable-state nested pass-through, dynamic-TSL and pass-through-output
reduce shapes, bridge output/REF/TSW residue, and C++/Python node-self
injection.

The projected-map follow-on closes the remaining EMPTY-REF deviation without
copying projected child outputs. Fresh macOS and Linux Release gates passed
1073/1073 native tests, and rebuilt ``cp312-abi3`` bridges installed under
Python 3.14.6 produced 1047 passed, 18 skipped, and 6 deselected on each
platform. The macOS wheel also passed strict ``abi3audit``. Linux Debug/ASan
passed the same 1073 native tests and complete Python suite with no sanitizer
report.

These are execution results, not collection-only inventory.

Replacement Readiness
---------------------

The C++ engine already replaces the Python runtime for the surface exercised by
the current package and compatibility suite.  The remaining work is not a
foundational bridge rewrite.  It is a finite set of Python-authoring gaps,
structural wiring cases, nested-boundary extensions, and additive catalogue
work.

A broad claim that the Python engine can be replaced should wait until all of
the following are true:

1. **Met (2026-07-14):** mutable Python ``_output`` views work for the
   supported output kinds and are callback-scoped, with equivalent native C++
   output-mutation coverage.
2. Compiled-boundary REF adaptation is met.  The remaining generic-graph cases
   are either implemented through C++ wiring or explicitly accepted as
   restrictions.
3. **Met (inventory, 2026-07-15):** the unported upstream ``_wiring`` and
   ``ts_tests`` tiers have been reviewed and classified in
   :doc:`parity_matrix`.  The baseline is ``ext/main`` at
   ``4760fccadd5368b0482393e5acb0ceaac48518e9``; future inventory updates must
   record any baseline change explicitly.  Required behavioural cases still
   need implementation and paired public C++/Python tests before replacement
   readiness is complete.
4. Every supported Python-visible runtime behaviour has an equivalent public
   C++ wiring route and comparable behavioural tests.  Bridge-only syntax and
   arbitrary Python-object adaptation are the narrow exceptions.
5. **Met for A3 and compiled-boundary REFs:** the full macOS and Linux native and
   Python 3.14 gates pass.  Large ownership, nested-graph, or cross-language
   changes must additionally pass the local Linux gates specified in
   ``AGENTS.md``; lifetime and memory-safety work also requires the sanitizer
   gate.  A3 passed that sanitizer gate as recorded above.

Priority 0: Mutable Python Outputs
----------------------------------

**Landed (A3).**  Python compute nodes receive a callback-scoped mutable
``_output`` adapter over their C++-owned output.  Root and child views share a
lifetime guard and fail after the callback returns.  No Python-owned runtime
state was introduced.

Landed behaviour:

- mutable scalar ``TS_OUT.value`` assignment;
- mutable ``TSD_OUT`` access, including ``get_or_create``, key removal,
  ``clear``, removed-key reporting, and add/remove in one engine cycle;
- mutable ``TSB_OUT`` field access;
- mutable ``TSS_OUT`` add, remove, and clear operations;
- callback lifetime enforcement for every root and child view;
- mutations applied through the normal C++ ``TSOutputView`` APIs and node-owned
  storage; and
- public C++ ``eval_node`` tests at the same behavioural level as the Python
  tests.

Four A3 skips were removed: the TSD add/remove, invalid-child removal, clear,
and TSB output-access cases now execute.  The nearby
``test_removal_and_unbind_in_the_same_cycle`` case was not mutable-output work;
it has since landed as part of the keyed structural REF work below.

The C++ coverage uses public ``eval_node`` wiring for TSS mutation, TSD
same-cycle cancellation, invalid-child removal, and prior TSB output access.
TSD storage now records whether a child value was ever published, so removing
an invalid child cannot emit a dictionary removal that had no corresponding
add.  Structurally live keys remain visible through the key-set view, which is
required by mesh key discovery.  C++ output views remain the implementation;
the Python adapter does not form a second output runtime.

Priority 1: Authoring Compatibility
-----------------------------------

These are the highest-value remaining gaps for existing Python graph authors.

Structural references
~~~~~~~~~~~~~~~~~~~~~

**Landed for keyed structures (2026-07-14):** sampled ``TSS`` and ``TSD``
retargets reconcile their old and new published key sets.  Unbinding to an
EMPTY REF makes the current collection invalid but publishes one removal cycle
for keys that were previously visible.  A same-cycle source add that was never
published through the branch, or a ``TSD`` child that never became valid,
cannot produce a removal.  The implementation borrows the source slot store
only until its normal erase phase rather than copying keys into retired side
storage.  Public C++ ``eval_node`` tests cover ``if_`` through both ``filter_``
and the zero-copy ``keys_`` projection; the Python empty-REF/key-set and
same-cycle removal cases now execute.

**Landed for fixed shapes (2026-07-14):** REF-flipping composition for
fixed-size ``TSL`` and ``TSB`` structural ports preserves sampled retarget
semantics across live, EMPTY, and replacement references.  From-REF output
projection now works over either ordinary output storage or its planned
input-shaped alternative, and fixed-list child projection preserves the
per-child link tracking already used by bundles.  Public C++ ``eval_node``
tests cover both shapes and the corresponding ported Python tests now execute.
Those Python contracts also exercise generator sources yielding an EMPTY
reference at relative ``timedelta(0)``, so the bridge generator accepts both
relative ``timedelta`` and absolute ``datetime`` schedules.

**Landed for compiled sub-graph boundaries (2026-07-14):** REF-transparent
schema negotiation now works in both directions across a nested graph input,
through ``ParentInput`` pass-through, and from a child-produced REF output to
an outer plain consumer.  The boundary continues to carry ordinary output
handles; to-REF and from-REF alternatives remain the single implementation of
reference adaptation.  Nested schedule push delegation clamps notifications
from an idle child's stale clock to the parent's active cycle, which preserves
sampled retargeting without scheduling the parent in the past.  Public C++
``eval_node`` tests cover live scalar retargets, pass-through, child output,
and fixed structural source adaptation.

The current value-only Python REF contract remains in force: a Python REF has
no ``.output``.  Fixes must use the C++ binding alternatives and sampled
retarget semantics, not expose a borrowed output pointer to Python.

Generic wiring
~~~~~~~~~~~~~~

**Landed:** node-level ``TsVar`` / ``ScalarVar`` resolution, Python ``TypeVar``
matching, ``AUTO_RESOLVE``, overload requirements, variadics, registry-owned
Python overload dispatch, and ``eval_node`` ``resolution_dict`` typing for raw
operators.  Fixed-TSL integer ``getitem_`` uses the same zero-copy structural
projection as normal Python indexing and the public C++ ``tsl_element`` wiring
API.  Both routes have executable tests.  ``Generic[SCALAR]``
``TimeSeriesSchema`` aliases also specialize through the C++ ``TypePattern``
system: generic fields remain structural patterns, scalar variables may be
renamed between graph signatures, and concrete arguments produce distinct
interned TSB specializations.  The corresponding upstream Python contract and
the C++ pattern substitution contract execute in the test suites.  Constrained
``typing.TypeVar`` annotations now lower their complete alternative lists into
the same C++ matcher for both whole time-series and scalar variables.  Direct
C++ node wiring enforces the equivalent ``TsVar`` / ``ScalarVar`` constraints;
public ``eval_node`` tests cover every accepted alternative and rejection of an
undeclared schema.

**Remaining:**

- a decision on whether remaining ``Type[...]``-style call-site resolution is
  useful public syntax or sufficiently covered by expected-output resolution.

Compatibility inventory
~~~~~~~~~~~~~~~~~~~~~~~

**Landed:** all 48 operator files and a selected 24-file wiring pack.  The
ported TSS union contract for retargeting to an empty reference now executes;
its stale skip was removed after the keyed structural-REF implementation
landed.  The upstream switch pack now covers output and all-sink branches,
scalar branch configuration, lifecycle hooks, nested switch/map composition,
fixed bundles, TSS, and both switch/reduce nesting directions.

**Audit complete (2026-07-15):** the baseline had 11 unported ``_wiring``
modules plus root ``test_wiring.py`` containing 130 tests. Error handling has
since joined the ported tier, leaving 10 modules plus the root file (120
tests). A public subset of ``test_service.py`` is now represented without
reclassifying its entire upstream module as portable. The 215 ``ts_tests``
cases were also run diagnostically; 159 passed
without behavioural adaptation after aliasing ``REMOVE_IF_EXISTS`` to
``REMOVE`` in the temporary test copy.  The module dispositions, per-type
results, accepted restrictions, and upstream revision are saved in
:doc:`parity_matrix`.

**Required compatibility slices:**

- **Landed:** public Python error result schemas/call syntax and native
  activation-trace settings; and
- **Landed:** the ``NUMBER``/``NUMBER_2`` constrained vocabulary and public
  generic reference/request-reply service specializations; and
- **Landed:** ``SetDelta`` recording shape, safe opaque-REF metadata,
  mutable-output status/delta/clear/invalidation conveniences, and the two
  recorded TSW view differences (valid from the first value and
  ``removed_value is None`` before eviction).

**Accepted from this audit:** old ``GraphBuilder``/``WiringNodeInstance`` and
peering/binding introspection are private; ``const_fn`` remains replaced by
plain functions plus const-evaluable operators; Polars ``lower`` is not a
runtime target; REF never exposes ``.output``; and unparameterized container
outputs must either declare concrete element/value types or explicitly use
``TS[object]``.  Bare container inputs remain generic family patterns and
specialize from their connected source.

Duration-based TSW execution already has matching Python and public C++
``eval_node`` operator tests in addition to lower-level native storage
coverage.  Its exact duration shape still has no compile-time static marker;
the public C++ route is the erased ``to_window`` operator with a ``TimeDelta``
period.

Priority 2: Nested Graphs and Boundaries
----------------------------------------

**Landed:**

- planned, in-place nested storage with stop-before-destroy lifetimes;
- two-slot ``switch_`` storage;
- slot-observed keyed ``map_`` and ``mesh_`` instances;
- stable-slot arbitrary-function and sink ``map_`` instances over grow-only
  dynamic TSLs, with lifted scalar kernels retaining their single-node path;
- explicit-key-only ``map_`` instances whose child inputs are all broadcast,
  including anonymous nested maps with forwarding TSD terminals, plus
  outputless keyed maps over native or Python-authored sink children;
- fixed-TSL and associative dynamic-TSD ``reduce``, including live zero inputs
  and projectable fixed-composite results, plus non-associative fixed-TSL and
  contiguous-integer dynamic-TSD/tuple ordered reduction;
- associative and ordered dynamic-TSL ``reduce``, plus dynamic-TSD
  pass-through combiner outputs resolved through the compiled parent-input
  boundary;
- ``try_except_``, feedback, closed-union ``dispatch_``, closure capture, key
  injection, ``pass_through`` / ``no_key``, keyed write-through outputs, and
  sampled transfer of unchanged nested collections into a new switch branch;
- keyed TSD ``map_`` exception capture with a planned
  ``TSD[K, TS[NodeError]]`` hidden output whose entries follow child slot
  lifetimes;
- value-producing and all-sink ``switch_`` branches, including scalar Python
  branch configuration adapted onto the native runtime;
- reference, subscription, and request/reply services;
- source, sink, duplex, and service-adaptor foundations, including Python
  declarations and implementations over the erased native runtime; and
- contexts in the same wiring and across compiled ``nested_``, ``try_except_``,
  ``switch_``, ``dispatch_``, ``map_``, and ``mesh_`` boundaries. Context
  imports reuse static outer-port capture, including leaf-wise fixed structural
  bindings, rather than adding a runtime relay;
- ``Context<>`` injection on implementations selected through
  ``register_overload``; and
- recordable-state pass-through across nested graph boundaries.

Dynamic-TSL mesh is not a Python parity target: the upstream Python mesh
contract is TSD-only and :doc:`mesh` records the same accepted restriction.
Dynamic TSL mapping and reduction remain fully supported independently.

**Remaining boundary mode:** push sources inside nested graphs if a concrete
adaptor requires them.

**Remaining external-resource work:** subscription-adaptor flows not already
covered by the service-adaptor exchange, scheduler integration for external
events, explicit lifecycle ownership, and concrete adaptor families such as
catalogue, messaging, or database adaptors.  Build these from the common C++
boundary model when demanded by a real integration.

Priority 3: Catalogue and Operations
------------------------------------

The catalogue is no longer the critical path for Python authoring.  Additions
should be driven by supported applications or the compatibility inventory.

**Landed:** 135 upstream public operator definitions are registry-resolvable;
the stream, window, analytical, conversion, JSON, table, dataframe/Frame,
Series, TSD/TSL/TSS, IO, compare, and record/replay families all have usable
coverage.  ``Frame`` and ``Series`` use Arrow-native storage and bridge through
the Arrow C interfaces. ``dedup`` covers scalar, tolerance-aware float, TSD,
TSS, fixed TSL, and TSB inputs; Python ``lift(..., dedup_output=True)`` and the
``drop_dups`` compatibility name both delegate to that native operator.

The ``table_shape``, ``table_shape_from_schema``, and ``shape_of_table_type``
helpers are exposed as Python wiring-time projections of the C++-owned table
layout. ``dedup_builder`` and ``collect_builder`` are upstream Python
implementation-injection hooks; the native overload registry is their C++-first
equivalent and no parallel Python builder layer should be added.

**Remaining inventory:**

- larger optional libraries such as the ``hgraph.stream`` status model and the
  arrow-combinator package.

**Accepted deviation:** upstream ``apply`` evaluates a dynamic ``TS[Callable]``.
There is no native scalar schema or statically resolvable invocation ABI for
that value. Python authors should use ``@compute_node`` or ``lift`` and C++
authors should use statically typed nodes or lifted kernels. Do not introduce a
Python-object callable runtime into the C++ operator catalogue merely to expose
this name.

Native lifecycle observers, ``EngineControlView``, the C++ ``stop_engine``
sink, and Python's guarded ``EvaluationEngineApi`` projection have landed and
cover nested graphs through the root executor. Remaining observability work is
evaluation tracing and profiling. These are additive unless a target
application demonstrates that one is required for migration.

Priority 4: Boundary Products
-----------------------------

The common runtime model should precede concrete products.

**Remaining:**

- durable/pluggable record and replay stores beyond the default in-memory
  stores and current Arrow frame backend;
- data-catalogue publish/subscribe;
- concrete external adaptor families and their operational lifecycle; and
- broader JSON/table/Arrow scalar and serialization forms only where a real
  data path requires them.

These are product capabilities, not blockers for Python-authored compute,
sink, generator, graph, service, adaptor, or component code already covered by
the bridge.

Accepted Deviations
-------------------

The following are intentional unless separately re-opened:

- Python ``REF`` is an opaque value and does not expose ``.output``.
- ``None`` in CompoundScalar/Bundle construction means an unset field.
- TSB deltas are canonically dense; sparse-bundle delta parity is not required.
- CompoundScalar is a C++ Bundle closed union, not an independent Python object
  runtime.  Its diagnostic string representation may therefore differ.
- Arrow is the table/frame substrate.  Polars interop is through Arrow rather
  than a Polars runtime dependency.
- C++ contexts are name-based.  The Python bridge preserves the compatible
  type/name authoring syntax by lowering it onto that model.
- Arbitrary Python object-class dispatch is a bridge adaptation; native
  CompoundScalar dispatch uses the closed-union C++ path.
- Python-only implementation hooks and private ``hgraph._impl`` internals are
  not compatibility targets when the public behaviour is available.
- Python ``GraphBuilder``/``WiringNodeInstance`` layouts and peering/binding
  topology are private diagnostics; public graph behaviour is the contract.
- ``nested_<G>`` is a C++ authoring primitive. Python constructs that need a
  child graph expose registered C++ higher-order operators; a generic Python
  ``nested_graph(...)`` primitive is not a compatibility target.
- Bare ``TS[tuple]``, ``TS[dict]``, and ``TS[frozenset]`` inputs specialize
  from their connected concrete source.  The same annotations are rejected as
  outputs because there is no source from which to resolve their parameters;
  use a concrete container type or explicit ``TS[object]`` instead.
- ``const_fn`` is represented by ordinary wiring-time functions and
  const-evaluable operators, not a separate node class.
- Python ``NUMBER`` and ``NUMBER_2`` mean the native ``Int | Float`` scalar
  constraints. ``decimal.Decimal`` is not accepted until a native decimal
  scalar and its arithmetic operators exist.
- Polars ``lower`` is not a runtime target; dataframe interop is Arrow-based.
- ``GlobalState`` keeps C++ copy-in/copy-out ownership.  Python's thread-local
  seed and ``GlobalContext`` are authoring adapters, not alternate runtime
  global state.

Recorded Residue Requiring a Decision
-------------------------------------

The following existing skips/deviations still require a product decision; the
inventory did not silently classify them as accepted:

- frame-to-TSD key type is not inferred from a selected frame column;
- ``convert`` from ``TS[object]`` dispatches on the wiring-time schema;
- the engine's naive-datetime contract versus upstream time-zone-aware cases;
- the upstream arrow-combinator library.

Implementation Standards
------------------------

- Preserve C++ as the single runtime implementation.  Python adapters convert
  syntax, values, and callables into C++ wiring and views.
- Treat static memory size, keyed-slot capacity, and stop/erase lifetime as
  design inputs for every nested feature.
- Prefer operation tables and typed dispatch when behaviour belongs to a type;
  use selection switches only in builders and factories.
- Keep the parity matrix and this review snapshot current when a listed gap
  lands or a deviation is accepted.
- User-facing wiring APIs require explicit review.  Tests should use public
  wiring and ``eval_node`` rather than constructing runtime internals, except
  for focused low-level contracts.
