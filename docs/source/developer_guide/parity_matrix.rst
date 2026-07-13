Python parity matrix
====================

The Python-to-C++ inventory (roadmap Priority 3's first deliverable): what the
Python ``hgraph`` surface (the ``ext/main`` reference tree) offers, what the
C++ runtime provides today, and precisely what is missing — so "done" for the
Python bridge is measurable rather than discovered.

**Snapshot: 2026-07-11.** Regenerate the operator section by scanning
``ext/main/hgraph/_operators/*.py`` for public ``def`` names and comparing
against ``lib/std``'s ``Operator<"name">`` markers and ``register_*``
call sites (three states below). Update this page in the same change as any
operator addition — a stale matrix is a doc/code-divergence bug.

Operator states:

- **Registered** — resolvable by NAME through ``OperatorRegistry`` (the
  Python-bridge contract, proven template-free by
  ``tests/cpp/test_erased_wiring.cpp``). Reachable from Python on day one.
- *Declared-only* — an ``Operator<"name">`` marker exists in the catalogue but
  no implementation is registered. Invisible to name resolution (exactly the
  gap class the erased-wiring suite caught for ``record``/``replay``).
- **Missing** — no C++ counterpart at all.

Operator catalogue
------------------

Of the **165** public operator definitions in ``hgraph/_operators``:
**132 registered**, **2 declared-only**, **7 missing** — **24** further names
are covered by equivalent C++/bridge APIs (snapshot regenerated 2026-07-11 at
the close of the operator-test port; the counts come from comparing
``operator_names()`` and the catalogue markers against the upstream scan).

.. list-table::
   :header-rows: 1
   :widths: 28 8 8 8 48

   * - Python module (``hgraph/_operators``)
     - Reg.
     - Decl.
     - Miss.
     - Gaps (declared-only *italic*, missing **bold**, equiv-API plain)
   * - Analytical (``analytical_operators``)
     - 4
     - 0
     - 0
     - equiv-API: center_of_mass_to_alpha, span_to_alpha
   * - Apply / call (``apply``)
     - 1
     - 0
     - 1
     - **apply**
   * - Date & time (``date_operators``)
     - 4
     - 0
     - 0
     - —
   * - ``debug_tools``
     - 1
     - 0
     - 0
     - —
   * - Dedup (``dedup``)
     - 0
     - 0
     - 1
     - **dedup_builder**
   * - Flow control (``flow_control``)
     - 9
     - 0
     - 0
     - —
   * - Graph (``graph_operators``)
     - 6
     - 0
     - 1
     - **stop_engine** · equiv-API: pass_through_node
   * - ``json``
     - 2
     - 0
     - 0
     - —
   * - ``lift_operators``
     - 1
     - 0
     - 0
     - —
   * - Core operators (``operators``)
     - 49
     - 0
     - 0
     - equiv-API: accumulate, average
   * - Record / replay (``record_replay``)
     - 6
     - 0
     - 0
     - equiv-API: set_record_replay_model, record_replay_model, has_recordable_id_trait, get_fq_recordable_id, set_parent_recordable_id, record_replay_model_restriction
   * - Stream (``stream``)
     - 17
     - 0
     - 0
     - equiv-API: filter_by
   * - ``string``
     - 7
     - 0
     - 0
     - —
   * - Throttle (``throttle``)
     - 0
     - 0
     - 1
     - **collect_builder**
   * - Conversion (``time_series_conversion``)
     - 5
     - 0
     - 0
     - —
   * - ``time_series_properties``
     - 6
     - 0
     - 0
     - —
   * - JSON (``to_json``)
     - 2
     - 0
     - 0
     - equiv-API: to_json_builder, from_json_builder
   * - Table (``to_table``)
     - 3
     - 0
     - 3
     - **table_shape**, **table_shape_from_schema**, **shape_of_table_type** · equiv-API: set_as_of, get_as_of, get_table_schema_date_key, get_table_schema_as_of_key, set_table_schema_as_of_key, set_table_schema_date_key, make_table_schema, table_schema
   * - ``tsd_and_mapping``
     - 9
     - 0
     - 0
     - —
   * - TSS (``tss_operators``)
     - 0
     - 0
     - 0
     - equiv-API: compute_set_delta
   * - Type ops (``type_operators``)
     - 0
     - 2
     - 0
     - ``downcast_``, ``downcast_ref`` · equiv-API: ``cast_``

Notes on the residue:

- ``dedup_builder`` / ``collect_builder`` are Python implementation-injection
  hooks, not user operators — the C++ overload registry covers the need
  natively. ``apply`` (arbitrary-callable node lifting; ``call`` IS
  registered) and ``stop_engine`` (engine-control surface) await design
  decisions. The ``table_shape`` helper trio is upstream sugar over
  ``table_schema`` (add on demand).
- *downcast_* / *downcast_ref* remain declared-only.
- The equiv-API names live in ``hgraph._table`` (bitemporal config +
  ``TableSchema``), the record/replay config/traits shims, the JSON builders,
  and small python helpers (``accumulate``/``average``,
  ``center_of_mass_to_alpha``/``span_to_alpha``, ``filter_by``, ``cast_``,
  ``compute_set_delta``).

Ported operator-test suite (the behaviour yardstick)
----------------------------------------------------

**All 48** upstream ``hgraph_unit_tests/_operators`` files are ported into
``python/tests/ported`` (the ctest gate ``hgraph_python_ported_suite``).
Standing residue, each marked precisely in the test file:

- **3 accepted gaps** — TSS rebind-to-nothing removal deltas (REF-rebind
  semantics; ``linking_strategies`` doc-first), sparse TSB deltas (the
  canonical bundle delta is dense), and the ``hgraph.stream`` status library
  (``Base[COMPOUND_SCALAR]`` python generics).
- **Recorded deviations** — naive-datetime handling (4 xfails), the TSD key
  type resolved from a frame column, python wiring-node signature
  introspection, map children over EMPTY-REF projections, CompoundScalar
  string representation, ``convert`` from ``TS[object]`` dispatching on the
  wiring-time schema, and ``test_to_table_dispatch`` (upstream ``_impl``
  internals; behaviour covered through the public surface).

Follow-on tiers (recorded, not planned): upstream ``ts_tests/`` (215) and
``_wiring/`` (244).


Types and scalars
-----------------

.. list-table::
   :header-rows: 1
   :widths: 22 18 60

   * - Python
     - C++ status
     - Notes
   * - ``TS``, ``TSS``, ``TSD``, ``TSB``, ``SIGNAL``
     - Full
     - TSB is structural (field lists + named wrapper) by design — no schema
       inheritance / ``from_scalar_schema``.
   * - ``TSL``
     - Full
     - Fixed and dynamic.
   * - ``REF``
     - Full
     - Wiring marker + runtime retargeting; executor-level tests
       (``test_ref_executor.cpp``, std REF operators).
   * - ``TSW``
     - Partial
     - Tick-based windows execute end-to-end; duration-based windows have
       registry+runtime ops but **no compile-time marker** and no executing
       test.
   * - ``CONTEXT``
     - Full (divergent)
     - ``context::scope<"name">`` / ``Context<"name", S>`` /
       ``context::get`` — **name-based** where Python resolves by type
       (recorded divergence); nested import/export deferred.
   * - ``STATE`` / ``RECORDABLE_STATE``
     - Full / partial
     - RecordableState storage+eval works; graph traits + recordable-id
       resolution landed (step 2 of :doc:`record_replay_table`);
       ``component<G>`` recording + RECOVER seeding landed (step 5 + P7).
   * - Scalars: ``bool int float str date datetime timedelta``
     - Full
     - Plus C++ extras Python lacks (width-specific ints, CyclicBuffer,
       Queue, mutable slot-store containers).
   * - ``time``, ``bytes``
     - Full
     - Landed 2026-07-04 as distinct strong types with standard-vocabulary
       registration.
   * - Enums
     - Partial
     - C++ ``enum`` scalars register (the ``DivideByZero`` pattern);
       **runtime-defined** enums (Python-created, no C++ type) are
       bridge-time work.
   * - DataFrame / Series, numpy arrays, JSON scalar
     - ``Frame`` landed; rest missing
     - Value kinds; gate the serialization operator families. **Ruling
       (2026-07-04): the Frame/table specification maps onto Apache Arrow
       tables, not Polars** — Polars sits on Arrow, so Polars (and other
       Arrow-native frame libraries) remain reachable by zero-cost
       transformation. Arrow is a **formal dependency**; the table is a
       first-class type behind the ``Frame`` marker (schema-less legal;
       input schema = minimum columns; output schema = exact). Full design:
       :doc:`record_replay_table`.
   * - Generics (``TypeVar``, ``AUTO_RESOLVE``, ``Type[...]``)
     - Re-architected
     - ``TsVar``/``ScalarVar``/``SizeVar`` + wiring ``ResolutionMap`` +
       runtime ``TypePattern``. ``AUTO_RESOLVE``/``Type[...]`` have no
       counterpart (mitigated: erased views carry their schema).

Wiring and node-authoring surface
---------------------------------

.. list-table::
   :header-rows: 1
   :widths: 26 14 60

   * - Python
     - C++ status
     - Notes
   * - ``@graph`` / ``@compute_node`` / sources / sinks
     - Full
     - Graph-as-struct + static node structs; all five node kinds including
       push sources (Queue/Conflating).
   * - Operator overloading, named args, defaults, kwargs, variadics
     - Full
     - Python calling rules in ``OperatorRegistry::resolve``.
   * - ``map_`` / ``switch_`` / ``reduce`` / ``mesh_`` / ``try_except_``
     - Full
     - Full Python call shapes (``__keys__``, key detection,
       ``pass_through``/``no_key``…). Deferred: dynamic-TSL multiplexing,
       non-associative reduce, sink maps, all-sink switches.
   * - ``feedback``
     - Full
     - One-cycle delay, sink/source pair.
   * - Services (reference / subscription / request-reply)
     - Full
     - Path-aware, multi-interface impls, template descriptors.
   * - ``@adaptor`` / service adaptors
     - Full (first pass)
     - Source/sink/duplex + per-client keyed exchange; subscription and
       request/reply *adaptor flows* and concrete families deferred.
   * - Contexts
     - Full (same-wiring)
     - See *Types* row; nested import/export deferred.
   * - ``@component``
     - Full (first pass)
     - ``stdlib::component<G>`` over the mode scope + record/replay
       — the full mode set (Record/Replay/ReplayOutput/Recover/Compare;
       step 5 + P7 of :doc:`record_replay_table`).
   * - Injectables: STATE, SCHEDULER, CLOCK, GlobalState, OUTPUT
     - Full
     -
   * - Injectables: LOGGER
     - Full
     - ``LoggerView`` (``runtime/logger.h``) — spdlog in
       ``SPDLOG_FMT_EXTERNAL`` mode against the project fmt (the ruling); a
       transparent stateless injectable borrowing the process logger
       (``log::logger()`` / ``log::set_logger``); per-tick logging is
       allocation-light and refcount-free.
   * - Injectables: node self
     - Missing
     -
   * - ``stop_engine``, lifecycle observers, evaluation trace/profiling
     - Missing
     - Observability is the weakest runtime dimension.
   * - Graph recovery (start-from-state)
     - Missing
     - Note: restart of a stopped instance is out of contract by design;
       recovery means *fresh* instances seeded from recorded state.

Runtime
-------

Both evaluation modes (Simulation, RealTime) are at full parity — Python has
no additional modes. Error handling is functional but shallower than Python
(``__trace_back_depth__`` / ``__capture_values__`` / ``map_`` error variant
``TSD[K, TS[NodeError]]`` are deferred; see :doc:`error_handling`).

What blocks the bridge vs. what is additive
-------------------------------------------

**Bridge-blocking** (must exist before the Python surface is useful):
runtime-defined enums; ``Type[...]``-style explicit resolution where a Python
call supplies types (covered by the ``expected_output`` / resolution-map path
— verify per-operator as the bridge lands); nothing else — the wiring
contract itself is proven template-free.

**Additive** (each lands independently and becomes available to Python
through the registry): every declared-only/missing operator above, the
serialization value kinds, observability (observers/trace),
``@component`` + traits, nested context import/export, dynamic-TSL
higher-order support.
