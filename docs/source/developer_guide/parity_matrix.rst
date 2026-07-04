Python parity matrix
====================

The Python-to-C++ inventory (roadmap Priority 3's first deliverable): what the
Python ``hgraph`` surface (the ``ext/main`` reference tree) offers, what the
C++ runtime provides today, and precisely what is missing — so "done" for the
Python bridge is measurable rather than discovered.

**Snapshot: 2026-07-04.** Regenerate the operator section by scanning
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
**106 registered**, **23 declared-only**, **23 missing** — 13 further names are covered by equivalent C++ APIs (snapshot updated 2026-07-04: json + record/replay config/traits + Arrow ``Frame``/``to_table``/``from_table`` landed).

.. list-table::
   :header-rows: 1
   :widths: 28 10 10 10 42

   * - Python module (``hgraph/_operators``)
     - Reg.
     - Decl.
     - Miss.
     - Gaps (declared-only *italic*, missing **bold**)
   * - Analytical (``analytical_operators``)
     - 4
     - 0
     - 2
     - **center_of_mass_to_alpha**, **span_to_alpha**
   * - Apply / call (``apply``)
     - 0
     - 0
     - 2
     - **apply**, **call**
   * - Conversion (``time_series_conversion``)
     - 1
     - 4
     - 0
     - *collect*, *combine*, *convert*, *emit*
   * - Core operators (``operators``)
     - 47
     - 2
     - 2
     - *setattr_*, *type_* · **accumulate**, **average**
   * - Date & time (``date_operators``)
     - 4
     - 0
     - 0
     - —
   * - Debug tools (``debug_tools``)
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
   * - Graph operators (``graph_operators``)
     - 3
     - 3
     - 2
     - *assert_*, *log_*, *print_* · **pass_through_node**, **stop_engine**
   * - JSON scalars (``json``)
     - 0
     - 0
     - 2
     - **json_decode**, **json_encode**
   * - JSON serialization (``to_json``)
     - 2
     - 0
     - 2
     - **from_json_builder**, **to_json_builder** (wiring-time builder
       helpers — the C++ counterpart is the interned ``json_converter``)
   * - Lifted operators (``lift_operators``)
     - 0
     - 0
     - 1
     - **round_**
   * - Record / replay (``record_replay``)
     - 2
     - 2
     - 2
     - *compare*, *replay_const* · **from_data_frame**, **to_data_frame**.
       The six config/traits functions are covered by the C++ API
       (``record_replay::set_config``/``config``/``model_is``/
       ``fq_recordable_id``/``has_recordable_id`` + ``Wiring::set_trait`` —
       step 2 of :doc:`record_replay_table`)
   * - Stream (``stream``)
     - 14
     - 4
     - 0
     - *batch*, *filter_by*, *to_window*, *window*
   * - String (``string``)
     - 7
     - 0
     - 0
     - —
   * - TS properties (``time_series_properties``)
     - 5
     - 1
     - 0
     - *evaluation_time_in_range*
   * - TSD & mapping (``tsd_and_mapping``)
     - 5
     - 4
     - 0
     - *collapse_keys*, *flip_keys*, *uncollapse_keys*, *values_*
   * - TSS operators (``tss_operators``)
     - 0
     - 0
     - 1
     - **compute_set_delta**
   * - Table serialization (``to_table``)
     - 2
     - 0
     - 5
     - **from_table_const**, **make_table_schema**, **shape_of_table_type**,
       **table_shape**, **table_shape_from_schema**. ``to_table``/
       ``from_table`` are registered (Arrow ``Frame``, step 3 of
       :doc:`record_replay_table`); ``table_schema`` maps onto the
       ``TableConverter``; the six as-of / column-key config functions are
       covered by ``record_replay::Config``
   * - Throttle (``throttle``)
     - 0
     - 0
     - 1
     - **collect_builder**
   * - Type operators (``type_operators``)
     - 0
     - 3
     - 0
     - *cast_*, *downcast_*, *downcast_ref*
   * - **Total (165 public defs)**
     - **106**
     - **23**
     - **23** (+13 API-covered)
     - 


Notes on the gap clusters (deliberate ordering per :doc:`roadmap` P3):

- **Serialization families** — ``to_json``/``from_json`` are **registered**
  (the interned ``JsonConverter``, step 1 of :doc:`record_replay_table`).
  The ``to_table`` schema ecosystem, ``from_data_frame``/``to_data_frame``
  and ``json_encode``/``json_decode`` remain blocked on the table / DataFrame
  / JSON **value kinds** (Arrow ``Frame`` design approved).
- **Record/replay ecosystem** — ``record``/``replay`` are registered
  (in-memory GlobalState backend), and the config/traits layer landed in
  step 2 of :doc:`record_replay_table` (``record_replay::Config`` + mode
  scope + graph traits with ``fq_recordable_id``). Remaining: the Arrow
  backend (``from_data_frame``/``to_data_frame``, step 4) and
  ``@component`` (step 5).
- **Conversion/type operators** (*convert*, *combine*, *collect*, *emit*,
  *cast_*, *downcast_*…) — declared-only: the markers record the intended
  API; implementations are P3 catalogue work.
- **Window tail** (*window*, *to_window*, *batch*, *filter_by*) —
  declared-only; duration-based ``TSW`` needs its compile-time marker first
  (see *Types* below).
- ``pass_through_node`` exists as a plain C++ node (``std_nodes.h``) but is
  not registry-resolvable; ``dedup_builder`` / ``collect_builder`` /
  ``*_builder`` names are Python implementation-injection hooks rather than
  user operators — the C++ overload registry covers the same need natively.
- ``apply`` / ``call`` (arbitrary-callable lifting) and ``stop_engine``
  need design decisions (callable representation; engine-control surface).

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
       ``component<G>`` recording landed (step 5); RECOVER seeding pending.
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
       (Record/Replay/ReplayOutput; Compare + Recover pending — step 5 of
       :doc:`record_replay_table`).
   * - Injectables: STATE, SCHEDULER, CLOCK, GlobalState, OUTPUT
     - Full
     -
   * - Injectables: LOGGER, node self
     - Missing
     - **Ruling (2026-07-04): LOGGER is a C++-native logger — spdlog** (built
       in ``SPDLOG_FMT_EXTERNAL`` mode against the fmt the project already
       vendors). The injectable hands nodes a logger view; per-tick logging
       must stay allocation-light.
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
serialization value kinds, observability (LOGGER/observers/trace),
``@component`` + traits, nested context import/export, dynamic-TSL
higher-order support.
