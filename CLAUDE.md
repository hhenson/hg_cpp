# CLAUDE.md

Working guide for AI sessions on **hg_cpp**. This file is the *operational* layer; it
does not restate project direction.

- **Canonical direction:** [`AGENTS.md`](AGENTS.md) — project goals, build philosophy,
  source layout, dependency policy, git hygiene. Read it first; it wins on any conflict
  about *direction*.
- **This file** adds: the doc-vs-code discipline, guardrails, an architecture map, the
  honest current state, the working commands, and pointers to the design corpus.

---

## 1. What this project is (one paragraph)

A clean-slate, **C++-first** reimplementation of the `hgraph` runtime. The C++ runtime
is the source of truth; Python is a wiring/compat bridge, never the foundation
(`AGENTS.md`). It is a deliberate re-do of the working-but-messy `ext/2603` attempt:
keep its proven runtime ideas, drop its Python-first build assumptions and accumulated
implicit state. Main target: `hgraph_core` (alias `hgraph::core`), C++23, CMake.

---

## 2. The non-negotiable workflow: design-first, *enforced*

The recurring failure on this project is **code outrunning the design docs**. The rule
going forward:

> **The developer-guide docs in `docs/source/developer_guide/` are authoritative.
> Change the doc in the *same* change as the code. A doc/code divergence is a bug.**

Concretely, for any non-trivial change:

1. **Doc first.** If you are adding/altering a structure, layer, or invariant, update
   the relevant `.rst` (and `AGENTS.md`/memory if direction-level) *before or with*
   the implementation — never "later".
2. **No silent structural change.** If the code needs a shape the docs don't describe
   (a new file, type, link kind, ops table), either (a) update the doc to make it the
   intended design, or (b) stop and flag it. Do not just write the code.
3. **Cite the doc** the change implements in your summary/commit, so drift is visible.
4. **If you find existing drift**, treat the *current intended* structure as the target:
   update the doc up to what the code now does (when the code is the better design), or
   fix the code to match the doc (when the doc is right). Record which way and why.
   Do **not** revert good code to stale docs.
5. **Keep the tree green** (see §6). It is green now; every change lands green.

---

## 3. Guardrails (the failure modes to actively prevent)

- **(i) Ahead-of-design code.** Do not build machinery the current milestone (§5) does
  not need. The simple-TS path does **not** need REF alternatives, TSD proxies, window
  views, or container kinds. If a task tempts you toward them, confirm scope first.
- **(ii) Structural drift.** Names/layout must match the docs *and the established
  vocabulary* (§4). New name → goes in the doc with a rationale, same change. Note the
  historical gap: docs/memory predating the runtime may say
  `ts_value`/`TSValue`/`ts_state`/`TSState`; the code uses `ts_data`/`TSData` +
  `ts_input`/`ts_output`. The code names are current — reconcile *toward the code*.
- **(iii) Parallel abstractions.** v2 principle: **one runtime model, no generic
  fallback**. Subscription/notification, delta cleanup, and modified-time tracking
  already exist (§4). Two ways to do one thing is the smell to kill, not add to.

---

## 4. Architecture map (matches the current tree)

**Universal vocabulary** (every layer reuses it — memory `core_data_structure_model`,
`docs/.../data_structures/core_concepts.rst`): `Plan` (memory layout + lifecycle ops) ·
`Schema` (layout-free type identity) · `Ops` (struct of fn-ptrs; **first param is always
the memory pointer**) · `Builder` (only place Schema binds to a concrete Plan+Ops) ·
`Value` (owns memory) · `View` (borrows memory + Ops). Plans/Schemas/Ops/Builders are
**interned** (`InternTable<Key,Value>`); Values are not. Registries are thin wrappers
over an `InternTable`.

| Path (`include/hgraph/`, mirrored in `src/`) | Layer |
|---|---|
| `util/` | `date_time.h` (`engine_time_t`, `MIN_ST`, `MAX_ET`), `scope.h`, `tagged_ptr.h` |
| `types/utils/` | foundation: `intern_table`, `memory_utils` (StoragePlan/StorageHandle/LifecycleOps), slot stores, `slot_observer` |
| `types/value/` | **Value layer**: `value`, `value_view`, `value_ops`, `value_builder`, compact/container storage, `specialized_views` |
| `types/metadata/` | **Schemas + bindings + registries**: `*_type_meta_data`, `type_binding`, `type_registry`, `value_plan_factory`, `ts_data_plan_factory` |
| `types/time_series/ts_data/` | **TS data structures** — payload+delta substrate used by both output and input |
| `types/time_series/ts_output/` | **TS output impls** — owning output endpoints |
| `types/time_series/ts_input/` | **TS input proxies** — non-owning endpoints; `target_link` is the peered binding |
| `types/time_series/` | umbrellas `ts_{data,input,output}.h`, `endpoint_schema.h` (`TSEndpointSchema`), `time_series_reference.h` |
| `runtime/` | **Execution**: `node.h`, `graph.h`, `executor.h`, `runtime.h` |

**The TS three-way split (intended meaning — keep it this way):**
- `ts_data` = basic data types holding/accessing value + delta; the substrate both
  `TSOutput` (output value) and `TSInput` (input value) use to implement behaviour.
- `ts_output` = the **output implementations** (owning endpoints that mutate/tick data).
- `ts_input` = the **input proxies** (non-owning endpoints that bind to an output and
  read it).

**Reference branches (read-only, never edit):** `ext/2603` (mature design + *working*
runtime; primary reference for node/graph/execution; works in-sample only), `ext/2604`
(structural cleanup). Memory: `reference_branches`, `ext_2603_design_corpus`,
`ext_2604_cleanup_steps`.

---

## 5. Current state (honest) & current milestone

**Works today** (verified, green): the **simple TS[T] path executes a real graph in
simulation mode**. `tests/cpp/test_runtime_value_view.cpp` builds `source → add_one`,
runs it through `GraphExecutorValue` (Simulation), gets correct values. The runtime has
node/graph/executor builders+views (`runtime/`), **notification-driven scheduling**
(output tick → `TSDataObserverSet::notify` → input notifier → `graph->schedule_node`,
wired via `NodeRuntimeStorage : Notifiable` in `node.cpp`), readiness/active-input gating
(`ready_to_evaluate`, `active_input_modified`), and a single-loop simulation executor.

**Multi-cycle, data-driven evaluation now works** (the former milestone gap, closed):
a source that reschedules itself via the **`NodeScheduler`** injectable drives the graph
over successive simulated times, and downstream nodes re-evaluate each cycle. Proven by
`tests/cpp/test_simulation_execution.cpp` ("a self-rescheduling source drives multiple
cycles over time") — the old `[!shouldfail]` placeholder is retired and the suite is fully
green with no expected-failures. The foundation for the testing toolkit is also in place:
`Any` value kind, mutable `List`/`Map` (slot-store-backed), and the **`GlobalState`**
injectable (owning value on the graph + `GlobalStateView`; seedable at wiring via
`Wiring::global_state()`/`GraphBuilder`, read/written at runtime).

**Design decision (recorded):** the 2603-style separate `EvaluationEngine` /
`EvaluationClock` is **not** the chosen direction — with the type-erased design,
run-level state and the evaluation loop fold into the **executor ops**
(`runtime/executor.h`). A separate engine/clock is a recorded alternative to revisit
only if that proves insufficient.

**Graph unit-testing toolkit — DONE.** Built on the above: `replay<T>`/`record<T>`
nodes over a cycle-aligned `List<Any>` buffer in `GlobalState`, the `eval_node<NodeT>`
harness (`tests deal in `vector<optional<T>>`; first slice = single In + single Out),
and a small `lib/std` (`stdlib::const_`, `debug_print`, `null_sink`). Sources are
**not scheduled by default** — they initiate via `schedule_on_start = true` (declarative),
`SingleShotScheduler` (lightweight one-shot in `start`), or `NodeScheduler` (full,
stateful). See `docs/.../testing_graphs_cpp.rst` + memory `value-any-globalstate-testing-plan`.
`ext/2603` is the working reference.

**Next milestone:** to be decided — candidates include multi-input/scalar `eval_node`,
more `lib/std` operators, or beginning the non-flattening nested-graph work
(`map_`/`reduce`/`switch_`). **C++ only for now** — keep Python out of the
configure/build/run path.

---

## 6. Build & test

```sh
cmake -S . -B build                 # configure (fmt + Catch2 fetched if absent)
cmake --build build -j              # build hgraph_core + tests
ctest --test-dir build --output-on-failure
./build/tests/cpp/hgraph_unit_tests # or run the Catch2 suite directly
```

- Tests on by default (`BUILD_TESTING`, CTest). Catch2 suite target `hgraph_unit_tests`
  — **add new test files to `tests/cpp/CMakeLists.txt`**. Also `hgraph_smoke_test`,
  `hgraph_header_compile_check`.
- Options: `-DHGRAPH_WARNINGS_AS_ERRORS=ON`, `-DHGRAPH_ENABLE_ASAN=ON
  -DHGRAPH_ENABLE_UBSAN=ON` (Clang/GCC; ASAN/UBSAN exclusive with TSAN).
- **Never** make the default build need Python/nanobind. Opt-in only:
  `-DHGRAPH_BUILD_PYTHON_BINDINGS=ON` / `-DHGRAPH_ENABLE_PYTHON_USER_NODES=ON`.

---

## 7. Conventions

- **C++23**; prefer std features that simplify.
- **Single-threaded**: no locks/atomics in value/TS/runtime infra.
- **Ops tables**: structs of fn-ptrs, first param = the structure's memory; metadata
  (header) separate from operations (`*_ops`).
- **Lifetime**: builders are build-time only; no `shared_ptr` for builder lifetime in
  graph code; long-lived immutable artifacts live in registries (stable addresses).
- **Containers**: `ankerl::unordered_dense`; index-based indirection for stable
  addresses; default SBO `<sizeof(void*),alignof(void*)>`.
- **Tests close to behaviour**; write the failing test that captures the intended
  semantic before the fix.

---

## 8. Design corpus & memory (read before designing)

- **Developer guide** (authoritative): `docs/source/developer_guide/`, esp.
  `data_structures/` (`core_concepts`, `overview/`, `schemas/`, `plans_and_ops/`,
  `linking_strategies`, `refinements`).
- **`ext/2603/docs/`** (intent; ranked in memory `ext_2603_design_corpus`): Tier 1 =
  `ts_value/design/09_SIMPLIFIED_RUNTIME.md`, `08_IMPLEMENTATION_REVIEW.md`, nested-graphs
  RFC, builder-lifetime rules, sampled-runtime contract.
- **Auto-memory** (`MEMORY.md` index): `v2_design_principles`,
  `core_data_structure_model`, `developer_guide_doc_decisions`, branch maps. Memory is
  point-in-time — verify file:line claims against current code.
