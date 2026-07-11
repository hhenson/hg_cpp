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
| `util/` | `date_time.h` (`DateTime`, `MIN_ST`, `MAX_ET`), `scope.h`, `tagged_ptr.h` |
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

**Reference trees (read-only, never edit):** `ext/main` is the **canonical Python
`hgraph` reference — use it (not `ext/2603`) whenever you look at the Python code** (e.g.
operator/node signatures, `_operators/`, semantics). `ext/2603` (mature design + *working*
runtime; reference for node/graph/execution; works in-sample only) and `ext/2604`
(structural cleanup) are older snapshots — keep them for prior C++ runtime ideas, but
treat `ext/main` as authoritative for Python behaviour. Memory: `reference_branches`,
`ext_2603_design_corpus`, `ext_2604_cleanup_steps`.

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
harness (tests deal in `vector<optional<T>>`; supports **multiple TS inputs and scalar
params**, and an **operator overload** `eval_node<Op>(...)` that dispatches the operator
at wiring time and returns type-erased `vector<optional<Value>>` checked with `Value`
equality — write the expected with the same `values<T>(...)` helper used for inputs),
and a small `lib/std`. `const_`/`debug_print`/`null_sink` are now **operators** (catalogue
`operators/` + impls `operators/impl/`, registered via `register_standard_operators()`),
matching the Python target API; `pass_through_node` stays a plain node in `std_nodes.h`.
Sources are
**not scheduled by default** — they initiate via `schedule_on_start = true` (declarative),
`SingleShotScheduler` (lightweight one-shot in `start`), or `NodeScheduler` (full,
stateful). See `docs/.../testing_graphs_cpp.rst` + memory `value-any-globalstate-testing-plan`.
`ext/2603` is the working reference.

**Real-time execution + push sources — DONE.** The executor now runs in
`GraphExecutorMode::RealTime`: wall-clock waiting on a condition variable,
`request_stop()` wakes a sleeping executor, and **push-source nodes** deliver values
from external threads (`runtime/push_source_node.{h,cpp}`): `PushSourcePolicy` with
**Queue** (FIFO, drains one value per cycle) and **Conflating** (delta-merging
accumulator) policies, a thread-safe `PushSourceSender` handed to user code at `start`,
schema validation on send, and executor wake-up via the push-pending signal. Push
sources require a real-time **root** graph (rejected in simulation mode and in nested
graphs). Tests: `tests/cpp/test_realtime_execution.cpp`. Docs: `architecture.rst`
(push-source subsection), `data_structures/overview/execution_layer.rst`, `wiring.rst`,
`python_integration.rst`. The single-threaded rule stands: senders are the only
cross-thread entry point.

**Non-flattening nested graphs — core DONE** (design record:
`docs/source/developer_guide/nested_graphs.rst`, authoritative). Landed, ASAN-verified:
sub-graph compilation (`CompiledSubGraph`/`compile_subgraph<G>`/`nested_<G>`, boundary
placeholders — no stub nodes), `alias_parent_input` pass-through, structural boundary
args, scheduling delegation (pull + idle-push), and the **higher-order operators** —
all ordinary registry operators (ext/main pattern; callable arg = the `WiredFn` scalar,
`fn<X>()`, which both inlines and compiles): `reduce` over fixed TSL (leaves =
`default(ts[i], zero)`, op-aware `zero_`, explicit-zero arity), `switch_` (one branch
child, sampled retarget, key as ordinary boundary input), `map_` over TSD (keyed child
instances from the dict delta; per-key elements instantiated in the owned TSD output,
child terminals re-homed as forwarding outputs that write them directly — no copy). Catalogue:
`lib/std/operators/higher_order.h` + `impl/higher_order_impl.h`; runtime nodes
`runtime/{nested_graph,switch,map}_node.*` on shared `runtime/nested_bindings.h`.
Also landed: **dynamic-TSD `reduce`** (2603 design ported into the doc first;
`runtime/reduce_node.*` — alias leaves, minimal combiner tree, zero = empty-result only,
sampled root re-publication). Also: `map_` over fixed
TSL (wiring-time expansion, Python `_map_no_index` parity) and `reload_on_ticked`
exposed on `SwitchCases` (`switch_cases({…}).reload()`). **Variadic operator args — DONE** (`VarIn<Pattern>` trailing
compose param; runtime-matcher capability — tail args matched per-arg in a throwaway
binding scope, fixed-arity candidates preferred; `switch_`/`map_` are single variadic
overloads now; Python-port constraint recorded in memory `python-port-operator-compat`).
**Multi-multiplexed `map_` — DONE** (Python parity:
every TSD in the tail multiplexes, union key set, absent-key inputs stay invalid;
same-size TSLs multiplex per index). **Named args + defaults + kwargs — DONE** (Python
calling rules as call normalisation in `OperatorRegistry::resolve`; `arg<"name">(v)`
sugar, `defaults()` hook, `VarKwIn<"kwargs">` collector; see *Operators > Named
arguments, defaults and kwargs*). Graph-overload ports are named via
`NamedPort<"name",S>` (port-like everywhere, incl. sub-graphs/WiredFn funcs); TS
defaults convert Python-style (value → `const`, empty/None → null source). `map_` /
`switch_` take the full Python call shape — `map_(func, *args, **kwargs)` (no anchor
param; kwargs resolve onto func's parameter names via `WiredFn::param_names`; TSD/TSL
kernel selection uses the resolved function-parameter order),
`switch_(key, cases, *ts, **kwargs)` (kwargs resolve per branch). `map_` supports the
Python specials: `__keys__` (explicit TSS key set), name-based key detection +
`__key_arg__` (keyword-only rename, `""` disables), and `pass_through`/`no_key`
(wiring-time `WiringPortRef::ArgTag` port tags, folded into node identity via the
`MapCallConfig` scalar). Remaining (deferred — see the doc's roadmap + non-goals):
dynamic-TSL multiplexing/reduce, non-associative reduce, sink maps/switches.
**C++ only for
now** — keep Python out of the configure/build/run path.

**Python operator-test port — DONE (2026-07-11).** All **48** upstream
`hgraph_unit_tests/_operators` files are ported into `python/tests/ported`
(ctest gate `hgraph_python_ported_suite`, ~790 tests green). En route the
runtime gained: real py-node input activity, first-class **enums**, the
**TABLE tuple-row protocol** + data-frame operators (`record_replay_table.rst`
step 6), `exception_time_series` bridged, `type_`, TSW `std`, tuple-of-CS
`getattr_`, strict unnamed-TSB combine, and **map value holes** (None-valued
entries; scalar.rst). Calling conventions are REGISTRY-driven (scalar kwargs
lift to const in call normalisation; subscript meaning via
`operator_output_is_selective`; generic targets via
`resolve_convert/collect/combine_target` — never label/name tests in the
bridge). Standing residue (marked precisely in the tests, see
`parity_matrix.rst`): 3 accepted gaps (TSS rebind removals, sparse TSB
deltas, `hgraph.stream` generics) + recorded deviations. Follow-on tiers
(not planned): upstream `ts_tests/` (215), `_wiring/` (244).

**Mesh, services & shared outputs — DONE** (design records:
`docs/source/developer_guide/mesh.rst` and `docs/source/developer_guide/services.rst`,
authoritative). `mesh_` over TSD executes (on-demand instances via
`mesh_subscribe`/`mesh_ref`, dependency ranking + cycle detection;
`runtime/mesh_node.*`, `tests/cpp/test_mesh.cpp`; dynamic-TSL mesh still deferred).
All three service flavours execute end-to-end with path-aware addressing —
reference (client reads the impl output by REF), subscription (TSS key set via
source/capture, ref-counted), request/reply (`TSD<int,request>` cumulative request
dictionary) — `types/service_wiring.h`, `runtime/service_node.*`,
`tests/cpp/test_service_wiring.cpp`. Service descriptors are **flavour-tagged by
their schema aliases** (`output_schema` / `key_type`+`value_schema` /
`request_schema`+`response_schema`, mutually exclusive, concept-checked); clients
consume through the ordinary **`wire<Service>(w[, service::path(…)][, port])`**
verb (path first) via a `wire_customization` extension point; impl registration
(`register_*_service<Service,Impl>`) is separate from client wiring. **Impl side:**
an impl is an ordinary node/graph (first TS param = flavour input, output captured;
optional `Scalar<"path",Str>` receives the path); one graph can implement several
interfaces via `register_services<Impl, Services…>` + `impl_input<S>`/`impl_output<S>`.
**Adaptor foundations landed** (first pass): descriptors derive from
`adaptor::interface` (+ `input_schema`/`output_schema`; omit one for sink/source-only),
`register_adaptor(s)`, client `wire<Interface>(w[, path][, in])`, impl-side
`from_graph<I>`/`to_graph<I>` (`types/adaptor_wiring.h`, `test_adaptor_wiring.cpp`;
built on the shared-output substrate). **Service adaptors** (per-client keyed
exchange): `service_adaptor::interface`; impl sees `TSD<Int, input_schema>` via
`from_graph` and replies keyed by the same client id via `to_graph`. Paths may be
scalar-qualified (`path("p", arg<"k">(v))`), descriptors may set `default_path`,
service descriptors may be templates (instantiations bind as concrete interfaces),
and duplicate registrations on one path throw at build time. Shared outputs
(`runtime/shared_output_node.*`) and the context **runtime primitive**
(`runtime/context_node.*`) use the same feedback-style source/capture model.
**Contexts — user wiring API DONE** (approved 2026-07-04; `types/context_wiring.h`,
design record services.rst *Contexts*): `context::scope<"name"> ctx{w, port}`
publishes for a wiring scope (stack on `OperatorRegistry`, mesh-scope
precedent); `Context<"name", S>` is an In alias tagged `InputBinding::Context`
— auto-wired from the nearest scope, keyword-overridable
(`arg<"name">(port)`), no positional slot (`call_args` auto-param machinery);
`context::get`/`has` are the function forms. Same-Wiring only: cross-wiring
lookups throw (nested import/export deferred). Real-time wall-clock scheduler
alarms landed (`NodeScheduler(..., on_wall_clock=true)`, real-time executors only).
TSW (tick-based windows) also executes end-to-end; duration-based windows have
registry+runtime ops but no compile-time marker yet. Remaining at the boundary:
request/reply + subscription adaptor flows and concrete adaptor families,
`@component`, nested context import/export, `Context<>` on registered operator
impls.

**Error handling — DONE** (design record:
`docs/source/developer_guide/error_handling.rst`, authoritative). `NodeError` is a
value-layer compound scalar (a `bundle` of `str` fields; `TS<NodeError>` now resolves —
the runtime gained whole-value `TS` over a `Bundle` value, via
`is_compact_atomic_ts_data`). **Per-node capture**: `evaluate_impl` wraps the user
eval in try/catch when `captures_errors`, writes a `NodeError` to the node error
output, and uses `error_msg = "unknown error"` for non-`std::exception` throws;
ordinary output on an error cycle is intentionally unspecified because writes
before the throw are not rolled back. `exception_time_series(port)` activates
capture by re-binding the node (`NodeBuilder::with_error_capture`,
`Wiring::activate_error_capture`) and returns `Port<TS<NodeError>>`. **`try_except_<G>`**
wraps a sub-graph on the `single_nested_graph_node` substrate (custom start/evaluate,
`manage_output_externally`): runs the child under try/catch, output is an owned
`TSB{exception, out}` (sink → bare `TS<NodeError>`), copying the child output into `out`
on success and writing `exception` on a throw. `runtime/{node_error,try_except_node}.*`,
`exception_time_series`/`try_except_` in `types/subgraph_wiring.h`. Tests:
`tests/cpp/test_error_handling.cpp`. ASAN/UBSAN-verified. Deferred: `map_` error variant
(`TSD[K, TS[NodeError]]`), `__trace_back_depth__`/`__capture_values__` + richer traces,
zero-copy `out` forwarding, capture on custom-ops nodes.

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
- **Single-threaded evaluation** (ruling 2026-07-02): the **per-tick runtime path**
  (value/TS/runtime ops invoked during evaluation) must be lock-free and
  `shared_ptr`-free. **Build-time machinery** (interning, plan/ops-synthesis caches,
  registries) MAY use mutexes to guard shared resources — that is sanctioned, not
  drift. Push-source senders + the real-time executor CV remain the only
  cross-thread runtime boundary.
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
