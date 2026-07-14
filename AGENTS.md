# AGENTS.md

## Project Direction

This repository is the C++-first implementation of hgraph. The runtime, system
nodes, graph execution, time-series values, and native graph/node APIs are C++
and are the primary source of truth.

Python remains a supported authoring and compatibility surface for:

- wiring against the current Python hgraph ecosystem,
- Python user-authored nodes running inside the C++ runtime,
- packaging and bindings through the optional Python bridge.

Do not implement runtime semantics independently in Python when they belong in
C++. A feature exposed to Python must also have an equivalent, first-class C++
wiring path and comparable C++ tests. Python should adapt Python values and
callables to that path rather than become a second runtime implementation.

## Working Method

- Read the relevant implementation, tests, and nearby documentation before
  editing. Prefer established repository patterns over new abstractions.
- Use a short plan when work spans multiple modules or changes ownership,
  lifecycle, type-system, or public API behavior. Keep the plan current while
  implementing; do not stop after proposing it.
- Use focused tests during development, then run the acceptance gates below.
- Resolve decisions from the codebase when possible. Ask only when a missing
  product or architecture decision would materially change the result.
- Keep changes scoped. Do not combine unrelated cleanup with the requested
  work, and do not rewrite user changes encountered in the worktree.
- Treat compiler warnings as potential defects. Fix their cause; suppress one
  only when it is a documented false positive with a narrow suppression.
- Report exactly what changed, what was validated, and any remaining risk. Do
  not describe code work as complete when a required gate was skipped or failed.

## Definition Of Done

Focused tests are useful while iterating but are not completion evidence. For
every code change, completion requires both of these local gates to pass:

1. The complete native C++ test suite.
2. The complete non-WIP Python compatibility suite using the newest supported
   Python version (currently Python 3.14).

Run a clean native build rather than relying on a stale IDE build:

```sh
native_root="$(mktemp -d)"
cmake -S . -B "$native_root/build" -GNinja \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=ON \
  -DHGRAPH_BUILD_PYTHON_BINDINGS=OFF \
  -DHGRAPH_ENABLE_PYTHON_USER_NODES=OFF \
  -DHGRAPH_ENABLE_IDE_PYTHON_HEADER_HINTS=OFF \
  -DHGRAPH_USE_PYARROW_ARROW=ON \
  -DHGRAPH_FETCH_SIMDJSON=ON \
  -DHGRAPH_WARNINGS_AS_ERRORS=ON \
  -DPython_EXECUTABLE="$PWD/.venv/bin/python"
cmake --build "$native_root/build" --parallel
ctest --test-dir "$native_root/build" --output-on-failure --parallel 2
```

Build the stable-ABI wheel with Python 3.12, install that wheel into a fresh
environment, and run the suite with Python 3.14:

```sh
wheel_dir="$(mktemp -d)"
test_env="$(mktemp -d)"
uv build --wheel --python 3.12 --out-dir "$wheel_dir"
uv venv --python 3.14 "$test_env"
uv pip install --python "$test_env/bin/python" \
  "$wheel_dir"/*.whl "pytest>=8" "frozendict>=2.4" trove-classifiers
"$test_env/bin/python" -m pytest python/tests -q -m "not wip"
```

For large changes, especially runtime, ownership, memory-layout, type-erasure,
or cross-language changes, run the same full C++ and Python validation in the
local Linux VM before reporting completion. Use the Ubuntu/OrbStack workflow in
`docs/source/developer_guide/debugging.rst`; add ASan when lifetime or memory
safety is involved. macOS is the normal local gate. Windows remains a
best-effort CI platform, but portable code should still be maintained.

GitHub CI runs after a push and is not a substitute for these local gates. The
user monitors post-push CI and will report platform-specific failures. Do not
wait for or monitor CI unless asked.

Documentation-only changes do not require the full runtime suites; validate
the edited documentation, commands, links, or configuration directly.

## Runtime Invariants

- System nodes are implemented in C++ only. C++ graph and node authoring must
  remain first-class, not a binding layer over Python concepts.
- Keep Python-specific code behind explicit build and ownership boundaries.
- Graph/run configuration belongs in `GlobalState`. Preserve its copy-in before
  execution and copy-out after execution semantics for Python callers; do not
  create unrelated process globals for graph-specific configuration.
- Use planned storage and in-place construction for graph static memory.
  Keyed nested graphs such as map and mesh use the slot-store and slot-observer
  protocols for capacity, keys, and lifetime.
- Nested graph deletion stops the graph and unsubscribes it; erase performs the
  destructor. Do not add retired-object side containers to bypass that protocol.
- Prefer existing scope guards from `scope.h` for cleanup and rollback.
- Preserve type information and ownership explicitly across type-erased APIs;
  do not extend a temporary's lifetime through a reference.

## Test Guidelines

- C++ tests live under `tests/cpp`; Python compatibility tests live under
  `python/tests`.
- Test behavior through public wiring APIs. Graph and node behavior tests should
  use `eval_node`; construct runtime internals directly only for lower-level unit
  tests whose subject is that internal contract.
- When generic or erased wiring needs a concrete test signature, wrap the item
  in a minimal graph with concrete `Port` parameters and return type, then call
  `eval_node` on that graph. Do not hand-wire replay/record or run an executor
  directly to work around type inference in a behavior test.
- Every Python-visible behavior needs equivalent C++ wiring coverage at the same
  behavioral level. Python tests additionally prove bridge and authoring parity.
- Scale coverage with risk, including lifecycle, teardown, invalid input, and
  mixed C++/Python execution where relevant.
- Register new C++ test files in `tests/cpp/CMakeLists.txt`.

## Build System

- Use CMake as the authoritative build system. Keep `pyproject.toml` as the
  Python packaging bridge.
- A normal CMake configure/build must not depend on Python, nanobind, or Python
  package installation unless the relevant Python option is enabled.
- Keep the main runtime target `hgraph_core` with public alias `hgraph::core`.
- Prefer explicit CMake targets over directory-global include or link state.
- Python bindings are opt-in through `HGRAPH_BUILD_PYTHON_BINDINGS`; Python
  user-node support is opt-in through `HGRAPH_ENABLE_PYTHON_USER_NODES`.
- For CMake, packaging, or public-header changes, also validate install and a
  consumer build as described in `docs/source/developer_guide/build_system.rst`.

## Source Layout

- `include/hgraph/...`: public C++ headers.
- `src/...`: C++ runtime implementation.
- `python/...`: Python package, optional extension bridge, and compatibility
  layer.
- `tests/cpp/...`: native C++ tests.
- `python/tests/...`: Python bridge, authoring, and compatibility tests.
- `docs/source/...`: Sphinx user and developer documentation.

`include/hgraph/version.h` is generated from `include/hgraph/version.h.in` into
the CMake build tree and installed with the public headers. Do not commit build
trees, CMake caches, virtual environments, Python bytecode, or other generated
artifacts.

## Dependency Policy

- Prefer CMake packages when available.
- Use `FetchContent` deliberately and behind normal CMake options or dependency
  discovery.
- Do not add heavyweight runtime dependencies without a clear need.
- Python packaging dependencies belong in `pyproject.toml`; C++ runtime
  dependencies belong in CMake.

## Git Hygiene

- The worktree may contain unrelated user or concurrent-agent changes. Ignore
  them unless they directly affect the task; never revert them.
- Keep edits and staging scoped to the requested task.
- Do not delete or rewrite existing staged work unless explicitly asked.
- Do not commit or push unless the user explicitly requests it.
