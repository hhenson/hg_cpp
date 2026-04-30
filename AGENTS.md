# AGENTS.md

## Project Direction

This repository is the C++ first implementation of hgraph. The runtime, system nodes, graph execution, time-series values, and native C++ graph/node APIs should be implemented in C++ as the primary source of truth.

Python remains important, but it is not the runtime foundation. Python support is for:

- wiring and compatibility with the current Python hgraph ecosystem,
- Python user-authored nodes running inside the C++ runtime,
- optional packaging and bindings through the Python bridge.

Do not make normal CMake configure/build depend on Python, nanobind, or Python package installation unless the Python bridge is explicitly enabled.

## Build System

- Use CMake as the primary build system.
- Keep `CMakeLists.txt` usable for a pure C++ build.
- Keep `pyproject.toml` as the Python packaging bridge, not as the authoritative build definition.
- Prefer explicit CMake targets over global include/link state.
- The main runtime target should remain `hgraph_core` with the public alias `hgraph::core`.
- Python bindings should be opt-in through `HGRAPH_BUILD_PYTHON_BINDINGS`.
- Python user-node support should be opt-in through `HGRAPH_ENABLE_PYTHON_USER_NODES`.

Useful local checks:

```sh
cmake -S . -B /tmp/hg_cpp-cmake-check
cmake --build /tmp/hg_cpp-cmake-check
cmake --install /tmp/hg_cpp-cmake-check --prefix /tmp/hg_cpp-install
python3 -c "import pathlib, tomllib; tomllib.loads(pathlib.Path('pyproject.toml').read_text())"
```

## Source Layout

Expected layout as the codebase grows:

- `include/hgraph/...`: public C++ headers.
- `src/...`: C++ runtime implementation.
- `bindings/python/...`: optional Python extension bridge.
- `tests/cpp/...`: C++ tests.
- `tests/python/...`: Python compatibility tests only where the bridge or wiring requires them.
- `docs/source/...`: Sphinx documentation for Read the Docs, split into user and developer guides.

`include/hgraph/version.h` is generated from `include/hgraph/version.h.in` into the CMake build tree and installed with the public headers.

Avoid committing generated build trees, CMake cache files, virtual environments, or Python bytecode.

## Implementation Guidelines

- Prefer C++23 library features where they simplify the code.
- Keep Python-specific code behind clear boundaries.
- System nodes should be C++ only.
- C++ graph and node authoring should be first-class, not a binding layer over Python concepts.
- When porting from older attempts, preserve useful C++ runtime ideas but avoid copying Python-first build assumptions.
- Add abstractions only when they clarify ownership, runtime behavior, or public API shape.
- Keep tests close to the behavior being introduced.

## Dependency Policy

- Prefer CMake packages when available.
- Use `FetchContent` only deliberately and behind normal CMake options or dependency discovery.
- Do not introduce heavyweight runtime dependencies without a clear need.
- Python packaging dependencies belong in `pyproject.toml`; C++ runtime dependencies belong in CMake.

## Git Hygiene

- The worktree may contain unrelated user changes. Do not revert them.
- Keep edits scoped to the requested task.
- Do not delete or rewrite existing staged work unless explicitly asked.
