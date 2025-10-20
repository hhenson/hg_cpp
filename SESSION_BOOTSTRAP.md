# Session Bootstrap Guide

**Last Updated:** 2025-10-16
**Purpose:** Quick-start reference for initializing debugging/development sessions on the hg_cpp C++ port.

---

## Project Overview

### What is hg_cpp?
A high-performance C++ port of the Python [hgraph](https://github.com/hhenson/hgraph) library, focusing on:
- **Runtime subsystem** (graph execution engine, scheduling, evaluation)
- **Type subsystem** (time-series types, builders, type compatibility)

### Key Principles
1. **Python is the reference implementation** - When in doubt, Python behavior is correct
2. **C++ must conform to Python** - Don't assume you've found a bug unless it's also in Python
3. **Toggle mechanism** - Tests can switch between Python/C++ via `import hg_cpp` at the top of test files
4. **Runtime purity** - Once the graph engine is running, ALL code (nodes, graphs, types, builders) MUST be C++. Seeing Python code executing during runtime is a bug indicator.

---

## First Time Setup

If starting fresh or after a clean checkout:

```bash
# 1. Create virtual environment with uv
uv venv --python 3.12

# 2. Install Python dependencies (including hgraph)
uv pip install -e .

# 3. Build C++ library with CMake
cmake --build cmake-build-debug

# 4. Remove the installed library
rm $(pwd)/cmake-build-debug/src/cpp/_hgraph.cpython-312-darwin.so

# 5. Create symlink for fast iteration (recommended)
ln -sf $(pwd)/cmake-build-debug/src/cpp/_hgraph.cpython-312-darwin.so \
       .venv/lib/python3.12/site-packages/_hgraph.cpython-312-darwin.so

# 5. Verify installation
pytest tests/python/unit/engine/test_ts.py::test_const -v
```

After this setup, you only need `cmake --build cmake-build-debug` when changing C++ code. The symlink ensures Python immediately sees the new build.

---

## Project Structure

```
hg_cpp/
├── tests/python/unit/engine/     # Test files (19 files, all with import hg_cpp toggle)
│   ├── test_mesh.py              # Example: mesh graph tests
│   ├── test_switch.py            # Switch node tests
│   ├── test_ts*.py               # Time-series type tests
│   └── ...
├── src/cpp/                       # C++ implementation
│   ├── types/                    # TS, TSL, TSS, TSD, TSB, REF implementations
│   ├── builders/                 # Input/Output builders
│   ├── nodes/                    # Node implementations (mesh, switch, reduce, etc.)
│   ├── runtime/                  # EvaluationEngine, GraphExecutor
│   └── python/                   # Nanobind bindings
├── include/hgraph/                # C++ headers (mirrors src/cpp structure)
├── hg_cpp/                        # Python bridge module
│   ├── __init__.py               # Factory registration, C++/Python integration
│   └── _builder_factories.py    # Builder factory implementations
├── reference/hgraph/              # Python reference implementation (submodule)
│   └── src/hgraph/               # Original Python code to reference
└── HG_CPP.todo                    # Comprehensive analysis and action plan
```

The location of the Python is: .venv/lib/python3.12/site-packages/hgraph

There is also a reference implementation in `reference/hgraph/src/hgraph/` that you can use as a reference, but not to modify
as this code is not evaluated.

---

## Debugging Workflow

### Standard Process
1. **Identify the failing test** in `tests/python/unit/engine/test_*.py`
2. **Comment out** `import hg_cpp` to run Python implementation
3. **Add tracing** to Python code (in `reference/hgraph/src/hgraph/`) to understand expected behavior
4. **Re-enable** `import hg_cpp` to run C++ implementation
5. **Add similar tracing** to C++ code to identify discrepancies
6. **Fix the C++ code** to match Python behavior
7. **Verify** the fix doesn't break other tests

### Toggle Pattern
```python
import hg_cpp  # Comment this out to use Python instead of C++
```

### Critical Runtime Rule
> Once the graph engine is running, **ALL** execution (nodes, graphs, types, builders) should be C++ code.
> If you see Python code executing, that's a strong indicator of missing C++ porting or incorrect factory registration.

---

## Architecture Quick Reference

### Time Series Types
| Type | Description | Status |
|------|-------------|--------|
| `TS[T]` | Scalar time-series | ✅ Implemented |
| `TSL[T, SIZE]` | List time-series | ✅ Implemented |
| `TSS[T]` | Set time-series | ✅ Implemented |
| `TSD[K, V]` | Dict/map time-series | ✅ Implemented |
| `TSB[Schema]` | Bundle time-series | ✅ Implemented |
| `REF[T]` | Reference time-series | ✅ Implemented |
| `TSW[T]` | Window time-series | ✅ Implemented |
| `SIGNAL` | Signal wrapper | ✅ Implemented (input only) |

### Node Types
- **PythonNode** / **PythonGeneratorNode** - User-defined compute/generator nodes
- **NestedGraphNode** - Graph-within-graph
- **ComponentNode** - Component lifecycle nodes
- **SwitchNode** - Conditional branching (key-based dispatch)
- **MeshNode** - Dynamic mesh/network nodes
- **TsdMapNode** - Map operations over TSD
- **ReduceNode** / **NonAssociativeReduceNode** - Reduction operations
- **TryExceptNode** - Exception handling
- **ContextNode** - Context management
- **LastValuePullNode** - Pull-based value access

### Builder Architecture
- **InputBuilder** - Constructs input time-series during wiring
- **OutputBuilder** - Constructs output time-series during wiring
- **NodeBuilder** - Constructs node instances during graph building
- **GraphBuilder** - Constructs entire graphs with edges

---

## Common Issues Checklist

When debugging, consider these frequent problem areas:

### 1. Type Compatibility
- [ ] `is_same_type()` implementations (especially for empty collections)
- [ ] Schema validation for bundles
- [ ] Nested type comparisons (e.g., `TSD[str, TSL[TS[int]]]`)
- [ ] Python/C++ type boundary crossing

### 2. Reference Counting & Lifecycle
- [ ] `inc_ref()` / `dec_ref()` balance
- [ ] Observer registration/unregistration
- [ ] Dangling raw pointers (especially in REF observer sets)
- [ ] Node lifecycle hooks: `do_start()`, `do_stop()`, `dispose()`

### 3. Delta Semantics
- [ ] Set delta: added/removed items
- [ ] Dict delta: added/removed/modified keys
- [ ] List delta: modifications vs. full replacement
- [ ] Delta application vs. value access

### 4. Graph Execution
- [ ] Node scheduling order
- [ ] Source node registration
- [ ] Dependency cycles detection
- [ ] Force-set semantics
- [ ] Clock management (evaluation_clock vs. engine_clock)

### 5. Python/C++ Bridge
- [ ] Factory registration in `hg_cpp/__init__.py`
- [ ] Nanobind type conversions
- [ ] Builder selection by type
- [ ] Edge/path representation

---

## Quick Start Commands

### Build
```bash
# Full rebuild
cmake --build cmake-build-debug --clean-first

# Incremental build
cmake --build cmake-build-debug

# Option 1: Install Python package (slower, copies library)
uv pip install -e .

# Option 2: Create symlink for faster iteration (recommended for development)
# After initial "uv pip install -e .", create a symlink to avoid reinstalls:
ln -sf $(pwd)/cmake-build-debug/src/cpp/_hgraph.cpython-312-darwin.so \
       .venv/lib/python3.12/site-packages/_hgraph.cpython-312-darwin.so

# With symlink in place, you only need to rebuild (no reinstall):
cmake --build cmake-build-debug
# The symlink automatically points to the new build

# Note: Adjust the .so filename if your Python version or platform differs
# (e.g., _hgraph.cpython-312-linux-x86_64.so on Linux)
```

### Testing
```bash
# Run all engine tests
pytest tests/python/unit/engine/

# Run single test file
pytest tests/python/unit/engine/test_mesh.py

# Run single test function
pytest tests/python/unit/engine/test_mesh.py::test_mesh

# Run with output
pytest tests/python/unit/engine/test_mesh.py -v -s

# Run Python implementation (for comparison)
# 1. Comment out "import hg_cpp" in test file
# 2. Run pytest as above
```

### Git Status
```bash
# Check current state
git status

# View recent work
git log --oneline -10

# Check which branch we're on
git branch --show-current
```

### Reference Implementation
```bash
# View Python reference implementation
cd reference/hgraph
git log --oneline -5  # Check reference version
```

---

## Key Files Reference

### Essential Documents
- **`SESSION_BOOTSTRAP.md`** (this file) - Quick session initialization

### Critical Source Files

#### Python Bridge
- **`hg_cpp/__init__.py`** - Factory declarations, C++/Python integration glue
  - NodeBuilder class assignments
  - GraphBuilderFactory and GraphEngineFactory declarations
  - Type-specific builder factory functions (switch, mesh, reduce, etc.)
- **`hg_cpp/_builder_factories.py`** - TimeSeriesBuilderFactory implementation
  - Input/output builder selection by type
  - Type-to-builder mapping dictionaries

#### C++ Core Headers
- **`include/hgraph/types/`** - Time-series type definitions
  - `ts.h`, `tsl.h`, `tss.h`, `tsd.h`, `tsb.h`, `ref.h`, `ts_signal.h`
- **`include/hgraph/builders/`** - Builder interfaces
  - `input_builder.h`, `output_builder.h`, `node_builder.h`, `graph_builder.h`
- **`include/hgraph/nodes/`** - Node type definitions
- **`include/hgraph/runtime/`** - Evaluation engine

#### C++ Nanobind Bindings
- **`src/cpp/python/_hgraph_module.cpp`** - Main module registration
- **`src/cpp/python/_hgraph_types.cpp`** - Type exports
- **`src/cpp/python/_hgraph_builder.cpp`** - Builder exports
- **`src/cpp/python/_hgraph_nodes.cpp`** - Node exports
- **`src/cpp/python/_hgraph_runtime.cpp`** - Runtime exports

---

## Test File Pattern

All test files follow this pattern:

```python
import hg_cpp  # Comment this out to use Python instead of C++
from hgraph import (
    # imports...
)
from hgraph.test import eval_node

def test_something():
    @graph
    def g(...) -> ...:
        # graph definition
        return ...

    assert eval_node(g, input=[...]) == expected_output
```

### Available Test Files (19 total)
- `test_ts.py`, `test_ts_auto_cast.py`, `test_ts_wiring.py`
- `test_tsl.py`, `test_tsl_wiring.py`
- `test_tss.py`
- `test_tsd.py`, `test_tsd_wiring.py`
- `test_tsb.py`, `test_tsb_wiring.py`
- `test_switch.py`
- `test_mesh.py`
- `test_map.py`
- `test_reduce.py`
- `test_ref.py`
- `test_service.py`
- `test_context.py`
- `test_node_signature.py`
- `test_injectables.py`
- `test_var_args.py`

---

## Typical Session Flow

1. **Read this document** (SESSION_BOOTSTRAP.md)
2. **Check `HG_CPP.todo`** for known issues and current priorities
3. **Review git status** to see what's changed
4. **Identify the test** you're working on
5. **Run the test** with C++ to see the failure
6. **Toggle to Python** to understand expected behavior
7. **Add tracing** to both implementations as needed
8. **Fix C++ code** to match Python behavior
9. **Verify** with tests
10. **Update `HG_CPP.todo`** if you discover new issues

---

## Important Notes

### Python Reference Implementation
The `reference/hgraph/` directory is a git submodule containing the original Python implementation. This is the **source of truth** for correct behavior.

```bash
# Update reference if needed
cd reference/hgraph
git pull origin main
cd ../..
git add reference/hgraph
```

### Known Limitations (from HG_CPP.todo)
- Some collection type compatibility checks are permissive (empty collections)
- REF observer set uses raw pointers (fragile)
- Signal `is_same_type` returns true unconditionally (by design for now)

### Development Environment
- **Python:** 3.12+ required
- **Build System:** CMake (builds C++ library directly for development)
  - `cmake --build cmake-build-debug` compiles to `cmake-build-debug/src/cpp/_hgraph.*.so`
  - scikit-build-core + Conan used for distribution builds
  - uv for overall build and packaging
- **Bindings:** nanobind 2.2.0+
- **Testing:** pytest
- **Dependency Manager:** uv (use `uv pip install -e .` for initial setup)
- **Development Workflow:**
  1. Build with CMake (produces `_hgraph.*.so` in build directory)
  2. Remove exiting `.so` files from `.venv/lib/python3.12/site-packages/`
  3. Symlink `.so` into `.venv/lib/python3.12/site-packages/`
  4. Rebuild only when changing C++ code (no reinstall needed with symlink)

---

## Getting Help

### When Stuck
1. Compare Python vs C++ behavior with tracing
2. Check `HG_CPP.todo` for known issues
3. Review Python reference implementation in `reference/hgraph/src/hgraph/`
4. Look for similar patterns in passing tests
5. Check nanobind registration in `src/cpp/python/_hgraph_*.cpp`

### Common Mistakes to Avoid
- ❌ Assuming C++ found a bug without checking Python
- ❌ Forgetting to rebuild after C++ changes
- ❌ Not checking if Python code is executing during graph runtime
- ❌ Missing factory registration in `hg_cpp/__init__.py`
- ❌ Type mismatches at Python/C++ boundary
- ❌ **Calling lifecycle methods directly** (`node->start()`) instead of using helpers (`start_component(*node)`) or RAII guards (`StartStopContext`)

---
