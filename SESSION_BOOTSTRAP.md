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

# 4. Create symlink for fast iteration (recommended)
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
| `TSW[T]` | Window time-series | ❌ Not implemented |
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
- **`HG_CPP.todo`** - Comprehensive technical analysis, findings, and prioritized action plan (MUST READ)
- **`SESSION_BOOTSTRAP.md`** (this file) - Quick session initialization
- **`ROADMAP.md`** - High-level feature roadmap
- **`README.md`** - Project setup instructions

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
- TSW (windowed time-series) not implemented
- Some collection type compatibility checks are permissive (empty collections)
- REF observer set uses raw pointers (fragile)
- Signal `is_same_type` returns true unconditionally (by design for now)

### Development Environment
- **Python:** 3.12+ required
- **Build System:** CMake (builds C++ library directly for development)
  - `cmake --build cmake-build-debug` compiles to `cmake-build-debug/src/cpp/_hgraph.*.so`
  - scikit-build-core + Conan used for distribution builds
- **Bindings:** nanobind 2.2.0+
- **Testing:** pytest
- **Dependency Manager:** uv (use `uv pip install -e .` for initial setup)
- **Development Workflow:**
  1. Build with CMake (produces `_hgraph.*.so` in build directory)
  2. Symlink `.so` into `.venv/lib/python3.12/site-packages/`
  3. Rebuild only when changing C++ code (no reinstall needed with symlink)

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

## Code Architecture Deep Dive

### Time Series Type Hierarchy

**Python Reference** (`reference/hgraph/src/hgraph/_types/_time_series_types.py`):
```python
class TimeSeries(ABC):
    @property value, delta_value, modified, valid, all_valid, last_modified_time

class TimeSeriesOutput(TimeSeries):
    # Observable pattern - schedules subscribers on change
    apply_result(value), invalidate(), mark_modified()

class TimeSeriesInput(TimeSeries, Notifiable):
    # Observer pattern - receives notifications
    bind_output(output), active/passive state
```

**C++ Implementation** (`include/hgraph/types/time_series_type.h`):
```cpp
struct TimeSeriesType : nb::intrusive_base {
    virtual nb::object py_value() const = 0;
    virtual nb::object py_delta_value() const = 0;
    virtual bool modified() const = 0;
    virtual bool valid() const = 0;
    virtual bool all_valid() const = 0;
    virtual engine_time_t last_modified_time() const = 0;
    virtual bool is_same_type(TimeSeriesType &other) const = 0;
};

struct TimeSeriesOutput : TimeSeriesType {
    virtual void apply_result(nb::object value) = 0;
    virtual void invalidate() = 0;
    void subscribe(Notifiable *node);  // Observer pattern
};

struct TimeSeriesInput : TimeSeriesType, Notifiable {
    virtual bool bind_output(time_series_output_ptr output_) = 0;
    virtual bool active() const;
    void notify(engine_time_t modified_time) override;
};
```

### Node Lifecycle

**Key Methods** (both Python and C++):
1. **`initialise()`** - Setup phase before graph starts
2. **`start()`** - Calls `do_start()`, initializes inputs/outputs
3. **`eval()`** - Calls `do_eval()`, the main computation
4. **`stop()`** - Calls `do_stop()`, cleanup
5. **`dispose()`** - Final cleanup

**Important - Lifecycle Method Access**:
- ⚠️ **NEVER call lifecycle methods directly** (`start()`, `stop()`, `initialise()`, `dispose()`)
- ✅ **Use helper functions** (defined in `include/hgraph/util/lifecycle.h`):
  - `initialise_component(component)`
  - `start_component(component)`
  - `stop_component(component)`
  - `dispose_component(component)`
- ✅ **Use RAII guards** (C++ only, defined in `include/hgraph/util/lifecycle.h`):
  - `InitialiseDisposeContext` - Calls `initialise()` in constructor, `dispose()` in destructor
  - `StartStopContext` - Calls `start()` in constructor, `stop()` in destructor

**Why?** These helpers/guards:
- Manage internal state flags (`_started`, `_transitioning`)
- Ensure proper cleanup even if exceptions are thrown
- Match Python's context manager behavior

**Example (C++)**:
```cpp
// WRONG - Don't do this:
node->start();
// ... do work ...
node->stop();  // Might not be called if exception thrown!

// CORRECT - Use RAII guard:
{
    StartStopContext context(*node);  // Calls start_component() in constructor
    // ... do work ...
}  // Calls stop_component() in destructor, guaranteed even with exceptions

// Or use helper functions directly:
start_component(*node);
// ... do work ...
stop_component(*node);  // Make sure this is called!

// For full lifecycle:
{
    InitialiseDisposeContext init_context(*node);
    {
        StartStopContext run_context(*node);
        // ... node is now running ...
    }  // stop() called here
}  // dispose() called here
```

**Lifecycle Order** (see `include/hgraph/util/lifecycle.h`):
1. **Construct** → 2. **`initialise()`** (once, forward topological order in graphs) →
3. **`start()`** (can be called multiple times) → 4. **`stop()`** (can be called multiple times) →
5. **`dispose()`** (once, reverse topological order in graphs) → 6. **Destruct**

**Node Implementation**:
- `start()` and `stop()` are framework methods (call `do_start()` / `do_stop()`)
- Override `do_start()`, `do_eval()`, `do_stop()` in custom nodes
- Python nodes use `_eval_fn`, `_start_fn`, `_stop_fn` callables

### Node Scheduling and Evaluation

**Push vs Pull Source Nodes**:
- **PUSH_SOURCE_NODE** - External events (real-time only), e.g., WebSocket feeds
- **PULL_SOURCE_NODE** - Time-series data (simulation or replay), e.g., CSV files
- **COMPUTE_NODE** - Reactive computation (scheduled when inputs change)
- **SINK_NODE** - Side effects (e.g., write to database)

**Scheduling Flow**:
1. Output marks modified → `mark_modified(modified_time)`
2. Output notifies subscribers → `_notify(modified_time)`
3. Input receives notification → `notify(modified_time)`
4. Input schedules owning node → `node.notify(modified_time)`
5. Graph schedules node for evaluation → `graph.schedule_node(node)`

### Mesh Node Architecture

**Key Concept**: Dynamic dependency graph with rank-based scheduling to avoid cycles.

**Components**:
- **Rank System**: Graphs are assigned ranks based on dependencies (0 = no deps, higher = more deps)
- **Dependency Tracking**: Each mesh graph tracks what it depends on (`_active_graphs_dependencies`)
- **Re-ranking**: When cycles detected, graphs are re-ranked to maintain acyclic ordering
- **Context Path**: Mesh nodes register a `TimeSeriesReference` in `GlobalState` for cross-graph access

**Python**: `reference/hgraph/src/hgraph/_impl/_runtime/_mesh_node.py`
**C++**: `src/cpp/nodes/mesh_node.cpp`, `include/hgraph/nodes/mesh_node.h`

**Critical Methods**:
- `create_new_graph(key, rank)` - Instantiate new sub-graph
- `schedule_graph(key, time)` - Schedule graph for evaluation at time
- `add_graph_dependency(key, depends_on)` - Track dependency
- `re_rank(key, depends_on)` - Adjust ranks to prevent cycles

### Evaluation Modes

**SIMULATION Mode**:
- Time advances to next scheduled event
- `SimulationEvaluationClock.now()` == `evaluation_time()`
- All events from PULL sources
- Deterministic, reproducible

**REAL_TIME Mode**:
- Time advances with wall clock
- `RealTimeEvaluationClock.now()` == actual system time
- Mix of PULL (historical) and PUSH (live) sources
- Non-deterministic

### Builder Architecture

**Two-Phase Construction**:
1. **Wiring Phase** (Python) - Type checking, graph structure
   - `make_instance(...)` creates `WiringNodeInstance`
   - `make_node(...)` creates `NodeBuilder`
2. **Build Phase** (C++) - Actual instantiation
   - `NodeBuilder.make_instance(...)` creates C++ `Node`
   - `InputBuilder/OutputBuilder` create time-series instances

**Factory Pattern** (`hg_cpp/__init__.py`):
```python
# Register C++ builders
hgraph.TimeSeriesBuilderFactory.declare(HgCppFactory())
hgraph.GraphBuilderFactory.declare(_make_cpp_graph_builder)
hgraph.GraphEngineFactory.declare(lambda ...: _hgraph.GraphExecutorImpl(...))

# Register node builders by type
SwitchWiringNodeClass.BUILDER_CLASS = _create_switch_node_builder_factory
MeshWiringNodeClass.BUILDER_CLASS = _create_mesh_node_builder_factory
```

### Key Type Mapping

| Python Type | C++ Type | Key Methods |
|-------------|----------|-------------|
| `TS[T]` | `TimeSeriesValueInput<T>` / `TimeSeriesValueOutput<T>` | `value()`, `delta_value()` |
| `TSL[T, SIZE]` | `TimeSeriesListInput<T>` / `TimeSeriesListOutput<T>` | `__getitem__()`, `items()` |
| `TSS[T]` | `TimeSeriesSetInput_T<T>` / `TimeSeriesSetOutput_T<T>` | `added()`, `removed()` |
| `TSD[K,V]` | `TimeSeriesDictInput_T<K>` / `TimeSeriesDictOutput_T<K>` | `added_keys()`, `removed_keys()`, `modified_keys()` |
| `TSB[Schema]` | `TimeSeriesBundleInput` / `TimeSeriesBundleOutput` | `__getitem__(attr)` |
| `REF[T]` | `TimeSeriesReferenceInput` / `TimeSeriesReferenceOutput` | `output()`, `bind_output()` |

---

## Debugging Techniques

### Using `eval_node` for Testing

The `eval_node` helper (`reference/hgraph/src/hgraph/test/_node_unit_tester.py`) runs a node in SIMULATION mode:

```python
from hgraph.test import eval_node

# Basic usage - input as list, each element is a tick
assert eval_node(my_node, [1, 2, None, 3]) == [1, 2, None, 3]

# Multiple inputs
assert eval_node(my_node,
    ts1=[1, 2, 3],
    ts2=["a", "b", "c"]
) == [expected_output]

# Enable tracing (commented out in some tests)
result = eval_node(my_node,
    input=[...],
    __trace__={"start": True, "stop": True}  # Prints lifecycle events
)
```

**How it works**:
- Wraps your node in a graph with replay sources
- Feeds inputs tick-by-tick starting at `MIN_ST` + `MIN_TD` increments
- Captures outputs as `delta_value`
- Returns list of results (with `None` for no-tick cycles)

### Adding Trace Statements

**Python Side** (in `reference/hgraph/`):
```python
# In any node implementation file
print(f"[TRACE] {self.signature.name}: {variable_name} = {value}")

# In do_eval():
def do_eval(self):
    print(f"[EVAL] Node {self.node_ndx} evaluating at {self.graph.evaluation_clock.evaluation_time}")
    # ... rest of logic
```

**C++ Side**:
```cpp
// In any .cpp file
#include <fmt/format.h>

// In do_eval():
void do_eval() override {
    fmt::print("[EVAL] Node {} evaluating\n", node_ndx());
    // ... rest of logic
}

// For Python object inspection:
nb::object py_obj = ...;
fmt::print("[DEBUG] py_obj = {}\n", nb::str(py_obj).c_str());
```

### Common Debugging Patterns

**1. Compare Python vs C++ Output**:
```bash
# Run with Python
cd tests/python/unit/engine
# Comment out: import hg_cpp
pytest test_mesh.py::test_mesh -v -s

# Run with C++
# Uncomment: import hg_cpp
pytest test_mesh.py::test_mesh -v -s

# Compare outputs
```

**2. Inspect NodeSignature**:
```python
# In test or node code
print(f"Signature: {node.signature.name}")
print(f"Inputs: {node.signature.time_series_inputs}")
print(f"Output: {node.signature.time_series_output}")
print(f"Scalars: {node.signature.scalars}")
print(f"Active inputs: {node.signature.active_inputs}")
```

**3. Check Type Compatibility**:
```python
# When debugging type binding issues
input_ts = ...
output_ts = ...
print(f"Same type? {input_ts.is_same_type(output_ts)}")
print(f"Input type: {type(input_ts)}")
print(f"Output type: {type(output_ts)}")
```

**4. Trace Graph Execution**:
```python
# Create a custom observer
from hgraph import EvaluationLifeCycleObserver

class DebugObserver(EvaluationLifeCycleObserver):
    def on_before_node_evaluation(self, node):
        print(f"[EVAL] {node.signature.name} (ndx={node.node_ndx})")

    def on_after_node_evaluation(self, node):
        if node.has_output():
            print(f"  → modified={node.output().modified()}")

# Use with eval_node
eval_node(my_graph, input=[...], __observers__=[DebugObserver()])
```

**5. Verify C++ is Actually Running**:
```python
# Add to top of test
import _hgraph
print(f"Using C++ Node: {_hgraph.Node}")
print(f"Node module: {_hgraph.Node.__module__}")

# During execution, check node types:
# If you see Python node classes executing, C++ port is incomplete
```

### Understanding Common Errors

**Type Binding Errors**:
```
IncorrectTypeBinding: Cannot bind TS[float] to TS[int]
```
→ Check `is_same_type()` implementations, ensure types match exactly

**Missing Factory Registration**:
```
NotImplementedError: Missing builder for HgTSLTypeMetaData[...]
```
→ Check `hg_cpp/_builder_factories.py` has entry for the type

**Reference Counting Issues**:
```
Segmentation fault (core dumped)
```
→ Check `inc_ref()` / `dec_ref()` balance, look for dangling pointers

**Scheduling Issues**:
```
Node evaluated before input valid
```
→ Check `active_inputs` specification in `NodeSignature`
→ Verify `bind_output()` calls `make_active()` on active inputs

---

## Project Statistics

- **C++ Files**: 46 `.cpp` + 50 `.h` = 96 files
- **Total Lines**: ~14,852 lines of C++ code
- **Test Files**: 19 test files in `tests/python/unit/engine/`
- **Python Reference**: Full hgraph library in `reference/hgraph/`

---

## Quick File Location Reference

### When Debugging Specific Node Types

| If working on... | Python Reference | C++ Implementation | C++ Header |
|------------------|------------------|-------------------|------------|
| **Mesh Node** | `reference/hgraph/src/hgraph/_impl/_runtime/_mesh_node.py` | `src/cpp/nodes/mesh_node.cpp` | `include/hgraph/nodes/mesh_node.h` |
| **Switch Node** | `reference/hgraph/src/hgraph/_impl/_runtime/_switch_node.py` | `src/cpp/nodes/switch_node.cpp` | `include/hgraph/nodes/switch_node.h` |
| **TSD Map Node** | `reference/hgraph/src/hgraph/_impl/_runtime/_map_node.py` | `src/cpp/nodes/tsd_map_node.cpp` | `include/hgraph/nodes/tsd_map_node.h` |
| **Reduce Node** | `reference/hgraph/src/hgraph/_impl/_runtime/_reduce_node.py` | `src/cpp/nodes/reduce_node.cpp` | `include/hgraph/nodes/reduce_node.h` |
| **Component Node** | `reference/hgraph/src/hgraph/_impl/_runtime/_component_node.py` | `src/cpp/nodes/component_node.cpp` | `include/hgraph/nodes/component_node.h` |
| **Base Node** | `reference/hgraph/src/hgraph/_impl/_runtime/_node.py` | `src/cpp/types/node.cpp` | `include/hgraph/types/node.h` |

### When Debugging Specific Types

| If working on... | Python Reference | C++ Implementation | C++ Header |
|------------------|------------------|-------------------|------------|
| **TS (scalar)** | `reference/hgraph/src/hgraph/_impl/_types/_ts_type_impl.py` | `src/cpp/types/ts.cpp` | `include/hgraph/types/ts.h` |
| **TSL (list)** | `reference/hgraph/src/hgraph/_impl/_types/_tsl_type_impl.py` | `src/cpp/types/tsl.cpp` | `include/hgraph/types/tsl.h` |
| **TSS (set)** | `reference/hgraph/src/hgraph/_impl/_types/_tss_type_impl.py` | `src/cpp/types/tss.cpp` | `include/hgraph/types/tss.h` |
| **TSD (dict)** | `reference/hgraph/src/hgraph/_impl/_types/_tsd_type_impl.py` | `src/cpp/types/tsd.cpp` | `include/hgraph/types/tsd.h` |
| **TSB (bundle)** | `reference/hgraph/src/hgraph/_impl/_types/_tsb_type_impl.py` | `src/cpp/types/tsb.cpp` | `include/hgraph/types/tsb.h` |
| **REF** | `reference/hgraph/src/hgraph/_impl/_types/_ref_type_impl.py` | `src/cpp/types/ref.cpp` | `include/hgraph/types/ref.h` |

### When Debugging Runtime/Engine

| Component | Python Reference | C++ Implementation | C++ Header |
|-----------|------------------|-------------------|------------|
| **EvaluationEngine** | `reference/hgraph/src/hgraph/_impl/_runtime/_evaluation_engine_impl.py` | `src/cpp/runtime/evaluation_engine.cpp` | `include/hgraph/runtime/evaluation_engine.h` |
| **Graph** | `reference/hgraph/src/hgraph/_impl/_runtime/_graph.py` | `src/cpp/types/graph.cpp` | `include/hgraph/types/graph.h` |
| **GraphExecutor** | `reference/hgraph/src/hgraph/_impl/_runtime/_graph_executor.py` | `src/cpp/runtime/graph_executor.cpp` | `include/hgraph/runtime/graph_executor.h` |
| **EvaluationContext** | `reference/hgraph/src/hgraph/_runtime/_evaluation_context.py` | `src/cpp/runtime/evaluation_context.cpp` | `include/hgraph/runtime/evaluation_context.h` |

### When Debugging Builders

| Component | Python Reference | C++ Implementation | C++ Header |
|-----------|------------------|-------------------|------------|
| **InputBuilder** | `reference/hgraph/src/hgraph/_impl/_builder/_input_builder.py` | `src/cpp/builders/input_builder.cpp` | `include/hgraph/builders/input_builder.h` |
| **OutputBuilder** | `reference/hgraph/src/hgraph/_impl/_builder/_output_builder.py` | `src/cpp/builders/output_builder.cpp` | `include/hgraph/builders/output_builder.h` |
| **NodeBuilder** | `reference/hgraph/src/hgraph/_impl/_builder/_node_builder.py` | `src/cpp/builders/node_builder.cpp` | `include/hgraph/builders/node_builder.h` |
| **GraphBuilder** | `reference/hgraph/src/hgraph/_impl/_builder/_graph_builder.py` | `src/cpp/builders/graph_builder.cpp` | `include/hgraph/builders/graph_builder.h` |

### Python/C++ Bridge Files

| Purpose | File | Description |
|---------|------|-------------|
| **Main module** | `src/cpp/python/_hgraph_module.cpp` | Registers all C++ types/functions with Python |
| **Type exports** | `src/cpp/python/_hgraph_types.cpp` | Exports time-series types (TS, TSL, TSD, etc.) |
| **Builder exports** | `src/cpp/python/_hgraph_builder.cpp` | Exports builder classes |
| **Node exports** | `src/cpp/python/_hgraph_nodes.cpp` | Exports node types |
| **Runtime exports** | `src/cpp/python/_hgraph_runtime.cpp` | Exports engine, graph, executor |
| **Factory registration** | `hg_cpp/__init__.py` | Registers C++ factories with Python framework |
| **Builder factories** | `hg_cpp/_builder_factories.py` | Maps Python types to C++ builders |

### Important Utility Files

| File | Description |
|------|-------------|
| **`include/hgraph/util/lifecycle.h`** | ⚠️ **CRITICAL**: Lifecycle helper functions and RAII guards (`StartStopContext`, `InitialiseDisposeContext`). **Always use these instead of calling lifecycle methods directly!** |
| **`include/hgraph/hgraph_base.h`** | Core type definitions, forward declarations |
| **`include/hgraph/python/global_state.h`** | GlobalState for mesh node context storage |
| **`include/hgraph/util/reference_count_subscriber.h`** | Observer pattern implementation |

---

*This document should be reviewed at the start of each debugging session to quickly re-establish context.*
