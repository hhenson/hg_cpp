# C++ Stack Trace Support

The project now includes backward-cpp for detailed C++ stack traces when errors occur.

## Features

1. **Automatic crash handling**: Stack traces are automatically printed when the C++ code crashes (SIGSEGV, SIGABRT, etc.)
2. **Manual stack traces**: You can get stack traces programmatically from Python or C++
3. **Symbol resolution**: Stack traces include function names, addresses, and offsets

## Usage from Python

```python
import _hgraph

# Print stack trace to stderr
_hgraph.print_stack_trace()

# Get stack trace as a string
trace = _hgraph.get_stack_trace()
print(trace)
```

## Usage from C++

```cpp
#include <hgraph/util/stack_trace.h>

// Print stack trace to stderr
hgraph::print_stack_trace();

// Get stack trace as a string
std::string trace = hgraph::get_stack_trace();
std::cerr << trace << std::endl;
```

## Automatic Crash Handling

Crash handlers are automatically installed when the `_hgraph` module is imported. When a crash occurs (segmentation fault, abort, etc.), a detailed stack trace will be printed to stderr before the program terminates.

## Example Output

```
Stack trace (most recent call last):
#5    Object "libpython3.12.dylib", at 0x1017a76a3, in _PyEval_EvalFrameDefault + 45375
#4    Object "_hgraph.cpython-312-darwin.so", at 0x108e74f03, in nanobind::detail::nb_func_vectorcall_simple_0
#3    Object "_hgraph.cpython-312-darwin.so", at 0x108a5c443, in func_create<>::lambda()::__invoke
#2    Object "_hgraph.cpython-312-darwin.so", at 0x108e5ee63, in hgraph::print_stack_trace()
#1    Object "_hgraph.cpython-312-darwin.so", at 0x108e5ed0f, in backward::StackTraceImpl<>::load_here
#0    Object "_hgraph.cpython-312-darwin.so", at 0x108e5f43f, in backward::details::unwind
```

## Adding Stack Traces to Error Messages

You can include stack traces in your error messages:

```cpp
#include <hgraph/util/stack_trace.h>

try {
    // Some operation
} catch (const std::exception& e) {
    std::string error_msg = std::string(e.what()) + "\n\nStack trace:\n" + hgraph::get_stack_trace();
    throw std::runtime_error(error_msg);
}
```

## Dependencies

- **backward-cpp**: Installed via conan (see `conandata.yml`)
- Automatically linked when building the project

## Configuration

The crash handlers are automatically installed. If you want to disable them, you would need to modify `src/cpp/python/_hgraph_module.cpp` and comment out the call to `hgraph::install_crash_handlers()`.
