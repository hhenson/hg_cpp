#include <hgraph/lib/std/operators/impl/io_impl.h>

namespace hgraph::stdlib
{
    void register_io_operators()
    {
        register_overload<debug_print, debug_print_impl>();
        register_overload<null_sink, null_sink_impl>();
        // ``record`` / ``replay`` register the in-memory GlobalState backend
        // (the testing toolkit's cycle-aligned ``List<Any>`` buffer) as the
        // operator implementations, making them name-resolvable — a Python
        // frontend needs the registry path, not the C++ ``wire<T>`` sugar
        // (tests/cpp/test_erased_wiring.cpp). Pluggable record/replay backends
        // (Python's model registry / DataWriter) remain roadmap P3.
        register_overload<record, testing::record>();
        register_overload<replay, testing::replay>();
        register_overload<log_, log_impl>();
    }
}  // namespace hgraph::stdlib
