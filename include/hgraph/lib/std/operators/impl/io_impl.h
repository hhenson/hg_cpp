#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_IO_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_IO_IMPL_H

#include <hgraph/lib/std/operators/io.h>        // debug_print / null_sink / record / replay / log_
#include <hgraph/runtime/logger.h>
#include <hgraph/lib/testing/record_replay.h>   // the in-memory (GlobalState) record/replay backend
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

#include <fmt/core.h>

namespace hgraph::stdlib
{
    /**
     * Implementations + registration for the I/O / debug operators. The abstract markers
     * are in ``<hgraph/lib/std/operators/io.h>``; this file provides the concrete sinks and
     * ``register_io_operators`` to register them. Only the diagnostic sinks (``debug_print``
     * / ``null_sink``) are implemented so far.
     */

    /**
     * ``debug_print`` implementation: a single generic sink that prints ``label: value`` on
     * each tick of ``ts`` (the value renders through the type-erased view ``to_string``).
     * The Python ``print_delta`` / ``sample`` parameters are not yet modelled.
     */
    struct debug_print_impl
    {
        static void eval(Scalar<"label", Str> label, In<"ts", TsVar<"S">> ts)
        {
            fmt::print("{}: {}\n", label.value(), ts.value().to_string());
        }
    };

    /**
     * ``log_`` implementation: formats the (runtime) format string with the
     * rendered argument and logs through the LOGGER injectable. ``level``
     * uses the spdlog scale (0 trace .. 5 critical; default info).
     */
    struct log_impl
    {
        static constexpr auto name = "log_";

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"level", Value{Int{2}}}};   // info
        }

        static void eval(In<"fmt", TS<Str>> format, In<"args", TsVar<"A">, InputValidity::Unchecked> args,
                         Scalar<"level", Int> level, LoggerView log)
        {
            log.log(static_cast<int>(level.value()),
                    fmt::format(fmt::runtime(format.value()),
                                args.valid() ? args.value().to_string() : std::string{"<n/a>"}));
        }
    };

    /** ``null_sink`` implementation: a single generic sink that consumes ``ts`` and does nothing. */
    struct null_sink_impl
    {
        static void eval(In<"ts", TsVar<"S">> ts) { static_cast<void>(ts); }
    };

    /** Register the I/O / debug operator overloads. */
    inline void register_io_operators()
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

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_IO_IMPL_H
