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

#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace hgraph::stdlib
{
    /**
     * Implementations + registration for the I/O / debug operators. The abstract markers
     * are in ``<hgraph/lib/std/operators/io.h>``; this file provides the concrete sinks and
     * ``register_io_operators`` to register them. Only the diagnostic sinks (``debug_print``
     * / ``null_sink``) are implemented so far.
     */

    /** The diagnostic-sink WRITER hook: defaults to C stdout/stderr; the
        python module re-points it at ``sys.stdout``/``sys.stderr`` so
        redirection (and pytest capture) behaves like hgraph's python
        prints. One line per call (no trailing newline in ``line``). */
    using IoWriteFn = void (*)(std::string_view line, bool to_stdout);
    [[nodiscard]] IoWriteFn &io_write_slot() noexcept;
    void io_write(std::string_view line, bool to_stdout);

    /**
     * ``debug_print`` implementation: a single generic sink that prints ``label: value`` on
     * each tick of ``ts`` (the value renders through the type-erased view ``to_string``).
     * ``sample=N`` prints every N-th tick with an ``[N]`` prefix (hgraph's
     * shape); ``print_delta`` is not yet modelled.
     */
    struct debug_print_impl
    {
        static void eval(Scalar<"label", Str> label, In<"ts", TsVar<"S">> ts, Scalar<"sample", Int> sample,
                         State<Int> ticks)
        {
            if (sample.value() > 1)
            {
                const Int seen = ticks.get() + 1;
                ticks.set(seen);
                if (seen % sample.value() != 0) { return; }
                io_write(fmt::format("[{}] {}: {}", sample.value(), label.value(), ts.value().to_string()), true);
                return;
            }
            io_write(fmt::format("{}: {}", label.value(), ts.value().to_string()), true);
        }

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"sample", Value{Int{1}}}};
        }
    };

    namespace io_impl_detail
    {
        /** python-style ``{}`` / ``{name}`` formatting over a packed
            structural bundle of arguments (positional entries are the
            leading unnamed fields in call order; kwargs carry names). */
        [[nodiscard]] std::string format_bundle(std::string_view format, const TSInputView &packed);
    }  // namespace io_impl_detail

    /** ``__print_sink``: the runtime half of ``print_`` — formats the packed
        argument bundle into ``fmt`` and writes one line to stdout/stderr. */
    struct print_sink_impl
    {
        static constexpr auto name = "print_sink";

        static void eval(In<"fmt", TS<Str>> format, In<"args", TsVar<"A">, InputValidity::Unchecked> args,
                         Scalar<"to_stdout", Bool> to_stdout)
        {
            io_write(io_impl_detail::format_bundle(format.value(), args.base()), to_stdout.value());
        }
    };

    /** ``__assert_sink``: the runtime half of the format-args ``assert_``. */
    struct assert_fmt_sink_impl
    {
        static constexpr auto name = "assert_fmt_sink";

        static void eval(In<"condition", TS<Bool>> condition, Scalar<"error_msg", Str> format,
                         In<"args", TsVar<"A">, InputValidity::Unchecked> args)
        {
            if (condition.value()) { return; }
            throw std::runtime_error(io_impl_detail::format_bundle(format.value(), args.base()));
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
            // Check the level FIRST: no formatting (and no erased to_string
            // rendering) when the message would be filtered out anyway.
            const auto lvl = static_cast<int>(level.value());
            if (!log.should_log(lvl)) { return; }
            log.log(lvl, fmt::format(fmt::runtime(format.value()),
                                     args.valid() ? args.value().to_string() : std::string{"<n/a>"}));
        }
    };

    /** ``null_sink`` implementation: a single generic sink that consumes ``ts`` and does nothing. */
    struct null_sink_impl
    {
        static void eval(In<"ts", TsVar<"S">> ts) { static_cast<void>(ts); }
    };

    namespace io_impl_detail
    {
        /** Pack positional format args (unnamed leading fields, call order)
            and kwargs (named fields) into one structural bundle. */
        [[nodiscard]] WiringPortRef pack_format_args(std::vector<WiringPortRef> positional,
                                                     std::vector<std::pair<std::string, WiringPortRef>> named);
    }  // namespace io_impl_detail

    /** ``print_(fmt, *args, **kwargs)`` — python-style formatting to
        stdout (``__std_out__=False`` writes stderr). */
    struct print_compose
    {
        static constexpr auto name = "print_compose";

        static auto compose(Wiring &w, NamedPort<"fmt", TS<Str>> fmt, VarIn<"args", TsVar<"B">> positional,
                            Scalar<"__std_out__", Bool> to_stdout, VarKwIn<"kwargs"> kwargs)
        {
            WiringPortRef packed = io_impl_detail::pack_format_args(
                std::vector<WiringPortRef>{positional.begin(), positional.end()},
                std::vector<std::pair<std::string, WiringPortRef>>{kwargs.begin(), kwargs.end()});
            return wire<print_sink_op>(w, fmt, Port<void>{w, std::move(packed)}, to_stdout.value());
        }

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"__std_out__", Value{true}}};
        }
    };

    /** ``assert_(condition, error_msg)`` — throw ``error_msg`` when the
        condition ticks false. */
    struct assert_plain_impl
    {
        static constexpr auto name = "assert_plain";

        static void eval(In<"condition", TS<Bool>> condition, Scalar<"error_msg", Str> message)
        {
            if (!condition.value()) { throw std::runtime_error(std::string{message.value()}); }
        }
    };

    /** ``assert_(condition, fmt, *args, **kwargs)`` — the format-args form. */
    struct assert_fmt_compose
    {
        static constexpr auto name = "assert_fmt";

        static auto compose(Wiring &w, NamedPort<"condition", TS<Bool>> condition,
                            Scalar<"error_msg", Str> message, VarIn<"args", TsVar<"B">> positional,
                            VarKwIn<"kwargs"> kwargs)
        {
            WiringPortRef packed = io_impl_detail::pack_format_args(
                std::vector<WiringPortRef>{positional.begin(), positional.end()},
                std::vector<std::pair<std::string, WiringPortRef>>{kwargs.begin(), kwargs.end()});
            return wire<assert_fmt_op>(w, condition, message.value(), Port<void>{w, std::move(packed)});
        }
    };

    /** Register the I/O / debug operator overloads. */
    void register_io_operators();
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_IO_IMPL_H
