#ifndef HGRAPH_LIB_STD_STD_NODES_H
#define HGRAPH_LIB_STD_STD_NODES_H

#include <hgraph/types/static_node.h>

#include <fmt/core.h>

#include <stdexcept>
#include <string>

namespace hgraph::stdlib
{
    /**
     * A small standard library of reusable C++ nodes. These are ordinary static
     * nodes (see *Authoring Nodes in C++*); they are grouped here as the building
     * blocks most graphs and tests reach for.
     */

    /**
     * Constant source: emits a fixed value once at the start cycle.
     *
     * Named ``const_`` because ``const`` is a C++ keyword (the Python operator is
     * ``const``). A SINGLE generic node: the value type is a ``ScalarVar`` inferred
     * from the configured value. Without an explicit output type it resolves to
     * ``TS<T>``; with an explicit output type the configured value must match that
     * time-series' current value schema. ``wire<stdlib::const_>(w, 42)`` emits a
     * ``TS<int>`` tick, while ``wire<stdlib::const_, TSS<int>>(w, set_value)`` emits
     * a constant set. It ticks once at start (``PullSource``) and does not reschedule.
     */
    struct const_
    {
        static constexpr auto name              = "const";
        static constexpr bool schedule_on_start = true;

        static void resolve_default_types(ResolutionMap &resolution)
        {
            const auto *value_schema = resolution.scalar("T");
            const auto *output_schema = resolution.find_ts("S");
            if (output_schema == nullptr)
            {
                resolution.bind_ts("S", TypeRegistry::instance().ts(value_schema));
                return;
            }
            if (output_schema->value_schema != value_schema)
            {
                throw std::logic_error("const: configured value schema does not match the resolved output value schema");
            }
        }

        static void eval(Scalar<"value", ScalarVar<"T">> value, Out<TsVar<"S">> out)
        {
            out.apply(value.value());  // erased copy of the configured value, ticked at the start cycle
        }
    };

    /**
     * Debug sink: prints ``label: value`` on each tick of its input (a diagnostic
     * aid, not a data path). A SINGLE generic node over any time-series type — the
     * value renders through the type-erased view ``to_string``.
     */
    struct debug_print
    {
        static constexpr auto name = "debug_print";
        static void           eval(In<"ts", TsVar<"S">> ts, Scalar<"label", std::string> label)
        {
            fmt::print("{}: {}\n", label.value(), ts.value().to_string());
        }
    };

    /**
     * Null sink: consumes its input and does nothing. A SINGLE generic node over any
     * time-series type. Useful to give a graph a terminal sink (so an output is
     * driven) without side effects.
     */
    struct null_sink
    {
        static constexpr auto name = "null_sink";
        static void           eval(In<"ts", TsVar<"S">> ts) { static_cast<void>(ts); }
    };
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_STD_NODES_H
