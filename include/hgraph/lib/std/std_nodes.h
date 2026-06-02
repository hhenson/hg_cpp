#ifndef HGRAPH_LIB_STD_STD_NODES_H
#define HGRAPH_LIB_STD_STD_NODES_H

#include <hgraph/types/static_node.h>

#include <fmt/core.h>

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
     * ``const``). It is a ``PullSource`` configured by its ``value`` scalar; it
     * ticks once at start and does not reschedule.
     */
    template <typename T>
    struct const_
    {
        static constexpr auto name              = "const";
        static constexpr bool schedule_on_start = true;
        static void           eval(Scalar<"value", T> value, Out<TS<T>> out) { out.set(value.value()); }
    };

    /**
     * Debug sink: prints ``label: value`` on each tick of its input (a diagnostic
     * aid, not a data path). ``T`` must be formattable by ``fmt``.
     */
    template <typename T>
    struct debug_print
    {
        static constexpr auto name = "debug_print";
        static void           eval(In<"ts", TS<T>> ts, Scalar<"label", std::string> label)
        {
            fmt::print("{}: {}\n", label.value(), ts.value());
        }
    };

    /**
     * Null sink: consumes its input and does nothing. Useful to give a graph a
     * terminal sink (so an output is driven) without side effects.
     */
    template <typename T>
    struct null_sink
    {
        static constexpr auto name = "null_sink";
        static void           eval(In<"ts", TS<T>> ts) { static_cast<void>(ts); }
    };
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_STD_NODES_H
