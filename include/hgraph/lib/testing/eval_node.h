#ifndef HGRAPH_LIB_TESTING_EVAL_NODE_H
#define HGRAPH_LIB_TESTING_EVAL_NODE_H

#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/static_node.h>
#include <hgraph/util/date_time.h>

#include <cstddef>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

namespace hgraph::testing
{
    namespace eval_node_detail
    {
        // The scalar value type of the node's single time-series input / output.
        template <typename NodeT>
        using input_value_t =
            typename std::tuple_element_t<0, typename StaticNodeSignature<NodeT>::input_schema_types>::value_type;

        template <typename NodeT>
        using output_value_t = typename StaticNodeSignature<NodeT>::output_schema_type::value_type;
    }  // namespace eval_node_detail

    /**
     * Evaluate a node over a sequence of per-cycle inputs and return its per-cycle
     * outputs — the C++ counterpart of the Python ``eval_node`` harness.
     *
     * It wires ``replay<TIn> -> NodeT -> record<TOut>``, seeds the inputs into the
     * graph's ``GlobalState``, runs a simulation from ``MIN_ST`` (one engine cycle
     * per input element, ``std::nullopt`` = no tick that cycle), and reads the
     * recorded outputs back as a cycle-aligned vector (``std::nullopt`` where the
     * node did not tick).
     *
     * First slice: ``NodeT`` has exactly one ``In<TS<TIn>>``, one ``Out<TS<TOut>>``
     * and no scalar inputs. ``TIn`` / ``TOut`` are inferred from its signature.
     */
    template <typename NodeT>
    [[nodiscard]] std::vector<std::optional<eval_node_detail::output_value_t<NodeT>>>
    eval_node(const std::vector<std::optional<eval_node_detail::input_value_t<NodeT>>> &input)
    {
        using sig = StaticNodeSignature<NodeT>;
        static_assert(sig::input_count() == 1, "eval_node (first slice): NodeT must have exactly one time-series input");
        static_assert(sig::output_count() == 1, "eval_node (first slice): NodeT must have exactly one output");
        static_assert(sig::scalar_count() == 0, "eval_node (first slice): NodeT must have no scalar inputs");

        using TIn  = eval_node_detail::input_value_t<NodeT>;
        using TOut = eval_node_detail::output_value_t<NodeT>;

        Wiring w;
        auto   in_port  = wire<replay<TIn>>(w, std::string{"eval_node::in"});
        auto   out_port = wire<NodeT>(w, in_port);
        wire<record<TOut>>(w, out_port, std::string{"eval_node::out"});
        GraphBuilder gb = std::move(w).finish();

        set_replay_values<TIn>(gb.global_state(), "eval_node::in", input);

        // Run one cycle per input element (plus a margin so the last cycle fires).
        const auto           span = static_cast<engine_time_delta_t::rep>(input.size() + 1);
        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + engine_time_delta_t{span});

        GraphExecutorValue executor = eb.make_executor();
        auto               view     = executor.view();
        view.run();

        return get_recorded_values<TOut>(view.graph().global_state(), "eval_node::out");
    }
}  // namespace hgraph::testing

#endif  // HGRAPH_LIB_TESTING_EVAL_NODE_H
