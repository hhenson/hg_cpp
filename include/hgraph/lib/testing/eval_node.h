#ifndef HGRAPH_LIB_TESTING_EVAL_NODE_H
#define HGRAPH_LIB_TESTING_EVAL_NODE_H

#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/static_node.h>
#include <hgraph/util/date_time.h>

#include <algorithm>
#include <cstddef>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace hgraph::testing
{
    /**
     * Per-time-series-schema harness traits — the extension point that lets
     * ``eval_node`` drive each input/output through the right replay / record node
     * and exchange the right per-cycle "harness element" with the test.
     *
     * For a schema ``S`` the trait names:
     *
     * - ``element`` — the per-cycle value the test deals in (``T`` for ``TS<T>``,
     *   ``SetDelta<T>`` for ``TSS<T>``);
     * - ``wire_replay`` — wires the source that emits a seeded buffer;
     * - ``seed`` — seeds that buffer from a ``vector<optional<element>>``;
     * - ``wire_record`` — wires the sink that captures the output;
     * - ``read`` — reads the captured buffer back as a ``vector<optional<element>>``.
     *
     * Adding a new time-series kind (``TSL`` / ``TSD`` next) is a new
     * specialisation here plus its ``replay`` / ``record`` pair — ``eval_node``
     * itself does not change.
     */
    template <typename S>
    struct ts_harness;  // primary intentionally undefined: unsupported schema -> hard error

    template <typename T>
    struct ts_harness<TS<T>>
    {
        using element = T;

        static auto wire_replay(Wiring &w, const std::string &key) { return wire<replay<T>>(w, key); }

        static void seed(const GlobalStateView &gs, std::string_view key,
                         const std::vector<std::optional<element>> &seq)
        {
            set_replay_values<T>(gs, key, seq);
        }

        template <typename Port>
        static void wire_record(Wiring &w, Port port, const std::string &key)
        {
            wire<record<T>>(w, port, key);
        }

        static std::vector<std::optional<element>> read(const GlobalStateView &gs, std::string_view key)
        {
            return get_recorded_values<T>(gs, key);
        }
    };

    template <typename T>
    struct ts_harness<TSS<T>>
    {
        using element = SetDelta<T>;

        static auto wire_replay(Wiring &w, const std::string &key) { return wire<replay_set<T>>(w, key); }

        static void seed(const GlobalStateView &gs, std::string_view key,
                         const std::vector<std::optional<element>> &seq)
        {
            set_replay_deltas<T>(gs, key, seq);
        }

        template <typename Port>
        static void wire_record(Wiring &w, Port port, const std::string &key)
        {
            wire<record_set<T>>(w, port, key);
        }

        static std::vector<std::optional<element>> read(const GlobalStateView &gs, std::string_view key)
        {
            return get_recorded_deltas<T>(gs, key);
        }
    };

    namespace eval_node_detail
    {
        // The node's wire-time parameters (In + Scalar, in eval order).
        template <typename NodeT>
        using wire_params_t = typename StaticNodeSignature<NodeT>::wire_param_types;

        // Time-series schema of the first wire parameter (required to be an input).
        template <typename NodeT>
        using first_input_schema_t = typename std::tuple_element_t<0, wire_params_t<NodeT>>::schema;

        // The node's (single) output time-series schema.
        template <typename NodeT>
        using output_schema_t = typename StaticNodeSignature<NodeT>::output_schema_type;

        // Per-cycle harness element of the first input / the output (T, SetDelta<T>, ...).
        template <typename NodeT>
        using first_input_element_t = typename ts_harness<first_input_schema_t<NodeT>>::element;

        template <typename NodeT>
        using output_element_t = typename ts_harness<output_schema_t<NodeT>>::element;

        inline std::string input_key(std::size_t index) { return "eval_node::in" + std::to_string(index); }
    }  // namespace eval_node_detail

    /**
     * Evaluate a node over per-cycle inputs and return its per-cycle outputs — the
     * C++ counterpart of the Python ``eval_node`` harness.
     *
     * Arguments are given in the node's **eval-parameter order** (its ``In`` and
     * ``Scalar`` parameters): a **time-series input** is a
     * ``std::vector<std::optional<E>>`` where ``E`` is that input's harness element
     * (``T`` for ``TS<T>``, ``SetDelta<T>`` for ``TSS<T>`` — see :cpp:class:`ts_harness`),
     * one element per engine cycle from ``MIN_ST`` (``none`` = no tick), and a
     * **scalar input** is the value itself. The harness wires the matching ``replay``
     * per time-series input, the node, then the matching ``record`` on the output,
     * seeds the inputs, runs to quiescence, and returns the recorded output
     * (cycle-aligned, at least as long as the longest input, never truncated — a node
     * may emit beyond the input window).
     *
     * The **first** parameter must be a time-series input (so it can be a braced
     * list — its element type is inferred from the node); any later time-series
     * inputs are passed as ``std::vector<std::optional<E>>``. The node must have
     * exactly one output. Both scalar (``TS``) and set (``TSS``) time-series are
     * supported on inputs and the output; other container kinds
     * (``TSB``/``TSL``/``TSD``/``TSW``) are a future extension (a new
     * :cpp:class:`ts_harness` specialisation plus its replay/record pair).
     */
    template <typename NodeT, typename... Rest>
    [[nodiscard]] std::vector<std::optional<eval_node_detail::output_element_t<NodeT>>>
    eval_node(const std::vector<std::optional<eval_node_detail::first_input_element_t<NodeT>>> &input0, Rest &&...rest)
    {
        using sig         = StaticNodeSignature<NodeT>;
        using wire_params = eval_node_detail::wire_params_t<NodeT>;
        using out_schema  = eval_node_detail::output_schema_t<NodeT>;
        constexpr std::size_t arg_count = 1 + sizeof...(Rest);

        static_assert(sig::output_count() == 1, "eval_node: NodeT must have exactly one output");
        static_assert(sig::input_count() >= 1, "eval_node: NodeT must have at least one time-series input");
        static_assert(arg_count == std::tuple_size_v<wire_params>,
                      "eval_node: argument count must match the node's In + Scalar parameters (in eval order)");
        static_assert(static_node_detail::is_input_selector<std::tuple_element_t<0, wire_params>>::value,
                      "eval_node: the first In/Scalar parameter must be a time-series input");

        Wiring w;
        auto   all = std::forward_as_tuple(input0, std::forward<Rest>(rest)...);

        // Pass 1: wire the matching replay per time-series input (returning its port)
        // and pass scalar values straight through, building the wire<NodeT> arg list.
        auto wire_arg = [&]<std::size_t I>() {
            using P = std::tuple_element_t<I, wire_params>;
            if constexpr (static_node_detail::is_input_selector<P>::value)
            {
                return ts_harness<typename P::schema>::wire_replay(w, eval_node_detail::input_key(I));
            }
            else
            {
                return std::get<I>(all);  // scalar value
            }
        };

        auto out_port = [&]<std::size_t... I>(std::index_sequence<I...>) {
            return wire<NodeT>(w, wire_arg.template operator()<I>()...);
        }(std::make_index_sequence<arg_count>{});

        ts_harness<out_schema>::wire_record(w, out_port, std::string{"eval_node::out"});
        GraphBuilder gb = std::move(w).finish();

        // Pass 2: seed each time-series input's replay buffer (same key per slot) and
        // track the longest input.
        std::size_t max_len = 0;
        [&]<std::size_t... I>(std::index_sequence<I...>) {
            (
                [&] {
                    using P = std::tuple_element_t<I, wire_params>;
                    if constexpr (static_node_detail::is_input_selector<P>::value)
                    {
                        const auto &seq = std::get<I>(all);
                        max_len         = std::max(max_len, seq.size());
                        ts_harness<typename P::schema>::seed(gb.global_state(), eval_node_detail::input_key(I), seq);
                    }
                }(),
                ...);
        }(std::make_index_sequence<arg_count>{});

        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MAX_ET);
        GraphExecutorValue executor = eb.make_executor();
        auto               view     = executor.view();
        view.run();

        // Cycle-align: pad to the longest input, never truncate (see record).
        auto out = ts_harness<out_schema>::read(view.graph().global_state(), "eval_node::out");
        if (out.size() < max_len) { out.resize(max_len); }
        return out;
    }
}  // namespace hgraph::testing

#endif  // HGRAPH_LIB_TESTING_EVAL_NODE_H
