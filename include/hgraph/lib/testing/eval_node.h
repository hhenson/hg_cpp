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
     * Per-time-series-schema harness trait — the single extension point that lets
     * ``eval_node`` exchange the right per-cycle "harness element" with the test and
     * wire the matching ``replay`` source / ``record`` sink.
     *
     * The harness element is the typed scalar ``T`` for scalar-delta leaves
     * (``TS<T>`` and tick-count ``TSW<T,...>``, so a test writes ``{1, none, 3}``),
     * ``bool`` for ``SIGNAL``, and the **canonical delta ``Value``** for collection
     * kinds (``TSS`` / ``TSD`` / ``TSL`` / …, built by ``set_delta`` / ``dict_delta`` /
     * ``list_delta`` and compared with ``Value::equals``). One adapter covers every
     * schema — ``replay`` / ``record`` are SINGLE erased nodes resolved at wiring
     * (``replay`` from the explicit type ``S``, ``record`` from its connected input
     * port), so no per-kind variant is needed.
     */
    template <typename S> struct harness_element { using type = Value; };
    template <typename T> struct harness_element<TS<T>> { using type = T; };
    template <typename T, std::size_t Period, std::size_t MinPeriod>
    struct harness_element<TSW<T, Period, MinPeriod>>
    {
        using type = T;
    };
    template <> struct harness_element<SIGNAL> { using type = bool; };

    template <typename S>
    struct ts_harness
    {
        using element                       = typename harness_element<S>::type;
        static constexpr bool is_scalar     = static_node_detail::is_scalar_ts<S>::value;

        // Source: the output type is supplied explicitly (S), giving a typed Port<S>.
        static auto wire_replay(Wiring &w, const std::string &key) { return wire<replay, S>(w, key); }

        static void seed(const GlobalStateView &gs, std::string_view key, const std::vector<std::optional<element>> &seq)
        {
            if constexpr (is_scalar) { set_replay_values<element>(gs, key, seq); }
            else { set_replay_deltas(gs, key, seq); }
        }

        // Sink: the input type resolves from the connected port.
        template <typename Port>
        static void wire_record(Wiring &w, Port port, const std::string &key)
        {
            wire<record>(w, port, key);
        }

        static std::vector<std::optional<element>> read(const GlobalStateView &gs, std::string_view key)
        {
            if constexpr (is_scalar) { return get_recorded_values<element>(gs, key); }
            else { return get_recorded_deltas(gs, key); }
        }
    };

    namespace eval_node_detail
    {
        // The node's wire-time parameters (In + Scalar, in eval order).
        template <typename NodeT>
        using wire_params_t = typename StaticNodeSignature<NodeT>::wire_param_types;

        // The node's (single) output time-series schema.
        template <typename NodeT>
        using output_schema_t = typename StaticNodeSignature<NodeT>::output_schema_type;

        template <typename NodeT, typename Params>
        struct first_input_element;

        template <typename NodeT>
        struct first_input_element<NodeT, std::tuple<>>
        {
            static_assert(static_node_detail::always_false_v<NodeT>,
                          "eval_node: NodeT must have at least one time-series input");
            using type = void;
        };

        template <typename NodeT, typename First, typename... Rest>
        struct first_input_element_impl
        {
            static_assert(static_node_detail::always_false_v<NodeT>,
                          "eval_node: the first In/Scalar parameter must be a time-series input");
            using type = void;
        };

        template <typename NodeT, fixed_string Name, typename Schema, typename... Rest>
        struct first_input_element_impl<NodeT, In<Name, Schema>, Rest...>
        {
            using type = typename ts_harness<Schema>::element;
        };

        template <typename NodeT, typename First, typename... Rest>
        struct first_input_element<NodeT, std::tuple<First, Rest...>>
            : first_input_element_impl<NodeT, First, Rest...>
        {
        };

        // Per-cycle harness element of the first input / the output (T, SetDelta<T>, ...).
        template <typename NodeT>
        using first_input_element_t = typename first_input_element<NodeT, wire_params_t<NodeT>>::type;

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
     * (``T`` for scalar-delta leaves, ``bool`` for ``SIGNAL``, and the canonical
     * delta ``Value`` for collection kinds such as ``TSS`` / ``TSD`` / fixed or
     * dynamic ``TSL`` — see :cpp:class:`ts_harness`),
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
     * exactly one output. Scalar (``TS``) and any container kind supported by the
     * erased replay/record delta path are accepted on inputs and the output.
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
