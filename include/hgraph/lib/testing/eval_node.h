#ifndef HGRAPH_LIB_TESTING_EVAL_NODE_H
#define HGRAPH_LIB_TESTING_EVAL_NODE_H

#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/static_node.h>
#include <hgraph/util/date_time.h>

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
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

        // A concrete static node has a static ``eval``; an operator marker does not.
        template <typename T>
        concept has_eval = requires { &T::eval; };

        // ``first_input_element_t`` / ``output_element_t`` appear in the *signature* of the
        // concrete ``eval_node`` overload. For an operator marker (no ``eval``) those
        // ``StaticNodeSignature``-based computations would be a hard error (not SFINAE), so
        // they are guarded: a non-eval type falls back to ``Value`` and the concrete overload
        // is removed by its ``operator_tag`` constraint instead of failing to compile.
        template <typename NodeT, bool = has_eval<NodeT>>
        struct first_input_element_lazy
        {
            using type = Value;
        };
        template <typename NodeT>
        struct first_input_element_lazy<NodeT, true>
        {
            using type = typename first_input_element<NodeT, wire_params_t<NodeT>>::type;
        };

        template <typename NodeT, bool = has_eval<NodeT>>
        struct output_element_lazy
        {
            using type = Value;
        };
        template <typename NodeT>
        struct output_element_lazy<NodeT, true>
        {
            using type = typename ts_harness<output_schema_t<NodeT>>::element;
        };

        // Per-cycle harness element of the first input / the output (T, SetDelta<T>, ...).
        template <typename NodeT>
        using first_input_element_t = typename first_input_element_lazy<NodeT>::type;

        template <typename NodeT>
        using output_element_t = typename output_element_lazy<NodeT>::type;

        // A time-series input argument to the operator harness is a ``vector<optional<T>>``;
        // any other argument is a wiring-time scalar.
        template <typename A>
        struct is_optional_vector : std::false_type
        {
        };
        template <typename T>
        struct is_optional_vector<std::vector<std::optional<T>>> : std::true_type
        {
        };
        template <typename A>
        struct optional_vector_element;
        template <typename T>
        struct optional_vector_element<std::vector<std::optional<T>>>
        {
            using type = T;
        };

        template <typename... A>
        struct first_arg_is_optional_vector : std::false_type
        {
        };
        template <typename A0, typename... Rest>
        struct first_arg_is_optional_vector<A0, Rest...>
            : is_optional_vector<std::remove_cvref_t<A0>>
        {
        };

        template <typename... A>
        inline constexpr bool first_arg_is_optional_vector_v = first_arg_is_optional_vector<A...>::value;

        template <typename... A>
        inline constexpr bool any_optional_vector_v =
            (false || ... || is_optional_vector<std::remove_cvref_t<A>>::value);

        inline std::string input_key(std::size_t index) { return "eval_node::in" + std::to_string(index); }
    }  // namespace eval_node_detail

    /**
     * Evaluate a source-style static node: no time-series input, exactly one output.
     *
     * This is the source counterpart of the input-driven ``eval_node`` overload below.
     * Scalar arguments are passed in the node's wire-parameter order, the output is recorded,
     * and the returned sequence contains every output tick produced until quiescence.
     */
    template <typename NodeT, typename... Args>
        requires(!std::derived_from<NodeT, operator_tag> &&
                 eval_node_detail::has_eval<NodeT> &&
                 StaticNodeSignature<NodeT>::input_count() == 0)
    [[nodiscard]] std::vector<std::optional<eval_node_detail::output_element_t<NodeT>>>
    eval_node(Args &&...args)
    {
        using sig         = StaticNodeSignature<NodeT>;
        using wire_params = eval_node_detail::wire_params_t<NodeT>;
        using out_schema  = eval_node_detail::output_schema_t<NodeT>;
        constexpr std::size_t arg_count = sizeof...(Args);

        static_assert(sig::output_count() == 1, "eval_node: source NodeT must have exactly one output");
        static_assert(arg_count == std::tuple_size_v<wire_params>,
                      "eval_node: argument count must match the source node's Scalar parameters");

        Wiring w;
        auto   out_port = wire<NodeT>(w, std::forward<Args>(args)...);
        ts_harness<out_schema>::wire_record(w, out_port, std::string{"eval_node::out"});

        GraphBuilder gb = std::move(w).finish();
        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MAX_ET);
        GraphExecutorValue executor = eb.make_executor();
        auto               view     = executor.view();
        view.run();

        return ts_harness<out_schema>::read(view.graph().global_state(), "eval_node::out");
    }

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
     * one element per evaluation cycle from ``MIN_ST`` (``none`` = no tick), and a
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
        requires(!std::derived_from<NodeT, operator_tag>)
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

    /**
     * Evaluate an **operator** over per-cycle inputs — the operator counterpart of
     * ``eval_node``. The overload to wire is resolved **at wiring time** from the supplied
     * argument schemas (operator dispatch), and the resolved port already carries the
     * output schema; the result is therefore returned **type-erased** as the per-cycle
     * canonical ``Value`` deltas and is compared with ``Value`` equality (``CHECK_OUTPUT``).
     *
     * Arguments are in the operator's call order: a **time-series input** is a
     * ``std::vector<std::optional<T>>`` (a scalar leaf ``T`` -> ``TS<T>``; build it with
     * ``testing::values<T>(...)``), a **scalar input** is the value itself. Scope: the
     * operator must have at least one time-series input and exactly one output — sinks (no
     * output) and sources (no time-series input), and collection time-series *inputs*, are
     * out of scope (wire those as a graph and use ``record`` directly).
     */
    template <typename Op, typename Input0, typename... Rest>
        requires(std::derived_from<Op, operator_tag> &&
                 eval_node_detail::is_optional_vector<std::remove_cvref_t<Input0>>::value)
    [[nodiscard]] std::vector<std::optional<Value>> eval_node(const Input0 &input0, Rest &&...rest)
    {
        static_assert(Op::has_output, "eval_node<Op>: the operator must have an output (sinks are out of scope)");

        constexpr std::size_t arg_count = 1 + sizeof...(Rest);
        Wiring                w;
        auto                  all = std::forward_as_tuple(input0, std::forward<Rest>(rest)...);

        // Pass 1: wire a replay per time-series input (vector<optional<T>> -> TS<T>), pass
        // scalar values straight through, dispatch the operator, then record its output.
        auto wire_arg = [&]<std::size_t I>() {
            using A = std::remove_cvref_t<std::tuple_element_t<I, decltype(all)>>;
            if constexpr (eval_node_detail::is_optional_vector<A>::value)
            {
                using T = typename eval_node_detail::optional_vector_element<A>::type;
                return wire<replay, TS<T>>(w, eval_node_detail::input_key(I));
            }
            else
            {
                return std::get<I>(all);  // scalar value
            }
        };

        auto out_port = [&]<std::size_t... I>(std::index_sequence<I...>) {
            return wire<Op>(w, wire_arg.template operator()<I>()...);
        }(std::make_index_sequence<arg_count>{});

        wire<record>(w, out_port, std::string{"eval_node::out"});
        GraphBuilder gb = std::move(w).finish();

        // Pass 2: seed each time-series input's replay buffer; track the longest input.
        std::size_t max_len = 0;
        [&]<std::size_t... I>(std::index_sequence<I...>) {
            (
                [&] {
                    using A = std::remove_cvref_t<std::tuple_element_t<I, decltype(all)>>;
                    if constexpr (eval_node_detail::is_optional_vector<A>::value)
                    {
                        using T         = typename eval_node_detail::optional_vector_element<A>::type;
                        const auto &seq = std::get<I>(all);
                        max_len         = std::max(max_len, seq.size());
                        set_replay_values<T>(gb.global_state(), eval_node_detail::input_key(I), seq);
                    }
                }(),
                ...);
        }(std::make_index_sequence<arg_count>{});

        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MAX_ET);
        GraphExecutorValue executor = eb.make_executor();
        auto               view     = executor.view();
        view.run();

        // Type-erased per-cycle deltas, read at the wiring-resolved output schema; pad to
        // the longest input, never truncate.
        auto out = get_recorded_deltas(view.graph().global_state(), "eval_node::out");
        if (out.size() < max_len) { out.resize(max_len); }
        return out;
    }

    /**
     * Evaluate a source-style operator: scalar arguments only, exactly one output.
     *
     * Use ``eval_node<Op>(args...)`` when the operator can resolve its output from its
     * scalar arguments, or ``eval_node<Op, OutSchema>(args...)`` when the output must be
     * supplied explicitly, mirroring ``wire<Op, OutSchema>(w, args...)``.
     */
    template <typename Op, typename... Args>
        requires(std::derived_from<Op, operator_tag> &&
                 Op::has_output &&
                 !eval_node_detail::first_arg_is_optional_vector_v<Args...>)
    [[nodiscard]] std::vector<std::optional<Value>> eval_node(Args &&...args)
    {
        static_assert(!eval_node_detail::any_optional_vector_v<Args...>,
                      "eval_node<Op>: source-style operator overload accepts scalar arguments only");

        Wiring w;
        auto   out_port = wire<Op>(w, std::forward<Args>(args)...);
        wire<record>(w, out_port, std::string{"eval_node::out"});

        GraphBuilder gb = std::move(w).finish();
        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MAX_ET);
        GraphExecutorValue executor = eb.make_executor();
        auto               view     = executor.view();
        view.run();

        return get_recorded_deltas(view.graph().global_state(), "eval_node::out");
    }

    template <typename Op, typename OutSchema, typename... Args>
        requires(std::derived_from<Op, operator_tag> &&
                 Op::has_output &&
                 !std::is_void_v<OutSchema> &&
                 !eval_node_detail::any_optional_vector_v<Args...>)
    [[nodiscard]] std::vector<std::optional<Value>> eval_node(Args &&...args)
    {
        Wiring w;
        auto   out_port = wire<Op, OutSchema>(w, std::forward<Args>(args)...);
        wire<record>(w, out_port, std::string{"eval_node::out"});

        GraphBuilder gb = std::move(w).finish();
        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MAX_ET);
        GraphExecutorValue executor = eb.make_executor();
        auto               view     = executor.view();
        view.run();

        return get_recorded_deltas(view.graph().global_state(), "eval_node::out");
    }
}  // namespace hgraph::testing

#endif  // HGRAPH_LIB_TESTING_EVAL_NODE_H
