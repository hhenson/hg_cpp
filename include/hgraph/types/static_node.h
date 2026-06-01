#ifndef HGRAPH_CPP_ROOT_STATIC_NODE_H
#define HGRAPH_CPP_ROOT_STATIC_NODE_H

#include <hgraph/runtime/node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/time_series/ts_input/bundle_view.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_view.h>

#include <cstddef>
#include <stdexcept>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace hgraph
{
    /**
     * Node-authoring selectors and the compile-time machinery that turns a
     * static node implementation into a runtime ``NodeBuilder``.
     *
     * A node implementation is a stateless struct with a static ``eval`` (and
     * optional ``start`` / ``stop``) whose parameters are the selectors below.
     * ``NodeBuilder::implementation<T>()`` reflects that signature and builds
     * the same ``(NodeTypeMetaData, NodeCallbacks, TSEndpointSchema)`` triple
     * that ``NodeBuilder::native`` already consumes, so static authoring is a
     * typed front-end over the one runtime node model rather than a parallel
     * mechanism.
     *
     * See ``data_structures`` developer guide: *Wiring* and
     * *Schemas > Static Schema*.
     *
     * This first slice covers the scalar time-series path: ``In<Name, TS<T>>``,
     * ``Out<TS<T>>`` and scalar ``State<T>``. Container selectors, recordable
     * state, clock/scheduler injection and push sources are deferred.
     */

    // -----------------------------------------------------------------
    // Selector markers
    // -----------------------------------------------------------------

    /** Typed input selector. Only the scalar ``TS<T>`` form is defined so far. */
    template <fixed_string Name, typename TSchema>
    class In;

    template <fixed_string Name, typename TValue>
    class In<Name, TS<TValue>>
    {
      public:
        using schema                    = TS<TValue>;
        using value_type                = TValue;
        static constexpr auto field_name = Name;

        explicit In(TSInputView view) noexcept : view_(std::move(view)) {}

        /** Current value of the input. */
        [[nodiscard]] value_type value() const { return view_.value().template checked_as<TValue>(); }

        [[nodiscard]] bool modified() const { return view_.modified(); }
        [[nodiscard]] bool valid() const { return view_.valid(); }

        [[nodiscard]] const TSInputView &view() const noexcept { return view_; }

      private:
        TSInputView view_;
    };

    /** Typed output selector. Only the scalar ``TS<T>`` form is defined so far. */
    template <typename TSchema>
    class Out;

    template <typename TValue>
    class Out<TS<TValue>>
    {
      public:
        using schema     = TS<TValue>;
        using value_type = TValue;

        Out(TSOutputView view, engine_time_t evaluation_time) noexcept
            : view_(std::move(view)), evaluation_time_(evaluation_time)
        {
        }

        /** Write ``value`` into the output and tick it at the current evaluation time. */
        template <typename U>
        void set(U &&value) const
        {
            Value wrapped{std::forward<U>(value)};
            auto  mutation = view_.begin_mutation(evaluation_time_);
            if (!mutation.copy_value_from(wrapped.view()))
            {
                throw std::logic_error("Out<TS<T>>::set failed to copy the value into the output");
            }
        }

        [[nodiscard]] bool modified() const { return view_.modified(); }
        [[nodiscard]] bool valid() const { return view_.valid(); }
        [[nodiscard]] const TSOutputView &view() const noexcept { return view_; }
        [[nodiscard]] engine_time_t evaluation_time() const noexcept { return evaluation_time_; }

      private:
        TSOutputView  view_;
        engine_time_t evaluation_time_{MIN_DT};
    };

    /** Typed handle into node-local (value-layer) state. One state slot per node. */
    template <typename TValue>
    class State
    {
      public:
        using value_type = TValue;

        explicit State(ValueView view) noexcept : view_(std::move(view)) {}

        [[nodiscard]] value_type get() const { return view_.template checked_as<TValue>(); }

        template <typename U>
        void set(U &&value)
        {
            auto mutation = view_.begin_mutation();
            mutation.set_scalar(std::forward<U>(value));
        }

        [[nodiscard]] ValueView       &view() noexcept { return view_; }
        [[nodiscard]] const ValueView &view() const noexcept { return view_; }

      private:
        ValueView view_;
    };

    // -----------------------------------------------------------------
    // Compile-time machinery
    // -----------------------------------------------------------------

    namespace static_node_detail
    {
        template <typename T>
        inline constexpr bool always_false_v = false;

        template <typename T>
        using selector_of = std::remove_cv_t<std::remove_reference_t<T>>;

        // ---- selector detection ----
        template <typename T> struct is_input_selector : std::false_type {};
        template <fixed_string N, typename S> struct is_input_selector<In<N, S>> : std::true_type {};

        template <typename T> struct is_output_selector : std::false_type {};
        template <typename S> struct is_output_selector<Out<S>> : std::true_type {};

        template <typename T> struct is_state_selector : std::false_type {};
        template <typename V> struct is_state_selector<State<V>> : std::true_type {};

        // ---- per-selector runtime metadata ----
        template <typename T> struct input_selector_traits;
        template <fixed_string N, typename S>
        struct input_selector_traits<In<N, S>>
        {
            static std::string                name() { return std::string{N.sv()}; }
            static const TSValueTypeMetaData *ts_meta() { return schema_descriptor<S>::ts_meta(); }
        };

        template <typename T> struct output_selector_traits;
        template <typename S>
        struct output_selector_traits<Out<S>>
        {
            static const TSValueTypeMetaData *ts_meta() { return schema_descriptor<S>::ts_meta(); }
        };

        template <typename T> struct state_selector_traits;
        template <typename V>
        struct state_selector_traits<State<V>>
        {
            static const ValueTypeMetaData *value_meta() { return scalar_descriptor<V>::value_meta(); }
        };

        // ---- function-pointer traits over a static hook ----
        template <typename F> struct fn_traits;
        template <typename R, typename... A>
        struct fn_traits<R (*)(A...)>
        {
            using return_type = R;
            using args_tuple  = std::tuple<A...>;
        };

        // ---- arg providers: build a selector from NodeView + evaluation time ----
        template <typename T>
        struct arg_provider
        {
            static_assert(always_false_v<T>, "Unsupported static node hook parameter type");
        };

        template <fixed_string N, typename V>
        struct arg_provider<In<N, V>>
        {
            static In<N, V> get(const NodeView &view, engine_time_t evaluation_time)
            {
                TSInputView root   = view.input(evaluation_time);
                auto        bundle = root.as_bundle();
                return In<N, V>{bundle.field(N.sv())};
            }
        };

        template <typename V>
        struct arg_provider<Out<V>>
        {
            static Out<V> get(const NodeView &view, engine_time_t evaluation_time)
            {
                return Out<V>{view.output(evaluation_time), evaluation_time};
            }
        };

        template <typename V>
        struct arg_provider<State<V>>
        {
            static State<V> get(const NodeView &view, engine_time_t) { return State<V>{view.state()}; }
        };

        template <>
        struct arg_provider<engine_time_t>
        {
            static engine_time_t get(const NodeView &, engine_time_t evaluation_time) noexcept
            {
                return evaluation_time;
            }
        };

        // ---- invoke a static hook by injecting each parameter by type ----
        template <auto Fn, std::size_t... I>
        void invoke_impl(const NodeView &view, engine_time_t evaluation_time, std::index_sequence<I...>)
        {
            using args = typename fn_traits<decltype(Fn)>::args_tuple;
            Fn(arg_provider<selector_of<std::tuple_element_t<I, args>>>::get(view, evaluation_time)...);
        }

        template <auto Fn>
        void invoke(const NodeView &view, engine_time_t evaluation_time)
        {
            using traits = fn_traits<decltype(Fn)>;
            static_assert(std::is_same_v<typename traits::return_type, void>,
                          "Static node hooks must return void");
            invoke_impl<Fn>(view, evaluation_time,
                            std::make_index_sequence<std::tuple_size_v<typename traits::args_tuple>>{});
        }

        // ---- hook / trait detection ----
        template <typename T> concept has_start     = requires { &T::start; };
        template <typename T> concept has_stop      = requires { &T::stop; };
        template <typename T> concept has_name      = requires { T::name; };
    }  // namespace static_node_detail

    // -----------------------------------------------------------------
    // Static node signature
    // -----------------------------------------------------------------

    /**
     * Compile-time reflection of a static node implementation. Derives the
     * runtime node contract (input/output/state schema, node kind, input
     * endpoint annotation) from the parameter list of ``TImplementation::eval``.
     */
    template <typename TImplementation>
    struct StaticNodeSignature
    {
      private:
        using eval_args = typename static_node_detail::fn_traits<decltype(&TImplementation::eval)>::args_tuple;

        static constexpr std::size_t arg_count = std::tuple_size_v<eval_args>;
        using indices                          = std::make_index_sequence<arg_count>;

        template <std::size_t... I>
        static void collect_inputs(std::vector<std::pair<std::string, const TSValueTypeMetaData *>> &fields,
                                   std::index_sequence<I...>)
        {
            (
                [&] {
                    using E = static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>;
                    if constexpr (static_node_detail::is_input_selector<E>::value)
                    {
                        fields.emplace_back(static_node_detail::input_selector_traits<E>::name(),
                                            static_node_detail::input_selector_traits<E>::ts_meta());
                    }
                }(),
                ...);
        }

        template <std::size_t... I>
        static const TSValueTypeMetaData *find_output(std::index_sequence<I...>)
        {
            const TSValueTypeMetaData *out = nullptr;
            (
                [&] {
                    using E = static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>;
                    if constexpr (static_node_detail::is_output_selector<E>::value)
                    {
                        out = static_node_detail::output_selector_traits<E>::ts_meta();
                    }
                }(),
                ...);
            return out;
        }

        template <std::size_t... I>
        static const ValueTypeMetaData *find_state(std::index_sequence<I...>)
        {
            const ValueTypeMetaData *state = nullptr;
            (
                [&] {
                    using E = static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>;
                    if constexpr (static_node_detail::is_state_selector<E>::value)
                    {
                        state = static_node_detail::state_selector_traits<E>::value_meta();
                    }
                }(),
                ...);
            return state;
        }

        template <std::size_t... I>
        static constexpr std::size_t count_inputs(std::index_sequence<I...>)
        {
            return (std::size_t{0} + ... +
                    (static_node_detail::is_input_selector<
                         static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>>::value
                         ? std::size_t{1}
                         : std::size_t{0}));
        }

        template <std::size_t... I>
        static constexpr std::size_t count_outputs(std::index_sequence<I...>)
        {
            return (std::size_t{0} + ... +
                    (static_node_detail::is_output_selector<
                         static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>>::value
                         ? std::size_t{1}
                         : std::size_t{0}));
        }

      public:
        [[nodiscard]] static constexpr std::size_t input_count() { return count_inputs(indices{}); }
        [[nodiscard]] static constexpr std::size_t output_count() { return count_outputs(indices{}); }

        [[nodiscard]] static const TSValueTypeMetaData *input_schema()
        {
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
            collect_inputs(fields, indices{});
            if (fields.empty()) { return nullptr; }
            return TypeRegistry::instance().un_named_tsb(fields);
        }

        [[nodiscard]] static const TSValueTypeMetaData *output_schema() { return find_output(indices{}); }
        [[nodiscard]] static const ValueTypeMetaData   *state_schema() { return find_state(indices{}); }

        [[nodiscard]] static NodeKind node_kind()
        {
            // The kind is always determined from the node's shape; there is no
            // override. A push source is distinguished by an apply_message hook
            // (not yet implemented); everything else is classified by which of
            // In / Out are present.
            const bool has_in  = input_count() > 0;
            const bool has_out = output_count() > 0;
            if (has_in && has_out) { return NodeKind::Compute; }
            if (has_out) { return NodeKind::PullSource; }
            if (has_in) { return NodeKind::Sink; }
            return NodeKind::Compute;
        }

        /** Input endpoint annotation: a non-peered TSB of peered terminals. */
        [[nodiscard]] static TSEndpointSchema input_endpoint()
        {
            const TSValueTypeMetaData *tsb = input_schema();
            if (tsb == nullptr) { return TSEndpointSchema{}; }
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
            collect_inputs(fields, indices{});
            std::vector<TSEndpointSchema> children;
            children.reserve(fields.size());
            for (const auto &field : fields) { children.push_back(TSEndpointSchema::peered(field.second)); }
            return TSEndpointSchema::non_peered(tsb, std::move(children));
        }
    };

    // -----------------------------------------------------------------
    // NodeBuilder front-end
    // -----------------------------------------------------------------

    template <typename TImplementation>
    NodeBuilder &NodeBuilder::implementation()
    {
        static_assert(std::is_class_v<TImplementation>, "Static node implementations must be class/struct types");
        static_assert(std::is_empty_v<TImplementation>, "Static node implementations must be stateless");

        using signature = StaticNodeSignature<TImplementation>;
        static_assert(signature::output_count() <= 1, "Static nodes support at most one Out<...> parameter");

        NodeTypeMetaData schema;
        if constexpr (static_node_detail::has_name<TImplementation>) { schema.display_name = TImplementation::name; }
        schema.input_schema  = signature::input_schema();
        schema.output_schema = signature::output_schema();
        schema.state_schema  = signature::state_schema();
        schema.node_kind     = signature::node_kind();

        NodeCallbacks callbacks;
        callbacks.evaluate = [](const NodeView &view, engine_time_t evaluation_time) {
            static_node_detail::invoke<&TImplementation::eval>(view, evaluation_time);
        };
        if constexpr (static_node_detail::has_start<TImplementation>)
        {
            callbacks.start = [](const NodeView &view, engine_time_t evaluation_time) {
                static_node_detail::invoke<&TImplementation::start>(view, evaluation_time);
            };
        }
        if constexpr (static_node_detail::has_stop<TImplementation>)
        {
            callbacks.stop = [](const NodeView &view, engine_time_t evaluation_time) {
                static_node_detail::invoke<&TImplementation::stop>(view, evaluation_time);
            };
        }

        std::string saved_label{label_};
        *this = NodeBuilder::native(std::move(schema), std::move(callbacks), signature::input_endpoint());
        if (!saved_label.empty()) { label(std::move(saved_label)); }
        return *this;
    }

}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_STATIC_NODE_H
