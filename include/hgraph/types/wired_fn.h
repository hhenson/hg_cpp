#ifndef HGRAPH_TYPES_WIRED_FN_H
#define HGRAPH_TYPES_WIRED_FN_H

#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/subgraph_wiring.h>

#include <cstddef>
#include <functional>
#include <array>
#include <span>
#include <string_view>
#include <stdexcept>
#include <typeinfo>
#include <utility>
#include <vector>

namespace hgraph
{
    /**
     * A wirable function value — the C++ analogue of Python's ``func: Callable``
     * argument to the higher-order operators (``reduce`` / ``map_`` / ``switch_``).
     *
     * ``fn<X>()`` erases an operator marker, static node, or sub-graph ``X`` into
     * a small **scalar value**: a stable identity (its ``type_info``) plus a
     * wiring thunk that dispatches ``wire<X>`` over erased ports. Because it is
     * an ordinary registered scalar, it participates fully in operator overload
     * selection — a ``Scalar<"func", WiredFn>`` parameter matches by type, an
     * overload's ``requires_`` can gate on the *value* (identity), and equal
     * functions intern/dedup like any other scalar configuration.
     *
     * Wiring an operator-marker ``X`` through the thunk requires
     * ``operator_dispatch.h`` at the ``fn<X>()`` instantiation point (the same
     * rule as ``wire<X>``).
     */
    struct WiredFn
    {
        using WireThunk = WiringPortRef (*)(Wiring &, std::span<const WiringPortRef>);
        using CompileThunk = CompiledSubGraph (*)(std::span<const TSValueTypeMetaData *const>);
        using ParamNamesThunk = std::span<const std::string_view> (*)();

        WireThunk             wire_fn{nullptr};
        CompileThunk          compile_fn{nullptr};
        ParamNamesThunk       param_names_fn{nullptr};
        const std::type_info *identity{nullptr};
        std::size_t           arity{0};
        bool                  has_output{false};

        /**
         * The function's time-series parameter names, in order (empty string =
         * unnamed). Sub-graphs name ports via ``NamedPort<"name", S>``, nodes
         * via ``In<"name", …>``, operator markers via their ``In`` selectors.
         * Higher-order operators use these to resolve ``**kwargs`` onto the
         * function's parameters, Python-style.
         */
        [[nodiscard]] std::span<const std::string_view> param_names() const
        {
            return param_names_fn != nullptr ? param_names_fn() : std::span<const std::string_view>{};
        }

        [[nodiscard]] bool valid() const noexcept { return wire_fn != nullptr && identity != nullptr; }

        /** Wire one application **inline** over the given (already-erased) argument ports. */
        [[nodiscard]] WiringPortRef wire(Wiring &w, std::span<const WiringPortRef> args) const
        {
            if (!valid()) { throw std::logic_error("WiredFn::wire called on an empty function value"); }
            return wire_fn(w, args);
        }

        /**
         * Compile one application as a **child graph** (``CompiledSubGraph``):
         * boundary args at the supplied runtime schemas, in order. This is what
         * the nested operators (``switch_`` / ``map_``) consume — the same
         * function value either inlines (``wire``) or compiles, the caller
         * chooses.
         */
        [[nodiscard]] CompiledSubGraph compile(std::span<const TSValueTypeMetaData *const> input_schemas) const
        {
            if (compile_fn == nullptr)
            {
                throw std::logic_error("WiredFn::compile called on an empty function value");
            }
            return compile_fn(input_schemas);
        }

        [[nodiscard]] bool operator==(const WiredFn &other) const noexcept
        {
            if (identity == other.identity) { return true; }
            if (identity == nullptr || other.identity == nullptr) { return false; }
            return *identity == *other.identity;
        }
    };

    namespace static_schema_detail
    {
        template <>
        struct scalar_name<WiredFn>
        {
            static constexpr std::string_view value{"fn"};
        };
    }  // namespace static_schema_detail

    namespace wired_fn_detail
    {
        template <typename X>
        [[nodiscard]] consteval std::size_t arity_of()
        {
            if constexpr (std::is_base_of_v<operator_tag, X>)
            {
                using params = typename X::param_types;
                return []<std::size_t... I>(std::index_sequence<I...>) {
                    return (std::size_t{0} + ... +
                            (static_node_detail::is_input_selector<std::tuple_element_t<I, params>>::value
                                 ? std::size_t{1}
                                 : std::size_t{0}));
                }(std::make_index_sequence<std::tuple_size_v<params>>{});
            }
            else if constexpr (graph_wiring_detail::is_graph_def<X>)
            {
                static_assert(StaticGraphSignature<X>::scalar_count() == 0,
                              "fn<X>: a wirable sub-graph must take time-series inputs only");
                return StaticGraphSignature<X>::input_count();
            }
            else
            {
                static_assert(StaticNodeSignature<X>::scalar_count() == 0,
                              "fn<X>: a wirable node must take time-series inputs only");
                return StaticNodeSignature<X>::input_count();
            }
        }

        template <typename X>
        [[nodiscard]] consteval bool has_output_of()
        {
            if constexpr (std::is_base_of_v<operator_tag, X>) { return X::has_output; }
            else if constexpr (graph_wiring_detail::is_graph_def<X>)
            {
                return !std::is_void_v<typename StaticGraphSignature<X>::output_type>;
            }
            else { return StaticNodeSignature<X>::has_output(); }
        }

        template <typename X>
        [[nodiscard]] WiringPortRef wire_thunk(Wiring &w, std::span<const WiringPortRef> args)
        {
            constexpr std::size_t arity = arity_of<X>();
            if (args.size() != arity)
            {
                throw std::invalid_argument("fn<X>: wired argument count does not match the function's inputs");
            }
            return [&]<std::size_t... I>(std::index_sequence<I...>) -> WiringPortRef {
                if constexpr (has_output_of<X>())
                {
                    auto out = wire<X>(w, Port<void>{w, args[I]}...);
                    return out.erased();
                }
                else
                {
                    wire<X>(w, Port<void>{w, args[I]}...);
                    return {};
                }
            }(std::make_index_sequence<arity>{});
        }

        // Compile one application of X as a child graph: a fresh Wiring whose
        // arguments are boundary placeholders at the supplied runtime schemas,
        // wired through the same wire<X> dispatch (node / operator / sub-graph),
        // then converted to binding specs by finish_subgraph.
        template <typename X>
        [[nodiscard]] CompiledSubGraph compile_thunk(std::span<const TSValueTypeMetaData *const> input_schemas)
        {
            constexpr std::size_t arity = arity_of<X>();
            if (input_schemas.size() != arity)
            {
                throw std::invalid_argument("fn<X>: compiled input schema count does not match the function's inputs");
            }

            Wiring w;
            std::vector<const TSValueTypeMetaData *> schemas{input_schemas.begin(), input_schemas.end()};
            return [&]<std::size_t... I>(std::index_sequence<I...>) -> CompiledSubGraph {
                if constexpr (has_output_of<X>())
                {
                    auto out = wire<X>(w, Port<void>{w, WiringPortRef::boundary_source(I, {}, input_schemas[I])}...);
                    return std::move(w).finish_subgraph(out.erased(), std::move(schemas));
                }
                else
                {
                    wire<X>(w, Port<void>{w, WiringPortRef::boundary_source(I, {}, input_schemas[I])}...);
                    return std::move(w).finish_subgraph(std::nullopt, std::move(schemas));
                }
            }(std::make_index_sequence<arity>{});
        }
        template <typename X>
        [[nodiscard]] std::span<const std::string_view> param_names_thunk()
        {
            static const auto names = [] {
                std::array<std::string_view, arity_of<X>()> out{};
                std::size_t                                 next = 0;
                if constexpr (std::is_base_of_v<operator_tag, X>)
                {
                    using params = typename X::param_types;
                    [&]<std::size_t... I>(std::index_sequence<I...>) {
                        (
                            [&] {
                                using P = std::tuple_element_t<I, params>;
                                if constexpr (static_node_detail::is_input_selector<P>::value)
                                {
                                    out[next++] = P::field_name.sv();
                                }
                            }(),
                            ...);
                    }(std::make_index_sequence<std::tuple_size_v<params>>{});
                }
                else if constexpr (graph_wiring_detail::is_graph_def<X>)
                {
                    using params = typename StaticGraphSignature<X>::param_types;
                    [&]<std::size_t... I>(std::index_sequence<I...>) {
                        (
                            [&] {
                                using P = std::tuple_element_t<I, params>;
                                if constexpr (graph_wiring_detail::is_named_port<P>::value)
                                {
                                    out[next++] = P::field_name.sv();
                                }
                                else if constexpr (graph_wiring_detail::is_port<P>::value)
                                {
                                    out[next++] = std::string_view{};
                                }
                            }(),
                            ...);
                    }(std::make_index_sequence<std::tuple_size_v<params>>{});
                }
                else
                {
                    using params = typename StaticNodeSignature<X>::wire_param_types;
                    [&]<std::size_t... I>(std::index_sequence<I...>) {
                        (
                            [&] {
                                using P = std::tuple_element_t<I, params>;
                                if constexpr (static_node_detail::is_input_selector<P>::value)
                                {
                                    out[next++] = P::field_name.sv();
                                }
                            }(),
                            ...);
                    }(std::make_index_sequence<std::tuple_size_v<params>>{});
                }
                return out;
            }();
            return {names.data(), names.size()};
        }
    }  // namespace wired_fn_detail

    /** Erase the operator marker / node / sub-graph ``X`` into a ``WiredFn`` value. */
    template <typename X>
    [[nodiscard]] WiredFn fn()
    {
        return WiredFn{
            .wire_fn        = &wired_fn_detail::wire_thunk<X>,
            .compile_fn     = &wired_fn_detail::compile_thunk<X>,
            .param_names_fn = &wired_fn_detail::param_names_thunk<X>,
            .identity       = &typeid(X),
            .arity          = wired_fn_detail::arity_of<X>(),
            .has_output     = wired_fn_detail::has_output_of<X>(),
        };
    }
}  // namespace hgraph

template <>
struct std::hash<hgraph::WiredFn>
{
    [[nodiscard]] std::size_t operator()(const hgraph::WiredFn &fn) const noexcept
    {
        return fn.identity != nullptr ? fn.identity->hash_code() : 0;
    }
};

#endif  // HGRAPH_TYPES_WIRED_FN_H
