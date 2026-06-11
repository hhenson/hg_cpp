#ifndef HGRAPH_TYPES_WIRED_FN_H
#define HGRAPH_TYPES_WIRED_FN_H

#include <hgraph/types/graph_wiring.h>

#include <cstddef>
#include <functional>
#include <span>
#include <stdexcept>
#include <typeinfo>
#include <utility>

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

        WireThunk             wire_fn{nullptr};
        const std::type_info *identity{nullptr};
        std::size_t           arity{0};
        bool                  has_output{false};

        [[nodiscard]] bool valid() const noexcept { return wire_fn != nullptr && identity != nullptr; }

        /** Wire one application over the given (already-erased) argument ports. */
        [[nodiscard]] WiringPortRef wire(Wiring &w, std::span<const WiringPortRef> args) const
        {
            if (!valid()) { throw std::logic_error("WiredFn::wire called on an empty function value"); }
            return wire_fn(w, args);
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
    }  // namespace wired_fn_detail

    /** Erase the operator marker / node / sub-graph ``X`` into a ``WiredFn`` value. */
    template <typename X>
    [[nodiscard]] WiredFn fn()
    {
        return WiredFn{
            .wire_fn    = &wired_fn_detail::wire_thunk<X>,
            .identity   = &typeid(X),
            .arity      = wired_fn_detail::arity_of<X>(),
            .has_output = wired_fn_detail::has_output_of<X>(),
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
