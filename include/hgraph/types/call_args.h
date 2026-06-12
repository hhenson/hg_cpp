#ifndef HGRAPH_TYPES_CALL_ARGS_H
#define HGRAPH_TYPES_CALL_ARGS_H

#include <hgraph/types/static_schema.h>

#include <cstddef>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>

namespace hgraph
{
    /**
     * Call-site keyword argument: ``arg<"name">(value)`` — the C++ spelling of
     * Python's ``name=value``. Erased dispatch uses the runtime ``name`` field;
     * static call paths can still unwrap the payload when they remain positional.
     */
    template <typename T>
    struct NamedArg
    {
        std::string_view name{};
        T                value;
    };

    template <fixed_string Name, typename T>
    [[nodiscard]] NamedArg<std::decay_t<T>> arg(T &&value)
    {
        return NamedArg<std::decay_t<T>>{Name.sv(), std::forward<T>(value)};
    }

    namespace call_args_detail
    {
        template <typename T>
        struct is_named_arg : std::false_type
        {
        };
        template <typename T>
        struct is_named_arg<NamedArg<T>> : std::true_type
        {
        };

        template <typename A>
        struct named_payload
        {
            using type                  = A;
            static constexpr bool named = false;
        };
        template <typename T>
        struct named_payload<NamedArg<T>>
        {
            using type                  = T;
            static constexpr bool named = true;
        };

        template <typename A>
        using payload_t = typename named_payload<std::remove_cvref_t<A>>::type;
        template <typename A>
        inline constexpr bool is_named_arg_v = named_payload<std::remove_cvref_t<A>>::named;

        template <typename Arg>
        [[nodiscard]] const auto &payload_of(const Arg &argument)
        {
            if constexpr (is_named_arg_v<Arg>) { return argument.value; }
            else { return argument; }
        }

        template <std::size_t I, typename Tuple>
        [[nodiscard]] const auto &payload_at(const Tuple &arguments)
        {
            return payload_of(std::get<I>(arguments));
        }
    }  // namespace call_args_detail
}  // namespace hgraph

#endif  // HGRAPH_TYPES_CALL_ARGS_H
