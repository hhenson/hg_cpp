#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_STRING_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_STRING_IMPL_H

#include <hgraph/lib/std/operators/string.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

#include <algorithm>
#include <cstddef>
#include <regex>

namespace hgraph::stdlib
{
    namespace string_impl_detail
    {
        [[nodiscard]] inline std::size_t normalize_slice_index(Int index, std::size_t size)
        {
            const Int signed_size = static_cast<Int>(size);
            Int       normalized  = index < 0 ? signed_size + index : index;
            normalized            = std::clamp(normalized, Int{0}, signed_size);
            return static_cast<std::size_t>(normalized);
        }
    }  // namespace string_impl_detail

    struct replace_impl
    {
        static void eval(In<"pattern", TS<Str>> pattern, In<"repl", TS<Str>> repl, In<"s", TS<Str>> s,
                         Out<TS<Str>> out)
        {
            out.set(std::regex_replace(s.value(), std::regex{pattern.value()}, repl.value()));
        }
    };

    struct substr_impl
    {
        static void eval(In<"s", TS<Str>> s, In<"start", TS<Int>> start, In<"end", TS<Int>> end, Out<TS<Str>> out)
        {
            const Str        value       = s.value();
            const std::size_t begin      = string_impl_detail::normalize_slice_index(start.value(), value.size());
            const std::size_t finish     = string_impl_detail::normalize_slice_index(end.value(), value.size());
            const std::size_t safe_end   = std::max(begin, finish);
            const std::size_t char_count = safe_end - begin;
            out.set(value.substr(begin, char_count));
        }
    };

    inline void register_string_operators()
    {
        register_overload<replace, replace_impl>();
        register_overload<substr, substr_impl>();
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_STRING_IMPL_H
