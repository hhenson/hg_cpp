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
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

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

        [[nodiscard]] inline std::vector<Str> split_parts(const Str &value, const Str &separator, std::size_t max_parts)
        {
            if (separator.empty()) { throw std::invalid_argument("split: separator must not be empty"); }

            std::vector<Str> parts;
            std::size_t      start = 0;
            if (max_parts != 0) { parts.reserve(max_parts); }

            while ((max_parts == 0 || parts.size() + 1 < max_parts))
            {
                const std::size_t pos = value.find(separator, start);
                if (pos == Str::npos) { break; }
                parts.push_back(value.substr(start, pos - start));
                start = pos + separator.size();
            }
            parts.push_back(value.substr(start));
            return parts;
        }

        [[nodiscard]] inline const std::string *find_named_format_value(
            std::string_view name,
            const std::vector<std::pair<std::string, std::string>> &named_values)
        {
            for (const auto &[key, value] : named_values)
            {
                if (key == name) { return &value; }
            }
            return nullptr;
        }

        [[nodiscard]] inline std::string placeholder_name(std::string_view placeholder)
        {
            const std::size_t end = placeholder.find_first_of(":!");
            return std::string{placeholder.substr(0, end)};
        }

        [[nodiscard]] inline Str render_format_string(
            std::string_view fmt,
            const std::vector<std::string> &positional_values,
            const std::vector<std::pair<std::string, std::string>> &named_values)
        {
            Str         rendered;
            std::size_t next_positional = 0;
            for (std::size_t i = 0; i < fmt.size();)
            {
                const char c = fmt[i];
                if (c == '{')
                {
                    if (i + 1 < fmt.size() && fmt[i + 1] == '{')
                    {
                        rendered.push_back('{');
                        i += 2;
                        continue;
                    }

                    const std::size_t close = fmt.find('}', i + 1);
                    if (close == std::string_view::npos)
                    {
                        throw std::invalid_argument("format_: unmatched '{' in format string");
                    }

                    const std::string name = placeholder_name(fmt.substr(i + 1, close - i - 1));
                    if (name.empty())
                    {
                        if (next_positional >= positional_values.size())
                        {
                            throw std::invalid_argument("format_: not enough positional arguments");
                        }
                        rendered += positional_values[next_positional++];
                    }
                    else
                    {
                        const std::string *value = find_named_format_value(name, named_values);
                        if (value == nullptr)
                        {
                            throw std::invalid_argument("format_: missing keyword argument '" + name + "'");
                        }
                        rendered += *value;
                    }
                    i = close + 1;
                    continue;
                }

                if (c == '}')
                {
                    if (i + 1 < fmt.size() && fmt[i + 1] == '}')
                    {
                        rendered.push_back('}');
                        i += 2;
                        continue;
                    }
                    throw std::invalid_argument("format_: unmatched '}' in format string");
                }

                rendered.push_back(c);
                ++i;
            }
            return rendered;
        }

        struct FormatValues
        {
            std::vector<std::string>                         positional;
            std::vector<std::pair<std::string, std::string>> named;
        };

        [[nodiscard]] inline FormatValues collect_format_values(TSBInputView &bundle, std::size_t positional_count,
                                                                bool strict)
        {
            FormatValues values;
            values.positional.reserve(std::min(positional_count, bundle.size()));
            values.named.reserve(bundle.size() > positional_count ? bundle.size() - positional_count : 0);

            std::size_t index = 0;
            for (auto [name, child] : bundle.items())
            {
                if (!strict && !child.valid())
                {
                    ++index;
                    continue;
                }

                if (index < positional_count) { values.positional.push_back(child.value().to_string()); }
                else { values.named.emplace_back(std::string{name}, child.value().to_string()); }
                ++index;
            }
            return values;
        }

        [[nodiscard]] inline WiringNamedStructuralSourceArg format_bundle_arg(
            const std::vector<std::pair<std::string, WiringPortRef>> &fields)
        {
            std::vector<WiringNamedPortRef> refs;
            refs.reserve(fields.size());
            for (const auto &[name, port] : fields)
            {
                refs.emplace_back(name, port);
            }
            return WiringNamedStructuralSourceArg{std::move(refs)};
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

    using MatchResult = UnNamedTSB<Field<"is_match", TS<Bool>>, Field<"groups", TSL<TS<Str>>>>;

    struct match_impl
    {
        static void eval(In<"pattern", TS<Str>> pattern, In<"s", TS<Str>> s, Out<MatchResult> out)
        {
            const Str   value = s.value();
            std::smatch match;
            const bool  is_match = std::regex_search(value, match, std::regex{pattern.value()});
            out.field<"is_match">().set(is_match);
            if (!is_match) { return; }

            auto groups = out.field<"groups">();
            for (std::size_t i = 1; i < match.size(); ++i)
            {
                groups[i - 1].set(match[i].str());
            }
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

    struct split_tsl_impl
    {
        /*
         * ``N`` is intentionally resolved from the caller's requested output
         * schema. There is no TSL size in the inputs, so generic calls must use:
         *
         *     wire<stdlib::split, TSL<TS<Str>, 2>>(w, s, Str{","})
         */
        static void eval(In<"s", TS<Str>> s, Scalar<"separator", Str> separator,
                         Out<TSL<TS<Str>, SIZE<"N">>> out)
        {
            const std::vector<Str> parts = string_impl_detail::split_parts(s.value(), separator.value(), out.size());
            for (std::size_t i = 0; i < parts.size(); ++i)
            {
                auto child = out[i];
                if (!child.valid() || child.value().template checked_as<Str>() != parts[i]) { child.set(parts[i]); }
            }
        }
    };

    struct join_tsl_impl
    {
        static void eval(In<"strings", TSL<TS<Str>, SIZE<"N">>, InputValidity::Unchecked> strings,
                         Scalar<"separator", Str> separator, Scalar<"__strict__", Bool> strict, Out<TS<Str>> out)
        {
            Str  joined;
            bool first = true;
            for (std::size_t i = 0; i < strings.size(); ++i)
            {
                auto item = strings[i];
                if (!item.valid())
                {
                    if (strict.value()) { return; }
                    continue;
                }
                if (!first) { joined += separator.value(); }
                joined += item.value();
                first = false;
            }
            out.set(joined);
        }

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"__strict__", Value{Bool{false}}}};
        }
    };

    struct format_bundle_impl
    {
        static constexpr auto name = "format";

        static void eval(In<"fmt", TS<Str>> fmt,
                         In<"__args__", Kwargs<>, InputValidity::Unchecked> args,
                         Scalar<"__pos_count__", Int> pos_count, Scalar<"__sample__", Int> sample,
                         Scalar<"__strict__", Bool> strict, State<Int> count, Out<TS<Str>> out)
        {
            const bool require_all = strict.value();
            if (require_all && !args.all_valid()) { return; }

            if (sample.value() > 1)
            {
                const Int next = count.get() + 1;
                count.set(next);
                if (next % sample.value() != 0) { return; }
            }

            const string_impl_detail::FormatValues values =
                string_impl_detail::collect_format_values(args, static_cast<std::size_t>(pos_count.value()), require_all);
            out.set(string_impl_detail::render_format_string(fmt.value(), values.positional, values.named));
        }
    };

    struct format_no_args_impl
    {
        static constexpr auto name = "format";

        static void eval(In<"fmt", TS<Str>> fmt, Scalar<"__sample__", Int> sample, Scalar<"__strict__", Bool> strict,
                         State<Int> count, Out<TS<Str>> out)
        {
            static_cast<void>(strict);
            if (sample.value() > 1)
            {
                const Int next = count.get() + 1;
                count.set(next);
                if (next % sample.value() != 0) { return; }
            }
            out.set(string_impl_detail::render_format_string(fmt.value(), {}, {}));
        }
    };

    struct format_graph_impl
    {
        static constexpr auto name = "format";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Str>> fmt, VarIn<"args", TsVar<"A">> args,
                                     Scalar<"__sample__", Int> sample, Scalar<"__strict__", Bool> strict,
                                     VarKwIn<"kwargs"> kwargs)
        {
            std::vector<std::pair<std::string, WiringPortRef>> positional_fields;
            positional_fields.reserve(args.size());
            for (std::size_t i = 0; i < args.size(); ++i)
            {
                positional_fields.emplace_back(std::to_string(i), args[i]);
            }

            std::vector<std::pair<std::string, WiringPortRef>> fields;
            fields.reserve(args.size() + kwargs.size());
            fields.insert(fields.end(), positional_fields.begin(), positional_fields.end());
            fields.insert(fields.end(), kwargs.begin(), kwargs.end());

            if (fields.empty())
            {
                return wire<format_no_args_impl>(w,
                                                 fmt,
                                                 arg<"__sample__">(sample.value()),
                                                 arg<"__strict__">(strict.value()));
            }

            return wire<format_bundle_impl, TS<Str>>(w,
                                                     fmt,
                                                     arg<"__args__">(string_impl_detail::format_bundle_arg(fields)),
                                                     arg<"__pos_count__">(static_cast<Int>(args.size())),
                                                     arg<"__sample__">(sample.value()),
                                                     arg<"__strict__">(strict.value()));
        }

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"__sample__", Value{Int{-1}}}, {"__strict__", Value{Bool{true}}}};
        }
    };

    inline void register_string_operators()
    {
        register_overload<match_, match_impl>();
        register_overload<replace, replace_impl>();
        register_overload<substr, substr_impl>();
        register_overload<split, split_tsl_impl>();
        register_overload<join, join_tsl_impl>();
        register_graph_overload<format_, format_graph_impl>();
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_STRING_IMPL_H
