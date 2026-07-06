#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_JSON_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_JSON_IMPL_H

#include <hgraph/lib/std/operators/json.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/value/json_codec.h>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<JsonCodecState>
    {
        static constexpr std::string_view value{"JsonCodecState"};
    };
}  // namespace hgraph::static_schema_detail

namespace hgraph::stdlib
{
    /**
     * ``to_json`` — erased implementations over any time-series. The composed
     * ``JsonConverter`` is resolved ONCE in ``start`` and carried in node
     * State (the lifecycle form of the builder pattern); ``eval`` is a plain
     * converter invocation. The ``delta`` flag is a wiring-time constant, so
     * value vs delta is resolved by OVERLOAD SELECTION (``requires_`` on the
     * flag) — no hot-path branches.
     */
    struct to_json_value_impl
    {
        static constexpr auto name = "to_json";

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"delta", Value{Bool{false}}}};
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            const auto *delta = context.scalar_as<Bool>("delta");
            return delta != nullptr && !*delta;
        }

        static void start(In<"ts", TsVar<"S">> ts, State<JsonCodecState> codec)
        {
            codec.set(JsonCodecState{&json_converter(ts.base().schema()->value_schema)});
        }

        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"delta", Bool> delta, State<JsonCodecState> codec,
                         Out<TS<Str>> out)
        {
            static_cast<void>(delta);   // resolved at wiring; always false here
            std::string text;
            codec.get().converter->write(ts.value(), text);
            out.set(std::move(text));
        }
    };

    /** ``to_json(ts, delta=true)`` — serialises the canonical per-tick delta. */
    struct to_json_delta_impl
    {
        static constexpr auto name = "to_json";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            const auto *delta = context.scalar_as<Bool>("delta");
            return delta != nullptr && *delta;
        }

        static void start(In<"ts", TsVar<"S">> ts, State<JsonCodecState> codec)
        {
            codec.set(JsonCodecState{&json_converter(ts.base().schema()->delta_value_schema)});
        }

        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"delta", Bool> delta, State<JsonCodecState> codec,
                         Out<TS<Str>> out)
        {
            static_cast<void>(delta);   // resolved at wiring; always true here
            const Value tick_delta = capture_delta(ts.base());
            std::string text;
            codec.get().converter->write(tick_delta.view(), text);
            out.set(std::move(text));
        }
    };

    /**
     * ``from_json`` — parses the incoming string into the resolved output's
     * VALUE schema (converter resolved once in ``start``) and applies the
     * parsed value as the tick's delta. The output type is supplied at the
     * wiring site: ``wire<from_json, TS<MySchema>>(w, ts)``.
     */
    struct from_json_impl
    {
        static constexpr auto name = "from_json";

        static void start(Out<TsVar<"O">> out, State<JsonCodecState> codec)
        {
            // ``Out``'s ``schema`` type alias hides the inherited accessor;
            // reach the runtime schema through the erased view.
            const auto &erased = static_cast<const TSOutputView &>(out);
            codec.set(JsonCodecState{&json_converter(erased.schema()->value_schema)});
        }

        static void eval(In<"ts", TS<Str>> ts, State<JsonCodecState> codec, Out<TsVar<"O">> out)
        {
            Value parsed = from_json_string(*codec.get().converter, ts.value());
            apply_delta(out, parsed.view());
        }
    };


    // -----------------------------------------------------------------
    // Dynamic-JSON value tree (ruling 2026-07-06, parity_matrix.rst):
    // JSON = an Any-storage value holding Bool | Int | Float | Str |
    // List<JSON> | Map<Str, JSON>; an EMPTY box is JSON null. Erased
    // C++ operators are the PRIMARY API; python is sugar.
    // -----------------------------------------------------------------

    namespace json_tree
    {
        [[nodiscard]] inline const ValueTypeMetaData *json_meta() { return TypeRegistry::instance().json(); }

        [[nodiscard]] inline const ValueTypeBinding &json_value_binding()
        {
            return *ValuePlanFactory::instance().binding_for(json_meta());
        }

        [[nodiscard]] inline bool is_json_ts(const TSValueTypeMetaData *ts) noexcept
        {
            return ts != nullptr && ts->kind == TSTypeKind::TS && ts->value_schema == json_meta();
        }

        /** Box an inner value into a JSON node (empty inner = null). */
        [[nodiscard]] inline Value box(Value inner)
        {
            Value node{json_value_binding()};
            if (inner.has_value()) { MutableAnyView{node.begin_mutation()}.set(std::move(inner)); }
            return node;
        }

        /** Convert an arbitrary VALUE into a JSON node (recursive). */
        [[nodiscard]] inline Value to_node(const ValueView &value)
        {
            const auto *meta = value.schema();
            if (meta == json_meta()) { return Value{value}; }
            switch (meta->kind)
            {
                case ValueTypeKind::Atomic: return box(Value{value});
                case ValueTypeKind::List:
                case ValueTypeKind::Tuple: {
                    ListBuilder items{json_value_binding()};
                    for (const auto element : value.as_indexed_view())
                    {
                        Value node = to_node(element);
                        items.push_back_copy(node.view().data());
                    }
                    return box(items.build());
                }
                case ValueTypeKind::Map: {
                    MapBuilder entries{*ValuePlanFactory::instance().binding_for(
                                           scalar_descriptor<Str>::value_meta()),
                                       json_value_binding()};
                    for (const auto [key, item] : value.as_map())
                    {
                        // JSON object keys are STRINGS: stringify atomic keys
                        // (python json.dumps convention) and reject the rest.
                        Value key_text;
                        const auto *key_meta = key.schema();
                        if (key_meta == scalar_descriptor<Str>::value_meta()) { key_text = Value{key}; }
                        else if (key_meta == scalar_descriptor<Int>::value_meta())
                        {
                            key_text = Value{Str{fmt::format("{}", key.checked_as<Int>())}};
                        }
                        else if (key_meta == scalar_descriptor<Float>::value_meta())
                        {
                            key_text = Value{Str{fmt::format("{}", key.checked_as<Float>())}};
                        }
                        else if (key_meta == scalar_descriptor<Bool>::value_meta())
                        {
                            key_text = Value{Str{key.checked_as<Bool>() ? "true" : "false"}};
                        }
                        else { throw std::invalid_argument("JSON tree: object keys must be strings"); }
                        Value node = to_node(item);
                        entries.set_item_copy(key_text.view().data(), node.view().data());
                    }
                    return box(entries.build());
                }
                case ValueTypeKind::Any: {
                    auto boxed = value.as_any();
                    return boxed.has_value() ? to_node(boxed.get()) : box(Value{});
                }
                default:
                    throw std::invalid_argument("JSON tree: unsupported value kind");
            }
        }

        [[nodiscard]] inline ValueView unbox(const ValueView &node)
        {
            auto any = node.as_any();
            return any.has_value() ? any.get() : ValueView{};
        }

        inline void escape_into(const Str &text, std::string &out)
        {
            out += '"';
            for (const char c : text)
            {
                switch (c)
                {
                    case '"': out += "\\\""; break;
                    case '\\': out += "\\\\"; break;
                    case '\n': out += "\\n"; break;
                    case '\t': out += "\\t"; break;
                    case '\r': out += "\\r"; break;
                    default: out += c;
                }
            }
            out += '"';
        }

        inline void encode(const ValueView &node, std::string &out)
        {
            auto inner = unbox(node);
            if (!inner.valid()) { out += "null"; return; }
            const auto *meta = inner.schema();
            if (meta == scalar_descriptor<Bool>::value_meta())
            {
                out += inner.checked_as<Bool>() ? "true" : "false";
                return;
            }
            if (meta == scalar_descriptor<Int>::value_meta())
            {
                out += fmt::format("{}", inner.checked_as<Int>());
                return;
            }
            if (meta == scalar_descriptor<Float>::value_meta())
            {
                out += fmt::format("{}", inner.checked_as<Float>());
                return;
            }
            if (meta == scalar_descriptor<Str>::value_meta())
            {
                escape_into(inner.checked_as<Str>(), out);
                return;
            }
            if (meta->kind == ValueTypeKind::List)
            {
                out += '[';
                bool first = true;
                for (const auto element : inner.as_list())
                {
                    if (!first) { out += ", "; }
                    first = false;
                    encode(element, out);
                }
                out += ']';
                return;
            }
            if (meta->kind == ValueTypeKind::Map)
            {
                out += '{';
                bool first = true;
                for (const auto [key, item] : inner.as_map())
                {
                    if (!first) { out += ", "; }
                    first = false;
                    escape_into(key.checked_as<Str>(), out);
                    out += ": ";
                    encode(item, out);
                }
                out += '}';
                return;
            }
            throw std::invalid_argument("JSON tree: unsupported node content");
        }

        /** Minimal recursive-descent parser -> JSON node. */
        struct Parser
        {
            std::string_view text;
            std::size_t      at{0};

            void skip() { while (at < text.size() && std::isspace(static_cast<unsigned char>(text[at]))) { ++at; } }

            [[noreturn]] void fail(const char *what) const
            {
                throw std::invalid_argument(fmt::format("json_decode: {} at offset {}", what, at));
            }

            Value parse()
            {
                skip();
                if (at >= text.size()) { fail("unexpected end"); }
                const char c = text[at];
                if (c == '{') { return object(); }
                if (c == '[') { return array(); }
                if (c == '"') { return box(Value{string()}); }
                if (c == 't' || c == 'f') { return boolean(); }
                if (c == 'n')
                {
                    expect("null");
                    return box(Value{});
                }
                return number();
            }

            void expect(std::string_view word)
            {
                if (text.substr(at, word.size()) != word) { fail("invalid literal"); }
                at += word.size();
            }

            Str string()
            {
                ++at;   // opening quote
                Str result;
                while (at < text.size() && text[at] != '"')
                {
                    char c = text[at++];
                    if (c == '\\' && at < text.size())
                    {
                        const char escaped = text[at++];
                        switch (escaped)
                        {
                            case 'n': c = '\n'; break;
                            case 't': c = '\t'; break;
                            case 'r': c = '\r'; break;
                            default: c = escaped;
                        }
                    }
                    result += c;
                }
                if (at >= text.size()) { fail("unterminated string"); }
                ++at;   // closing quote
                return result;
            }

            Value boolean()
            {
                if (text[at] == 't')
                {
                    expect("true");
                    return box(Value{Bool{true}});
                }
                expect("false");
                return box(Value{Bool{false}});
            }

            Value number()
            {
                // Strict JSON grammar (json.loads parity):
                //   -? (0 | [1-9][0-9]*) (\.[0-9]+)? ([eE][+-]?[0-9]+)?
                const std::size_t start = at;
                if (at < text.size() && text[at] == '-') { ++at; }
                const auto digit = [&] { return at < text.size() && std::isdigit(static_cast<unsigned char>(text[at])); };
                if (!digit()) { fail("invalid number"); }
                if (text[at] == '0') { ++at; }
                else
                {
                    while (digit()) { ++at; }
                }
                bool is_float = false;
                if (at < text.size() && text[at] == '.')
                {
                    is_float = true;
                    ++at;
                    if (!digit()) { fail("invalid number"); }
                    while (digit()) { ++at; }
                }
                if (at < text.size() && (text[at] == 'e' || text[at] == 'E'))
                {
                    is_float = true;
                    ++at;
                    if (at < text.size() && (text[at] == '+' || text[at] == '-')) { ++at; }
                    if (!digit()) { fail("invalid number"); }
                    while (digit()) { ++at; }
                }
                const std::string token{text.substr(start, at - start)};
                if (is_float) { return box(Value{std::stod(token)}); }
                return box(Value{static_cast<Int>(std::stoll(token))});
            }

            Value array()
            {
                ++at;   // '['
                ListBuilder items{json_value_binding()};
                skip();
                if (at < text.size() && text[at] == ']') { ++at; return box(items.build()); }
                while (true)
                {
                    Value node = parse();
                    items.push_back_copy(node.view().data());
                    skip();
                    if (at < text.size() && text[at] == ',') { ++at; continue; }
                    if (at < text.size() && text[at] == ']') { ++at; break; }
                    fail("expected ',' or ']'");
                }
                return box(items.build());
            }

            Value object()
            {
                ++at;   // '{'
                MapBuilder entries{*ValuePlanFactory::instance().binding_for(
                                       scalar_descriptor<Str>::value_meta()),
                                   json_value_binding()};
                skip();
                if (at < text.size() && text[at] == '}') { ++at; return box(entries.build()); }
                while (true)
                {
                    skip();
                    if (at >= text.size() || text[at] != '"') { fail("expected object key"); }
                    const Str key = string();
                    skip();
                    if (at >= text.size() || text[at] != ':') { fail("expected ':'"); }
                    ++at;
                    Value node = parse();
                    Value key_value{key};
                    entries.set_item_copy(key_value.view().data(), node.view().data());
                    skip();
                    if (at < text.size() && text[at] == ',') { ++at; continue; }
                    if (at < text.size() && text[at] == '}') { ++at; break; }
                    fail("expected ',' or '}'");
                }
                return box(entries.build());
            }
        };

        inline void publish(const TSOutputView &out, Value &&node)
        {
            auto mutation = out.begin_mutation(out.evaluation_time());
            static_cast<void>(mutation.move_value_from(std::move(node)));
        }
    }  // namespace json_tree

    /** Runtime node behind ``combine_json``: an un-named TSB of values +
        the parallel key names -> a JSON object node. */
    struct json_object_impl
    {
        static constexpr auto name = "__json_object";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext)
        {
            if (resolution.find_ts("O") != nullptr) { return; }
            resolution.bind_ts("O", TypeRegistry::instance().ts(json_tree::json_meta()));
        }

        static void eval(In<"values", TsVar<"V">> values, Scalar<"keys", ScalarVar<"K">> keys, Out<TsVar<"O">> out)
        {
            // keys is a tuple[str, ...] scalar parallel to the value bundle.
            auto       names = keys.value().as_list();
            MapBuilder entries{*ValuePlanFactory::instance().binding_for(
                                   scalar_descriptor<Str>::value_meta()),
                               json_tree::json_value_binding()};
            const auto count = values.base().schema()->field_count();
            for (std::size_t index = 0; index < count && index < names.size(); ++index)
            {
                auto child = values.base().indexed_child_at(index);
                if (!child.valid()) { continue; }
                Value node = json_tree::to_node(child.value());
                entries.set_item_copy(names.at(index).data(), node.view().data());
            }
            json_tree::publish(static_cast<const TSOutputView &>(out), json_tree::box(entries.build()));
        }
    };

    /** combine_json(**kwargs) — composes the value bundle + key list onto
        the runtime node. */
    struct combine_json_impl
    {
        static constexpr auto name = "combine_json";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext)
        {
            higher_order_impl_detail::bind_graph_output(
                resolution, TypeRegistry::instance().ts(json_tree::json_meta()), "O");
        }

        static WiringPortRef compose(Wiring &w, VarKwIn<"kwargs"> kwargs)
        {
            auto &registry = TypeRegistry::instance();
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
            std::vector<WiringPortRef>                                       children;
            ListBuilder names{*ValuePlanFactory::instance().binding_for(scalar_descriptor<Str>::value_meta())};
            fields.reserve(kwargs.size());
            children.reserve(kwargs.size());
            for (const auto &[key, port] : kwargs)
            {
                names.push_back(Str{key});
                fields.emplace_back(key, port.schema);
                children.push_back(port);
            }
            WiringPortRef packed =
                WiringPortRef::structural_source(registry.un_named_tsb(fields), std::move(children));

            WiringArg values_arg;
            values_arg.kind = WiringArg::Kind::TimeSeries;
            values_arg.port = packed;
            values_arg.name = "values";
            WiringArg keys_arg;
            keys_arg.kind         = WiringArg::Kind::Scalar;
            keys_arg.scalar_value = names.build();
            keys_arg.scalar_meta  = keys_arg.scalar_value.schema();
            keys_arg.name         = "keys";
            std::array<WiringArg, 2> args{values_arg, keys_arg};
            return wire_operator(w, "__json_object", args).output.erased();
        }
    };

    struct json_encode_impl
    {
        static constexpr auto name = "json_encode";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
        {
            return json_tree::is_json_ts(resolution.find_ts("S"));
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TS<Str>> out)
        {
            std::string result;
            json_tree::encode(ts.base().value(), result);
            out.set(Str{result});
        }
    };

    /** json_encode[bytes]: the encoded text as UTF-8 bytes. */
    struct json_encode_bytes_impl
    {
        static constexpr auto name = "json_encode_bytes";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
        {
            return json_tree::is_json_ts(resolution.find_ts("S"));
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TS<Bytes>> out)
        {
            std::string result;
            json_tree::encode(ts.base().value(), result);
            out.set(Bytes{std::move(result)});
        }
    };

    struct json_decode_impl
    {
        static constexpr auto name = "json_decode";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext)
        {
            if (resolution.find_ts("O") != nullptr) { return; }
            resolution.bind_ts("O", TypeRegistry::instance().ts(json_tree::json_meta()));
        }

        static Value parse_all(const Str &text)
        {
            json_tree::Parser parser{text};
            Value             node = parser.parse();
            parser.skip();
            if (parser.at != text.size()) { parser.fail("trailing characters"); }
            return node;
        }

        static void eval(In<"ts", TS<Str>> ts, Out<TsVar<"O">> out)
        {
            const Str text = ts.value();   // keep the storage alive past the view
            json_tree::publish(static_cast<const TSOutputView &>(out), parse_all(text));
        }
    };

    /** json_decode over UTF-8 bytes. */
    struct json_decode_bytes_impl
    {
        static constexpr auto name = "json_decode_bytes";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext)
        {
            if (resolution.find_ts("O") != nullptr) { return; }
            resolution.bind_ts("O", TypeRegistry::instance().ts(json_tree::json_meta()));
        }

        static void eval(In<"ts", TS<Bytes>> ts, Out<TsVar<"O">> out)
        {
            const Bytes text = ts.value();
            json_tree::publish(static_cast<const TSOutputView &>(out),
                               json_decode_impl::parse_all(Str{text.data}));
        }
    };

    /** getitem_ over a JSON node: string key (object) / int index (array);
        SILENT when absent or the node has the wrong shape. */
    template <bool ByName>
    struct getitem_json_impl
    {
        static constexpr auto name = ByName ? "getitem_json_by_name" : "getitem_json_by_index";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            if (context.args.size() != 2 || context.args[0].kind != WiringArg::Kind::TimeSeries) { return false; }
            if (!json_tree::is_json_ts(context.args[0].port.schema)) { return false; }
            if (context.args[1].kind != WiringArg::Kind::Scalar) { return false; }
            const auto *key_meta = context.args[1].scalar_value.schema();
            return ByName ? key_meta == scalar_descriptor<Str>::value_meta()
                          : key_meta == scalar_descriptor<Int>::value_meta();
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext)
        {
            if (resolution.find_ts("O") != nullptr) { return; }
            resolution.bind_ts("O", TypeRegistry::instance().ts(json_tree::json_meta()));
        }

        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"key", std::conditional_t<ByName, Str, Int>> key,
                         Out<TsVar<"O">> out)
        {
            auto inner = json_tree::unbox(ts.base().value());
            if (!inner.valid()) { return; }
            if constexpr (ByName)
            {
                if (inner.schema()->kind != ValueTypeKind::Map) { return; }
                auto map = inner.as_map();
                Value key_value{key.value()};
                if (!map.contains(key_value.view())) { return; }
                json_tree::publish(static_cast<const TSOutputView &>(out), Value{map.at(key_value.view())});
            }
            else
            {
                if (inner.schema()->kind != ValueTypeKind::List) { return; }
                auto list  = inner.as_list();
                Int  index = key.value();
                if (index < 0) { index += static_cast<Int>(list.size()); }   // python semantics
                if (index < 0 || index >= static_cast<Int>(list.size())) { return; }
                json_tree::publish(static_cast<const TSOutputView &>(out),
                                   Value{list.at(static_cast<std::size_t>(index))});
            }
        }
    };

    /** Leaf coercions: json_as_int / float / str / bool. */
    template <typename T>
    struct json_as_impl
    {
        static constexpr auto name = std::is_same_v<T, Int>     ? "json_as_int"
                                     : std::is_same_v<T, Float> ? "json_as_float"
                                     : std::is_same_v<T, Str>   ? "json_as_str"
                                                                : "json_as_bool";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
        {
            return json_tree::is_json_ts(resolution.find_ts("S"));
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TS<T>> out)
        {
            auto inner = json_tree::unbox(ts.base().value());
            if (!inner.valid()) { return; }
            if (inner.schema() == scalar_descriptor<T>::value_meta())
            {
                out.set(inner.checked_as<T>());
                return;
            }
            if constexpr (std::is_same_v<T, Float>)
            {
                if (inner.schema() == scalar_descriptor<Int>::value_meta())
                {
                    out.set(static_cast<Float>(inner.checked_as<Int>()));
                }
            }
        }
    };

    /** Register the JSON operator overloads. */
    inline void register_json_operators()
    {
        register_overload<json_object_, json_object_impl>();
        register_graph_overload<combine_json, combine_json_impl>();
        register_overload<json_encode, json_encode_impl>();
        register_overload<json_encode, json_encode_bytes_impl>();
        register_overload<json_decode, json_decode_impl>();
        register_overload<json_decode, json_decode_bytes_impl>();
        register_overload<getitem_, getitem_json_impl<true>>();
        register_overload<getitem_, getitem_json_impl<false>>();
        register_overload<json_as_int, json_as_impl<Int>>();
        register_overload<json_as_float, json_as_impl<Float>>();
        register_overload<json_as_str, json_as_impl<Str>>();
        register_overload<json_as_bool, json_as_impl<Bool>>();
        register_overload<to_json, to_json_value_impl>();
        register_overload<to_json, to_json_delta_impl>();
        register_overload<from_json, from_json_impl>();
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_JSON_IMPL_H
