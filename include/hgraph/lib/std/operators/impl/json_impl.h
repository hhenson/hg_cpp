#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_JSON_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_JSON_IMPL_H

#include <hgraph/lib/std/operators/impl/higher_order_impl.h>
#include <hgraph/lib/std/operators/json.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/value/json_codec.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <simdjson.h>

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

        class JsonValue
        {
          public:
            JsonValue() = default;

            [[nodiscard]] static JsonValue parse(Str text);

            [[nodiscard]] std::optional<JsonValue> child(Str key) const;
            [[nodiscard]] std::optional<JsonValue> child(Int index) const;
            [[nodiscard]] std::optional<Int>       as_int() const;
            [[nodiscard]] std::optional<Float>     as_float() const;
            [[nodiscard]] std::optional<Str>       as_str() const;
            [[nodiscard]] std::optional<Bool>      as_bool() const;
            [[nodiscard]] Value                    materialize() const;
            [[nodiscard]] std::string              encode() const;
            [[nodiscard]] std::size_t              hash() const noexcept;

            [[nodiscard]] bool operator==(const JsonValue &other) const noexcept;
            [[nodiscard]] std::partial_ordering operator<=>(const JsonValue &other) const noexcept;

          private:
            struct Document;

            JsonValue(std::shared_ptr<Document> document, std::string pointer)
                : document_(std::move(document)), pointer_(std::move(pointer))
            {
            }

            [[nodiscard]] std::string append_key(Str key) const;
            [[nodiscard]] std::string append_index(Int index) const;
            [[nodiscard]] std::optional<simdjson::dom::element> try_element() const;
            [[nodiscard]] simdjson::dom::element element() const;

            std::shared_ptr<Document>  document_{};
            std::string                pointer_{};
            mutable std::shared_ptr<const Value> materialized_{};
        };

        std::ostream &operator<<(std::ostream &out, const JsonValue &value);
    }  // namespace json_tree
}  // namespace hgraph::stdlib

template <>
struct std::hash<hgraph::stdlib::json_tree::JsonValue>
{
    std::size_t operator()(const hgraph::stdlib::json_tree::JsonValue &value) const noexcept
    {
        return value.hash();
    }
};

namespace hgraph::stdlib
{
    namespace json_tree
    {
        [[nodiscard]] inline const ValueTypeMetaData *json_lazy_meta()
        {
            return TypeRegistry::instance().register_scalar<JsonValue>("__hgraph_json_lazy");
        }

        [[nodiscard]] inline const JsonValue *try_lazy(const ValueView &value)
        {
            return value.valid() && value.schema() == json_lazy_meta() ? &value.checked_as<JsonValue>() : nullptr;
        }

        [[nodiscard]] inline Value lazy_value(JsonValue value)
        {
            static_cast<void>(json_lazy_meta());
            return Value{std::move(value)};
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

        [[nodiscard]] inline bool equals(const ValueView &lhs, const ValueView &rhs)
        {
            auto lhs_inner = unbox(lhs);
            auto rhs_inner = unbox(rhs);
            if (!lhs_inner.valid() || !rhs_inner.valid()) { return !lhs_inner.valid() && !rhs_inner.valid(); }

            const auto *lhs_lazy = try_lazy(lhs_inner);
            const auto *rhs_lazy = try_lazy(rhs_inner);
            if (lhs_lazy != nullptr && rhs_lazy != nullptr) { return *lhs_lazy == *rhs_lazy; }
            if (lhs_lazy != nullptr)
            {
                Value materialized = lhs_lazy->materialize();
                return equals(materialized.view(), rhs);
            }
            if (rhs_lazy != nullptr)
            {
                Value materialized = rhs_lazy->materialize();
                return equals(lhs, materialized.view());
            }
            return lhs.equals(rhs);
        }

        [[nodiscard]] inline std::partial_ordering compare(const ValueView &lhs, const ValueView &rhs)
        {
            if (equals(lhs, rhs)) { return std::partial_ordering::equivalent; }

            auto lhs_inner = unbox(lhs);
            auto rhs_inner = unbox(rhs);
            if (!lhs_inner.valid() || !rhs_inner.valid())
            {
                return lhs_inner.valid() ? std::partial_ordering::greater : std::partial_ordering::less;
            }

            if (const auto *lhs_lazy = try_lazy(lhs_inner))
            {
                Value materialized = lhs_lazy->materialize();
                return compare(materialized.view(), rhs);
            }
            if (const auto *rhs_lazy = try_lazy(rhs_inner))
            {
                Value materialized = rhs_lazy->materialize();
                return compare(lhs, materialized.view());
            }
            return lhs.compare(rhs);
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
            if (const auto *lazy = try_lazy(inner))
            {
                out += lazy->encode();
                return;
            }
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

        struct JsonValue::Document
        {
            explicit Document(Str text)
                : padded(text)
                , parser(std::make_unique<simdjson::dom::parser>())
            {
                auto parsed = parser->parse(padded);
                if (auto error = parsed.get(root))
                {
                    throw std::invalid_argument(fmt::format("json_decode: {}", simdjson::error_message(error)));
                }
            }

            simdjson::padded_string              padded;
            std::unique_ptr<simdjson::dom::parser> parser;
            simdjson::dom::element               root;
        };

        [[nodiscard]] inline JsonValue JsonValue::parse(Str text)
        {
            return JsonValue{std::make_shared<Document>(std::move(text)), std::string{}};
        }

        [[nodiscard]] inline std::string JsonValue::append_key(Str key) const
        {
            std::string out = pointer_;
            out += '/';
            for (const char c : key)
            {
                if (c == '~') { out += "~0"; }
                else if (c == '/') { out += "~1"; }
                else { out += c; }
            }
            return out;
        }

        [[nodiscard]] inline std::string JsonValue::append_index(Int index) const
        {
            std::string out = pointer_;
            out += '/';
            out += std::to_string(index);
            return out;
        }

        [[nodiscard]] inline std::optional<simdjson::dom::element> JsonValue::try_element() const
        {
            if (!document_) { return std::nullopt; }
            if (pointer_.empty()) { return document_->root; }
            simdjson::dom::element result;
            auto                   lookup = document_->root.at_pointer(pointer_);
            if (lookup.get(result)) { return std::nullopt; }
            return result;
        }

        [[nodiscard]] inline simdjson::dom::element JsonValue::element() const
        {
            auto result = try_element();
            if (!result.has_value()) { throw std::invalid_argument("JSON tree: path does not exist"); }
            return *result;
        }

        [[nodiscard]] inline Value value_from_simdjson(simdjson::dom::element element)
        {
            switch (element.type())
            {
                case simdjson::dom::element_type::NULL_VALUE:
                    return box(Value{});
                case simdjson::dom::element_type::BOOL:
                    return box(Value{Bool{static_cast<bool>(element)}});
                case simdjson::dom::element_type::INT64:
                    return box(Value{static_cast<Int>(static_cast<std::int64_t>(element))});
                case simdjson::dom::element_type::UINT64:
                {
                    const auto value = static_cast<std::uint64_t>(element);
                    if (value > static_cast<std::uint64_t>(std::numeric_limits<Int>::max()))
                    {
                        throw std::invalid_argument("JSON tree: unsigned integer is too large for Int");
                    }
                    return box(Value{static_cast<Int>(value)});
                }
                case simdjson::dom::element_type::DOUBLE:
                    return box(Value{static_cast<Float>(static_cast<double>(element))});
                case simdjson::dom::element_type::STRING:
                    return box(Value{Str{std::string_view(element)}});
                case simdjson::dom::element_type::ARRAY:
                {
                    ListBuilder items{json_value_binding()};
                    for (simdjson::dom::element child : simdjson::dom::array(element))
                    {
                        Value node = value_from_simdjson(child);
                        items.push_back_copy(node.view().data());
                    }
                    return box(items.build());
                }
                case simdjson::dom::element_type::OBJECT:
                {
                    MapBuilder entries{*ValuePlanFactory::instance().binding_for(
                                           scalar_descriptor<Str>::value_meta()),
                                       json_value_binding()};
                    for (simdjson::dom::key_value_pair field : simdjson::dom::object(element))
                    {
                        Value key{Str{std::string_view(field.key)}};
                        Value node = value_from_simdjson(field.value);
                        entries.set_item_copy(key.view().data(), node.view().data());
                    }
                    return box(entries.build());
                }
                case simdjson::dom::element_type::BIGINT:
                    throw std::invalid_argument("JSON tree: bigint is not supported");
            }
            throw std::invalid_argument("JSON tree: unsupported simdjson element");
        }

        inline void encode_simdjson(simdjson::dom::element element, std::string &out)
        {
            switch (element.type())
            {
                case simdjson::dom::element_type::NULL_VALUE:
                    out += "null";
                    return;
                case simdjson::dom::element_type::BOOL:
                    out += static_cast<bool>(element) ? "true" : "false";
                    return;
                case simdjson::dom::element_type::INT64:
                    out += fmt::format("{}", static_cast<std::int64_t>(element));
                    return;
                case simdjson::dom::element_type::UINT64:
                    out += fmt::format("{}", static_cast<std::uint64_t>(element));
                    return;
                case simdjson::dom::element_type::DOUBLE:
                    out += fmt::format("{}", static_cast<double>(element));
                    return;
                case simdjson::dom::element_type::STRING:
                    escape_into(Str{std::string_view(element)}, out);
                    return;
                case simdjson::dom::element_type::ARRAY:
                {
                    out += '[';
                    bool first = true;
                    for (simdjson::dom::element child : simdjson::dom::array(element))
                    {
                        if (!first) { out += ", "; }
                        first = false;
                        encode_simdjson(child, out);
                    }
                    out += ']';
                    return;
                }
                case simdjson::dom::element_type::OBJECT:
                {
                    out += '{';
                    bool first = true;
                    for (simdjson::dom::key_value_pair field : simdjson::dom::object(element))
                    {
                        if (!first) { out += ", "; }
                        first = false;
                        escape_into(Str{std::string_view(field.key)}, out);
                        out += ": ";
                        encode_simdjson(field.value, out);
                    }
                    out += '}';
                    return;
                }
                case simdjson::dom::element_type::BIGINT:
                    throw std::invalid_argument("JSON tree: bigint is not supported");
            }
            throw std::invalid_argument("JSON tree: unsupported simdjson element");
        }

        [[nodiscard]] inline bool simdjson_equal(simdjson::dom::element lhs, simdjson::dom::element rhs)
        {
            const auto lhs_type = lhs.type();
            if (lhs_type != rhs.type()) { return false; }
            switch (lhs_type)
            {
                case simdjson::dom::element_type::NULL_VALUE:
                    return true;
                case simdjson::dom::element_type::BOOL:
                    return static_cast<bool>(lhs) == static_cast<bool>(rhs);
                case simdjson::dom::element_type::INT64:
                    return static_cast<std::int64_t>(lhs) == static_cast<std::int64_t>(rhs);
                case simdjson::dom::element_type::UINT64:
                    return static_cast<std::uint64_t>(lhs) == static_cast<std::uint64_t>(rhs);
                case simdjson::dom::element_type::DOUBLE:
                    return static_cast<double>(lhs) == static_cast<double>(rhs);
                case simdjson::dom::element_type::STRING:
                    return std::string_view(lhs) == std::string_view(rhs);
                case simdjson::dom::element_type::ARRAY:
                {
                    auto lhs_array = simdjson::dom::array(lhs);
                    auto rhs_array = simdjson::dom::array(rhs);
                    if (lhs_array.size() != rhs_array.size()) { return false; }
                    auto rhs_it = rhs_array.begin();
                    for (simdjson::dom::element lhs_child : lhs_array)
                    {
                        if (rhs_it == rhs_array.end()) { return false; }
                        if (!simdjson_equal(lhs_child, *rhs_it)) { return false; }
                        ++rhs_it;
                    }
                    return true;
                }
                case simdjson::dom::element_type::OBJECT:
                {
                    auto lhs_object = simdjson::dom::object(lhs);
                    auto rhs_object = simdjson::dom::object(rhs);
                    if (lhs_object.size() != rhs_object.size()) { return false; }
                    for (simdjson::dom::key_value_pair lhs_field : lhs_object)
                    {
                        bool found = false;
                        for (simdjson::dom::key_value_pair rhs_field : rhs_object)
                        {
                            if (std::string_view(lhs_field.key) == std::string_view(rhs_field.key))
                            {
                                if (!simdjson_equal(lhs_field.value, rhs_field.value)) { return false; }
                                found = true;
                                break;
                            }
                        }
                        if (!found) { return false; }
                    }
                    return true;
                }
                case simdjson::dom::element_type::BIGINT:
                    return false;
            }
            return false;
        }

        [[nodiscard]] inline Value JsonValue::materialize() const
        {
            if (materialized_) { return *materialized_; }
            Value result = value_from_simdjson(element());
            materialized_ = std::make_shared<Value>(result);
            return result;
        }

        [[nodiscard]] inline std::string JsonValue::encode() const
        {
            std::string out;
            encode_simdjson(element(), out);
            return out;
        }

        [[nodiscard]] inline std::optional<JsonValue> JsonValue::child(Str key) const
        {
            if (!document_) { return std::nullopt; }
            JsonValue candidate{document_, append_key(std::move(key))};
            return candidate.try_element().has_value() ? std::optional<JsonValue>{std::move(candidate)} : std::nullopt;
        }

        [[nodiscard]] inline std::optional<JsonValue> JsonValue::child(Int index) const
        {
            if (!document_) { return std::nullopt; }
            Int resolved = index;
            if (resolved < 0)
            {
                auto current = try_element();
                if (!current.has_value()) { return std::nullopt; }
                if (current->type() != simdjson::dom::element_type::ARRAY) { return std::nullopt; }
                const auto size = simdjson::dom::array(*current).size();
                resolved += static_cast<Int>(size);
            }
            if (resolved < 0) { return std::nullopt; }
            JsonValue candidate{document_, append_index(resolved)};
            return candidate.try_element().has_value() ? std::optional<JsonValue>{std::move(candidate)} : std::nullopt;
        }

        [[nodiscard]] inline std::optional<Int> JsonValue::as_int() const
        {
            auto current = try_element();
            if (!current.has_value()) { return std::nullopt; }
            try
            {
                if (current->type() == simdjson::dom::element_type::INT64)
                {
                    return static_cast<Int>(static_cast<std::int64_t>(*current));
                }
                if (current->type() == simdjson::dom::element_type::UINT64)
                {
                    const auto value = static_cast<std::uint64_t>(*current);
                    if (value <= static_cast<std::uint64_t>(std::numeric_limits<Int>::max()))
                    {
                        return static_cast<Int>(value);
                    }
                }
            }
            catch (...) { return std::nullopt; }
            return std::nullopt;
        }

        [[nodiscard]] inline std::optional<Float> JsonValue::as_float() const
        {
            auto current = try_element();
            if (!current.has_value()) { return std::nullopt; }
            try
            {
                if (current->type() == simdjson::dom::element_type::DOUBLE)
                {
                    return static_cast<Float>(static_cast<double>(*current));
                }
                if (current->type() == simdjson::dom::element_type::INT64)
                {
                    return static_cast<Float>(static_cast<std::int64_t>(*current));
                }
                if (current->type() == simdjson::dom::element_type::UINT64)
                {
                    return static_cast<Float>(static_cast<std::uint64_t>(*current));
                }
            }
            catch (...) { return std::nullopt; }
            return std::nullopt;
        }

        [[nodiscard]] inline std::optional<Str> JsonValue::as_str() const
        {
            auto current = try_element();
            if (!current.has_value()) { return std::nullopt; }
            try
            {
                if (current->type() == simdjson::dom::element_type::STRING)
                {
                    return Str{std::string_view(*current)};
                }
            }
            catch (...) { return std::nullopt; }
            return std::nullopt;
        }

        [[nodiscard]] inline std::optional<Bool> JsonValue::as_bool() const
        {
            auto current = try_element();
            if (!current.has_value()) { return std::nullopt; }
            try
            {
                if (current->type() == simdjson::dom::element_type::BOOL)
                {
                    return Bool{static_cast<bool>(*current)};
                }
            }
            catch (...) { return std::nullopt; }
            return std::nullopt;
        }

        [[nodiscard]] inline std::size_t JsonValue::hash() const noexcept
        {
            try { return materialize().hash(); }
            catch (...) { return 0; }
        }

        [[nodiscard]] inline bool JsonValue::operator==(const JsonValue &other) const noexcept
        {
            try
            {
                auto lhs = try_element();
                auto rhs = other.try_element();
                if (lhs.has_value() && rhs.has_value()) { return simdjson_equal(*lhs, *rhs); }
                return materialize().equals(other.materialize());
            }
            catch (...) { return false; }
        }

        [[nodiscard]] inline std::partial_ordering JsonValue::operator<=>(const JsonValue &other) const noexcept
        {
            try { return materialize().compare(other.materialize()); }
            catch (...) { return std::partial_ordering::unordered; }
        }

        inline std::ostream &operator<<(std::ostream &out, const JsonValue &value)
        {
            try { out << value.encode(); }
            catch (...) { out << "<invalid JSON>"; }
            return out;
        }

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
            return json_tree::box(json_tree::lazy_value(json_tree::JsonValue::parse(text)));
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
            if (const auto *lazy = json_tree::try_lazy(inner))
            {
                auto child = [&]() {
                    if constexpr (ByName) { return lazy->child(key.value()); }
                    else { return lazy->child(key.value()); }
                }();
                if (!child.has_value()) { return; }
                json_tree::publish(static_cast<const TSOutputView &>(out),
                                   json_tree::box(json_tree::lazy_value(std::move(*child))));
                return;
            }
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
            if (const auto *lazy = json_tree::try_lazy(inner))
            {
                if constexpr (std::is_same_v<T, Int>)
                {
                    if (auto value = lazy->as_int()) { out.set(*value); }
                }
                else if constexpr (std::is_same_v<T, Float>)
                {
                    if (auto value = lazy->as_float()) { out.set(*value); }
                }
                else if constexpr (std::is_same_v<T, Str>)
                {
                    if (auto value = lazy->as_str()) { out.set(std::move(*value)); }
                }
                else if constexpr (std::is_same_v<T, Bool>)
                {
                    if (auto value = lazy->as_bool()) { out.set(*value); }
                }
                return;
            }
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
