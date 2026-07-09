#include <hgraph/lib/std/operators/convert_target.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/operator_type_resolution.h>

#include <fmt/format.h>

#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph::stdlib
{
    namespace
    {
        using hgraph::operator_type_resolution::ts_value_schema;

        [[nodiscard]] std::string schema_name(const TSValueTypeMetaData *schema)
        {
            return schema != nullptr && schema->display_name != nullptr ? std::string{schema->display_name}
                                                                        : std::string{"<null>"};
        }

        [[nodiscard]] const TSValueTypeMetaData *deref(const TSValueTypeMetaData *schema)
        {
            return TypeRegistry::instance().dereference(schema);
        }

        [[nodiscard]] const ValueTypeMetaData *homogeneous_tuple_element(const ValueTypeMetaData *value)
        {
            if (value == nullptr || value->kind != ValueTypeKind::Tuple || value->field_count == 0)
            {
                return nullptr;
            }
            const ValueTypeMetaData *first = value->fields[0].type;
            for (std::size_t index = 1; index < value->field_count; ++index)
            {
                if (value->fields[index].type != first) { return nullptr; }
            }
            return first;
        }

        [[nodiscard]] const ValueTypeMetaData *collection_element_or_self(const ValueTypeMetaData *value)
        {
            if (value == nullptr) { return nullptr; }
            if (value->kind == ValueTypeKind::List || value->kind == ValueTypeKind::Set) { return value->element_type; }
            return value;
        }

        [[nodiscard]] bool is_collection(const ValueTypeMetaData *value)
        {
            return value != nullptr && (value->kind == ValueTypeKind::List || value->kind == ValueTypeKind::Set);
        }

        [[nodiscard]] const TSValueTypeMetaData *ts_element_value_as_ts(const TSValueTypeMetaData *schema)
        {
            const ValueTypeMetaData *value = ts_value_schema(deref(schema));
            return value != nullptr && is_collection(value) ? TypeRegistry::instance().ts(value->element_type) : schema;
        }

        [[nodiscard]] const ValueTypeMetaData *tss_element(const TSValueTypeMetaData *schema)
        {
            schema = deref(schema);
            return schema != nullptr && schema->kind == TSTypeKind::TSS && schema->value_schema != nullptr
                       ? schema->value_schema->element_type
                       : nullptr;
        }

        [[nodiscard]] const ValueTypeMetaData *tsd_value_scalar(const TSValueTypeMetaData *schema)
        {
            schema = deref(schema);
            if (schema == nullptr || schema->kind != TSTypeKind::TSD) { return nullptr; }
            return ts_value_schema(deref(schema->element_ts()));
        }

        [[nodiscard]] const ValueTypeMetaData *tsl_element_scalar(const TSValueTypeMetaData *schema)
        {
            schema = deref(schema);
            if (schema == nullptr || schema->kind != TSTypeKind::TSL) { return nullptr; }
            return ts_value_schema(deref(schema->element_ts()));
        }

        [[nodiscard]] const ValueTypeMetaData *key_scalar_from_schema(const TSValueTypeMetaData *schema)
        {
            schema = deref(schema);
            if (schema == nullptr) { return nullptr; }
            if (schema->kind == TSTypeKind::TSS) { return tss_element(schema); }
            return collection_element_or_self(ts_value_schema(schema));
        }

        [[nodiscard]] std::vector<std::size_t> selected_tsb_field_indices(
            const TSValueTypeMetaData *schema,
            std::span<const std::string> selected_keys)
        {
            std::vector<std::size_t> indices;
            if (schema == nullptr || schema->kind != TSTypeKind::TSB) { return indices; }
            if (selected_keys.empty())
            {
                indices.reserve(schema->field_count());
                for (std::size_t index = 0; index < schema->field_count(); ++index) { indices.push_back(index); }
                return indices;
            }

            for (const std::string &key : selected_keys)
            {
                bool found = false;
                for (std::size_t index = 0; index < schema->field_count(); ++index)
                {
                    const char *name = schema->fields()[index].name;
                    if (name != nullptr && key == name)
                    {
                        indices.push_back(index);
                        found = true;
                        break;
                    }
                }
                if (!found)
                {
                    throw std::invalid_argument(
                        fmt::format("TSB conversion key '{}' is not present in {}", key, schema_name(schema)));
                }
            }
            return indices;
        }

        [[nodiscard]] const TSValueTypeMetaData *homogeneous_tsb_value_ts(
            const TSValueTypeMetaData *schema,
            std::span<const std::string> selected_keys)
        {
            auto indices = selected_tsb_field_indices(schema, selected_keys);
            if (indices.empty())
            {
                throw std::invalid_argument("cannot infer a TSD value type from an empty TSB field selection");
            }

            const TSValueTypeMetaData *first = nullptr;
            for (std::size_t index : indices)
            {
                const TSValueTypeMetaData *field = schema->fields()[index].type;
                const ValueTypeMetaData   *value = ts_value_schema(field);
                if (value == nullptr)
                {
                    throw std::invalid_argument("TSB conversion requires selected fields to be plain TS values");
                }
                if (first == nullptr)
                {
                    first = field;
                    continue;
                }
                if (first->value_schema != value)
                {
                    throw std::invalid_argument(
                        "TSB conversion requires all selected fields to have the same value type");
                }
            }
            return first;
        }

        [[nodiscard]] const ValueTypeMetaData *homogeneous_tsb_value_scalar(
            const TSValueTypeMetaData *schema,
            std::span<const std::string> selected_keys)
        {
            const TSValueTypeMetaData *field = homogeneous_tsb_value_ts(schema, selected_keys);
            return field != nullptr ? field->value_schema : nullptr;
        }

        [[nodiscard]] const TSValueTypeMetaData *tsb_for_bundle_value(const ValueTypeMetaData *bundle)
        {
            if (bundle == nullptr || bundle->kind != ValueTypeKind::Bundle || bundle->display_name == nullptr)
            {
                return nullptr;
            }
            auto &registry = TypeRegistry::instance();
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
            fields.reserve(bundle->field_count);
            for (std::size_t index = 0; index < bundle->field_count; ++index)
            {
                const ValueFieldMetaData &field = bundle->fields[index];
                fields.emplace_back(std::string{field.name != nullptr ? field.name : ""},
                                    registry.ts(field.type));
            }
            return registry.tsb(bundle->display_name, fields);
        }

        [[nodiscard]] const TSValueTypeMetaData *require_one_input(
            std::span<const TSValueTypeMetaData *const> inputs,
            std::string_view pattern)
        {
            if (inputs.empty())
            {
                throw std::invalid_argument(fmt::format("{} target resolution requires at least one input", pattern));
            }
            return deref(inputs[0]);
        }

        [[nodiscard]] const TSValueTypeMetaData *require_one_raw_input(
            std::span<const TSValueTypeMetaData *const> inputs,
            std::string_view pattern)
        {
            if (inputs.empty())
            {
                throw std::invalid_argument(fmt::format("{} target resolution requires at least one input", pattern));
            }
            return inputs[0];
        }

        [[nodiscard]] const TSValueTypeMetaData *match_pattern_target(
            const TypePattern &pattern,
            const TSValueTypeMetaData *candidate)
        {
            if (candidate == nullptr)
            {
                throw std::invalid_argument(
                    fmt::format("cannot resolve type pattern {} against <null>", ts_pattern_to_string(pattern)));
            }
            ResolutionMap resolution;
            if (!output_ts_pattern_match(pattern, candidate, resolution))
            {
                throw std::invalid_argument(
                    fmt::format("resolved target {} does not satisfy requested {}",
                                schema_name(candidate),
                                ts_pattern_to_string(pattern)));
            }
            return candidate;
        }

        [[nodiscard]] const ValueTypeMetaData *infer_tuple_value_candidate(
            std::span<const TSValueTypeMetaData *const> inputs)
        {
            auto &registry = TypeRegistry::instance();
            const TSValueTypeMetaData *input = require_one_input(inputs, "TS[tuple]");
            if (input->kind == TSTypeKind::TS)
            {
                const ValueTypeMetaData *value = input->value_schema;
                if (value != nullptr && (value->kind == ValueTypeKind::Tuple || value->kind == ValueTypeKind::List))
                {
                    return value;
                }
                const ValueTypeMetaData *element = collection_element_or_self(value);
                if (element != nullptr) { return registry.list(element, 0, true); }
            }
            if (input->kind == TSTypeKind::TSS)
            {
                const ValueTypeMetaData *element = tss_element(input);
                if (element != nullptr) { return registry.list(element, 0, true); }
            }
            if (input->kind == TSTypeKind::TSL)
            {
                const ValueTypeMetaData *element = tsl_element_scalar(input);
                if (element == nullptr || input->fixed_size() == 0)
                {
                    throw std::invalid_argument(fmt::format("cannot infer fixed tuple target from {}", schema_name(input)));
                }
                std::vector<const ValueTypeMetaData *> fields(input->fixed_size(), element);
                return registry.tuple(fields);
            }
            throw std::invalid_argument(fmt::format("cannot infer TS[tuple] target from {}", schema_name(input)));
        }

        [[nodiscard]] const ValueTypeMetaData *infer_set_value_candidate(
            std::span<const TSValueTypeMetaData *const> inputs)
        {
            auto &registry = TypeRegistry::instance();
            const TSValueTypeMetaData *input = require_one_input(inputs, "TS[set]");
            const ValueTypeMetaData *element = nullptr;
            if (input->kind == TSTypeKind::TS) { element = collection_element_or_self(input->value_schema); }
            else if (input->kind == TSTypeKind::TSS) { element = tss_element(input); }
            if (element == nullptr)
            {
                throw std::invalid_argument(fmt::format("cannot infer TS[set] target from {}", schema_name(input)));
            }
            return registry.set(element);
        }

        [[nodiscard]] const ValueTypeMetaData *infer_mapping_value_candidate(
            std::span<const TSValueTypeMetaData *const> inputs)
        {
            auto &registry = TypeRegistry::instance();
            const TSValueTypeMetaData *first = require_one_input(inputs, "TS[Mapping]");
            if (inputs.size() == 1)
            {
                if (first->kind == TSTypeKind::TS)
                {
                    if (first->value_schema != nullptr && first->value_schema->kind == ValueTypeKind::Map)
                    {
                        return first->value_schema;
                    }
                }
                if (first->kind == TSTypeKind::TSD)
                {
                    const ValueTypeMetaData *value = tsd_value_scalar(first);
                    if (value != nullptr) { return registry.map(first->key_type(), value); }
                }
                if (first->kind == TSTypeKind::TSL)
                {
                    const ValueTypeMetaData *value = tsl_element_scalar(first);
                    if (value != nullptr) { return registry.map(registry.value_type("int"), value); }
                }
                if (first->kind == TSTypeKind::TSB)
                {
                    const ValueTypeMetaData *value = homogeneous_tsb_value_scalar(first, {});
                    return registry.map(registry.value_type("str"), value);
                }
                throw std::invalid_argument(fmt::format("cannot infer TS[Mapping] target from {}", schema_name(first)));
            }

            const ValueTypeMetaData *key = key_scalar_from_schema(first);
            const ValueTypeMetaData *value = ts_value_schema(deref(inputs[1]));
            value = collection_element_or_self(value);
            if (key == nullptr || value == nullptr)
            {
                throw std::invalid_argument("cannot infer TS[Mapping] target from key/value inputs");
            }
            return registry.map(key, value);
        }

        [[nodiscard]] const TSValueTypeMetaData *infer_tsd_candidate(
            std::span<const TSValueTypeMetaData *const> inputs,
            std::span<const std::string> selected_keys)
        {
            auto &registry = TypeRegistry::instance();
            const TSValueTypeMetaData *first = require_one_input(inputs, "TSD");
            if (inputs.size() == 1)
            {
                if (first->kind == TSTypeKind::TSB)
                {
                    return registry.tsd(registry.value_type("str"), homogeneous_tsb_value_ts(first, selected_keys));
                }
                if (first->kind == TSTypeKind::TS)
                {
                    const ValueTypeMetaData *value = first->value_schema;
                    if (value != nullptr && value->kind == ValueTypeKind::Map)
                    {
                        return registry.tsd(value->key_type, registry.ts(value->element_type));
                    }
                }
                if (first->kind == TSTypeKind::TSL)
                {
                    const TSValueTypeMetaData *element = deref(first->element_ts());
                    if (element != nullptr)
                    {
                        return registry.tsd(registry.value_type("int"), registry.ref(element));
                    }
                }
                throw std::invalid_argument(fmt::format("cannot infer TSD target from {}", schema_name(first)));
            }

            const ValueTypeMetaData *key = key_scalar_from_schema(first);
            const TSValueTypeMetaData *value = ts_element_value_as_ts(deref(inputs[1]));
            if (key == nullptr || value == nullptr)
            {
                throw std::invalid_argument("cannot infer TSD target from key/value inputs");
            }
            return registry.tsd(key, value);
        }

        [[nodiscard]] const TSValueTypeMetaData *infer_tsl_candidate(
            std::span<const TSValueTypeMetaData *const> inputs)
        {
            auto &registry = TypeRegistry::instance();
            const TSValueTypeMetaData *input = require_one_input(inputs, "TSL");
            if (input->kind == TSTypeKind::TSL) { return input; }
            if (input->kind == TSTypeKind::TS && input->value_schema != nullptr)
            {
                const ValueTypeMetaData *value = input->value_schema;
                if (value->kind == ValueTypeKind::List && value->fixed_size > 0)
                {
                    return registry.tsl(registry.ts(value->element_type), value->fixed_size);
                }
                if (value->kind == ValueTypeKind::Tuple)
                {
                    if (const ValueTypeMetaData *element = homogeneous_tuple_element(value))
                    {
                        return registry.tsl(registry.ts(element), value->field_count);
                    }
                    throw std::invalid_argument("cannot infer TSL[V, SIZE] target from heterogeneous fixed tuple");
                }
            }
            throw std::invalid_argument(fmt::format("cannot infer TSL target from {}", schema_name(input)));
        }

        [[nodiscard]] const TSValueTypeMetaData *infer_tsb_candidate(
            std::span<const TSValueTypeMetaData *const> inputs)
        {
            const TSValueTypeMetaData *input = require_one_input(inputs, "TSB");
            if (input->kind == TSTypeKind::TSB) { return input; }
            const ValueTypeMetaData *value = ts_value_schema(input);
            if (const TSValueTypeMetaData *bundle = tsb_for_bundle_value(value)) { return bundle; }
            throw std::invalid_argument(fmt::format("cannot infer TSB target from {}", schema_name(input)));
        }

        [[nodiscard]] const TSValueTypeMetaData *infer_ts_candidate(
            const ScalarPattern &scalar_pattern,
            std::span<const TSValueTypeMetaData *const> inputs)
        {
            auto &registry = TypeRegistry::instance();
            switch (scalar_pattern.kind)
            {
                case ScalarPattern::Kind::Var:
                {
                    const TSValueTypeMetaData *input = require_one_input(inputs, "TS[T]");
                    if (input->kind != TSTypeKind::TS)
                    {
                        throw std::invalid_argument(
                            fmt::format("cannot infer TS[T] target from {}", schema_name(input)));
                    }
                    return input;
                }
                case ScalarPattern::Kind::Concrete: return registry.ts(scalar_pattern.meta);
                case ScalarPattern::Kind::UnknownTuple:
                case ScalarPattern::Kind::HomogeneousTuple:
                case ScalarPattern::Kind::FixedTuple: return registry.ts(infer_tuple_value_candidate(inputs));
                case ScalarPattern::Kind::Set: return registry.ts(infer_set_value_candidate(inputs));
                case ScalarPattern::Kind::Map: return registry.ts(infer_mapping_value_candidate(inputs));
                case ScalarPattern::Kind::Bundle:
                {
                    const TSValueTypeMetaData *input = require_one_input(inputs, "TS[CompoundScalar]");
                    if (input->kind == TSTypeKind::TSB && input->value_schema != nullptr)
                    {
                        return registry.ts(input->value_schema);
                    }
                    if (input->kind == TSTypeKind::TS && input->value_schema != nullptr &&
                        input->value_schema->kind == ValueTypeKind::Bundle)
                    {
                        return input;
                    }
                    throw std::invalid_argument(
                        fmt::format("cannot infer TS[CompoundScalar] target from {}", schema_name(input)));
                }
            }
            throw std::invalid_argument("unsupported scalar pattern for TS target inference");
        }

        [[nodiscard]] const TSValueTypeMetaData *infer_convert_candidate(
            const TypePattern &pattern,
            std::span<const TSValueTypeMetaData *const> inputs,
            std::span<const std::string> selected_keys)
        {
            auto &registry = TypeRegistry::instance();
            switch (pattern.kind)
            {
                case TypePattern::Kind::Var: return require_one_input(inputs, ts_pattern_to_string(pattern));
                case TypePattern::Kind::Concrete: return pattern.meta;
                case TypePattern::Kind::TS: return infer_ts_candidate(pattern.scalar, inputs);
                case TypePattern::Kind::TSS:
                {
                    const TSValueTypeMetaData *input = require_one_input(inputs, ts_pattern_to_string(pattern));
                    const ValueTypeMetaData *element = nullptr;
                    if (input->kind == TSTypeKind::TS) { element = collection_element_or_self(input->value_schema); }
                    else if (input->kind == TSTypeKind::TSS) { element = tss_element(input); }
                    else if (input->kind == TSTypeKind::TSD) { element = input->key_type(); }
                    if (element == nullptr)
                    {
                        throw std::invalid_argument(
                            fmt::format("cannot infer TSS target from {}", schema_name(input)));
                    }
                    return registry.tss(element);
                }
                case TypePattern::Kind::TSD: return infer_tsd_candidate(inputs, selected_keys);
                case TypePattern::Kind::TSL: return infer_tsl_candidate(inputs);
                case TypePattern::Kind::TSB: return infer_tsb_candidate(inputs);
                case TypePattern::Kind::REF:
                {
                    const TSValueTypeMetaData *input = require_one_raw_input(inputs, ts_pattern_to_string(pattern));
                    if (input->kind != TSTypeKind::REF)
                    {
                        throw std::invalid_argument(
                            fmt::format("cannot infer REF target from {}", schema_name(input)));
                    }
                    return input;
                }
                case TypePattern::Kind::TSW:
                case TypePattern::Kind::Signal:
                    throw std::invalid_argument(
                        fmt::format("cannot infer convert target for {}", ts_pattern_to_string(pattern)));
            }
            throw std::invalid_argument("unsupported time-series pattern for convert target inference");
        }

        [[nodiscard]] const TSValueTypeMetaData *infer_collect_tsd_candidate(
            std::span<const TSValueTypeMetaData *const> inputs)
        {
            auto &registry = TypeRegistry::instance();
            const TSValueTypeMetaData *first = require_one_input(inputs, "collect[TSD]");
            if (first != nullptr && first->kind == TSTypeKind::TSD) { return first; }
            if (inputs.size() == 1 && first != nullptr && first->kind == TSTypeKind::TS &&
                first->value_schema != nullptr && first->value_schema->kind == ValueTypeKind::Map)
            {
                return registry.tsd(first->value_schema->key_type, registry.ts(first->value_schema->element_type));
            }
            if (inputs.size() >= 2)
            {
                const ValueTypeMetaData *key = key_scalar_from_schema(first);
                const TSValueTypeMetaData *value = ts_element_value_as_ts(deref(inputs[1]));
                if (key != nullptr && value != nullptr) { return registry.tsd(key, value); }
            }
            throw std::invalid_argument(fmt::format("cannot infer collect TSD target from {}", schema_name(first)));
        }

        [[nodiscard]] bool is_tsd_pattern(const TypePattern &pattern)
        {
            return pattern.kind == TypePattern::Kind::TSD;
        }

        [[nodiscard]] bool is_ts_mapping_pattern(const TypePattern &pattern)
        {
            return pattern.kind == TypePattern::Kind::TS && pattern.scalar.kind == ScalarPattern::Kind::Map;
        }
    }  // namespace

    const TSValueTypeMetaData *resolve_convert_target(const TypePattern &pattern,
                                                      std::span<const TSValueTypeMetaData *const> inputs,
                                                      std::span<const std::string> selected_keys)
    {
        const TSValueTypeMetaData *candidate = infer_convert_candidate(pattern, inputs, selected_keys);
        return match_pattern_target(pattern, candidate);
    }

    const TSValueTypeMetaData *resolve_collect_target(const TypePattern &pattern,
                                                      std::span<const TSValueTypeMetaData *const> inputs)
    {
        if (inputs.empty()) { throw std::invalid_argument("collect target resolution requires at least one input"); }
        const TSValueTypeMetaData *candidate = nullptr;
        if (is_tsd_pattern(pattern))
        {
            candidate = infer_collect_tsd_candidate(inputs);
        }
        else if (is_ts_mapping_pattern(pattern))
        {
            candidate = TypeRegistry::instance().ts(infer_mapping_value_candidate(inputs));
        }
        else
        {
            candidate = infer_convert_candidate(pattern, inputs, {});
        }
        return match_pattern_target(pattern, candidate);
    }
}  // namespace hgraph::stdlib
