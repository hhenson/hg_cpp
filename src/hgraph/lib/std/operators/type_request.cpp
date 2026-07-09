#include <hgraph/lib/std/operators/type_request.h>
#include <hgraph/types/metadata/type_registry.h>

#include <fmt/format.h>

#include <stdexcept>
#include <vector>

namespace hgraph::stdlib
{
    namespace
    {
        [[nodiscard]] std::string schema_name(const TSValueTypeMetaData *schema)
        {
            return schema != nullptr && schema->display_name != nullptr ? std::string{schema->display_name}
                                                                        : std::string{"<null>"};
        }

        [[nodiscard]] const TSValueTypeMetaData *deref(const TSValueTypeMetaData *schema)
        {
            return TypeRegistry::instance().dereference(schema);
        }

        [[nodiscard]] const ValueTypeMetaData *plain_ts_value(const TSValueTypeMetaData *schema)
        {
            schema = deref(schema);
            return schema != nullptr && schema->kind == TSTypeKind::TS ? schema->value_schema : nullptr;
        }

        [[nodiscard]] const ValueTypeMetaData *collection_element_or_self(const ValueTypeMetaData *value)
        {
            if (value == nullptr) { return nullptr; }
            if (value->kind == ValueTypeKind::List || value->kind == ValueTypeKind::Set) { return value->element_type; }
            return value;
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

        [[nodiscard]] bool is_collection(const ValueTypeMetaData *value)
        {
            return value != nullptr && (value->kind == ValueTypeKind::List || value->kind == ValueTypeKind::Set);
        }

        [[nodiscard]] const TSValueTypeMetaData *ts_element_value_as_ts(const TSValueTypeMetaData *schema)
        {
            const ValueTypeMetaData *value = plain_ts_value(schema);
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
            const TSValueTypeMetaData *element = deref(schema->element_ts());
            return element != nullptr && element->kind == TSTypeKind::TS ? element->value_schema : nullptr;
        }

        [[nodiscard]] const ValueTypeMetaData *tsl_element_scalar(const TSValueTypeMetaData *schema)
        {
            schema = deref(schema);
            if (schema == nullptr || schema->kind != TSTypeKind::TSL) { return nullptr; }
            const TSValueTypeMetaData *element = deref(schema->element_ts());
            return element != nullptr && element->kind == TSTypeKind::TS ? element->value_schema : nullptr;
        }

        [[nodiscard]] const ValueTypeMetaData *key_scalar_from_schema(const TSValueTypeMetaData *schema)
        {
            schema = deref(schema);
            if (schema == nullptr) { return nullptr; }
            if (schema->kind == TSTypeKind::TSS) { return tss_element(schema); }
            const ValueTypeMetaData *value = plain_ts_value(schema);
            return collection_element_or_self(value);
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
                for (std::size_t index = 0; index < schema->field_count(); ++index)
                {
                    const char *name = schema->fields()[index].name;
                    if (name != nullptr && key == name)
                    {
                        indices.push_back(index);
                        break;
                    }
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
                if (field == nullptr || field->kind != TSTypeKind::TS)
                {
                    throw std::invalid_argument("TSB conversion requires selected fields to be plain TS values");
                }
                if (first == nullptr)
                {
                    first = field;
                    continue;
                }
                if (first->value_schema != field->value_schema)
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
            TypeRequestKind kind)
        {
            if (inputs.empty())
            {
                throw std::invalid_argument(fmt::format("{} target resolution requires at least one input",
                                                        type_request_to_string(TypeRequest{kind})));
            }
            return deref(inputs[0]);
        }

        [[nodiscard]] const TSValueTypeMetaData *resolve_tss_target(
            std::span<const TSValueTypeMetaData *const> inputs)
        {
            const TSValueTypeMetaData *input = require_one_input(inputs, TypeRequestKind::TSS);
            const ValueTypeMetaData *element = nullptr;
            if (input->kind == TSTypeKind::TS)
            {
                element = collection_element_or_self(input->value_schema);
            }
            else if (input->kind == TSTypeKind::TSS)
            {
                element = tss_element(input);
            }
            else if (input->kind == TSTypeKind::TSD)
            {
                element = input->key_type();
            }
            if (element == nullptr)
            {
                throw std::invalid_argument(fmt::format("cannot infer TSS target from {}", schema_name(input)));
            }
            return TypeRegistry::instance().tss(element);
        }

        [[nodiscard]] const TSValueTypeMetaData *resolve_tsd_target(
            std::span<const TSValueTypeMetaData *const> inputs,
            std::span<const std::string> selected_keys)
        {
            auto &registry = TypeRegistry::instance();
            const TSValueTypeMetaData *first = require_one_input(inputs, TypeRequestKind::TSD);
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

        [[nodiscard]] const TSValueTypeMetaData *resolve_tuple_target(
            std::span<const TSValueTypeMetaData *const> inputs)
        {
            auto &registry = TypeRegistry::instance();
            const TSValueTypeMetaData *input = require_one_input(inputs, TypeRequestKind::TSTuple);
            const ValueTypeMetaData *element = nullptr;
            if (input->kind == TSTypeKind::TS)
            {
                element = collection_element_or_self(input->value_schema);
                return registry.ts(registry.list(element, 0, true));
            }
            if (input->kind == TSTypeKind::TSS)
            {
                element = tss_element(input);
                return registry.ts(registry.list(element, 0, true));
            }
            if (input->kind == TSTypeKind::TSL)
            {
                element = tsl_element_scalar(input);
                if (element == nullptr || input->fixed_size() == 0)
                {
                    throw std::invalid_argument(fmt::format("cannot infer fixed tuple target from {}", schema_name(input)));
                }
                std::vector<const ValueTypeMetaData *> fields(input->fixed_size(), element);
                return registry.ts(registry.tuple(fields));
            }
            throw std::invalid_argument(fmt::format("cannot infer TS[tuple] target from {}", schema_name(input)));
        }

        [[nodiscard]] const TSValueTypeMetaData *resolve_set_target(
            std::span<const TSValueTypeMetaData *const> inputs)
        {
            auto &registry = TypeRegistry::instance();
            const TSValueTypeMetaData *input = require_one_input(inputs, TypeRequestKind::TSSet);
            const ValueTypeMetaData *element = nullptr;
            if (input->kind == TSTypeKind::TS) { element = collection_element_or_self(input->value_schema); }
            else if (input->kind == TSTypeKind::TSS) { element = tss_element(input); }
            if (element == nullptr)
            {
                throw std::invalid_argument(fmt::format("cannot infer TS[set] target from {}", schema_name(input)));
            }
            return registry.ts(registry.set(element));
        }

        [[nodiscard]] const TSValueTypeMetaData *resolve_mapping_target(
            std::span<const TSValueTypeMetaData *const> inputs)
        {
            auto &registry = TypeRegistry::instance();
            const TSValueTypeMetaData *first = require_one_input(inputs, TypeRequestKind::TSMapping);
            if (inputs.size() == 1)
            {
                if (first->kind == TSTypeKind::TS)
                {
                    if (first->value_schema != nullptr && first->value_schema->kind == ValueTypeKind::Map)
                    {
                        return first;
                    }
                }
                if (first->kind == TSTypeKind::TSD)
                {
                    const ValueTypeMetaData *value = tsd_value_scalar(first);
                    if (value != nullptr) { return registry.ts(registry.map(first->key_type(), value)); }
                }
                if (first->kind == TSTypeKind::TSL)
                {
                    const ValueTypeMetaData *value = tsl_element_scalar(first);
                    if (value != nullptr) { return registry.ts(registry.map(registry.value_type("int"), value)); }
                }
                if (first->kind == TSTypeKind::TSB)
                {
                    const ValueTypeMetaData *value = homogeneous_tsb_value_scalar(first, {});
                    return registry.ts(registry.map(registry.value_type("str"), value));
                }
                throw std::invalid_argument(fmt::format("cannot infer TS[Mapping] target from {}", schema_name(first)));
            }

            const ValueTypeMetaData *key = key_scalar_from_schema(first);
            const ValueTypeMetaData *value = plain_ts_value(deref(inputs[1]));
            value = collection_element_or_self(value);
            if (key == nullptr || value == nullptr)
            {
                throw std::invalid_argument("cannot infer TS[Mapping] target from key/value inputs");
            }
            return registry.ts(registry.map(key, value));
        }

        [[nodiscard]] const TSValueTypeMetaData *resolve_tsb_target(
            std::span<const TSValueTypeMetaData *const> inputs)
        {
            const TSValueTypeMetaData *input = require_one_input(inputs, TypeRequestKind::TSB);
            if (input->kind == TSTypeKind::TSB) { return input; }
            const ValueTypeMetaData *value = plain_ts_value(input);
            if (const TSValueTypeMetaData *bundle = tsb_for_bundle_value(value)) { return bundle; }
            throw std::invalid_argument(fmt::format("cannot infer TSB target from {}", schema_name(input)));
        }

        [[nodiscard]] const TSValueTypeMetaData *resolve_tsl_target(
            std::span<const TSValueTypeMetaData *const> inputs)
        {
            auto &registry = TypeRegistry::instance();
            const TSValueTypeMetaData *input = require_one_input(inputs, TypeRequestKind::TSL);
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

        [[nodiscard]] const TSValueTypeMetaData *resolve_compound_scalar_target(
            std::span<const TSValueTypeMetaData *const> inputs)
        {
            const TSValueTypeMetaData *input = require_one_input(inputs, TypeRequestKind::TSCompoundScalar);
            if (input->kind == TSTypeKind::TSB && input->value_schema != nullptr)
            {
                return TypeRegistry::instance().ts(input->value_schema);
            }
            if (input->kind == TSTypeKind::TS && input->value_schema != nullptr &&
                input->value_schema->kind == ValueTypeKind::Bundle)
            {
                return input;
            }
            throw std::invalid_argument(fmt::format("cannot infer TS[CompoundScalar] target from {}", schema_name(input)));
        }
    }  // namespace

    std::string type_request_to_string(const TypeRequest &request)
    {
        switch (request.kind)
        {
            case TypeRequestKind::TSS: return "TSS[T]";
            case TypeRequestKind::TSD: return "TSD[K, V]";
            case TypeRequestKind::TSL: return "TSL[V, SIZE]";
            case TypeRequestKind::TSB: return "TSB[SCHEMA]";
            case TypeRequestKind::TSTuple: return "TS[tuple[T, ...]]";
            case TypeRequestKind::TSSet: return "TS[set[T]]";
            case TypeRequestKind::TSMapping: return "TS[Mapping[K, V]]";
            case TypeRequestKind::TSCompoundScalar: return "TS[CompoundScalar]";
        }
        return "<type request>";
    }

    const TSValueTypeMetaData *resolve_convert_target(const TypeRequest &request,
                                                      std::span<const TSValueTypeMetaData *const> inputs,
                                                      std::span<const std::string> selected_keys)
    {
        switch (request.kind)
        {
            case TypeRequestKind::TSS: return resolve_tss_target(inputs);
            case TypeRequestKind::TSD: return resolve_tsd_target(inputs, selected_keys);
            case TypeRequestKind::TSL: return resolve_tsl_target(inputs);
            case TypeRequestKind::TSB: return resolve_tsb_target(inputs);
            case TypeRequestKind::TSTuple: return resolve_tuple_target(inputs);
            case TypeRequestKind::TSSet: return resolve_set_target(inputs);
            case TypeRequestKind::TSMapping: return resolve_mapping_target(inputs);
            case TypeRequestKind::TSCompoundScalar: return resolve_compound_scalar_target(inputs);
        }
        throw std::invalid_argument("unknown convert target request");
    }

    const TSValueTypeMetaData *resolve_collect_target(const TypeRequest &request,
                                                      std::span<const TSValueTypeMetaData *const> inputs)
    {
        if (inputs.empty()) { throw std::invalid_argument("collect target resolution requires at least one input"); }
        auto &registry = TypeRegistry::instance();
        if (request.kind == TypeRequestKind::TSD)
        {
            const TSValueTypeMetaData *first = deref(inputs[0]);
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
        if (request.kind == TypeRequestKind::TSMapping)
        {
            return resolve_mapping_target(inputs);
        }
        return resolve_convert_target(request, inputs);
    }
}  // namespace hgraph::stdlib
