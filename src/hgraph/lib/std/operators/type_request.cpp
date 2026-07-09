#include <hgraph/lib/std/operators/type_request.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/type_pattern.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <stdexcept>
#include <utility>
#include <vector>

namespace hgraph::stdlib
{
    namespace
    {
        template <typename... Ts>
        struct overloaded : Ts...
        {
            using Ts::operator()...;
        };

        template <typename... Ts>
        overloaded(Ts...) -> overloaded<Ts...>;

        [[nodiscard]] ScalarRequestPtr ptr(ScalarRequest request)
        {
            return std::make_shared<ScalarRequest>(std::move(request));
        }

        [[nodiscard]] TypeRequestPtr ptr(TypeRequest request)
        {
            return std::make_shared<TypeRequest>(std::move(request));
        }

        [[nodiscard]] std::string schema_name(const TSValueTypeMetaData *schema)
        {
            return schema != nullptr && schema->display_name != nullptr ? std::string{schema->display_name}
                                                                        : std::string{"<null>"};
        }

        [[nodiscard]] std::string value_name(const ValueTypeMetaData *schema)
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
            return collection_element_or_self(plain_ts_value(schema));
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
            std::string_view request)
        {
            if (inputs.empty())
            {
                throw std::invalid_argument(fmt::format("{} target resolution requires at least one input", request));
            }
            return deref(inputs[0]);
        }

        [[nodiscard]] ScalarPattern request_to_scalar_pattern(const ScalarRequest &request);
        [[nodiscard]] TypePattern request_to_type_pattern(const TypeRequest &request);

        [[nodiscard]] ScalarPattern request_to_scalar_pattern(const ScalarRequest &request)
        {
            return std::visit(overloaded{
                                  [](const ScalarVariableRequest &node) -> ScalarPattern {
                                      return ScalarPattern::var(node.name);
                                  },
                                  [](const ScalarConcreteRequest &node) -> ScalarPattern {
                                      return ScalarPattern::concrete(node.schema);
                                  },
                                  [](const ScalarUnknownTupleRequest &node) -> ScalarPattern {
                                      return node.element ? ScalarPattern::unknown_tuple(request_to_scalar_pattern(*node.element))
                                                          : ScalarPattern::unknown_tuple();
                                  },
                                  [](const ScalarHomogeneousTupleRequest &node) -> ScalarPattern {
                                      return ScalarPattern::homogeneous_tuple(request_to_scalar_pattern(*node.element));
                                  },
                                  [](const ScalarFixedTupleRequest &node) -> ScalarPattern {
                                      std::vector<ScalarPattern> elements;
                                      elements.reserve(node.elements.size());
                                      for (const ScalarRequestPtr &element : node.elements)
                                      {
                                          elements.push_back(request_to_scalar_pattern(*element));
                                      }
                                      return ScalarPattern::fixed_tuple(std::move(elements));
                                  },
                                  [](const ScalarSetRequest &node) -> ScalarPattern {
                                      return ScalarPattern::set(request_to_scalar_pattern(*node.element));
                                  },
                                  [](const ScalarMapRequest &node) -> ScalarPattern {
                                      return ScalarPattern::map(request_to_scalar_pattern(*node.key),
                                                                request_to_scalar_pattern(*node.value));
                                  },
                                  [](const ScalarBundleRequest &node) -> ScalarPattern {
                                      return node.schema_variable ? ScalarPattern::bundle_var(*node.schema_variable)
                                                                  : ScalarPattern::bundle();
                                  }},
                              request.node);
        }

        [[nodiscard]] TypePattern request_to_tsl_pattern(const TypeTslRequest &node)
        {
            return std::visit(overloaded{
                                  [&](const SizeVariableRequest &size) -> TypePattern {
                                      return TypePattern::tsl_var(request_to_type_pattern(*node.element), size.name);
                                  },
                                  [&](const SizeConcreteRequest &size) -> TypePattern {
                                      return TypePattern::tsl(request_to_type_pattern(*node.element), size.value);
                                  }},
                              node.size.node);
        }

        [[nodiscard]] TypePattern request_to_type_pattern(const TypeRequest &request)
        {
            return std::visit(overloaded{
                                  [](const TypeVariableRequest &node) -> TypePattern {
                                      return TypePattern::var(node.name);
                                  },
                                  [](const TypeConcreteRequest &node) -> TypePattern {
                                      return TypePattern::concrete(node.schema);
                                  },
                                  [](const TypeTsRequest &node) -> TypePattern {
                                      return TypePattern::ts(request_to_scalar_pattern(node.value));
                                  },
                                  [](const TypeTssRequest &node) -> TypePattern {
                                      return TypePattern::tss(request_to_scalar_pattern(node.element));
                                  },
                                  [](const TypeTsdRequest &node) -> TypePattern {
                                      return TypePattern::tsd(request_to_scalar_pattern(node.key),
                                                              request_to_type_pattern(*node.value));
                                  },
                                  [](const TypeTslRequest &node) -> TypePattern {
                                      return request_to_tsl_pattern(node);
                                  },
                                  [](const TypeTsbRequest &node) -> TypePattern {
                                      return TypePattern::tsb_var(node.schema_variable.value_or("SCHEMA"));
                                  },
                                  [](const TypeRefRequest &node) -> TypePattern {
                                      return TypePattern::ref(request_to_type_pattern(*node.target));
                                  }},
                              request.node);
        }

        [[nodiscard]] const TSValueTypeMetaData *match_request_target(
            const TypeRequest &request,
            const TSValueTypeMetaData *candidate)
        {
            if (candidate == nullptr)
            {
                throw std::invalid_argument(
                    fmt::format("cannot resolve type request {} against <null>", type_request_to_string(request)));
            }
            const TypePattern pattern = request_to_type_pattern(request);
            ResolutionMap     resolution;
            if (!ts_pattern_match(pattern, candidate, resolution))
            {
                throw std::invalid_argument(
                    fmt::format("resolved target {} does not satisfy requested {}",
                                schema_name(candidate),
                                type_request_to_string(request)));
            }
            return candidate;
        }

        [[nodiscard]] const ValueTypeMetaData *infer_tuple_value_candidate(
            const ScalarRequest &,
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
            const ValueTypeMetaData *value = plain_ts_value(deref(inputs[1]));
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
            const ValueTypeMetaData *value = plain_ts_value(input);
            if (const TSValueTypeMetaData *bundle = tsb_for_bundle_value(value)) { return bundle; }
            throw std::invalid_argument(fmt::format("cannot infer TSB target from {}", schema_name(input)));
        }

        [[nodiscard]] const TSValueTypeMetaData *infer_ts_candidate(
            const ScalarRequest &scalar_request,
            std::span<const TSValueTypeMetaData *const> inputs)
        {
            auto &registry = TypeRegistry::instance();
            return std::visit(overloaded{
                                  [&](const ScalarVariableRequest &) -> const TSValueTypeMetaData * {
                                      const TSValueTypeMetaData *input = require_one_input(inputs, "TS[T]");
                                      if (input->kind != TSTypeKind::TS)
                                      {
                                          throw std::invalid_argument(
                                              fmt::format("cannot infer TS[T] target from {}", schema_name(input)));
                                      }
                                      return input;
                                  },
                                  [&](const ScalarConcreteRequest &node) -> const TSValueTypeMetaData * {
                                      return registry.ts(node.schema);
                                  },
                                  [&](const ScalarUnknownTupleRequest &) -> const TSValueTypeMetaData * {
                                      return registry.ts(infer_tuple_value_candidate(scalar_request, inputs));
                                  },
                                  [&](const ScalarHomogeneousTupleRequest &) -> const TSValueTypeMetaData * {
                                      return registry.ts(infer_tuple_value_candidate(scalar_request, inputs));
                                  },
                                  [&](const ScalarFixedTupleRequest &) -> const TSValueTypeMetaData * {
                                      return registry.ts(infer_tuple_value_candidate(scalar_request, inputs));
                                  },
                                  [&](const ScalarSetRequest &) -> const TSValueTypeMetaData * {
                                      return registry.ts(infer_set_value_candidate(inputs));
                                  },
                                  [&](const ScalarMapRequest &) -> const TSValueTypeMetaData * {
                                      return registry.ts(infer_mapping_value_candidate(inputs));
                                  },
                                  [&](const ScalarBundleRequest &) -> const TSValueTypeMetaData * {
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
                                  }},
                              scalar_request.node);
        }

        [[nodiscard]] const TSValueTypeMetaData *infer_convert_candidate(
            const TypeRequest &request,
            std::span<const TSValueTypeMetaData *const> inputs,
            std::span<const std::string> selected_keys)
        {
            auto &registry = TypeRegistry::instance();
            return std::visit(overloaded{
                                  [&](const TypeVariableRequest &) -> const TSValueTypeMetaData * {
                                      return require_one_input(inputs, type_request_to_string(request));
                                  },
                                  [&](const TypeConcreteRequest &node) -> const TSValueTypeMetaData * {
                                      return node.schema;
                                  },
                                  [&](const TypeTsRequest &node) -> const TSValueTypeMetaData * {
                                      return infer_ts_candidate(node.value, inputs);
                                  },
                                  [&](const TypeTssRequest &) -> const TSValueTypeMetaData * {
                                      const TSValueTypeMetaData *input =
                                          require_one_input(inputs, type_request_to_string(request));
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
                                          throw std::invalid_argument(
                                              fmt::format("cannot infer TSS target from {}", schema_name(input)));
                                      }
                                      return registry.tss(element);
                                  },
                                  [&](const TypeTsdRequest &) -> const TSValueTypeMetaData * {
                                      return infer_tsd_candidate(inputs, selected_keys);
                                  },
                                  [&](const TypeTslRequest &) -> const TSValueTypeMetaData * {
                                      return infer_tsl_candidate(inputs);
                                  },
                                  [&](const TypeTsbRequest &) -> const TSValueTypeMetaData * {
                                      return infer_tsb_candidate(inputs);
                                  },
                                  [&](const TypeRefRequest &) -> const TSValueTypeMetaData * {
                                      const TSValueTypeMetaData *input =
                                          require_one_input(inputs, type_request_to_string(request));
                                      if (input->kind != TSTypeKind::REF)
                                      {
                                          throw std::invalid_argument(
                                              fmt::format("cannot infer REF target from {}", schema_name(input)));
                                      }
                                      return input;
                                  }},
                              request.node);
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

        [[nodiscard]] bool is_tsd_request(const TypeRequest &request)
        {
            return std::holds_alternative<TypeTsdRequest>(request.node);
        }

        [[nodiscard]] bool is_ts_mapping_request(const TypeRequest &request)
        {
            const auto *ts = std::get_if<TypeTsRequest>(&request.node);
            return ts != nullptr && std::holds_alternative<ScalarMapRequest>(ts->value.node);
        }
    }  // namespace

    ScalarRequest ScalarRequest::variable(std::string name)
    {
        return ScalarRequest{ScalarVariableRequest{std::move(name)}};
    }

    ScalarRequest ScalarRequest::concrete(const ValueTypeMetaData *schema)
    {
        return ScalarRequest{ScalarConcreteRequest{schema}};
    }

    ScalarRequest ScalarRequest::unknown_tuple(ScalarRequestPtr element)
    {
        return ScalarRequest{ScalarUnknownTupleRequest{std::move(element)}};
    }

    ScalarRequest ScalarRequest::homogeneous_tuple(ScalarRequest element)
    {
        return ScalarRequest{ScalarHomogeneousTupleRequest{ptr(std::move(element))}};
    }

    ScalarRequest ScalarRequest::fixed_tuple(std::vector<ScalarRequest> elements)
    {
        std::vector<ScalarRequestPtr> stored;
        stored.reserve(elements.size());
        for (ScalarRequest &element : elements) { stored.push_back(ptr(std::move(element))); }
        return ScalarRequest{ScalarFixedTupleRequest{std::move(stored)}};
    }

    ScalarRequest ScalarRequest::set(ScalarRequest element)
    {
        return ScalarRequest{ScalarSetRequest{ptr(std::move(element))}};
    }

    ScalarRequest ScalarRequest::map(ScalarRequest key, ScalarRequest value)
    {
        return ScalarRequest{ScalarMapRequest{ptr(std::move(key)), ptr(std::move(value))}};
    }

    ScalarRequest ScalarRequest::bundle(std::optional<std::string> schema_variable)
    {
        return ScalarRequest{ScalarBundleRequest{std::move(schema_variable)}};
    }

    SizeRequest SizeRequest::variable(std::string name)
    {
        return SizeRequest{SizeVariableRequest{std::move(name)}};
    }

    SizeRequest SizeRequest::concrete(std::size_t value)
    {
        return SizeRequest{SizeConcreteRequest{value}};
    }

    TypeRequest TypeRequest::variable(std::string name)
    {
        return TypeRequest{TypeVariableRequest{std::move(name)}};
    }

    TypeRequest TypeRequest::concrete(const TSValueTypeMetaData *schema)
    {
        return TypeRequest{TypeConcreteRequest{schema}};
    }

    TypeRequest TypeRequest::ts(ScalarRequest value)
    {
        return TypeRequest{TypeTsRequest{std::move(value)}};
    }

    TypeRequest TypeRequest::tss(ScalarRequest element)
    {
        return TypeRequest{TypeTssRequest{std::move(element)}};
    }

    TypeRequest TypeRequest::tsd(ScalarRequest key, TypeRequest value)
    {
        return TypeRequest{TypeTsdRequest{std::move(key), ptr(std::move(value))}};
    }

    TypeRequest TypeRequest::tsl(TypeRequest element, SizeRequest size)
    {
        return TypeRequest{TypeTslRequest{ptr(std::move(element)), std::move(size)}};
    }

    TypeRequest TypeRequest::tsb(std::optional<std::string> schema_variable)
    {
        return TypeRequest{TypeTsbRequest{std::move(schema_variable)}};
    }

    TypeRequest TypeRequest::ref(TypeRequest target)
    {
        return TypeRequest{TypeRefRequest{ptr(std::move(target))}};
    }

    std::string scalar_request_to_string(const ScalarRequest &request)
    {
        return std::visit(overloaded{
                              [](const ScalarVariableRequest &node) { return node.name; },
                              [](const ScalarConcreteRequest &node) { return value_name(node.schema); },
                              [](const ScalarUnknownTupleRequest &node) {
                                  return node.element ? fmt::format("UnknownTuple[{}]", scalar_request_to_string(*node.element))
                                                      : std::string{"UnknownTuple"};
                              },
                              [](const ScalarHomogeneousTupleRequest &node) {
                                  return fmt::format("tuple[{}, ...]", scalar_request_to_string(*node.element));
                              },
                              [](const ScalarFixedTupleRequest &node) {
                                  std::vector<std::string> parts;
                                  parts.reserve(node.elements.size());
                                  for (const ScalarRequestPtr &element : node.elements)
                                  {
                                      parts.push_back(scalar_request_to_string(*element));
                                  }
                                  return fmt::format("tuple[{}]", fmt::join(parts, ", "));
                              },
                              [](const ScalarSetRequest &node) {
                                  return fmt::format("set[{}]", scalar_request_to_string(*node.element));
                              },
                              [](const ScalarMapRequest &node) {
                                  return fmt::format("Mapping[{}, {}]",
                                                     scalar_request_to_string(*node.key),
                                                     scalar_request_to_string(*node.value));
                              },
                              [](const ScalarBundleRequest &node) {
                                  return node.schema_variable ? fmt::format("CompoundScalar[{}]", *node.schema_variable)
                                                              : std::string{"CompoundScalar"};
                              }},
                          request.node);
    }

    std::string size_request_to_string(const SizeRequest &request)
    {
        return std::visit(overloaded{
                              [](const SizeVariableRequest &node) { return node.name; },
                              [](const SizeConcreteRequest &node) { return fmt::format("{}", node.value); }},
                          request.node);
    }

    std::string type_request_to_string(const TypeRequest &request)
    {
        return std::visit(overloaded{
                              [](const TypeVariableRequest &node) { return node.name; },
                              [](const TypeConcreteRequest &node) { return schema_name(node.schema); },
                              [](const TypeTsRequest &node) {
                                  return fmt::format("TS[{}]", scalar_request_to_string(node.value));
                              },
                              [](const TypeTssRequest &node) {
                                  return fmt::format("TSS[{}]", scalar_request_to_string(node.element));
                              },
                              [](const TypeTsdRequest &node) {
                                  return fmt::format("TSD[{}, {}]",
                                                     scalar_request_to_string(node.key),
                                                     type_request_to_string(*node.value));
                              },
                              [](const TypeTslRequest &node) {
                                  return fmt::format("TSL[{}, {}]",
                                                     type_request_to_string(*node.element),
                                                     size_request_to_string(node.size));
                              },
                              [](const TypeTsbRequest &node) {
                                  return node.schema_variable ? fmt::format("TSB[{}]", *node.schema_variable)
                                                              : std::string{"TSB[SCHEMA]"};
                              },
                              [](const TypeRefRequest &node) {
                                  return fmt::format("REF[{}]", type_request_to_string(*node.target));
                              }},
                          request.node);
    }

    const TSValueTypeMetaData *resolve_convert_target(const TypeRequest &request,
                                                      std::span<const TSValueTypeMetaData *const> inputs,
                                                      std::span<const std::string> selected_keys)
    {
        const TSValueTypeMetaData *candidate = infer_convert_candidate(request, inputs, selected_keys);
        return match_request_target(request, candidate);
    }

    const TSValueTypeMetaData *resolve_collect_target(const TypeRequest &request,
                                                      std::span<const TSValueTypeMetaData *const> inputs)
    {
        if (inputs.empty()) { throw std::invalid_argument("collect target resolution requires at least one input"); }
        const TSValueTypeMetaData *candidate = nullptr;
        if (is_tsd_request(request))
        {
            candidate = infer_collect_tsd_candidate(inputs);
        }
        else if (is_ts_mapping_request(request))
        {
            candidate = TypeRegistry::instance().ts(infer_mapping_value_candidate(inputs));
        }
        else
        {
            candidate = infer_convert_candidate(request, inputs, {});
        }
        return match_request_target(request, candidate);
    }
}  // namespace hgraph::stdlib
