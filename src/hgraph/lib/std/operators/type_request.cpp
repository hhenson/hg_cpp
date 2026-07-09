#include <hgraph/lib/std/operators/type_request.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/endpoint_schema.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <stdexcept>
#include <type_traits>
#include <unordered_map>
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

        [[nodiscard]] bool ts_equivalent(const TSValueTypeMetaData *lhs, const TSValueTypeMetaData *rhs)
        {
            return time_series_schema_equivalent(deref(lhs), deref(rhs));
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

        struct RequestBindings
        {
            std::unordered_map<std::string, const ValueTypeMetaData *> scalar;
            std::unordered_map<std::string, const TSValueTypeMetaData *> ts;
            std::unordered_map<std::string, std::size_t> size;
        };

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

        [[nodiscard]] const ValueTypeMetaData *resolve_scalar_against(
            const ScalarRequest &request,
            const ValueTypeMetaData *candidate,
            RequestBindings &bindings);

        [[nodiscard]] const TSValueTypeMetaData *resolve_type_against(
            const TypeRequest &request,
            const TSValueTypeMetaData *candidate,
            RequestBindings &bindings);

        [[nodiscard]] std::size_t resolve_size_against(
            const SizeRequest &request,
            std::size_t candidate,
            RequestBindings &bindings)
        {
            return std::visit(overloaded{
                                  [&](const SizeVariableRequest &node) {
                                      auto [it, inserted] = bindings.size.emplace(node.name, candidate);
                                      if (!inserted && it->second != candidate)
                                      {
                                          throw std::invalid_argument(
                                              fmt::format("size variable {} resolved to both {} and {}",
                                                          node.name, it->second, candidate));
                                      }
                                      return candidate;
                                  },
                                  [&](const SizeConcreteRequest &node) {
                                      if (node.value != candidate)
                                      {
                                          throw std::invalid_argument(
                                              fmt::format("expected size {}, got {}", node.value, candidate));
                                      }
                                      return candidate;
                                  }},
                              request.node);
        }

        [[nodiscard]] const ValueTypeMetaData *resolve_scalar_against(
            const ScalarRequest &request,
            const ValueTypeMetaData *candidate,
            RequestBindings &bindings)
        {
            if (candidate == nullptr)
            {
                throw std::invalid_argument(
                    fmt::format("cannot resolve scalar request {} against <null>", scalar_request_to_string(request)));
            }
            return std::visit(overloaded{
                                  [&](const ScalarVariableRequest &node) -> const ValueTypeMetaData * {
                                      auto [it, inserted] = bindings.scalar.emplace(node.name, candidate);
                                      if (!inserted && it->second != candidate)
                                      {
                                          throw std::invalid_argument(
                                              fmt::format("scalar variable {} resolved to both {} and {}",
                                                          node.name, value_name(it->second), value_name(candidate)));
                                      }
                                      return candidate;
                                  },
                                  [&](const ScalarConcreteRequest &node) -> const ValueTypeMetaData * {
                                      if (node.schema != candidate)
                                      {
                                          throw std::invalid_argument(
                                              fmt::format("expected scalar {}, got {}",
                                                          value_name(node.schema), value_name(candidate)));
                                      }
                                      return candidate;
                                  },
                                  [&](const ScalarUnknownTupleRequest &node) -> const ValueTypeMetaData * {
                                      if (candidate->kind == ValueTypeKind::List)
                                      {
                                          if (node.element)
                                          {
                                              static_cast<void>(
                                                  resolve_scalar_against(*node.element, candidate->element_type, bindings));
                                          }
                                          return candidate;
                                      }
                                      if (candidate->kind == ValueTypeKind::Tuple)
                                      {
                                          if (node.element)
                                          {
                                              const ValueTypeMetaData *element = homogeneous_tuple_element(candidate);
                                              if (element == nullptr)
                                              {
                                                  throw std::invalid_argument(
                                                      "cannot bind an element variable from a heterogeneous tuple");
                                              }
                                              static_cast<void>(resolve_scalar_against(*node.element, element, bindings));
                                          }
                                          return candidate;
                                      }
                                      throw std::invalid_argument(
                                          fmt::format("expected tuple scalar, got {}", value_name(candidate)));
                                  },
                                  [&](const ScalarHomogeneousTupleRequest &node) -> const ValueTypeMetaData * {
                                      const ValueTypeMetaData *element = nullptr;
                                      if (candidate->kind == ValueTypeKind::List) { element = candidate->element_type; }
                                      else if (candidate->kind == ValueTypeKind::Tuple) { element = homogeneous_tuple_element(candidate); }
                                      if (element == nullptr)
                                      {
                                          throw std::invalid_argument(
                                              fmt::format("expected homogeneous tuple scalar, got {}", value_name(candidate)));
                                      }
                                      static_cast<void>(resolve_scalar_against(*node.element, element, bindings));
                                      return candidate;
                                  },
                                  [&](const ScalarFixedTupleRequest &node) -> const ValueTypeMetaData * {
                                      if (candidate->kind != ValueTypeKind::Tuple ||
                                          candidate->field_count != node.elements.size())
                                      {
                                          throw std::invalid_argument(
                                              fmt::format("expected fixed tuple with {} fields, got {}",
                                                          node.elements.size(), value_name(candidate)));
                                      }
                                      for (std::size_t index = 0; index < node.elements.size(); ++index)
                                      {
                                          static_cast<void>(resolve_scalar_against(
                                              *node.elements[index], candidate->fields[index].type, bindings));
                                      }
                                      return candidate;
                                  },
                                  [&](const ScalarSetRequest &node) -> const ValueTypeMetaData * {
                                      if (candidate->kind != ValueTypeKind::Set)
                                      {
                                          throw std::invalid_argument(
                                              fmt::format("expected set scalar, got {}", value_name(candidate)));
                                      }
                                      static_cast<void>(resolve_scalar_against(*node.element, candidate->element_type, bindings));
                                      return candidate;
                                  },
                                  [&](const ScalarMapRequest &node) -> const ValueTypeMetaData * {
                                      if (candidate->kind != ValueTypeKind::Map)
                                      {
                                          throw std::invalid_argument(
                                              fmt::format("expected map scalar, got {}", value_name(candidate)));
                                      }
                                      static_cast<void>(resolve_scalar_against(*node.key, candidate->key_type, bindings));
                                      static_cast<void>(resolve_scalar_against(*node.value, candidate->element_type, bindings));
                                      return candidate;
                                  },
                                  [&](const ScalarBundleRequest &node) -> const ValueTypeMetaData * {
                                      if (candidate->kind != ValueTypeKind::Bundle)
                                      {
                                          throw std::invalid_argument(
                                              fmt::format("expected bundle scalar, got {}", value_name(candidate)));
                                      }
                                      if (node.schema_variable)
                                      {
                                          auto [it, inserted] = bindings.scalar.emplace(*node.schema_variable, candidate);
                                          if (!inserted && it->second != candidate)
                                          {
                                              throw std::invalid_argument(
                                                  fmt::format("bundle schema variable {} resolved to both {} and {}",
                                                              *node.schema_variable,
                                                              value_name(it->second),
                                                              value_name(candidate)));
                                          }
                                      }
                                      return candidate;
                                  }},
                              request.node);
        }

        [[nodiscard]] const TSValueTypeMetaData *resolve_type_against(
            const TypeRequest &request,
            const TSValueTypeMetaData *candidate,
            RequestBindings &bindings)
        {
            candidate = deref(candidate);
            if (candidate == nullptr)
            {
                throw std::invalid_argument(
                    fmt::format("cannot resolve type request {} against <null>", type_request_to_string(request)));
            }
            auto &registry = TypeRegistry::instance();
            return std::visit(overloaded{
                                  [&](const TypeVariableRequest &node) -> const TSValueTypeMetaData * {
                                      auto [it, inserted] = bindings.ts.emplace(node.name, candidate);
                                      if (!inserted && !ts_equivalent(it->second, candidate))
                                      {
                                          throw std::invalid_argument(
                                              fmt::format("time-series variable {} resolved to both {} and {}",
                                                          node.name, schema_name(it->second), schema_name(candidate)));
                                      }
                                      return candidate;
                                  },
                                  [&](const TypeConcreteRequest &node) -> const TSValueTypeMetaData * {
                                      if (!ts_equivalent(node.schema, candidate))
                                      {
                                          throw std::invalid_argument(
                                              fmt::format("expected time-series {}, got {}",
                                                          schema_name(node.schema), schema_name(candidate)));
                                      }
                                      return candidate;
                                  },
                                  [&](const TypeTsRequest &node) -> const TSValueTypeMetaData * {
                                      if (candidate->kind != TSTypeKind::TS)
                                      {
                                          throw std::invalid_argument(
                                              fmt::format("expected TS scalar, got {}", schema_name(candidate)));
                                      }
                                      const ValueTypeMetaData *value =
                                          resolve_scalar_against(node.value, candidate->value_schema, bindings);
                                      return registry.ts(value);
                                  },
                                  [&](const TypeTssRequest &node) -> const TSValueTypeMetaData * {
                                      if (candidate->kind != TSTypeKind::TSS)
                                      {
                                          throw std::invalid_argument(
                                              fmt::format("expected TSS, got {}", schema_name(candidate)));
                                      }
                                      const ValueTypeMetaData *element =
                                          resolve_scalar_against(node.element, tss_element(candidate), bindings);
                                      return registry.tss(element);
                                  },
                                  [&](const TypeTsdRequest &node) -> const TSValueTypeMetaData * {
                                      if (candidate->kind != TSTypeKind::TSD)
                                      {
                                          throw std::invalid_argument(
                                              fmt::format("expected TSD, got {}", schema_name(candidate)));
                                      }
                                      const ValueTypeMetaData *key =
                                          resolve_scalar_against(node.key, candidate->key_type(), bindings);
                                      const TSValueTypeMetaData *value =
                                          resolve_type_against(*node.value, candidate->element_ts(), bindings);
                                      return registry.tsd(key, value);
                                  },
                                  [&](const TypeTslRequest &node) -> const TSValueTypeMetaData * {
                                      if (candidate->kind != TSTypeKind::TSL)
                                      {
                                          throw std::invalid_argument(
                                              fmt::format("expected TSL, got {}", schema_name(candidate)));
                                      }
                                      const TSValueTypeMetaData *element =
                                          resolve_type_against(*node.element, candidate->element_ts(), bindings);
                                      const std::size_t size = resolve_size_against(node.size, candidate->fixed_size(), bindings);
                                      return registry.tsl(element, size);
                                  },
                                  [&](const TypeTsbRequest &node) -> const TSValueTypeMetaData * {
                                      if (candidate->kind != TSTypeKind::TSB)
                                      {
                                          throw std::invalid_argument(
                                              fmt::format("expected TSB, got {}", schema_name(candidate)));
                                      }
                                      if (node.schema_variable)
                                      {
                                          auto [it, inserted] = bindings.ts.emplace(*node.schema_variable, candidate);
                                          if (!inserted && !ts_equivalent(it->second, candidate))
                                          {
                                              throw std::invalid_argument(
                                                  fmt::format("TSB schema variable {} resolved to both {} and {}",
                                                              *node.schema_variable,
                                                              schema_name(it->second),
                                                              schema_name(candidate)));
                                          }
                                      }
                                      return candidate;
                                  },
                                  [&](const TypeRefRequest &node) -> const TSValueTypeMetaData * {
                                      if (candidate->kind != TSTypeKind::REF)
                                      {
                                          throw std::invalid_argument(
                                              fmt::format("expected REF, got {}", schema_name(candidate)));
                                      }
                                      const TSValueTypeMetaData *target =
                                          resolve_type_against(*node.target, candidate->referenced_ts(), bindings);
                                      return registry.ref(target);
                                  }},
                              request.node);
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
        RequestBindings bindings;
        const TSValueTypeMetaData *candidate = infer_convert_candidate(request, inputs, selected_keys);
        return resolve_type_against(request, candidate, bindings);
    }

    const TSValueTypeMetaData *resolve_collect_target(const TypeRequest &request,
                                                      std::span<const TSValueTypeMetaData *const> inputs)
    {
        if (inputs.empty()) { throw std::invalid_argument("collect target resolution requires at least one input"); }
        RequestBindings bindings;
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
        return resolve_type_against(request, candidate, bindings);
    }
}  // namespace hgraph::stdlib
