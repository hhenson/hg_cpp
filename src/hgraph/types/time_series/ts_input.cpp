#include <hgraph/types/time_series/ts_input.h>

#include "ts_input/detail.h"

#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/container_ops.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <fmt/format.h>
#include <iterator>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] constexpr std::size_t ts_kind_index(TSTypeKind kind) noexcept
        {
            return static_cast<std::size_t>(kind);
        }

        [[nodiscard]] bool output_view_bound(const TSOutputView &output) noexcept
        {
            return output.output() != nullptr && output.data_view().valid();
        }

        void validate_endpoint_kind(const TSValueTypeMetaData *schema, TSTypeKind expected, const char *what)
        {
            if (schema == nullptr || schema->kind != expected)
            {
                throw std::invalid_argument(std::string{what} + " requires a matching time-series shape");
            }
        }

        void validate_input_endpoint_schema(const TSEndpointSchema &endpoint_schema, bool root)
        {
            const auto *schema = endpoint_schema.schema();
            if (schema == nullptr) { throw std::invalid_argument("TSInput endpoint annotation requires a schema"); }

            if (root)
            {
                if (schema->kind != TSTypeKind::TSB || !endpoint_schema.is_non_peered())
                {
                    throw std::invalid_argument("TSInput root endpoint annotation must be a non-peered TSB");
                }
            }

            if (endpoint_schema.role() == TSEndpointRole::Peered)
            {
                return;
            }

            if (schema->kind != TSTypeKind::TSB && schema->kind != TSTypeKind::TSL)
            {
                throw std::invalid_argument(
                    "TSInput non-peered prefixes require TSB or fixed-size TSL schemas");
            }
            if (schema->kind == TSTypeKind::TSL && schema->fixed_size() == 0)
            {
                throw std::invalid_argument("TSInput non-peered TSL prefixes currently require a fixed size");
            }
            for (const auto &child : endpoint_schema.children())
            {
                validate_input_endpoint_schema(child, false);
            }
        }

        void append_endpoint_key(std::string &key, const TSEndpointSchema &endpoint_schema)
        {
            const auto role = static_cast<std::uint8_t>(endpoint_schema.role());
            key.append(reinterpret_cast<const char *>(&role), sizeof(role));

            const auto schema_bits = reinterpret_cast<std::uintptr_t>(endpoint_schema.schema());
            key.append(reinterpret_cast<const char *>(&schema_bits), sizeof(schema_bits));

            if (endpoint_schema.is_non_peered())
            {
                const auto &children = endpoint_schema.children();
                const auto  size     = children.size();
                key.append(reinterpret_cast<const char *>(&size), sizeof(size));
                for (const auto &child : children) { append_endpoint_key(key, child); }
            }
        }

        [[nodiscard]] std::string plan_cache_key(const TSInputConstructionPlan &plan)
        {
            std::string key;
            key.reserve(128);
            append_endpoint_key(key, plan.endpoint_schema());
            return key;
        }

        [[nodiscard]] const TSDataView &empty_ts_data_view() noexcept
        {
            static const TSDataView empty{};
            return empty;
        }

        inline constexpr std::size_t input_npos = static_cast<std::size_t>(-1);

        [[nodiscard]] std::size_t no_endpoint_child_count(const TSValueTypeMetaData *) noexcept
        {
            return 0;
        }

        [[nodiscard]] std::string_view no_endpoint_key_at(const TSValueTypeMetaData *, std::size_t) noexcept
        {
            return {};
        }

        [[nodiscard]] std::size_t no_endpoint_find_key(const TSValueTypeMetaData *, std::string_view) noexcept
        {
            return input_npos;
        }

        [[nodiscard]] const TSValueTypeMetaData *no_endpoint_child_schema(const TSValueTypeMetaData *,
                                                                          std::size_t) noexcept
        {
            return nullptr;
        }

        [[nodiscard]] std::size_t tsb_endpoint_child_count(const TSValueTypeMetaData *schema) noexcept
        {
            return schema != nullptr ? schema->field_count() : 0;
        }

        [[nodiscard]] std::string_view tsb_endpoint_key_at(const TSValueTypeMetaData *schema,
                                                           std::size_t                index) noexcept
        {
            if (schema == nullptr || index >= schema->field_count()) { return {}; }
            const auto *name = schema->fields()[index].name;
            return name != nullptr ? std::string_view{name} : std::string_view{};
        }

        [[nodiscard]] std::size_t tsb_endpoint_find_key(const TSValueTypeMetaData *schema,
                                                        std::string_view          name) noexcept
        {
            if (schema == nullptr) { return input_npos; }
            for (std::size_t index = 0; index < schema->field_count(); ++index)
            {
                const auto *field_name = schema->fields()[index].name;
                if (field_name != nullptr && name == field_name) { return index; }
            }
            return input_npos;
        }

        [[nodiscard]] const TSValueTypeMetaData *tsb_endpoint_child_schema(const TSValueTypeMetaData *schema,
                                                                           std::size_t                index) noexcept
        {
            return schema != nullptr && index < schema->field_count() ? schema->fields()[index].type : nullptr;
        }

        [[nodiscard]] std::size_t tsl_endpoint_child_count(const TSValueTypeMetaData *schema) noexcept
        {
            return schema != nullptr ? schema->fixed_size() : 0;
        }

        [[nodiscard]] const TSValueTypeMetaData *tsl_endpoint_child_schema(const TSValueTypeMetaData *schema,
                                                                           std::size_t                index) noexcept
        {
            return schema != nullptr && index < schema->fixed_size() ? schema->element_ts() : nullptr;
        }

        [[nodiscard]] const TSValueTypeMetaData *tsd_endpoint_child_schema(const TSValueTypeMetaData *schema,
                                                                           std::size_t) noexcept
        {
            return schema != nullptr ? schema->element_ts() : nullptr;
        }

        void require_planned_children(const detail::TSInputNode &node, std::size_t actual_size, const char *what)
        {
            if (node.children.size() != actual_size)
            {
                throw std::logic_error(std::string{what} + " child count does not match its schema");
            }
        }

        detail::TSInputNode &checked_child_node(detail::TSInputNode &node, std::size_t index, const char *what)
        {
            auto *child = index < node.children.size() ? node.children[index].get() : nullptr;
            if (child == nullptr) { throw std::logic_error(std::string{what} + " contains an unplanned child"); }
            return *child;
        }

        void bind_tsb_endpoint_children(detail::TSInputNode &node, TSOutputView output)
        {
            auto data = output.data_view();
            auto bundle = data.as_bundle();
            require_planned_children(node, bundle.size(), "TSInput non-peered TSB");
            for (std::size_t index = 0; index < node.children.size(); ++index)
            {
                checked_child_node(node, index, "TSInput non-peered TSB")
                    .bind_output_tree(TSOutputView{output.output(), bundle.at(index), output.evaluation_time()});
            }
        }

        void bind_tsl_endpoint_children(detail::TSInputNode &node, TSOutputView output)
        {
            auto data = output.data_view();
            auto list = data.as_list();
            require_planned_children(node, list.size(), "TSInput non-peered TSL");
            for (std::size_t index = 0; index < node.children.size(); ++index)
            {
                checked_child_node(node, index, "TSInput non-peered TSL")
                    .bind_output_tree(TSOutputView{output.output(), list.at(index), output.evaluation_time()});
            }
        }

        [[nodiscard]] TSDataView tsb_target_child_at(TSDataView parent, std::size_t index)
        {
            auto bundle = parent.as_bundle();
            return bundle.at(index);
        }

        [[nodiscard]] TSDataView tsl_target_child_at(TSDataView parent, std::size_t index)
        {
            auto list = parent.as_list();
            return list.at(index);
        }

        [[nodiscard]] TSDataView tsd_target_child_at(TSDataView parent, std::size_t slot)
        {
            auto dict = parent.as_dict();
            return dict.at_slot(slot);
        }

        const detail::TSInputEndpointOps endpoint_ts_ops{
            .name = "TS",
            .child_count = &no_endpoint_child_count,
            .key_at = &no_endpoint_key_at,
            .find_key = &no_endpoint_find_key,
            .child_schema = &no_endpoint_child_schema,
        };

        const detail::TSInputEndpointOps endpoint_tss_ops{
            .name = "TSS",
            .child_count = &no_endpoint_child_count,
            .key_at = &no_endpoint_key_at,
            .find_key = &no_endpoint_find_key,
            .child_schema = &no_endpoint_child_schema,
        };

        const detail::TSInputEndpointOps endpoint_tsd_ops{
            .name = "TSD",
            .child_count = &no_endpoint_child_count,
            .key_at = &no_endpoint_key_at,
            .find_key = &no_endpoint_find_key,
            .child_schema = &tsd_endpoint_child_schema,
            .target_child = &tsd_target_child_at,
        };

        const detail::TSInputEndpointOps endpoint_tsl_ops{
            .name = "TSL",
            .supports_input_projection = true,
            .child_count = &tsl_endpoint_child_count,
            .key_at = &no_endpoint_key_at,
            .find_key = &no_endpoint_find_key,
            .child_schema = &tsl_endpoint_child_schema,
            .bind_children = &bind_tsl_endpoint_children,
            .target_child = &tsl_target_child_at,
        };

        const detail::TSInputEndpointOps endpoint_tsw_ops{
            .name = "TSW",
            .child_count = &no_endpoint_child_count,
            .key_at = &no_endpoint_key_at,
            .find_key = &no_endpoint_find_key,
            .child_schema = &no_endpoint_child_schema,
        };

        const detail::TSInputEndpointOps endpoint_tsb_ops{
            .name = "TSB",
            .supports_input_projection = true,
            .named_value_projection = true,
            .value_open = '{',
            .value_close = '}',
            .child_count = &tsb_endpoint_child_count,
            .key_at = &tsb_endpoint_key_at,
            .find_key = &tsb_endpoint_find_key,
            .child_schema = &tsb_endpoint_child_schema,
            .bind_children = &bind_tsb_endpoint_children,
            .target_child = &tsb_target_child_at,
        };

        const detail::TSInputEndpointOps endpoint_ref_ops{
            .name = "REF",
            .child_count = &no_endpoint_child_count,
            .key_at = &no_endpoint_key_at,
            .find_key = &no_endpoint_find_key,
            .child_schema = &no_endpoint_child_schema,
        };

        const detail::TSInputEndpointOps endpoint_signal_ops{
            .name = "SIGNAL",
            .child_count = &no_endpoint_child_count,
            .key_at = &no_endpoint_key_at,
            .find_key = &no_endpoint_find_key,
            .child_schema = &no_endpoint_child_schema,
        };

        [[nodiscard]] const detail::TSInputEndpointOps &input_endpoint_ops_for(const TSValueTypeMetaData *schema)
        {
            static constexpr std::size_t kind_count = ts_kind_index(TSTypeKind::SIGNAL) + 1U;
            static const std::array<const detail::TSInputEndpointOps *, kind_count> table{
                &endpoint_ts_ops,
                &endpoint_tss_ops,
                &endpoint_tsd_ops,
                &endpoint_tsl_ops,
                &endpoint_tsw_ops,
                &endpoint_tsb_ops,
                &endpoint_ref_ops,
                &endpoint_signal_ops,
            };

            if (schema == nullptr) { throw std::logic_error("TSInput endpoint ops require a schema"); }
            const auto index = ts_kind_index(schema->kind);
            if (index >= table.size() || table[index] == nullptr)
            {
                throw std::logic_error("TSInput endpoint ops are not registered for the schema kind");
            }
            return *table[index];
        }

        struct InputStructuredOpsEntry
        {
            const TSValueTypeMetaData *schema{nullptr};
            const detail::TSInputEndpointOps *endpoint_ops{nullptr};
            TSDataLayout              layout{};
            IndexedValueOps           value_ops{};
            IndexedTSDataOps          ts_data_ops{};
            const ValueTypeBinding   *value_binding{nullptr};
            const ValueTypeBinding   *delta_binding{nullptr};
            const TSDataBinding      *ts_data_binding{nullptr};
        };

        [[nodiscard]] const detail::TSInputNode *input_node(const void *memory) noexcept
        {
            return static_cast<const detail::TSInputNode *>(memory);
        }

        [[nodiscard]] detail::TSInputNode *mutable_input_node(void *memory) noexcept
        {
            return static_cast<detail::TSInputNode *>(memory);
        }

        [[nodiscard]] bool input_node_has_current_value(const detail::TSInputNode *node)
        {
            if (node == nullptr) { return false; }
            if (node->role == TSEndpointRole::Peered)
            {
                return output_view_bound(node->target) && node->target.valid();
            }
            for (const auto &child : node->children)
            {
                if (input_node_has_current_value(child.get())) { return true; }
            }
            return false;
        }

        [[nodiscard]] bool input_node_all_valid(const detail::TSInputNode *node)
        {
            if (node == nullptr) { return false; }
            if (node->role == TSEndpointRole::Peered)
            {
                return output_view_bound(node->target) && node->target.all_valid();
            }
            for (const auto &child : node->children)
            {
                if (!input_node_all_valid(child.get())) { return false; }
            }
            return true;
        }

        [[nodiscard]] std::size_t input_child_count(const detail::TSInputNode *node) noexcept
        {
            return node != nullptr ? node->children.size() : 0;
        }

        [[nodiscard]] const detail::TSInputNode *input_child_at(const detail::TSInputNode *node,
                                                                std::size_t                index)
        {
            if (node == nullptr || index >= node->children.size()) { return nullptr; }
            return node->children[index].get();
        }

        [[nodiscard]] const TSDataBinding *regular_ts_data_binding_for(const TSValueTypeMetaData *schema)
        {
            return TSDataPlanFactory::instance().binding_for(schema);
        }

        [[nodiscard]] const ValueTypeBinding *regular_value_binding_for(const TSValueTypeMetaData *schema)
        {
            return schema != nullptr ? ValuePlanFactory::instance().binding_for(schema->value_schema) : nullptr;
        }

        [[nodiscard]] const ValueTypeBinding *value_binding_for_data_binding(const TSDataBinding *binding)
        {
            if (binding == nullptr) { return nullptr; }
            const auto &ops = binding->checked_ops();
            const auto *layout = ops.layout_impl(ops.context);
            return layout != nullptr ? layout->value_binding : nullptr;
        }

        [[nodiscard]] const TSDataBinding *child_data_binding(const detail::TSInputNode *child)
        {
            if (child == nullptr) { return nullptr; }
            if (child->role == TSEndpointRole::NonPeered) { return child->data_binding; }
            if (output_view_bound(child->target)) { return child->target.binding(); }
            return regular_ts_data_binding_for(child->schema);
        }

        [[nodiscard]] const void *child_data_memory(const detail::TSInputNode *child)
        {
            if (child == nullptr) { return nullptr; }
            if (child->role == TSEndpointRole::NonPeered) { return child; }
            return output_view_bound(child->target) ? child->target.data_view().data() : nullptr;
        }

        [[nodiscard]] const ValueTypeBinding *child_value_binding(const detail::TSInputNode *child)
        {
            if (child == nullptr) { return nullptr; }
            if (child->role == TSEndpointRole::NonPeered)
            {
                return value_binding_for_data_binding(child->data_binding);
            }
            if (output_view_bound(child->target)) { return child->target.value().binding(); }
            return regular_value_binding_for(child->schema);
        }

        [[nodiscard]] const void *child_value_memory(const detail::TSInputNode *child)
        {
            if (child == nullptr) { return nullptr; }
            if (child->role == TSEndpointRole::NonPeered) { return child; }
            return output_view_bound(child->target) ? child->target.value().data() : nullptr;
        }

        [[nodiscard]] const TSDataLayout *input_ts_data_layout(const void *context)
        {
            return &static_cast<const InputStructuredOpsEntry *>(context)->layout;
        }

        [[nodiscard]] const TSDataTracking *input_ts_data_tracking(const void *, const void *memory)
        {
            const auto *node = input_node(memory);
            if (node == nullptr) { throw std::logic_error("TSInput virtual TSData requires live node memory"); }
            return &node->tracking;
        }

        [[nodiscard]] TSDataTracking *input_ts_data_mutable_tracking(const void *, void *memory)
        {
            auto *node = mutable_input_node(memory);
            if (node == nullptr) { throw std::logic_error("TSInput virtual TSData requires live node memory"); }
            return &node->tracking;
        }

        [[nodiscard]] bool input_ts_data_has_current_value(const void *, const void *memory)
        {
            return input_node_has_current_value(input_node(memory));
        }

        [[nodiscard]] bool input_ts_data_all_valid(const void *, const void *memory)
        {
            return input_node_all_valid(input_node(memory));
        }

        [[nodiscard]] const void *input_ts_data_value_memory(const void *, const void *memory)
        {
            return memory;
        }

        [[nodiscard]] const void *input_ts_data_delta_memory(const void *, const void *)
        {
            return nullptr;
        }

        void input_ts_data_cleanup_delta(const void *, void *, engine_time_t)
        {
        }

        void input_ts_data_record_child_modified(const void *, void *memory, std::size_t, engine_time_t modified_time)
        {
            if (auto *node = mutable_input_node(memory); node != nullptr) { node->record_modified(modified_time); }
        }

        [[nodiscard]] std::size_t input_ts_data_indexed_size(const void *, const void *memory)
        {
            return input_child_count(input_node(memory));
        }

        [[nodiscard]] const TSDataBinding *input_ts_data_element_binding(const void *,
                                                                         const void *memory,
                                                                         std::size_t index)
        {
            return child_data_binding(input_child_at(input_node(memory), index));
        }

        [[nodiscard]] const void *input_ts_data_element_memory(const void *, const void *memory, std::size_t index)
        {
            return child_data_memory(input_child_at(input_node(memory), index));
        }

        [[nodiscard]] std::size_t input_value_size(const void *, const void *memory) noexcept
        {
            return input_child_count(input_node(memory));
        }

        [[nodiscard]] const void *input_value_element_at(const void *, const void *memory, std::size_t index)
        {
            return child_value_memory(input_child_at(input_node(memory), index));
        }

        [[nodiscard]] const ValueTypeBinding *input_value_element_binding(const void *,
                                                                          const void *memory,
                                                                          std::size_t index) noexcept
        {
            return fallback_on_exception<const ValueTypeBinding *>(nullptr, [&] {
                return child_value_binding(input_child_at(input_node(memory), index));
            });
        }

        [[nodiscard]] ValueView input_value_project_value(const void *context, const void *memory, std::size_t index)
        {
            return ValueView{input_value_element_binding(context, memory, index),
                             input_value_element_at(context, memory, index)};
        }

        [[nodiscard]] Range<ValueView> input_value_make_range(const void *context, const void *memory)
        {
            return Range<ValueView>{.context = context, .memory = memory, .limit = input_value_size(context, memory),
                                    .predicate = nullptr, .projector = &input_value_project_value};
        }

        [[nodiscard]] std::size_t input_value_hash(const void *context, const void *memory)
        {
            std::size_t seed = 0;
            const auto  size = input_value_size(context, memory);
            for (std::size_t index = 0; index < size; ++index)
            {
                const auto *binding = input_value_element_binding(context, memory, index);
                const auto *child   = input_value_element_at(context, memory, index);
                const auto  value   = child != nullptr && binding != nullptr ? binding->checked_ops().hash(child) : 0;
                seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
            }
            return seed;
        }

        [[nodiscard]] bool input_value_equals(const void *context, const void *lhs, const void *rhs) noexcept
        {
            if (lhs == nullptr || rhs == nullptr) { return lhs == rhs; }
            return fallback_on_exception(false, [&] {
                const auto size = input_value_size(context, lhs);
                if (input_value_size(context, rhs) != size) { return false; }
                for (std::size_t index = 0; index < size; ++index)
                {
                    const auto *binding = input_value_element_binding(context, lhs, index);
                    const auto *a       = input_value_element_at(context, lhs, index);
                    const auto *b       = input_value_element_at(context, rhs, index);
                    if (a == nullptr || b == nullptr)
                    {
                        if (a != b) { return false; }
                        continue;
                    }
                    if (binding == nullptr || !binding->checked_ops().equals(a, b)) { return false; }
                }
                return true;
            });
        }

        [[nodiscard]] std::partial_ordering input_value_compare(const void *context,
                                                                const void *lhs,
                                                                const void *rhs) noexcept
        {
            if (const auto order = value_ops_detail::null_order(lhs, rhs)) { return *order; }
            return fallback_on_exception(std::partial_ordering::unordered, [&] {
                const auto size = std::min(input_value_size(context, lhs), input_value_size(context, rhs));
                for (std::size_t index = 0; index < size; ++index)
                {
                    const auto *binding = input_value_element_binding(context, lhs, index);
                    const auto *a       = input_value_element_at(context, lhs, index);
                    const auto *b       = input_value_element_at(context, rhs, index);
                    if (binding == nullptr)
                    {
                        if (a != b) { return std::partial_ordering::unordered; }
                        continue;
                    }
                    const auto order = binding->checked_ops().compare(a, b);
                    if (order != 0) { return order; }
                }
                const auto lhs_size = input_value_size(context, lhs);
                const auto rhs_size = input_value_size(context, rhs);
                if (lhs_size < rhs_size) { return std::partial_ordering::less; }
                if (lhs_size > rhs_size) { return std::partial_ordering::greater; }
                return std::partial_ordering::equivalent;
            });
        }

        [[nodiscard]] std::string input_value_to_string(const void *context, const void *memory)
        {
            if (memory == nullptr) { return {}; }
            const auto *entry = static_cast<const InputStructuredOpsEntry *>(context);
            const auto *node  = input_node(memory);
            const auto *endpoint_ops = entry != nullptr ? entry->endpoint_ops : nullptr;
            const bool  named = endpoint_ops != nullptr && endpoint_ops->named_value_projection;
            const char  open = endpoint_ops != nullptr ? endpoint_ops->value_open : '[';
            const char  close = endpoint_ops != nullptr ? endpoint_ops->value_close : ']';
            fmt::memory_buffer out;
            fmt::format_to(std::back_inserter(out), "{}", open);
            const auto size = input_value_size(context, memory);
            for (std::size_t index = 0; index < size; ++index)
            {
                if (index > 0) { fmt::format_to(std::back_inserter(out), ", "); }
                if (named)
                {
                    const auto *schema = node != nullptr ? node->schema : nullptr;
                    const auto  key = endpoint_ops != nullptr && endpoint_ops->key_at != nullptr
                                          ? endpoint_ops->key_at(schema, index)
                                          : std::string_view{};
                    fmt::format_to(std::back_inserter(out), "{}: ", key);
                }
                const auto *binding = input_value_element_binding(context, memory, index);
                const auto *child   = input_value_element_at(context, memory, index);
                if (binding != nullptr && child != nullptr)
                {
                    fmt::format_to(std::back_inserter(out), "{}", binding->checked_ops().to_string(child));
                }
            }
            fmt::format_to(std::back_inserter(out), "{}", close);
            return fmt::to_string(out);
        }

        [[nodiscard]] const TSDataBinding *input_data_binding_for(const TSValueTypeMetaData *schema)
        {
            if (schema == nullptr) { return nullptr; }
            const auto &endpoint_ops = input_endpoint_ops_for(schema);
            if (!endpoint_ops.supports_input_projection)
            {
                return regular_ts_data_binding_for(schema);
            }

            static std::mutex mutex;
            static std::unordered_map<const TSValueTypeMetaData *, std::unique_ptr<InputStructuredOpsEntry>> cache;

            std::lock_guard lock{mutex};
            if (const auto it = cache.find(schema); it != cache.end()) { return it->second->ts_data_binding; }

            auto entry = std::make_unique<InputStructuredOpsEntry>();
            entry->schema = schema;
            entry->endpoint_ops = &endpoint_ops;

            entry->value_ops.context        = entry.get();
            entry->value_ops.allows_mutation = false;
            entry->value_ops.hash_impl      = &input_value_hash;
            entry->value_ops.equals_impl    = &input_value_equals;
            entry->value_ops.compare_impl   = &input_value_compare;
            entry->value_ops.to_string_impl = &input_value_to_string;
            entry->value_ops.size           = &input_value_size;
            entry->value_ops.element_at     = &input_value_element_at;
            entry->value_ops.element_binding = &input_value_element_binding;
            entry->value_ops.make_range     = &input_value_make_range;

            const auto &node_plan = MemoryUtils::plan_for<detail::TSInputNode>();
            entry->value_binding =
                &ValueTypeBinding::intern(*schema->value_schema, node_plan, entry->value_ops);
            entry->delta_binding = ValuePlanFactory::instance().binding_for(schema->delta_value_schema);

            entry->layout.value_binding = entry->value_binding;
            entry->layout.delta_binding = entry->delta_binding;

            entry->ts_data_ops.context                  = entry.get();
            entry->ts_data_ops.allows_mutation          = false;
            entry->ts_data_ops.layout_impl              = &input_ts_data_layout;
            entry->ts_data_ops.tracking_impl            = &input_ts_data_tracking;
            entry->ts_data_ops.mutable_tracking_impl    = &input_ts_data_mutable_tracking;
            entry->ts_data_ops.has_current_value_impl   = &input_ts_data_has_current_value;
            entry->ts_data_ops.all_valid_impl           = &input_ts_data_all_valid;
            entry->ts_data_ops.value_memory_impl        = &input_ts_data_value_memory;
            entry->ts_data_ops.delta_memory_impl        = &input_ts_data_delta_memory;
            entry->ts_data_ops.cleanup_delta_impl       = &input_ts_data_cleanup_delta;
            entry->ts_data_ops.record_child_modified_impl = &input_ts_data_record_child_modified;
            entry->ts_data_ops.size_impl                = &input_ts_data_indexed_size;
            entry->ts_data_ops.element_binding_impl     = &input_ts_data_element_binding;
            entry->ts_data_ops.element_memory_impl      = &input_ts_data_element_memory;

            entry->ts_data_binding = &TSDataBinding::intern(*schema, node_plan, entry->ts_data_ops);
            const auto *result = entry->ts_data_binding;
            cache.emplace(schema, std::move(entry));
            return result;
        }

    }  // namespace

    namespace detail
    {
        bool output_view_bound(const TSOutputView &output) noexcept
        {
            return ::hgraph::output_view_bound(output);
        }

        const TSDataView &empty_ts_data_view() noexcept
        {
            return ::hgraph::empty_ts_data_view();
        }

        void validate_input_view_kind(const TSValueTypeMetaData *schema, TSTypeKind expected, const char *what)
        {
            ::hgraph::validate_endpoint_kind(schema, expected, what);
        }

        const TSInputEndpointOps &input_endpoint_ops_for(const TSValueTypeMetaData *schema)
        {
            return ::hgraph::input_endpoint_ops_for(schema);
        }

        const TSDataBinding *input_data_binding_for(const TSValueTypeMetaData *schema)
        {
            return ::hgraph::input_data_binding_for(schema);
        }
    }  // namespace detail

    TSInputConstructionPlan::TSInputConstructionPlan(const TSValueTypeMetaData &root_schema,
                                                     TSEndpointSchema           endpoint_schema)
        : schema_(&root_schema),
          endpoint_schema_(std::move(endpoint_schema))
    {
        if (!time_series_schema_equivalent(&root_schema, endpoint_schema_.schema()))
        {
            throw std::invalid_argument("TSInput construction annotation schema does not match the root schema");
        }
        validate_input_endpoint_schema(endpoint_schema_, true);
    }

    const TSValueTypeMetaData &TSInputConstructionPlan::schema() const noexcept
    {
        return *schema_;
    }

    const TSEndpointSchema &TSInputConstructionPlan::endpoint_schema() const noexcept
    {
        return endpoint_schema_;
    }

    TSInputConstructionPlan TSInputPlanFactory::compile(const TSValueTypeMetaData &root_schema,
                                                        const TSEndpointSchema    &endpoint_schema)
    {
        return TSInputConstructionPlan{root_schema, endpoint_schema};
    }

    TSInputBuilder::TSInputBuilder(TSInputConstructionPlan plan)
        : plan_(std::move(plan))
    {
    }

    const TSValueTypeMetaData &TSInputBuilder::schema() const noexcept
    {
        return plan_.schema();
    }

    TSInput TSInputBuilder::make_input() const
    {
        return TSInput{*this};
    }

    const TSInputBuilder *TSInputBuilderFactory::builder_for(const TSValueTypeMetaData &root_schema,
                                                            const TSEndpointSchema    &endpoint_schema)
    {
        return builder_for(TSInputPlanFactory::compile(root_schema, endpoint_schema));
    }

    const TSInputBuilder &TSInputBuilderFactory::checked_builder_for(const TSValueTypeMetaData &root_schema,
                                                                    const TSEndpointSchema    &endpoint_schema)
    {
        return checked_builder_for(TSInputPlanFactory::compile(root_schema, endpoint_schema));
    }

    const TSInputBuilder *TSInputBuilderFactory::builder_for(const TSInputConstructionPlan &plan)
    {
        if (plan.endpoint_schema().schema() == nullptr || plan.endpoint_schema().schema()->kind != TSTypeKind::TSB ||
            !plan.endpoint_schema().is_non_peered())
        {
            return nullptr;
        }

        static std::unordered_map<std::string, std::unique_ptr<TSInputBuilder>> cache;
        static std::mutex mutex;

        const auto key = plan_cache_key(plan);
        std::lock_guard lock{mutex};
        if (const auto it = cache.find(key); it != cache.end()) { return it->second.get(); }

        auto builder = std::unique_ptr<TSInputBuilder>(new TSInputBuilder(plan));
        const auto *result = builder.get();
        cache.emplace(key, std::move(builder));
        return result;
    }

    const TSInputBuilder &TSInputBuilderFactory::checked_builder_for(const TSInputConstructionPlan &plan)
    {
        if (const auto *builder = builder_for(plan); builder != nullptr) { return *builder; }
        throw std::invalid_argument("TSInputBuilderFactory requires a non-peered TSB root annotation");
    }

    TSInput::TSInput() noexcept = default;

    TSInput::TSInput(const TSInputBuilder &builder)
        : builder_(&builder)
    {
        rebuild_from_plan(builder.plan_);
    }

    TSInput::TSInput(const TSInputConstructionPlan &plan)
    {
        rebuild_from_plan(plan);
    }

    TSInput::TSInput(const TSInput &other)
        : builder_(other.builder_),
          schema_(other.schema_),
          root_(other.root_ ? other.root_->deep_copy(nullptr) : nullptr)
    {
    }

    TSInput &TSInput::operator=(const TSInput &other)
    {
        if (this == &other) { return *this; }
        TSInput replacement{other};
        return *this = std::move(replacement);
    }

    TSInput::TSInput(TSInput &&other) noexcept
        : builder_(std::exchange(other.builder_, nullptr)),
          schema_(std::exchange(other.schema_, nullptr)),
          root_(std::move(other.root_))
    {
        relink_nodes();
    }

    TSInput &TSInput::operator=(TSInput &&other) noexcept
    {
        if (this != &other)
        {
            root_.reset();
            builder_ = std::exchange(other.builder_, nullptr);
            schema_ = std::exchange(other.schema_, nullptr);
            root_ = std::move(other.root_);
            relink_nodes();
        }
        return *this;
    }

    TSInput::~TSInput() = default;

    bool TSInput::has_value() const noexcept
    {
        return root_ != nullptr;
    }

    const TSValueTypeMetaData *TSInput::schema() const noexcept
    {
        return schema_;
    }

    TSInputView TSInput::view(Notifiable *scheduling_notifier, engine_time_t evaluation_time)
    {
        return TSInputView{this, root_.get(), {}, {}, scheduling_notifier, evaluation_time};
    }

    TSInputView TSInput::view(Notifiable *scheduling_notifier, engine_time_t evaluation_time) const
    {
        return TSInputView{const_cast<TSInput *>(this), root_.get(), {}, {}, scheduling_notifier, evaluation_time};
    }

    void TSInput::rebuild_from_plan(const TSInputConstructionPlan &plan)
    {
        schema_ = &plan.schema();
        root_ = detail::make_node_from_endpoint_schema(plan.endpoint_schema(), nullptr, TS_DATA_NO_CHILD_ID);
        relink_nodes();
    }

    void TSInput::relink_nodes() noexcept
    {
        if (root_) { root_->relink(nullptr); }
    }

}  // namespace hgraph
