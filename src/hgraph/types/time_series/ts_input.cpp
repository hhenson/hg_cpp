#include <hgraph/types/time_series/ts_input.h>

#include <hgraph/types/time_series/ts_input/detail.h>

#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/container_ops.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <fmt/format.h>
#include <iterator>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

namespace hgraph
{
    namespace
    {
        inline constexpr std::size_t input_npos = static_cast<std::size_t>(-1);

        [[nodiscard]] constexpr std::size_t ts_kind_index(TSTypeKind kind) noexcept
        {
            return static_cast<std::size_t>(kind);
        }

        [[nodiscard]] bool output_view_bound(const TSOutputView &output) noexcept
        {
            return output.output() != nullptr && output.data_view().valid();
        }

        [[nodiscard]] const TSDataView &empty_ts_data_view() noexcept
        {
            static const TSDataView empty{};
            return empty;
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

            if (endpoint_schema.is_peered()) { return; }
            if (schema->kind != TSTypeKind::TSB && schema->kind != TSTypeKind::TSL)
            {
                throw std::invalid_argument("TSInput non-peered prefixes require TSB or fixed-size TSL schemas");
            }
            if (schema->kind == TSTypeKind::TSL && schema->fixed_size() == 0)
            {
                throw std::invalid_argument("TSInput non-peered TSL prefixes currently require a fixed size");
            }
            for (const auto &child : endpoint_schema.children()) { validate_input_endpoint_schema(child, false); }
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

        [[nodiscard]] std::string binding_cache_key(const TSEndpointSchema           &endpoint_schema,
                                                    const MemoryUtils::StoragePlan   &root_plan,
                                                    std::size_t                       storage_offset)
        {
            std::string key;
            key.reserve(160);
            const auto root_bits = reinterpret_cast<std::uintptr_t>(&root_plan);
            key.append(reinterpret_cast<const char *>(&root_bits), sizeof(root_bits));
            key.append(reinterpret_cast<const char *>(&storage_offset), sizeof(storage_offset));
            append_endpoint_key(key, endpoint_schema);
            return key;
        }

        [[nodiscard]] std::size_t no_endpoint_child_count(const TSValueTypeMetaData *) noexcept { return 0; }
        [[nodiscard]] std::string_view no_endpoint_key_at(const TSValueTypeMetaData *, std::size_t) noexcept { return {}; }
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

        [[nodiscard]] const MemoryUtils::StoragePlan &input_storage_plan(const TSEndpointSchema &endpoint_schema);
        [[nodiscard]] const TSDataBinding *input_data_binding_for(const TSEndpointSchema         &endpoint_schema,
                                                                  const MemoryUtils::StoragePlan &root_plan,
                                                                  std::size_t storage_offset);

        [[nodiscard]] std::string child_component_name(const TSValueTypeMetaData *schema, std::size_t index)
        {
            if (schema != nullptr && schema->kind == TSTypeKind::TSB)
            {
                return fmt::format("field_{}", index);
            }
            return fmt::format("element_{}", index);
        }

        [[nodiscard]] std::size_t child_storage_offset(const TSEndpointSchema           &endpoint_schema,
                                                       const MemoryUtils::StoragePlan   &storage_plan,
                                                       std::size_t                       index)
        {
            const auto *schema = endpoint_schema.schema();
            const auto *component = storage_plan.find_component(child_component_name(schema, index));
            if (component == nullptr)
            {
                throw std::logic_error("TSInput storage plan is missing a child component");
            }
            return component->offset;
        }

        [[nodiscard]] std::size_t tracking_offset(const MemoryUtils::StoragePlan &storage_plan)
        {
            const auto *component = storage_plan.find_component("tracking");
            if (component == nullptr) { throw std::logic_error("TSInput storage plan is missing tracking"); }
            return component->offset;
        }

        [[nodiscard]] const MemoryUtils::StoragePlan &input_storage_plan(const TSEndpointSchema &endpoint_schema)
        {
            if (endpoint_schema.is_peered()) { return MemoryUtils::plan_for<detail::TSInputTargetLinkStorage>(); }

            const auto *schema = endpoint_schema.schema();
            auto        builder = MemoryUtils::named_tuple();
            builder.reserve(endpoint_schema.children().size() + 1);
            for (std::size_t index = 0; index < endpoint_schema.children().size(); ++index)
            {
                builder.add_field(child_component_name(schema, index),
                                  input_storage_plan(endpoint_schema.children()[index]));
            }
            builder.add_field("tracking", MemoryUtils::plan_for<TSDataTracking>());
            return builder.build();
        }

        struct InputChild
        {
            const TSValueTypeMetaData *schema{nullptr};
            const TSDataBinding       *input_binding{nullptr};
            const TSDataBinding       *regular_binding{nullptr};
            const ValueTypeBinding    *regular_value_binding{nullptr};
            bool                       target_link{false};
        };

        struct InputBindingContext
        {
            const TSValueTypeMetaData      *schema{nullptr};
            TSDataLayout                    layout{};
            IndexedTSDataOps                ts_data_ops{};
            IndexedValueOps                 value_ops{};
            const ValueTypeBinding         *value_binding{nullptr};
            const ValueTypeBinding         *delta_binding{nullptr};
            std::vector<InputChild>         children{};
        };

        struct TargetLinkContext
        {
            const TSValueTypeMetaData      *schema{nullptr};
            std::size_t                     storage_offset{0};
            TSDataLayout                    layout{};
            TSDataOps                       ts_data_ops{};
            const TSDataBinding            *regular_binding{nullptr};
        };

        [[nodiscard]] const InputBindingContext *input_context_for(const TSDataBinding *binding) noexcept;
        [[nodiscard]] const TargetLinkContext *target_context_for(const TSDataBinding *binding) noexcept;

        [[nodiscard]] const void *advance(const void *memory, std::size_t offset) noexcept
        {
            return static_cast<const std::byte *>(memory) + offset;
        }

        [[nodiscard]] void *advance(void *memory, std::size_t offset) noexcept
        {
            return static_cast<std::byte *>(memory) + offset;
        }

        [[nodiscard]] const detail::TSInputTargetLinkStorage *target_storage(const TargetLinkContext *context,
                                                                             const void              *memory) noexcept
        {
            return MemoryUtils::cast<detail::TSInputTargetLinkStorage>(advance(memory, context->storage_offset));
        }

        [[nodiscard]] detail::TSInputTargetLinkStorage *target_storage(const TargetLinkContext *context,
                                                                       void                    *memory) noexcept
        {
            return MemoryUtils::cast<detail::TSInputTargetLinkStorage>(advance(memory, context->storage_offset));
        }

        [[nodiscard]] const detail::TSInputTargetLinkStorage *target_storage(const TSDataView &view) noexcept
        {
            const auto *context = target_context_for(view.binding());
            return context != nullptr && view.data() != nullptr ? target_storage(context, view.data()) : nullptr;
        }

        [[nodiscard]] detail::TSInputTargetLinkStorage *mutable_target_storage(const TSDataView &view)
        {
            const auto *context = target_context_for(view.binding());
            return context != nullptr && view.data() != nullptr
                       ? target_storage(context, const_cast<void *>(view.data()))
                       : nullptr;
        }

        [[nodiscard]] const detail::TSInputTargetLinkStorage *child_target_storage(const InputChild &child,
                                                                                   const void       *memory) noexcept
        {
            if (!child.target_link || memory == nullptr) { return nullptr; }
            const auto *context = target_context_for(child.input_binding);
            return context != nullptr ? target_storage(context, memory) : nullptr;
        }

        [[nodiscard]] const TSDataLayout *input_layout(const void *context) noexcept
        {
            return &static_cast<const InputBindingContext *>(context)->layout;
        }

        [[nodiscard]] const TSDataTracking *input_tracking(const void *context, const void *memory) noexcept
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            return MemoryUtils::cast<TSDataTracking>(advance(memory, state->layout.tracking_offset));
        }

        [[nodiscard]] TSDataTracking *input_mutable_tracking(const void *context, void *memory) noexcept
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            return MemoryUtils::cast<TSDataTracking>(advance(memory, state->layout.tracking_offset));
        }

        [[nodiscard]] const TSDataLayout *target_link_layout(const void *context) noexcept
        {
            return &static_cast<const TargetLinkContext *>(context)->layout;
        }

        [[nodiscard]] const TSDataTracking *target_link_tracking(const void *context, const void *memory) noexcept
        {
            return &target_storage(static_cast<const TargetLinkContext *>(context), memory)->tracking;
        }

        [[nodiscard]] TSDataTracking *target_link_mutable_tracking(const void *context, void *memory) noexcept
        {
            return &target_storage(static_cast<const TargetLinkContext *>(context), memory)->tracking;
        }

        [[nodiscard]] const TSDataBinding *input_element_binding(const void *context,
                                                                 const void *memory,
                                                                 std::size_t index) noexcept
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            if (index >= state->children.size()) { return nullptr; }
            const auto &child = state->children[index];
            if (!child.target_link) { return child.input_binding; }
            const auto *link = child_target_storage(child, memory);
            return link != nullptr && link->bound() ? link->target_output().binding() : child.regular_binding;
        }

        [[nodiscard]] const void *input_element_memory(const void *context,
                                                       const void *memory,
                                                       std::size_t index) noexcept
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            if (index >= state->children.size()) { return nullptr; }
            const auto &child = state->children[index];
            if (!child.target_link) { return memory; }
            const auto *link = child_target_storage(child, memory);
            return link != nullptr && link->bound() ? link->target_output().data_view().data() : nullptr;
        }

        [[nodiscard]] void *input_mutable_element_memory(const void *context, void *memory, std::size_t index) noexcept
        {
            return const_cast<void *>(input_element_memory(context, memory, index));
        }

        [[nodiscard]] bool input_has_current_value(const void *context, const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                const auto *binding = input_element_binding(context, memory, index);
                const auto *data    = input_element_memory(context, memory, index);
                if (binding == nullptr || data == nullptr) { continue; }
                const auto &ops = binding->checked_ops();
                if (ops.has_current_value_impl(ops.context, data)) { return true; }
            }
            return false;
        }

        [[nodiscard]] bool input_all_valid(const void *context, const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            if (!input_has_current_value(context, memory)) { return false; }
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                const auto *binding = input_element_binding(context, memory, index);
                const auto *data    = input_element_memory(context, memory, index);
                if (binding == nullptr || data == nullptr) { return false; }
                const auto &ops = binding->checked_ops();
                if (!ops.all_valid_impl(ops.context, data)) { return false; }
            }
            return true;
        }

        [[nodiscard]] const void *input_value_memory(const void *, const void *memory) noexcept { return memory; }
        [[nodiscard]] void *input_mutable_value_memory(const void *, void *memory) noexcept { return memory; }
        [[nodiscard]] const void *input_delta_memory(const void *, const void *memory) noexcept { return memory; }
        [[nodiscard]] void *input_mutable_delta_memory(const void *, void *memory) noexcept { return memory; }

        void input_cleanup_delta(const void *context, void *memory, engine_time_t modified_time)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                const auto *binding = input_element_binding(context, memory, index);
                const auto *data    = input_element_memory(context, memory, index);
                if (binding == nullptr || data == nullptr) { continue; }
                const auto &ops = binding->checked_ops();
                const auto *tracking = ops.tracking_impl(ops.context, data);
                if (tracking != nullptr && tracking->last_modified_time == modified_time)
                {
                    ops.cleanup_delta_impl(ops.context, const_cast<void *>(data), modified_time);
                }
            }
        }

        [[nodiscard]] std::size_t input_indexed_size(const void *context, const void *) noexcept
        {
            return static_cast<const InputBindingContext *>(context)->children.size();
        }

        [[nodiscard]] const ValueTypeBinding *input_value_element_binding(const void *context,
                                                                          const void *memory,
                                                                          std::size_t index) noexcept
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            if (index >= state->children.size()) { return nullptr; }
            const auto &child = state->children[index];
            if (!child.target_link) { return value_binding_for_data_binding(child.input_binding); }
            const auto *link = child_target_storage(child, memory);
            return link != nullptr && link->bound() ? link->target_view().value().binding() : child.regular_value_binding;
        }

        [[nodiscard]] const void *input_value_element_at(const void *context,
                                                         const void *memory,
                                                         std::size_t index) noexcept
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            if (index >= state->children.size()) { return nullptr; }
            const auto &child = state->children[index];
            if (!child.target_link)
            {
                const auto *binding = child.input_binding;
                const auto &ops = binding->checked_ops();
                return ops.value_memory_impl(ops.context, memory);
            }
            const auto *link = child_target_storage(child, memory);
            return link != nullptr && link->bound() ? link->target_view().value().data() : nullptr;
        }

        [[nodiscard]] ValueView input_value_project_value(const void *context, const void *memory, std::size_t index)
        {
            return ValueView{input_value_element_binding(context, memory, index),
                             input_value_element_at(context, memory, index)};
        }

        [[nodiscard]] Range<ValueView> input_value_make_range(const void *context, const void *memory)
        {
            return Range<ValueView>{.context = context, .memory = memory, .limit = input_indexed_size(context, memory),
                                    .predicate = nullptr, .projector = &input_value_project_value};
        }

        [[nodiscard]] std::size_t input_value_hash(const void *context, const void *memory)
        {
            std::size_t seed = 0;
            const auto  size = input_indexed_size(context, memory);
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
                const auto size = input_indexed_size(context, lhs);
                if (input_indexed_size(context, rhs) != size) { return false; }
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
                const auto size = std::min(input_indexed_size(context, lhs), input_indexed_size(context, rhs));
                for (std::size_t index = 0; index < size; ++index)
                {
                    const auto *binding = input_value_element_binding(context, lhs, index);
                    const auto *a       = input_value_element_at(context, lhs, index);
                    const auto *b       = input_value_element_at(context, rhs, index);
                    if (const auto order = value_ops_detail::null_order(a, b)) { return *order; }
                    if (binding == nullptr)
                    {
                        if (a != b) { return std::partial_ordering::unordered; }
                        continue;
                    }
                    const auto order = binding->checked_ops().compare(a, b);
                    if (order != 0) { return order; }
                }
                const auto lhs_size = input_indexed_size(context, lhs);
                const auto rhs_size = input_indexed_size(context, rhs);
                if (lhs_size < rhs_size) { return std::partial_ordering::less; }
                if (lhs_size > rhs_size) { return std::partial_ordering::greater; }
                return std::partial_ordering::equivalent;
            });
        }

        [[nodiscard]] std::string input_value_to_string(const void *context, const void *memory)
        {
            if (memory == nullptr) { return {}; }
            const auto *state = static_cast<const InputBindingContext *>(context);
            const auto &endpoint_ops = input_endpoint_ops_for(state->schema);
            const bool  named = endpoint_ops.named_value_projection;
            fmt::memory_buffer out;
            fmt::format_to(std::back_inserter(out), "{}", endpoint_ops.value_open);
            const auto size = input_indexed_size(context, memory);
            for (std::size_t index = 0; index < size; ++index)
            {
                if (index > 0) { fmt::format_to(std::back_inserter(out), ", "); }
                if (named)
                {
                    const auto key = endpoint_ops.key_at != nullptr ? endpoint_ops.key_at(state->schema, index)
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
            fmt::format_to(std::back_inserter(out), "{}", endpoint_ops.value_close);
            return fmt::to_string(out);
        }

        [[nodiscard]] bool target_link_has_current_value(const void *context, const void *memory)
        {
            const auto *link = target_storage(static_cast<const TargetLinkContext *>(context), memory);
            const auto  target = link != nullptr ? link->target_view() : TSDataView{};
            return target.valid() && target.has_current_value();
        }

        [[nodiscard]] bool target_link_all_valid(const void *context, const void *memory)
        {
            const auto *link = target_storage(static_cast<const TargetLinkContext *>(context), memory);
            const auto  target = link != nullptr ? link->target_view() : TSDataView{};
            return target.valid() && target.all_valid();
        }

        [[nodiscard]] const void *target_link_value_memory(const void *context, const void *memory)
        {
            const auto *link = target_storage(static_cast<const TargetLinkContext *>(context), memory);
            const auto  target = link != nullptr ? link->target_view() : TSDataView{};
            return target.valid() ? target.value().data() : nullptr;
        }

        [[nodiscard]] const void *target_link_delta_memory(const void *context, const void *memory)
        {
            const auto *link = target_storage(static_cast<const TargetLinkContext *>(context), memory);
            const auto  target = link != nullptr ? link->target_view() : TSDataView{};
            return target.valid() ? target.delta_value(link->tracking.last_modified_time).data() : nullptr;
        }

        void target_link_cleanup_delta(const void *, void *, engine_time_t) {}

        [[nodiscard]] const TSDataBinding *make_target_link_binding(const TSEndpointSchema           &endpoint_schema,
                                                                    const MemoryUtils::StoragePlan   &root_plan,
                                                                    std::size_t                       storage_offset)
        {
            static std::mutex mutex;
            static std::unordered_map<std::string, std::unique_ptr<TargetLinkContext>> cache;

            const auto key = binding_cache_key(endpoint_schema, root_plan, storage_offset);
            std::lock_guard lock{mutex};
            if (const auto it = cache.find(key); it != cache.end())
            {
                return TypeBinding<TSValueTypeMetaData, TSDataOps>::find(endpoint_schema.schema(),
                                                                          &root_plan,
                                                                          &it->second->ts_data_ops);
            }

            auto context = std::make_unique<TargetLinkContext>();
            context->schema = endpoint_schema.schema();
            context->storage_offset = storage_offset;
            context->regular_binding = regular_ts_data_binding_for(context->schema);

            context->layout.value_binding = ValuePlanFactory::instance().binding_for(context->schema->value_schema);
            context->layout.delta_binding = ValuePlanFactory::instance().binding_for(context->schema->delta_value_schema);
            context->layout.value_offset = 0;
            context->layout.tracking_offset =
                storage_offset;

            context->ts_data_ops = TSDataOps{
                .context = context.get(),
                .allows_mutation = true,
                .layout_impl = &target_link_layout,
                .tracking_impl = &target_link_tracking,
                .mutable_tracking_impl = &target_link_mutable_tracking,
                .has_current_value_impl = &target_link_has_current_value,
                .all_valid_impl = &target_link_all_valid,
                .value_memory_impl = &target_link_value_memory,
                .delta_memory_impl = &target_link_delta_memory,
                .cleanup_delta_impl = &target_link_cleanup_delta,
            };

            const auto &binding = TSDataBinding::intern(*context->schema, root_plan, context->ts_data_ops);
            cache.emplace(key, std::move(context));
            return &binding;
        }

        [[nodiscard]] const TSDataBinding *input_data_binding_for(const TSEndpointSchema         &endpoint_schema,
                                                                  const MemoryUtils::StoragePlan &root_plan,
                                                                  std::size_t storage_offset)
        {
            if (endpoint_schema.is_peered())
            {
                return make_target_link_binding(endpoint_schema, root_plan, storage_offset);
            }

            static std::recursive_mutex mutex;
            static std::unordered_map<std::string, std::unique_ptr<InputBindingContext>> cache;

            const auto key = binding_cache_key(endpoint_schema, root_plan, storage_offset);
            std::lock_guard lock{mutex};
            if (const auto it = cache.find(key); it != cache.end())
            {
                return TSDataBinding::find(endpoint_schema.schema(), &root_plan, &it->second->ts_data_ops);
            }

            const auto &local_plan = input_storage_plan(endpoint_schema);
            auto context = std::make_unique<InputBindingContext>();
            context->schema = endpoint_schema.schema();
            context->layout.tracking_offset = storage_offset + tracking_offset(local_plan);
            context->children.reserve(endpoint_schema.children().size());

            for (std::size_t index = 0; index < endpoint_schema.children().size(); ++index)
            {
                const auto &child_schema = endpoint_schema.children()[index];
                const auto child_offset = storage_offset + child_storage_offset(endpoint_schema, local_plan, index);
                const auto *child_binding = input_data_binding_for(child_schema, root_plan, child_offset);
                context->children.push_back(InputChild{
                    .schema = child_schema.schema(),
                    .input_binding = child_binding,
                    .regular_binding = regular_ts_data_binding_for(child_schema.schema()),
                    .regular_value_binding = regular_value_binding_for(child_schema.schema()),
                    .target_link = child_schema.is_peered(),
                });
            }

            context->value_ops = IndexedValueOps{
                {context.get(), false, &input_value_hash, &input_value_equals, &input_value_compare,
                 &input_value_to_string},
                &input_indexed_size,
                &input_value_element_at,
                &input_value_element_binding,
                &input_value_make_range,
                nullptr,
            };

            context->value_binding = &ValueTypeBinding::intern(*context->schema->value_schema, root_plan,
                                                               context->value_ops);
            context->delta_binding = ValuePlanFactory::instance().binding_for(context->schema->delta_value_schema);
            context->layout.value_binding = context->value_binding;
            context->layout.delta_binding = context->delta_binding;

            context->ts_data_ops = IndexedTSDataOps{};
            TSDataOps &base_ops = context->ts_data_ops;
            base_ops = TSDataOps{
                .context = context.get(),
                .allows_mutation = true,
                .layout_impl = &input_layout,
                .tracking_impl = &input_tracking,
                .mutable_tracking_impl = &input_mutable_tracking,
                .has_current_value_impl = &input_has_current_value,
                .all_valid_impl = &input_all_valid,
                .value_memory_impl = &input_value_memory,
                .mutable_value_memory_impl = &input_mutable_value_memory,
                .delta_memory_impl = &input_delta_memory,
                .mutable_delta_memory_impl = &input_mutable_delta_memory,
                .cleanup_delta_impl = &input_cleanup_delta,
            };
            context->ts_data_ops.size_impl = &input_indexed_size;
            context->ts_data_ops.element_binding_impl = &input_element_binding;
            context->ts_data_ops.element_memory_impl = &input_element_memory;
            context->ts_data_ops.mutable_element_memory_impl = &input_mutable_element_memory;

            const auto &binding = TSDataBinding::intern(*context->schema, root_plan, context->ts_data_ops);
            cache.emplace(key, std::move(context));
            return &binding;
        }

        [[nodiscard]] const InputBindingContext *input_context_for(const TSDataBinding *binding) noexcept
        {
            if (binding == nullptr) { return nullptr; }

            // The authoritative lookup is through the ops context. Input
            // bindings always use ``input_layout`` while regular TSData and
            // TargetLink bindings do not.
            const auto *ops = binding->ops;
            return ops != nullptr && ops->layout_impl == &input_layout
                       ? static_cast<const InputBindingContext *>(ops->context)
                       : nullptr;
        }

        [[nodiscard]] const TargetLinkContext *target_context_for(const TSDataBinding *binding) noexcept
        {
            if (binding == nullptr) { return nullptr; }
            const auto *ops = binding->ops;
            return ops != nullptr && ops->layout_impl == &target_link_layout
                       ? static_cast<const TargetLinkContext *>(ops->context)
                       : nullptr;
        }

        [[nodiscard]] const TSDataBinding *input_data_binding_for(const TSEndpointSchema &endpoint_schema)
        {
            const auto &root_plan = input_storage_plan(endpoint_schema);
            return input_data_binding_for(endpoint_schema, root_plan, 0);
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

        const TSDataBinding *input_data_binding_for(const TSEndpointSchema &endpoint_schema)
        {
            return ::hgraph::input_data_binding_for(endpoint_schema);
        }

        const TSDataBinding *regular_ts_data_binding_for(const TSValueTypeMetaData *schema)
        {
            return ::hgraph::regular_ts_data_binding_for(schema);
        }

        bool is_target_link_binding(const TSDataBinding *binding) noexcept
        {
            return ::hgraph::target_context_for(binding) != nullptr;
        }

        const TSInputTargetLinkStorage *target_link_storage(const TSDataView &view) noexcept
        {
            return ::hgraph::target_storage(view);
        }

        TSInputTargetLinkStorage *mutable_target_link_storage(const TSDataView &view)
        {
            return ::hgraph::mutable_target_storage(view);
        }

        const TSValueTypeMetaData *target_link_schema(const TSDataView &view) noexcept
        {
            const auto *context = ::hgraph::target_context_for(view.binding());
            return context != nullptr ? context->schema : nullptr;
        }

        TSInputChildProjection input_child_projection(TSDataView &parent, std::size_t index)
        {
            const auto *context = ::hgraph::input_context_for(parent.binding());
            if (context == nullptr || index >= context->children.size())
            {
                throw std::logic_error("TSInput child projection requires non-peered input storage");
            }

            const auto &child = context->children[index];
            if (child.target_link)
            {
                TSDataView link{child.input_binding, parent.data()};
                const auto *storage = ::hgraph::target_storage(link);
                if (storage != nullptr && storage->bound())
                {
                    return TSInputChildProjection{storage->target_view(), link};
                }
                return TSInputChildProjection{TSDataView{child.regular_binding, static_cast<const void *>(nullptr)}, link};
            }

            return TSInputChildProjection{TSDataView{child.input_binding, parent.data()}, {}};
        }

        void TSInputSchedulingNotifier::notify(engine_time_t modified_time)
        {
            if (target != nullptr) { target->notify(modified_time); }
        }

        TSInputActiveTarget::TSInputActiveTarget() noexcept
        {
        }

        TSInputActiveTarget::TSInputActiveTarget(TSInputActiveTarget *parent_, std::size_t slot_) noexcept
            : parent(parent_),
              slot(slot_)
        {
        }

        TSInputActiveTarget::~TSInputActiveTarget() noexcept
        {
            unsubscribe();
        }

        TSInputActiveTarget *TSInputActiveTarget::child_at(std::size_t slot_) const noexcept
        {
            if (const auto it = children.find(slot_); it != children.end()) { return it->second.get(); }
            return nullptr;
        }

        bool TSInputActiveTarget::has_any_active() const noexcept
        {
            if (active) { return true; }
            return std::ranges::any_of(children, [](const auto &entry) {
                return entry.second && entry.second->has_any_active();
            });
        }

        TSInputActiveTarget &TSInputActiveTarget::ensure_child(std::size_t slot_)
        {
            auto &child = children[slot_];
            if (!child) { child = std::make_unique<TSInputActiveTarget>(this, slot_); }
            return *child;
        }

        void TSInputActiveTarget::subscribe(TSDataView observed_, Notifiable *target_notifier)
        {
            if (observed_.valid() && observed_.data() == observed.data() && observed_.binding() == observed.binding())
            {
                notifier.target = target_notifier;
                return;
            }

            unsubscribe();
            notifier.target = target_notifier;
            if (target_notifier == nullptr || !observed_.valid()) { return; }
            observed = observed_;
            observed.subscribe(&notifier);
        }

        void TSInputActiveTarget::unsubscribe() noexcept
        {
            if (!observed.valid()) { return; }
            [[maybe_unused]] auto reset_observed = make_scope_exit([this]() noexcept { observed = {}; });
            [[maybe_unused]] auto unsubscribe_observer =
                make_scope_exit<true>([this] { observed.unsubscribe(&notifier); });
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
          data_(other.data_)
    {
    }

    TSInput &TSInput::operator=(const TSInput &other)
    {
        if (this == &other) { return *this; }
        builder_ = other.builder_;
        schema_ = other.schema_;
        data_ = other.data_;
        active_root_.reset();
        return *this;
    }

    TSInput::TSInput(TSInput &&other) noexcept
        : builder_(std::exchange(other.builder_, nullptr)),
          schema_(std::exchange(other.schema_, nullptr)),
          data_(std::move(other.data_)),
          active_root_(std::move(other.active_root_))
    {
    }

    TSInput &TSInput::operator=(TSInput &&other) noexcept
    {
        if (this != &other)
        {
            builder_ = std::exchange(other.builder_, nullptr);
            schema_ = std::exchange(other.schema_, nullptr);
            data_ = std::move(other.data_);
            active_root_ = std::move(other.active_root_);
        }
        return *this;
    }

    TSInput::~TSInput() = default;

    bool TSInput::has_value() const noexcept
    {
        return data_.has_value();
    }

    const TSValueTypeMetaData *TSInput::schema() const noexcept
    {
        return schema_;
    }

    TSInputView TSInput::view(Notifiable *scheduling_notifier, engine_time_t evaluation_time)
    {
        return TSInputView{this, data_.view(), {}, nullptr, scheduling_notifier, evaluation_time};
    }

    TSInputView TSInput::view(Notifiable *scheduling_notifier, engine_time_t evaluation_time) const
    {
        return TSInputView{const_cast<TSInput *>(this), data_.view(), {}, nullptr, scheduling_notifier, evaluation_time};
    }

    void TSInput::rebuild_from_plan(const TSInputConstructionPlan &plan)
    {
        schema_ = &plan.schema();
        const auto *binding = detail::input_data_binding_for(plan.endpoint_schema());
        if (binding == nullptr) { throw std::logic_error("TSInput could not resolve input data binding"); }
        data_ = TSData{*binding};
        active_root_.reset();
    }

    void TSInput::make_active(std::vector<std::size_t> path, TSDataView observed, Notifiable *target_notifier)
    {
        if (!active_root_) { active_root_ = std::make_unique<detail::TSInputActiveTarget>(); }
        auto *active = active_root_.get();
        for (const auto slot : path) { active = &active->ensure_child(slot); }
        active->active = true;
        active->subscribe(observed, target_notifier);
    }

    void TSInput::make_passive(const std::vector<std::size_t> &path)
    {
        auto *active = active_root_.get();
        for (const auto slot : path)
        {
            if (active == nullptr) { return; }
            active = active->child_at(slot);
        }
        if (active == nullptr || !active->active) { return; }

        active->unsubscribe();
        active->active = false;

        while (active != nullptr && !active->has_any_active())
        {
            auto *parent = active->parent;
            if (parent == nullptr)
            {
                active_root_.reset();
                return;
            }
            const auto slot = active->slot;
            active = parent;
            active->children.erase(slot);
        }
    }

    bool TSInput::active(const std::vector<std::size_t> &path) const noexcept
    {
        auto *active = active_root_.get();
        for (const auto slot : path)
        {
            if (active == nullptr) { return false; }
            active = active->child_at(slot);
        }
        return active != nullptr && active->active;
    }

}  // namespace hgraph
