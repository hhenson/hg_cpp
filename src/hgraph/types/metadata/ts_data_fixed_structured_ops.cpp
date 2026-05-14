#include <hgraph/types/metadata/ts_data_plan_factory_detail.h>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/util/scope.h>

#include <fmt/format.h>

#include <algorithm>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace hgraph::ts_data_plan_factory_detail
{
    [[nodiscard]] std::size_t combine_hash(std::size_t seed, std::size_t value) noexcept
    {
        seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
        return seed;
    }

    struct FixedTSDataContext
    {
        const TSValueTypeMetaData      *schema{nullptr};
        const MemoryUtils::StoragePlan *plan{nullptr};
        FixedTSBDataLayout              bundle_layout{};
        FixedTSLDataLayout              list_layout{};
        IndexedTSDataOps                ops{};
        IndexedValueOps                 delta_bundle_ops{};
        MapValueOps                     delta_map_ops{};
        SetValueOps                     delta_key_set_ops{};
        const ValueTypeBinding         *ordinal_key_binding{nullptr};
        const ValueTypeBinding         *delta_map_value_binding{nullptr};
        const ValueTypeBinding         *delta_key_set_binding{nullptr};
        std::vector<const TSDataBinding *> element_bindings{};
        std::vector<std::int64_t>       ordinal_keys{};

        FixedTSDataContext(const TSValueTypeMetaData &schema_, const MemoryUtils::StoragePlan &plan_,
                           std::size_t value_offset, std::size_t aux_offset, std::size_t tracking_offset,
                           std::vector<const TSDataBinding *> element_binding_cache)
            : schema(&schema_), plan(&plan_)
        {
            init_layout_base(value_offset, tracking_offset);
            if (schema->kind == TSTypeKind::TSB)
            {
                bundle_layout.fields.reserve(element_binding_cache.size());
            }
            else
            {
                element_bindings.reserve(element_binding_cache.size());
            }
            ordinal_keys.reserve(element_binding_cache.size());

            for (std::size_t index = 0; index < element_binding_cache.size(); ++index)
            {
                const auto *indexed_binding = element_binding_cache[index];
                if (indexed_binding == nullptr)
                {
                    throw std::logic_error("TSDataPlanFactory: fixed TSData element binding is not resolved");
                }

                const auto &indexed_table = indexed_binding->checked_ops();
                const auto *indexed_layout = indexed_table.layout_impl(indexed_table.context);
                if (indexed_layout == nullptr)
                {
                    throw std::logic_error("TSDataPlanFactory: fixed TSData element layout is not resolved");
                }
                if (schema->kind == TSTypeKind::TSB)
                {
                    bundle_layout.fields.push_back(FixedTSDataFieldLayout{
                        .binding     = indexed_binding,
                        .layout      = indexed_layout,
                        .data_offset = 0,
                    });
                }
                else
                {
                    element_bindings.push_back(indexed_binding);
                    if (index == 0)
                    {
                        list_layout.element_binding = indexed_binding;
                        list_layout.element_layout  = indexed_layout;
                    }
                    else if (indexed_binding->type_meta != list_layout.element_binding->type_meta)
                    {
                        throw std::logic_error("TSDataPlanFactory: fixed TSL element bindings are not homogeneous");
                    }
                }
                ordinal_keys.push_back(static_cast<std::int64_t>(index));
            }
            if (schema->kind == TSTypeKind::TSL)
            {
                configure_list_layout(aux_offset, element_binding_cache.size());
            }

            configure_ts_ops();
            configure_value_ops();
        }

        void bind_surfaces()
        {
            const auto *value_schema = schema->value_schema;
            const auto *delta_schema = schema->delta_value_schema;
            if (value_schema == nullptr || delta_schema == nullptr)
            {
                throw std::logic_error("TSDataPlanFactory: fixed TSData schemas are not populated");
            }

            active_layout().value_binding = ValuePlanFactory::instance().binding_for(value_schema);
            if (active_layout().value_binding == nullptr)
            {
                throw std::logic_error("TSDataPlanFactory: fixed TSData value binding is not resolved");
            }

            if (schema->kind == TSTypeKind::TSB)
            {
                active_layout().delta_binding = &ValueTypeBinding::intern(*delta_schema, *plan, delta_bundle_ops);
                return;
            }

            ordinal_key_binding     = ValuePlanFactory::instance().binding_for(delta_schema->key_type);
            delta_map_value_binding = element_count() == 0 ? nullptr : element_delta_binding(0);
            if (ordinal_key_binding == nullptr || delta_map_value_binding == nullptr)
            {
                throw std::logic_error("TSDataPlanFactory: fixed TSL delta bindings are not resolved");
            }
            const auto *key_set_schema = TypeRegistry::instance().set(delta_schema->key_type);
            delta_key_set_binding      = &ValueTypeBinding::intern(*key_set_schema, *plan, delta_key_set_ops);
            active_layout().delta_binding = &ValueTypeBinding::intern(*delta_schema, *plan, delta_map_ops);
        }

        [[nodiscard]] const TSDataLayout *layout_ptr() const noexcept
        {
            return schema->kind == TSTypeKind::TSB ? static_cast<const TSDataLayout *>(&bundle_layout)
                                                   : static_cast<const TSDataLayout *>(&list_layout);
        }

      private:
        void init_layout_base(std::size_t value_offset, std::size_t tracking_offset) noexcept
        {
            bundle_layout.value_offset    = value_offset;
            bundle_layout.tracking_offset = tracking_offset;
            list_layout.value_offset      = value_offset;
            list_layout.tracking_offset   = tracking_offset;
        }

        [[nodiscard]] TSDataLayout &active_layout() noexcept
        {
            return schema->kind == TSTypeKind::TSB ? static_cast<TSDataLayout &>(bundle_layout)
                                                   : static_cast<TSDataLayout &>(list_layout);
        }

        void configure_list_layout(std::size_t aux_offset, std::size_t element_count)
        {
            list_layout.element_count = element_count;

            const auto *value_plan = ValuePlanFactory::instance().plan_for(schema->value_schema);
            if (value_plan == nullptr || !value_plan->is_array())
            {
                throw std::logic_error("TSDataPlanFactory: fixed TSL value plan must be an array");
            }
            list_layout.element_value_stride = value_plan->array_stride();

            const auto &aux_plan           = ts_data_aux_plan(*schema);
            const auto *elements_component = aux_plan.find_component("elements");
            if (elements_component == nullptr || elements_component->plan == nullptr ||
                !elements_component->plan->is_array())
            {
                throw std::logic_error("TSDataPlanFactory: fixed TSL auxiliary plan must contain an element array");
            }
            list_layout.element_auxiliary_offset = aux_offset + elements_component->offset;
            list_layout.element_auxiliary_stride = elements_component->plan->array_stride();
        }

        [[nodiscard]] std::size_t element_count() const noexcept
        {
            return schema->kind == TSTypeKind::TSB ? bundle_layout.fields.size() : list_layout.element_count;
        }

        [[nodiscard]] const TSDataBinding *element_binding(std::size_t index) const noexcept
        {
            return schema->kind == TSTypeKind::TSB ? bundle_layout.fields[index].binding : element_bindings[index];
        }

        [[nodiscard]] const TSDataLayout *element_layout(std::size_t index) const
        {
            if (schema->kind == TSTypeKind::TSB)
            {
                return bundle_layout.fields[index].layout;
            }
            const auto *binding = element_binding(index);
            const auto &ops     = binding->checked_ops();
            return ops.layout_impl(ops.context);
        }

        [[nodiscard]] const ValueTypeBinding *element_value_binding(std::size_t index) const
        {
            const auto *layout = element_layout(index);
            return layout != nullptr ? layout->value_binding : nullptr;
        }

        [[nodiscard]] const ValueTypeBinding *element_delta_binding(std::size_t index) const
        {
            const auto *layout = element_layout(index);
            return layout != nullptr ? layout->delta_binding : nullptr;
        }

        void configure_ts_ops()
        {
            ops = IndexedTSDataOps{};
            TSDataOps &base_ops = ops;
            base_ops = TSDataOps{
                .context                   = this,
                .allows_mutation           = true,
                .layout_impl               = &fixed_layout,
                .tracking_impl             = &fixed_tracking,
                .mutable_tracking_impl     = &fixed_mutable_tracking,
                .has_current_value_impl    = &fixed_has_current_value,
                .all_valid_impl            = &fixed_all_valid,
                .value_memory_impl         = &fixed_value_memory,
                .mutable_value_memory_impl = &fixed_mutable_value_memory,
                .delta_memory_impl         = &fixed_delta_memory,
                .mutable_delta_memory_impl = &fixed_mutable_delta_memory,
                .cleanup_delta_impl        = &fixed_cleanup_delta,
                .copy_value_from_impl      = &fixed_copy_value_from,
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                .from_python_impl          = &fixed_from_python,
                .to_python_impl            = &fixed_to_python,
                .delta_to_python_impl      = &fixed_delta_to_python,
#endif
            };
            ops.size_impl                   = &fixed_indexed_size;
            ops.element_binding_impl        = &fixed_indexed_element_binding;
            ops.element_memory_impl         = &fixed_indexed_element_memory;
            ops.mutable_element_memory_impl = &fixed_mutable_indexed_element_memory;
        }

        void configure_value_ops()
        {
            delta_bundle_ops = IndexedValueOps{
                {this, false, &fixed_delta_bundle_hash, &fixed_delta_bundle_equals, &fixed_delta_bundle_compare,
                 &fixed_delta_bundle_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                 ,
                 &fixed_delta_bundle_to_python
#endif
                },
                &fixed_indexed_size,
                &fixed_delta_bundle_element_at,
                &fixed_delta_bundle_element_binding,
                &fixed_delta_bundle_make_range,
                nullptr,
            };

            delta_map_ops = MapValueOps{
                {{this, false, &fixed_delta_map_hash, &fixed_delta_map_equals, &fixed_delta_map_compare,
                  &fixed_delta_map_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                  ,
                  &fixed_delta_map_to_python
#endif
                 },
                 &fixed_delta_map_size,
                 &fixed_delta_map_key_at_index,
                 &fixed_delta_map_key_binding,
                 &fixed_delta_map_make_keys_range,
                 nullptr},
                &fixed_delta_map_contains,
                &fixed_delta_map_value_at,
                &fixed_delta_map_value_at_index,
                &fixed_delta_map_value_binding,
                &fixed_delta_map_make_keys_range,
                &fixed_delta_map_make_values_range,
                &fixed_delta_map_make_kv_range,
                &fixed_delta_map_key_set,
            };

            delta_key_set_ops = SetValueOps{
                {{this, false, &fixed_delta_key_set_hash, &fixed_delta_key_set_equals, &fixed_delta_key_set_compare,
                  &fixed_delta_key_set_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                  ,
                  &fixed_delta_key_set_to_python
#endif
                 },
                 &fixed_delta_map_size,
                 &fixed_delta_map_key_at_index,
                 &fixed_delta_map_key_binding,
                 &fixed_delta_map_make_keys_range,
                 nullptr},
                &fixed_delta_map_contains,
            };
        }

        [[nodiscard]] static const FixedTSDataContext *ctx(const void *context) noexcept
        {
            return static_cast<const FixedTSDataContext *>(context);
        }

        [[nodiscard]] static const TSDataLayout *fixed_layout(const void *context) noexcept
        {
            return ctx(context)->layout_ptr();
        }

        [[nodiscard]] static const void *advance(const void *memory, std::size_t offset) noexcept
        {
            return static_cast<const std::byte *>(memory) + offset;
        }

        [[nodiscard]] static void *advance(void *memory, std::size_t offset) noexcept
        {
            return static_cast<std::byte *>(memory) + offset;
        }

        [[nodiscard]] static const TSDataTracking *fixed_tracking(const void *context, const void *memory) noexcept
        {
            return MemoryUtils::cast<TSDataTracking>(advance(memory, ctx(context)->layout_ptr()->tracking_offset));
        }

        [[nodiscard]] static TSDataTracking *fixed_mutable_tracking(const void *context, void *memory) noexcept
        {
            return MemoryUtils::cast<TSDataTracking>(advance(memory, ctx(context)->layout_ptr()->tracking_offset));
        }

        [[nodiscard]] static bool fixed_has_current_value(const void *context, const void *memory) noexcept
        {
            return fixed_tracking(context, memory)->last_modified_time != MIN_DT;
        }

        [[nodiscard]] static bool fixed_all_valid(const void *context, const void *memory)
        {
            if (!fixed_has_current_value(context, memory)) { return false; }

            const auto *state = ctx(context);
            for (std::size_t index = 0; index < state->element_count(); ++index)
            {
                const auto *child = state->element_binding(index);
                const auto &ops   = child_ops(*child);
                const auto *data  = child_data(state, memory, index);
                if (!ops.all_valid_impl(ops.context, data)) { return false; }
            }
            return true;
        }

        static void fixed_cleanup_delta(const void *context, void *memory, engine_time_t modified_time)
        {
            const auto *state = ctx(context);
            for (std::size_t index = 0; index < state->element_count(); ++index)
            {
                const auto *child = state->element_binding(index);
                const auto &ops   = child_ops(*child);
                void       *data  = child_data(state, memory, index);
                const auto *child_tracking = ops.tracking_impl(ops.context, data);
                if (child_tracking != nullptr && child_tracking->last_modified_time == modified_time)
                {
                    ops.cleanup_delta_impl(ops.context, data, modified_time);
                }
            }
        }

        [[nodiscard]] static const void *fixed_value_memory(const void *context, const void *memory) noexcept
        {
            return advance(memory, ctx(context)->layout_ptr()->value_offset);
        }

        [[nodiscard]] static void *fixed_mutable_value_memory(const void *context, void *memory) noexcept
        {
            return advance(memory, ctx(context)->layout_ptr()->value_offset);
        }

        [[nodiscard]] static const void *fixed_delta_memory(const void *, const void *memory) noexcept
        {
            return memory;
        }

        [[nodiscard]] static void *fixed_mutable_delta_memory(const void *, void *memory) noexcept
        {
            return memory;
        }

        [[nodiscard]] static const void *child_data(const FixedTSDataContext *state, const void *memory,
                                                    std::size_t index) noexcept
        {
            if (state->schema->kind == TSTypeKind::TSB)
            {
                return advance(memory, state->bundle_layout.fields[index].data_offset);
            }
            return memory;
        }

        [[nodiscard]] static void *child_data(const FixedTSDataContext *state, void *memory, std::size_t index) noexcept
        {
            if (state->schema->kind == TSTypeKind::TSB)
            {
                return advance(memory, state->bundle_layout.fields[index].data_offset);
            }
            return memory;
        }

        [[nodiscard]] static const TSDataOps &child_ops(const TSDataBinding &child)
        {
            return child.checked_ops();
        }

        [[nodiscard]] static ValueView child_value_view(const FixedTSDataContext *state, const void *memory,
                                                        std::size_t index)
        {
            const auto *child = state->element_binding(index);
            const auto &ops   = child_ops(*child);
            const auto *data  = child_data(state, memory, index);
            return ValueView{state->element_value_binding(index), ops.value_memory_impl(ops.context, data)};
        }

        [[nodiscard]] static ValueView child_delta_view(const FixedTSDataContext *state, const void *memory,
                                                        std::size_t index)
        {
            const auto *child = state->element_binding(index);
            const auto &ops   = child_ops(*child);
            const auto *data  = child_data(state, memory, index);
            const auto  time  = fixed_tracking(state, memory)->last_modified_time;
            if (ops.tracking_impl(ops.context, data)->last_modified_time != time)
            {
                return ValueView{state->element_delta_binding(index), nullptr};
            }
            return ValueView{state->element_delta_binding(index), ops.delta_memory_impl(ops.context, data)};
        }

        [[nodiscard]] static std::size_t fixed_indexed_size(const void *context, const void *) noexcept
        {
            return ctx(context)->element_count();
        }

        [[nodiscard]] static const TSDataBinding *fixed_indexed_element_binding(const void *context, const void *,
                                                                                std::size_t index) noexcept
        {
            return ctx(context)->element_binding(index);
        }

        [[nodiscard]] static const void *fixed_indexed_element_memory(const void *context, const void *memory,
                                                                      std::size_t index) noexcept
        {
            return child_data(ctx(context), memory, index);
        }

        [[nodiscard]] static void *fixed_mutable_indexed_element_memory(const void *context, void *memory,
                                                                        std::size_t index) noexcept
        {
            return child_data(ctx(context), memory, index);
        }

        [[nodiscard]] static const void *fixed_delta_bundle_element_at(const void *context, const void *memory,
                                                                       std::size_t index)
        {
            return child_delta_view(ctx(context), memory, index).data();
        }

        [[nodiscard]] static const ValueTypeBinding *
        fixed_delta_bundle_element_binding(const void *context, const void *, std::size_t index) noexcept
        {
            return ctx(context)->element_delta_binding(index);
        }

        [[nodiscard]] static ValueView fixed_delta_bundle_projector(const void *context, const void *memory,
                                                                    std::size_t index)
        {
            return child_delta_view(ctx(context), memory, index);
        }

        [[nodiscard]] static Range<ValueView> fixed_delta_bundle_make_range(const void *context, const void *memory)
        {
            return Range<ValueView>{
                .context   = context,
                .memory    = memory,
                .limit     = fixed_indexed_size(context, memory),
                .predicate = nullptr,
                .projector = &fixed_delta_bundle_projector,
            };
        }

        [[nodiscard]] static std::size_t view_hash(ValueView view)
        {
            if (!view.has_value())
            {
                return std::hash<const ValueTypeBinding *>{}(view.binding());
            }
            return view.hash();
        }

        [[nodiscard]] static std::string indexed_to_string(const FixedTSDataContext *state, const void *memory,
                                                           bool delta)
        {
            if (memory == nullptr)
            {
                return {};
            }
            const bool         bundle = state->schema->kind == TSTypeKind::TSB;
            fmt::memory_buffer out;
            fmt::format_to(std::back_inserter(out), "{}", bundle ? "{" : "[");
            for (std::size_t index = 0; index < state->element_count(); ++index)
            {
                if (index > 0)
                {
                    fmt::format_to(std::back_inserter(out), ", ");
                }
                if (bundle)
                {
                    const char *name = state->schema->fields()[index].name;
                    fmt::format_to(std::back_inserter(out), "{}: ", name != nullptr ? name : "");
                }
                const auto view =
                    delta ? child_delta_view(state, memory, index) : child_value_view(state, memory, index);
                fmt::format_to(std::back_inserter(out), "{}", view.has_value() ? view.to_string() : "<null>");
            }
            fmt::format_to(std::back_inserter(out), "{}", bundle ? "}" : "]");
            return fmt::to_string(out);
        }

        [[nodiscard]] static std::size_t fixed_delta_bundle_hash(const void *context, const void *memory)
        {
            const auto *state = ctx(context);
            std::size_t seed  = 0;
            for (std::size_t index = 0; index < state->element_count(); ++index)
            {
                seed = combine_hash(seed, view_hash(child_delta_view(state, memory, index)));
            }
            return seed;
        }

        [[nodiscard]] static bool fixed_delta_bundle_equals(const void *context, const void *lhs,
                                                            const void *rhs) noexcept
        {
            if (lhs == nullptr || rhs == nullptr)
            {
                return lhs == rhs;
            }
            return fallback_on_exception(false, [&] {
                const auto *state = ctx(context);
                for (std::size_t index = 0; index < state->element_count(); ++index)
                {
                    if (!child_delta_view(state, lhs, index).equals(child_delta_view(state, rhs, index)))
                    {
                        return false;
                    }
                }
                return true;
            });
        }

        [[nodiscard]] static std::partial_ordering fixed_delta_bundle_compare(const void *context, const void *lhs,
                                                                              const void *rhs) noexcept
        {
            if (const auto order = value_ops_detail::null_order(lhs, rhs))
            {
                return *order;
            }

            return fallback_on_exception(std::partial_ordering::unordered, [&]() {
                const auto *state = ctx(context);
                for (std::size_t index = 0; index < state->element_count(); ++index)
                {
                    const auto order =
                        child_delta_view(state, lhs, index).compare(child_delta_view(state, rhs, index));
                    if (order != 0)
                    {
                        return order;
                    }
                }
                return std::partial_ordering::equivalent;
            });
        }

        [[nodiscard]] static std::string fixed_delta_bundle_to_string(const void *context, const void *memory)
        {
            return indexed_to_string(ctx(context), memory, true);
        }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        [[nodiscard]] static nb::object fixed_delta_bundle_to_python(const void *context, const void *memory)
        {
            const auto *state = ctx(context);
            nb::dict    result;
            for (std::size_t index = 0; index < state->element_count(); ++index)
            {
                if (!child_modified_for_parent_time(state, memory, index)) { continue; }
                const char *name = state->schema->fields()[index].name;
                if (name == nullptr || *name == '\0') { continue; }
                result[nb::str{name}] = child_delta_view(state, memory, index).to_python();
            }
            return result;
        }
#endif

        [[nodiscard]] static bool child_modified_for_parent_time(const FixedTSDataContext *state, const void *memory,
                                                                 std::size_t index) noexcept
        {
            const auto *child = state->element_binding(index);
            const auto &ops   = child_ops(*child);
            const auto *data  = child_data(state, memory, index);
            const auto  time  = fixed_tracking(state, memory)->last_modified_time;
            return ops.tracking_impl(ops.context, data)->last_modified_time == time;
        }

        [[nodiscard]] static bool fixed_delta_child_predicate(const void *context, const void *memory,
                                                              std::size_t index)
        {
            return child_modified_for_parent_time(ctx(context), memory, index);
        }

        [[nodiscard]] static std::size_t fixed_delta_map_size(const void *context, const void *memory) noexcept
        {
            const auto *state = ctx(context);
            std::size_t count = 0;
            for (std::size_t index = 0; index < state->element_count(); ++index)
            {
                if (child_modified_for_parent_time(state, memory, index))
                {
                    ++count;
                }
            }
            return count;
        }

        [[nodiscard]] static std::size_t nth_modified_child(const FixedTSDataContext *state, const void *memory,
                                                            std::size_t ordinal)
        {
            std::size_t seen = 0;
            for (std::size_t index = 0; index < state->element_count(); ++index)
            {
                if (!child_modified_for_parent_time(state, memory, index))
                {
                    continue;
                }
                if (seen++ == ordinal)
                {
                    return index;
                }
            }
            throw std::out_of_range("fixed TSL delta map index out of range");
        }

        [[nodiscard]] static const void *fixed_delta_map_key_at_index(const void *context, const void *memory,
                                                                      std::size_t index)
        {
            const auto *state = ctx(context);
            return &state->ordinal_keys[nth_modified_child(state, memory, index)];
        }

        [[nodiscard]] static const ValueTypeBinding *fixed_delta_map_key_binding(const void *context, const void *,
                                                                                 std::size_t) noexcept
        {
            return ctx(context)->ordinal_key_binding;
        }

        [[nodiscard]] static const void *fixed_delta_map_value_at_index(const void *context, const void *memory,
                                                                        std::size_t index)
        {
            const auto *state = ctx(context);
            return child_delta_view(state, memory, nth_modified_child(state, memory, index)).data();
        }

        [[nodiscard]] static const ValueTypeBinding *fixed_delta_map_value_binding(const void *context,
                                                                                   const void *) noexcept
        {
            return ctx(context)->delta_map_value_binding;
        }

        [[nodiscard]] static bool fixed_delta_map_contains(const void *context, const void *memory, const void *key)
        {
            const auto *state = ctx(context);
            const auto  index = *MemoryUtils::cast<std::int64_t>(key);
            return index >= 0 && static_cast<std::size_t>(index) < state->element_count() &&
                   child_modified_for_parent_time(state, memory, static_cast<std::size_t>(index));
        }

        [[nodiscard]] static const void *fixed_delta_map_value_at(const void *context, const void *memory,
                                                                  const void *key)
        {
            const auto *state = ctx(context);
            const auto  index = *MemoryUtils::cast<std::int64_t>(key);
            if (index < 0)
            {
                return nullptr;
            }
            const auto slot = static_cast<std::size_t>(index);
            if (slot >= state->element_count() || !child_modified_for_parent_time(state, memory, slot))
            {
                return nullptr;
            }
            return child_delta_view(state, memory, slot).data();
        }

        [[nodiscard]] static ValueView fixed_delta_map_key_projector(const void *context, const void *,
                                                                     std::size_t index)
        {
            const auto *state = ctx(context);
            return ValueView{state->ordinal_key_binding, &state->ordinal_keys[index]};
        }

        [[nodiscard]] static ValueView fixed_delta_map_value_projector(const void *context, const void *memory,
                                                                       std::size_t index)
        {
            return child_delta_view(ctx(context), memory, index);
        }

        [[nodiscard]] static std::pair<ValueView, ValueView>
        fixed_delta_map_kv_projector(const void *context, const void *memory, std::size_t index)
        {
            return {fixed_delta_map_key_projector(context, memory, index),
                    fixed_delta_map_value_projector(context, memory, index)};
        }

        [[nodiscard]] static Range<ValueView> fixed_delta_map_make_keys_range(const void *context, const void *memory)
        {
            return Range<ValueView>{
                .context   = context,
                .memory    = memory,
                .limit     = fixed_indexed_size(context, memory),
                .predicate = &fixed_delta_child_predicate,
                .projector = &fixed_delta_map_key_projector,
            };
        }

        [[nodiscard]] static Range<ValueView> fixed_delta_map_make_values_range(const void *context, const void *memory)
        {
            return Range<ValueView>{
                .context   = context,
                .memory    = memory,
                .limit     = fixed_indexed_size(context, memory),
                .predicate = &fixed_delta_child_predicate,
                .projector = &fixed_delta_map_value_projector,
            };
        }

        [[nodiscard]] static KeyValueRange<ValueView, ValueView> fixed_delta_map_make_kv_range(const void *context,
                                                                                               const void *memory)
        {
            return KeyValueRange<ValueView, ValueView>{
                .context   = context,
                .memory    = memory,
                .limit     = fixed_indexed_size(context, memory),
                .predicate = &fixed_delta_child_predicate,
                .projector = &fixed_delta_map_kv_projector,
            };
        }

        [[nodiscard]] static SetView fixed_delta_map_key_set(const void *context, const ValueTypeBinding *,
                                                             const void *memory)
        {
            const auto *state = ctx(context);
            return SetView{ValueView{state->delta_key_set_binding, memory}};
        }

        [[nodiscard]] static std::size_t fixed_delta_map_hash(const void *context, const void *memory)
        {
            const auto *state  = ctx(context);
            std::size_t result = 0;
            for (std::size_t index = 0; index < state->element_count(); ++index)
            {
                if (!child_modified_for_parent_time(state, memory, index))
                {
                    continue;
                }
                const auto key_hash   = state->ordinal_key_binding->checked_ops().hash(&state->ordinal_keys[index]);
                const auto value_hash = view_hash(child_delta_view(state, memory, index));
                result ^= combine_hash(key_hash, value_hash);
            }
            return result;
        }

        [[nodiscard]] static bool fixed_delta_map_equals(const void *context, const void *lhs, const void *rhs) noexcept
        {
            if (lhs == nullptr || rhs == nullptr)
            {
                return lhs == rhs;
            }
            return fallback_on_exception(false, [&] {
                const auto *state = ctx(context);
                if (fixed_delta_map_size(context, lhs) != fixed_delta_map_size(context, rhs))
                {
                    return false;
                }
                for (std::size_t index = 0; index < state->element_count(); ++index)
                {
                    if (!child_modified_for_parent_time(state, lhs, index))
                    {
                        continue;
                    }
                    if (!child_modified_for_parent_time(state, rhs, index))
                    {
                        return false;
                    }
                    if (!child_delta_view(state, lhs, index).equals(child_delta_view(state, rhs, index)))
                    {
                        return false;
                    }
                }
                return true;
            });
        }

        [[nodiscard]] static std::partial_ordering fixed_delta_map_compare(const void *context, const void *lhs,
                                                                           const void *rhs) noexcept
        {
            if (const auto order = value_ops_detail::null_order(lhs, rhs))
            {
                return *order;
            }
            return fixed_delta_map_equals(context, lhs, rhs) ? std::partial_ordering::equivalent
                                                             : std::partial_ordering::unordered;
        }

        [[nodiscard]] static std::string fixed_delta_map_to_string(const void *context, const void *memory)
        {
            const auto        *state = ctx(context);
            fmt::memory_buffer out;
            fmt::format_to(std::back_inserter(out), "{{");
            bool first = true;
            for (std::size_t index = 0; index < state->element_count(); ++index)
            {
                if (!child_modified_for_parent_time(state, memory, index))
                {
                    continue;
                }
                if (!first)
                {
                    fmt::format_to(std::back_inserter(out), ", ");
                }
                first = false;
                fmt::format_to(std::back_inserter(out), "{}: {}", state->ordinal_keys[index],
                               child_delta_view(state, memory, index).to_string());
            }
            fmt::format_to(std::back_inserter(out), "}}");
            return fmt::to_string(out);
        }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        [[nodiscard]] static nb::object fixed_delta_map_to_python(const void *context, const void *memory)
        {
            const auto *state = ctx(context);
            nb::dict    result;
            for (std::size_t index = 0; index < state->element_count(); ++index)
            {
                if (!child_modified_for_parent_time(state, memory, index)) { continue; }
                result[nb::int_{state->ordinal_keys[index]}] = child_delta_view(state, memory, index).to_python();
            }
            return result;
        }
#endif

        [[nodiscard]] static std::size_t fixed_delta_key_set_hash(const void *context, const void *memory)
        {
            const auto *state  = ctx(context);
            std::size_t result = 0;
            for (std::size_t index = 0; index < state->element_count(); ++index)
            {
                if (!child_modified_for_parent_time(state, memory, index))
                {
                    continue;
                }
                result ^= state->ordinal_key_binding->checked_ops().hash(&state->ordinal_keys[index]);
            }
            return result;
        }

        [[nodiscard]] static bool fixed_delta_key_set_equals(const void *context, const void *lhs,
                                                             const void *rhs) noexcept
        {
            if (lhs == nullptr || rhs == nullptr)
            {
                return lhs == rhs;
            }
            const auto *state = ctx(context);
            for (std::size_t index = 0; index < state->element_count(); ++index)
            {
                if (child_modified_for_parent_time(state, lhs, index) !=
                    child_modified_for_parent_time(state, rhs, index))
                {
                    return false;
                }
            }
            return true;
        }

        [[nodiscard]] static std::partial_ordering fixed_delta_key_set_compare(const void *context, const void *lhs,
                                                                               const void *rhs) noexcept
        {
            if (const auto order = value_ops_detail::null_order(lhs, rhs))
            {
                return *order;
            }
            const auto lhs_size = fixed_delta_map_size(context, lhs);
            const auto rhs_size = fixed_delta_map_size(context, rhs);
            if (lhs_size < rhs_size)
            {
                return std::partial_ordering::less;
            }
            if (lhs_size > rhs_size)
            {
                return std::partial_ordering::greater;
            }
            return fixed_delta_key_set_equals(context, lhs, rhs) ? std::partial_ordering::equivalent
                                                                 : std::partial_ordering::unordered;
        }

        [[nodiscard]] static std::string fixed_delta_key_set_to_string(const void *context, const void *memory)
        {
            const auto        *state = ctx(context);
            fmt::memory_buffer out;
            fmt::format_to(std::back_inserter(out), "{{");
            bool first = true;
            for (std::size_t index = 0; index < state->element_count(); ++index)
            {
                if (!child_modified_for_parent_time(state, memory, index))
                {
                    continue;
                }
                if (!first)
                {
                    fmt::format_to(std::back_inserter(out), ", ");
                }
                first = false;
                fmt::format_to(std::back_inserter(out), "{}", state->ordinal_keys[index]);
            }
            fmt::format_to(std::back_inserter(out), "}}");
            return fmt::to_string(out);
        }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        [[nodiscard]] static nb::object fixed_delta_key_set_to_python(const void *context, const void *memory)
        {
            const auto *state = ctx(context);
            nb::set     result;
            for (std::size_t index = 0; index < state->element_count(); ++index)
            {
                if (!child_modified_for_parent_time(state, memory, index)) { continue; }
                result.add(nb::int_{state->ordinal_keys[index]});
            }
            return result;
        }
#endif

        [[nodiscard]] static bool fixed_copy_value_from(const void *context, void *memory, const ValueView &source,
                                                        engine_time_t modified_time)
        {
            if (memory == nullptr)
            {
                throw std::logic_error("fixed TSData copy requires live memory");
            }
            if (!source.has_value())
            {
                throw std::invalid_argument("fixed TSData copy requires a live source value");
            }
            if (modified_time == MIN_DT)
            {
                throw std::invalid_argument("fixed TSData copy requires a concrete engine time");
            }

            const auto *state = ctx(context);
            if (source.schema() != state->schema->value_schema)
            {
                throw std::invalid_argument("fixed TSData copy requires the parent value schema");
            }
            const IndexedValueView source_values{source};
            if (source_values.size() != state->element_count())
            {
                throw std::invalid_argument("fixed TSData copy source has the wrong child count");
            }

            bool newly_modified = false;
            for (std::size_t index = 0; index < state->element_count(); ++index)
            {
                const auto *child = state->element_binding(index);
                const auto &ops   = child_ops(*child);
                void       *data  = child_data(state, memory, index);
                if (ops.copy_value_from_impl(ops.context, data, source_values.at(index), modified_time))
                {
                    auto *tracking = ops.mutable_tracking_impl(ops.context, data);
                    if (tracking == nullptr) { throw std::logic_error("fixed TSData child has no tracking record"); }
                    if (!tracking->record_modified(modified_time))
                    {
                        throw std::logic_error("fixed TSData child reported a duplicate modification");
                    }
                    newly_modified = true;
                }
            }
            return newly_modified;
        }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        [[nodiscard]] static nb::object fixed_to_python(const void *context, const void *memory)
        {
            const auto *state = ctx(context);
            return state->layout_ptr()->value_binding->checked_ops().to_python(fixed_value_memory(context, memory));
        }

        [[nodiscard]] static nb::object fixed_delta_to_python(const void *context,
                                                              const void *memory,
                                                              engine_time_t evaluation_time)
        {
            const auto *state = ctx(context);
            if (fixed_tracking(state, memory)->last_modified_time != evaluation_time) { return nb::none(); }
            return state->layout_ptr()->delta_binding->checked_ops().to_python(fixed_delta_memory(context, memory));
        }

        [[nodiscard]] static bool is_python_sequence(nb::handle source)
        {
            nb::object object = nb::borrow<nb::object>(source);
            return nb::isinstance<nb::list>(object) || nb::isinstance<nb::tuple>(object);
        }

        [[nodiscard]] static bool is_python_mapping(nb::handle source)
        {
            nb::object object = nb::borrow<nb::object>(source);
            return nb::isinstance<nb::dict>(object) || nb::hasattr(object, "items");
        }

        template <typename Visitor>
        static void for_each_python_mapping_item(nb::handle source, const char *what, Visitor visitor)
        {
            if (!is_python_mapping(source))
            {
                throw std::invalid_argument(std::string{what} + " expects a Python mapping");
            }

            nb::object   object = nb::borrow<nb::object>(source);
            nb::object   items  = object.attr("items")();
            nb::iterator it     = nb::iter(items);
            while (it != nb::iterator::sentinel())
            {
                nb::tuple pair = nb::cast<nb::tuple>(*it);
                if (pair.size() != 2)
                {
                    throw std::invalid_argument(std::string{what} + " items() must yield key/value pairs");
                }
                visitor(nb::borrow<nb::object>(pair[0]), nb::borrow<nb::object>(pair[1]));
                ++it;
            }
        }

        [[nodiscard]] static std::size_t field_index_by_name(const FixedTSDataContext *state,
                                                              std::string_view          name) noexcept
        {
            for (std::size_t index = 0; index < state->element_count(); ++index)
            {
                const char *field_name = state->schema->fields()[index].name;
                if (field_name != nullptr && name == field_name) { return index; }
            }
            return TS_DATA_NO_CHILD_ID;
        }

        [[nodiscard]] static bool fixed_child_update_from_python(const FixedTSDataContext *state,
                                                                 void                     *memory,
                                                                 std::size_t               index,
                                                                 nb::handle                source,
                                                                 engine_time_t             modified_time)
        {
            if (source.is_none()) { return false; }

            const auto *child = state->element_binding(index);
            const auto &ops   = child_ops(*child);
            void       *data  = child_data(state, memory, index);
            if (!ops.from_python_impl(ops.context, data, source, modified_time)) { return false; }

            auto *tracking = ops.mutable_tracking_impl(ops.context, data);
            if (tracking == nullptr) { throw std::logic_error("fixed TSData child has no tracking record"); }
            if (!tracking->record_modified(modified_time))
            {
                throw std::logic_error("fixed TSData child reported a duplicate Python update modification");
            }
            return true;
        }

        [[nodiscard]] static bool fixed_from_python_sequence(const FixedTSDataContext *state,
                                                             void                     *memory,
                                                             nb::handle                source,
                                                             engine_time_t             modified_time,
                                                             const char                *what)
        {
            if (!is_python_sequence(source))
            {
                throw std::invalid_argument(std::string{what} + " expects a Python list or tuple");
            }

            nb::object   object   = nb::borrow<nb::object>(source);
            nb::sequence sequence = nb::cast<nb::sequence>(object);
            const auto   count    = static_cast<std::size_t>(nb::len(sequence));
            if (count != state->element_count())
            {
                throw std::invalid_argument(
                    fmt::format("{} expects {} elements, got {}", what, state->element_count(), count));
            }

            for (std::size_t index = 0; index < count; ++index)
            {
                nb::object child_source = sequence[index];
                static_cast<void>(fixed_child_update_from_python(state, memory, index, child_source, modified_time));
            }
            return true;
        }

        [[nodiscard]] static bool fixed_from_python_bundle(const FixedTSDataContext *state,
                                                           void                     *memory,
                                                           nb::handle                source,
                                                           engine_time_t             modified_time)
        {
            nb::object object = nb::borrow<nb::object>(source);
            if (is_python_mapping(source))
            {
                bool touched = true;
                for_each_python_mapping_item(source, "TSB from_python", [&](nb::handle key, nb::handle value) {
                    const auto field = nb::cast<std::string>(key);
                    const auto index = field_index_by_name(state, field);
                    if (index == TS_DATA_NO_CHILD_ID)
                    {
                        throw std::invalid_argument(fmt::format("TSB from_python unknown field '{}'", field));
                    }
                    static_cast<void>(fixed_child_update_from_python(state, memory, index, value, modified_time));
                });
                return touched;
            }

            if (is_python_sequence(source))
            {
                return fixed_from_python_sequence(state, memory, source, modified_time, "TSB from_python");
            }

            bool saw_field = false;
            for (std::size_t index = 0; index < state->element_count(); ++index)
            {
                const char *name = state->schema->fields()[index].name;
                if (name == nullptr || *name == '\0')
                {
                    throw std::invalid_argument("TSB from_python has an unnamed field and cannot load attributes");
                }
                if (!nb::hasattr(object, name)) { continue; }
                saw_field = true;
                nb::object child_source = nb::getattr(object, name);
                static_cast<void>(fixed_child_update_from_python(state, memory, index, child_source, modified_time));
            }
            if (!saw_field)
            {
                throw std::invalid_argument("TSB from_python expects a mapping, sequence, or field attributes");
            }
            return true;
        }

        [[nodiscard]] static bool fixed_from_python_list_mapping(const FixedTSDataContext *state,
                                                                 void                     *memory,
                                                                 nb::handle                source,
                                                                 engine_time_t             modified_time)
        {
            for_each_python_mapping_item(source, "fixed TSL from_python", [&](nb::handle key, nb::handle value) {
                const auto index = nb::cast<std::size_t>(key);
                if (index >= state->element_count())
                {
                    throw std::out_of_range("fixed TSL from_python index out of range");
                }
                static_cast<void>(fixed_child_update_from_python(state, memory, index, value, modified_time));
            });
            return true;
        }

        [[nodiscard]] static bool fixed_from_python(const void *context,
                                                    void       *memory,
                                                    nb::handle  source,
                                                    engine_time_t modified_time)
        {
            if (memory == nullptr)
            {
                throw std::logic_error("fixed TSData from_python requires live memory");
            }
            if (source.is_none())
            {
                throw std::invalid_argument("fixed TSData from_python requires a non-None source");
            }
            if (modified_time == MIN_DT)
            {
                throw std::invalid_argument("fixed TSData from_python requires a concrete engine time");
            }

            const auto *state = ctx(context);
            const bool  first_for_parent = fixed_tracking(state, memory)->last_modified_time != modified_time;
            bool        touched = false;
            if (state->schema->kind == TSTypeKind::TSB)
            {
                touched = fixed_from_python_bundle(state, memory, source, modified_time);
            }
            else if (is_python_mapping(source))
            {
                touched = fixed_from_python_list_mapping(state, memory, source, modified_time);
            }
            else
            {
                touched = fixed_from_python_sequence(state, memory, source, modified_time, "fixed TSL from_python");
            }
            return first_for_parent && touched;
        }
#endif
    };

    struct FixedTSDataContextKey
    {
        const TSValueTypeMetaData      *schema{nullptr};
        const MemoryUtils::StoragePlan *plan{nullptr};
        std::size_t                     value_offset{0};
        std::size_t                     aux_offset{0};
        std::size_t                     tracking_offset{0};

        [[nodiscard]] bool operator==(const FixedTSDataContextKey &) const noexcept = default;
    };

    struct FixedTSDataContextKeyHash
    {
        [[nodiscard]] std::size_t operator()(const FixedTSDataContextKey &key) const noexcept
        {
            auto seed = combine_hash(std::hash<const TSValueTypeMetaData *>{}(key.schema),
                                     std::hash<const MemoryUtils::StoragePlan *>{}(key.plan));
            seed      = combine_hash(seed, key.value_offset);
            seed      = combine_hash(seed, key.aux_offset);
            seed      = combine_hash(seed, key.tracking_offset);
            return seed;
        }
    };

    using FixedContextMap =
        std::unordered_map<FixedTSDataContextKey, std::unique_ptr<FixedTSDataContext>, FixedTSDataContextKeyHash>;

    [[nodiscard]] FixedContextMap &fixed_ts_data_contexts() noexcept
    {
        static FixedContextMap contexts;
        return contexts;
    }

    [[nodiscard]] std::mutex &fixed_ts_data_context_mutex() noexcept
    {
        static std::mutex mutex;
        return mutex;
    }

    [[nodiscard]] FixedTSDataContext &fixed_ts_data_context(const TSValueTypeMetaData      &schema,
                                                            const MemoryUtils::StoragePlan &plan,
                                                            std::size_t value_offset, std::size_t aux_offset,
                                                            std::size_t tracking_offset,
                                                            std::vector<const TSDataBinding *> element_bindings)
    {
        std::lock_guard<std::mutex> lock(fixed_ts_data_context_mutex());
        auto                       &contexts = fixed_ts_data_contexts();
        const FixedTSDataContextKey key{&schema, &plan, value_offset, aux_offset, tracking_offset};
        if (const auto it = contexts.find(key); it != contexts.end())
        {
            return *it->second;
        }

        auto  context = std::make_unique<FixedTSDataContext>(schema, plan, value_offset, aux_offset, tracking_offset,
                                                             std::move(element_bindings));
        auto *result  = context.get();
        contexts.emplace(key, std::move(context));
        result->bind_surfaces();
        return *result;
    }

    [[nodiscard]] const TSDataOps &fixed_structured_ts_data_ops(const TSValueTypeMetaData      &schema,
                                                                const MemoryUtils::StoragePlan &plan,
                                                                std::size_t value_offset, std::size_t aux_offset,
                                                                std::size_t tracking_offset,
                                                                std::vector<const TSDataBinding *> element_bindings)
    {
        auto &context =
            fixed_ts_data_context(schema, plan, value_offset, aux_offset, tracking_offset, std::move(element_bindings));
        return context.ops;
    }

    void clear_fixed_ts_data_contexts() noexcept
    {
        std::lock_guard<std::mutex> lock(fixed_ts_data_context_mutex());
        fixed_ts_data_contexts().clear();
    }
} // namespace hgraph::ts_data_plan_factory_detail
