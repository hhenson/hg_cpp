#include "target_link_ops.h"

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/util/scope.h>

#include <array>
#include <stdexcept>
#include <utility>

namespace hgraph::detail
{
    namespace
    {
        template <typename Layout, typename Ops>
        struct TargetLinkContextFor final : TSInputTargetLinkContext
        {
            Layout layout{};
            Ops    ops{};
        };

        struct TargetLinkDictContext final : TSInputTargetLinkContext
        {
            TSDDataLayout dict_layout{};
            TSDDataOps    dict_ops{};
            TSSDataOps    key_set_ops{};
        };

        [[nodiscard]] constexpr std::size_t ts_kind_index(TSTypeKind kind) noexcept
        {
            return static_cast<std::size_t>(kind);
        }

        [[nodiscard]] const void *advance(const void *memory, std::size_t offset) noexcept
        {
            return static_cast<const std::byte *>(memory) + offset;
        }

        [[nodiscard]] void *advance(void *memory, std::size_t offset) noexcept
        {
            return static_cast<std::byte *>(memory) + offset;
        }

        [[nodiscard]] const TSDataLayout *target_link_layout(const void *context) noexcept
        {
            return static_cast<const TSInputTargetLinkContext *>(context)->active_layout;
        }

        [[nodiscard]] const TSDataTracking *target_link_tracking(const void *context, const void *memory) noexcept
        {
            return &target_link_storage_at(*static_cast<const TSInputTargetLinkContext *>(context), memory)->tracking;
        }

        [[nodiscard]] TSDataTracking *target_link_mutable_tracking(const void *context, void *memory) noexcept
        {
            return &target_link_storage_at(*static_cast<const TSInputTargetLinkContext *>(context), memory)->tracking;
        }

        [[nodiscard]] bool target_link_has_current_value(const void *context, const void *memory)
        {
            const auto *link = target_link_storage_at(*static_cast<const TSInputTargetLinkContext *>(context), memory);
            const auto  target = link != nullptr ? link->target_view() : TSDataView{};
            return target.valid() && target.has_current_value();
        }

        [[nodiscard]] bool target_link_all_valid(const void *context, const void *memory)
        {
            const auto *link = target_link_storage_at(*static_cast<const TSInputTargetLinkContext *>(context), memory);
            const auto  target = link != nullptr ? link->target_view() : TSDataView{};
            return target.valid() && target.all_valid();
        }

        [[nodiscard]] const void *target_link_value_memory(const void *context, const void *memory)
        {
            const auto *link = target_link_storage_at(*static_cast<const TSInputTargetLinkContext *>(context), memory);
            const auto  target = link != nullptr ? link->target_view() : TSDataView{};
            return target.valid() ? target.value().data() : nullptr;
        }

        [[nodiscard]] const void *target_link_delta_memory(const void *context, const void *memory)
        {
            const auto *link = target_link_storage_at(*static_cast<const TSInputTargetLinkContext *>(context), memory);
            const auto  target = link != nullptr ? link->target_view() : TSDataView{};
            return target.valid() ? target.delta_value(link->tracking.last_modified_time).data() : nullptr;
        }

        void target_link_cleanup_delta(const void *, void *, engine_time_t) {}

        [[nodiscard]] TSDataView target_link_target_view(const void *context, const void *memory)
        {
            const auto *link = target_link_storage_at(*static_cast<const TSInputTargetLinkContext *>(context), memory);
            return link != nullptr ? link->target_view() : TSDataView{};
        }

        [[nodiscard]] TSDDataView target_link_dict_view(const void *context, const void *memory)
        {
            auto target = target_link_target_view(context, memory);
            if (!target.valid()) { throw std::logic_error("TSInput target-link dict access requires a bound target"); }
            return target.as_dict();
        }

        [[nodiscard]] std::size_t target_link_slot_capacity_or_zero(const void *context, const void *memory) noexcept
        {
            return fallback_on_exception(std::size_t{0}, [&] {
                auto target = target_link_target_view(context, memory);
                if (!target.valid()) { return std::size_t{0}; }
                if (target.schema() != nullptr && target.schema()->kind == TSTypeKind::TSD)
                {
                    return target.as_dict().slot_capacity();
                }
                return target.as_set().slot_capacity();
            });
        }

        [[nodiscard]] Range<ValueView> target_link_empty_value_range() noexcept
        {
            return Range<ValueView>{.context = nullptr, .memory = nullptr, .limit = 0,
                                    .predicate = nullptr, .projector = nullptr};
        }

        [[nodiscard]] Range<TSDataView> target_link_empty_ts_data_range() noexcept
        {
            return Range<TSDataView>{.context = nullptr, .memory = nullptr, .limit = 0,
                                     .predicate = nullptr, .projector = nullptr};
        }

        [[nodiscard]] KeyValueRange<ValueView, TSDataView> target_link_empty_ts_data_kv_range() noexcept
        {
            return KeyValueRange<ValueView, TSDataView>{.context = nullptr, .memory = nullptr, .limit = 0,
                                                        .predicate = nullptr, .projector = nullptr};
        }

        [[nodiscard]] std::size_t target_link_set_size(const void *context, const void *memory) noexcept
        {
            return fallback_on_exception(std::size_t{0}, [&] {
                auto target = target_link_target_view(context, memory);
                if (!target.valid()) { return std::size_t{0}; }
                if (target.schema() != nullptr && target.schema()->kind == TSTypeKind::TSD)
                {
                    return target.as_dict().size();
                }
                return target.as_set().size();
            });
        }

        [[nodiscard]] std::size_t target_link_set_slot_capacity(const void *context, const void *memory) noexcept
        {
            return target_link_slot_capacity_or_zero(context, memory);
        }

        [[nodiscard]] bool target_link_set_slot_occupied(const void *context, const void *memory, std::size_t slot)
        {
            auto target = target_link_target_view(context, memory);
            if (!target.valid()) { return false; }
            if (target.schema() != nullptr && target.schema()->kind == TSTypeKind::TSD)
            {
                return target.as_dict().slot_occupied(slot);
            }
            return target.as_set().slot_occupied(slot);
        }

        [[nodiscard]] bool target_link_set_slot_live(const void *context, const void *memory, std::size_t slot)
        {
            auto target = target_link_target_view(context, memory);
            if (!target.valid()) { return false; }
            if (target.schema() != nullptr && target.schema()->kind == TSTypeKind::TSD)
            {
                return target.as_dict().slot_live(slot);
            }
            return target.as_set().slot_live(slot);
        }

        [[nodiscard]] bool target_link_set_slot_added(const void *context, const void *memory, std::size_t slot)
        {
            auto target = target_link_target_view(context, memory);
            if (!target.valid()) { return false; }
            if (target.schema() != nullptr && target.schema()->kind == TSTypeKind::TSD)
            {
                return target.as_dict().slot_added(slot);
            }
            return target.as_set().slot_added(slot);
        }

        [[nodiscard]] bool target_link_set_slot_removed(const void *context, const void *memory, std::size_t slot)
        {
            auto target = target_link_target_view(context, memory);
            if (!target.valid()) { return false; }
            if (target.schema() != nullptr && target.schema()->kind == TSTypeKind::TSD)
            {
                return target.as_dict().slot_removed(slot);
            }
            return target.as_set().slot_removed(slot);
        }

        [[nodiscard]] const void *target_link_set_key_at_slot(const void *context,
                                                              const void *memory,
                                                              std::size_t slot)
        {
            auto target = target_link_target_view(context, memory);
            if (!target.valid()) { throw std::logic_error("TSInput target-link set access requires a bound target"); }
            if (target.schema() != nullptr && target.schema()->kind == TSTypeKind::TSD)
            {
                return target.as_dict().key_at_slot(slot).data();
            }
            return target.as_set().at_slot(slot).data();
        }

        [[nodiscard]] bool target_link_set_contains(const void *context, const void *memory, const ValueView &key)
        {
            auto target = target_link_target_view(context, memory);
            if (!target.valid()) { return false; }
            if (target.schema() != nullptr && target.schema()->kind == TSTypeKind::TSD)
            {
                return target.as_dict().contains(key);
            }
            return target.as_set().contains(key);
        }

        [[nodiscard]] std::size_t target_link_set_find_slot(const void *context,
                                                            const void *memory,
                                                            const ValueView &key)
        {
            auto target = target_link_target_view(context, memory);
            if (!target.valid()) { return TS_DATA_NO_CHILD_ID; }
            if (target.schema() != nullptr && target.schema()->kind == TSTypeKind::TSD)
            {
                return target.as_dict().find_slot(key);
            }
            return target.as_set().find_slot(key);
        }

        [[nodiscard]] ValueView target_link_set_key_projector(const void *context,
                                                              const void *memory,
                                                              std::size_t slot)
        {
            const auto *state = static_cast<const TSInputTargetLinkContext *>(context);
            const auto *layout = static_cast<const TSSDataLayout *>(state->active_layout);
            return ValueView{layout->key_binding, target_link_set_key_at_slot(context, memory, slot)};
        }

        [[nodiscard]] Range<ValueView> target_link_set_range(
            const void *context,
            const void *memory,
            Range<ValueView>::predicate_fn predicate)
        {
            if (target_link_target_view(context, memory).valid())
            {
                return Range<ValueView>{.context = context, .memory = memory,
                                        .limit = target_link_set_slot_capacity(context, memory),
                                        .predicate = predicate, .projector = &target_link_set_key_projector};
            }
            return target_link_empty_value_range();
        }

        [[nodiscard]] Range<ValueView> target_link_set_live_range(const void *context, const void *memory)
        {
            return target_link_set_range(context, memory, &target_link_set_slot_live);
        }

        [[nodiscard]] Range<ValueView> target_link_set_added_range(const void *context, const void *memory)
        {
            return target_link_set_range(context, memory, &target_link_set_slot_added);
        }

        [[nodiscard]] Range<ValueView> target_link_set_removed_range(const void *context, const void *memory)
        {
            return target_link_set_range(context, memory, &target_link_set_slot_removed);
        }

        [[nodiscard]] SlotTSDataMutationResult target_link_insert_key(const void *, void *, const ValueView &,
                                                                      engine_time_t)
        {
            throw std::logic_error("TSInput target-link set mutation is not supported");
        }

        [[nodiscard]] SlotTSDataMutationResult target_link_remove_key(const void *, void *, const ValueView &,
                                                                      engine_time_t)
        {
            throw std::logic_error("TSInput target-link set mutation is not supported");
        }

        [[nodiscard]] bool target_link_touch_slots(const void *, void *, engine_time_t)
        {
            throw std::logic_error("TSInput target-link set mutation is not supported");
        }

        void target_link_reserve_slots(const void *, void *, std::size_t)
        {
            throw std::logic_error("TSInput target-link set reserve is not supported");
        }

        void target_link_subscribe_slot_observer(const void *, void *, SlotObserver *)
        {
            throw std::logic_error("TSInput target-link slot observers are not supported");
        }

        void target_link_unsubscribe_slot_observer(const void *, void *, SlotObserver *)
        {
            throw std::logic_error("TSInput target-link slot observers are not supported");
        }

        [[nodiscard]] bool target_link_dict_slot_modified(const void *context,
                                                          const void *memory,
                                                          std::size_t slot)
        {
            auto target = target_link_target_view(context, memory);
            return target.valid() && target.as_dict().slot_modified(slot);
        }

        [[nodiscard]] const void *target_link_dict_child_at_slot(const void *context,
                                                                 const void *memory,
                                                                 std::size_t slot)
        {
            return target_link_dict_view(context, memory).at_slot(slot).data();
        }

        [[nodiscard]] TSDataView target_link_dict_ts_projector(const void *context,
                                                               const void *memory,
                                                               std::size_t slot)
        {
            return target_link_dict_view(context, memory).at_slot(slot);
        }

        [[nodiscard]] std::pair<ValueView, TSDataView> target_link_dict_kv_projector(const void *context,
                                                                                     const void *memory,
                                                                                     std::size_t slot)
        {
            return {target_link_set_key_projector(context, memory, slot),
                    target_link_dict_ts_projector(context, memory, slot)};
        }

        [[nodiscard]] bool target_link_dict_slot_valid(const void *context, const void *memory, std::size_t slot)
        {
            auto target = target_link_target_view(context, memory);
            return target.valid() && target.as_dict().slot_live(slot) && target.as_dict().at_slot(slot).valid();
        }

        [[nodiscard]] Range<TSDataView> target_link_dict_ts_range(
            const void *context,
            const void *memory,
            Range<TSDataView>::predicate_fn predicate)
        {
            if (target_link_target_view(context, memory).valid())
            {
                return Range<TSDataView>{.context = context, .memory = memory,
                                         .limit = target_link_set_slot_capacity(context, memory),
                                         .predicate = predicate, .projector = &target_link_dict_ts_projector};
            }
            return target_link_empty_ts_data_range();
        }

        [[nodiscard]] KeyValueRange<ValueView, TSDataView> target_link_dict_kv_range(
            const void *context,
            const void *memory,
            KeyValueRange<ValueView, TSDataView>::predicate_fn predicate)
        {
            if (target_link_target_view(context, memory).valid())
            {
                return KeyValueRange<ValueView, TSDataView>{.context = context, .memory = memory,
                                                            .limit = target_link_set_slot_capacity(context, memory),
                                                            .predicate = predicate,
                                                            .projector = &target_link_dict_kv_projector};
            }
            return target_link_empty_ts_data_kv_range();
        }

        [[nodiscard]] Range<ValueView> target_link_dict_valid_keys_range(const void *context, const void *memory)
        {
            return target_link_set_range(context, memory, &target_link_dict_slot_valid);
        }

        [[nodiscard]] Range<TSDataView> target_link_dict_values_range(const void *context, const void *memory)
        {
            return target_link_dict_ts_range(context, memory, &target_link_set_slot_live);
        }

        [[nodiscard]] Range<TSDataView> target_link_dict_valid_values_range(const void *context, const void *memory)
        {
            return target_link_dict_ts_range(context, memory, &target_link_dict_slot_valid);
        }

        [[nodiscard]] Range<ValueView> target_link_dict_modified_keys_range(const void *context, const void *memory)
        {
            return target_link_set_range(context, memory, &target_link_dict_slot_modified);
        }

        [[nodiscard]] Range<TSDataView> target_link_dict_modified_values_range(const void *context,
                                                                               const void *memory)
        {
            return target_link_dict_ts_range(context, memory, &target_link_dict_slot_modified);
        }

        [[nodiscard]] Range<TSDataView> target_link_dict_added_values_range(const void *context, const void *memory)
        {
            return target_link_dict_ts_range(context, memory, &target_link_set_slot_added);
        }

        [[nodiscard]] Range<TSDataView> target_link_dict_removed_values_range(const void *context, const void *memory)
        {
            return target_link_dict_ts_range(context, memory, &target_link_set_slot_removed);
        }

        [[nodiscard]] KeyValueRange<ValueView, TSDataView> target_link_dict_items_range(const void *context,
                                                                                        const void *memory)
        {
            return target_link_dict_kv_range(context, memory, &target_link_set_slot_live);
        }

        [[nodiscard]] KeyValueRange<ValueView, TSDataView> target_link_dict_valid_items_range(const void *context,
                                                                                              const void *memory)
        {
            return target_link_dict_kv_range(context, memory, &target_link_dict_slot_valid);
        }

        [[nodiscard]] KeyValueRange<ValueView, TSDataView> target_link_dict_modified_items_range(const void *context,
                                                                                                 const void *memory)
        {
            return target_link_dict_kv_range(context, memory, &target_link_dict_slot_modified);
        }

        [[nodiscard]] KeyValueRange<ValueView, TSDataView> target_link_dict_added_items_range(const void *context,
                                                                                              const void *memory)
        {
            return target_link_dict_kv_range(context, memory, &target_link_set_slot_added);
        }

        [[nodiscard]] KeyValueRange<ValueView, TSDataView> target_link_dict_removed_items_range(const void *context,
                                                                                                const void *memory)
        {
            return target_link_dict_kv_range(context, memory, &target_link_set_slot_removed);
        }

        [[nodiscard]] std::size_t target_link_indexed_size(const void *context, const void *memory) noexcept
        {
            return fallback_on_exception(std::size_t{0}, [&] {
                auto target = target_link_target_view(context, memory);
                if (!target.valid()) { return std::size_t{0}; }
                const auto *schema = static_cast<const TSInputTargetLinkContext *>(context)->schema;
                if (schema != nullptr && schema->kind == TSTypeKind::TSB) { return target.as_bundle().size(); }
                return target.as_list().size();
            });
        }

        [[nodiscard]] TSDataView target_link_indexed_child(const void *context,
                                                           const void *memory,
                                                           std::size_t index)
        {
            auto target = target_link_target_view(context, memory);
            if (!target.valid()) { return {}; }
            const auto *schema = static_cast<const TSInputTargetLinkContext *>(context)->schema;
            if (schema != nullptr && schema->kind == TSTypeKind::TSB)
            {
                auto bundle = target.as_bundle();
                return bundle.at(index);
            }
            auto list = target.as_list();
            return list.at(index);
        }

        [[nodiscard]] const TSDataBinding *target_link_indexed_element_binding(const void *context,
                                                                               const void *memory,
                                                                               std::size_t index) noexcept
        {
            return fallback_on_exception<const TSDataBinding *>(nullptr, [&] {
                return target_link_indexed_child(context, memory, index).binding();
            });
        }

        [[nodiscard]] const void *target_link_indexed_element_memory(const void *context,
                                                                     const void *memory,
                                                                     std::size_t index) noexcept
        {
            return fallback_on_exception<const void *>(nullptr, [&] {
                return target_link_indexed_child(context, memory, index).data();
            });
        }

        [[nodiscard]] void *target_link_indexed_mutable_element_memory(const void *context,
                                                                       void *memory,
                                                                       std::size_t index) noexcept
        {
            return const_cast<void *>(target_link_indexed_element_memory(context, memory, index));
        }

        [[nodiscard]] TSWDataView target_link_window_view(const void *context, const void *memory)
        {
            auto target = target_link_target_view(context, memory);
            if (!target.valid()) { throw std::logic_error("TSInput target-link window access requires a bound target"); }
            return target.as_window();
        }

        [[nodiscard]] std::size_t target_link_window_size(const void *context, const void *memory) noexcept
        {
            return fallback_on_exception(std::size_t{0}, [&] {
                auto target = target_link_target_view(context, memory);
                return target.valid() ? target.as_window().size() : std::size_t{0};
            });
        }

        [[nodiscard]] const void *target_link_window_element_at(const void *context,
                                                                const void *memory,
                                                                std::size_t index)
        {
            return target_link_window_view(context, memory).at(index).data();
        }

        [[nodiscard]] engine_time_t target_link_window_time_at(const void *context,
                                                               const void *memory,
                                                               std::size_t index)
        {
            return target_link_window_view(context, memory).time_at(index);
        }

        [[nodiscard]] const void *target_link_window_time_element_at(const void *context,
                                                                     const void *memory,
                                                                     std::size_t index)
        {
            return target_link_window_view(context, memory).time_value_at(index).data();
        }

        [[nodiscard]] std::size_t target_link_window_capacity(const void *context, const void *memory) noexcept
        {
            return fallback_on_exception(std::size_t{0}, [&] {
                auto target = target_link_target_view(context, memory);
                return target.valid() ? target.as_window().capacity() : std::size_t{0};
            });
        }

        [[nodiscard]] bool target_link_window_full(const void *context, const void *memory)
        {
            auto target = target_link_target_view(context, memory);
            return target.valid() && target.as_window().full();
        }

        void target_link_window_push(const void *, void *, const ValueView &, engine_time_t)
        {
            throw std::logic_error("TSInput target-link window mutation is not supported");
        }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        [[nodiscard]] nb::object target_link_to_python(const void *context, const void *memory)
        {
            const auto *link = target_link_storage_at(*static_cast<const TSInputTargetLinkContext *>(context), memory);
            const auto  target = link != nullptr ? link->target_view() : TSDataView{};
            return target.valid() ? target.value_to_python() : nb::none();
        }

        [[nodiscard]] nb::object target_link_delta_to_python(const void *context,
                                                             const void *memory,
                                                             engine_time_t evaluation_time)
        {
            const auto *link = target_link_storage_at(*static_cast<const TSInputTargetLinkContext *>(context), memory);
            const auto  target = link != nullptr ? link->target_view() : TSDataView{};
            return target.valid() ? target.delta_value_to_python(evaluation_time) : nb::none();
        }
#endif

        [[nodiscard]] TSDataOps target_link_base_ops(TSInputTargetLinkContext &context)
        {
            return TSDataOps{
                .context                   = &context,
                .kind                      = context.schema->kind,
                .allows_mutation           = true,
                .layout_impl               = &target_link_layout,
                .tracking_impl             = &target_link_tracking,
                .mutable_tracking_impl     = &target_link_mutable_tracking,
                .has_current_value_impl    = &target_link_has_current_value,
                .all_valid_impl            = &target_link_all_valid,
                .value_memory_impl         = &target_link_value_memory,
                .delta_memory_impl         = &target_link_delta_memory,
                .cleanup_delta_impl        = &target_link_cleanup_delta,
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                .to_python_impl            = &target_link_to_python,
                .delta_to_python_impl      = &target_link_delta_to_python,
#endif
            };
        }

        void configure_target_link_set_ops(TSSDataOps &ops)
        {
            ops.size_impl                      = &target_link_set_size;
            ops.slot_capacity_impl             = &target_link_set_slot_capacity;
            ops.slot_occupied_impl             = &target_link_set_slot_occupied;
            ops.slot_live_impl                 = &target_link_set_slot_live;
            ops.slot_added_impl                = &target_link_set_slot_added;
            ops.slot_removed_impl              = &target_link_set_slot_removed;
            ops.key_at_slot_impl               = &target_link_set_key_at_slot;
            ops.contains_impl                  = &target_link_set_contains;
            ops.find_slot_impl                 = &target_link_set_find_slot;
            ops.make_values_range_impl         = &target_link_set_live_range;
            ops.make_added_values_range_impl   = &target_link_set_added_range;
            ops.make_removed_values_range_impl = &target_link_set_removed_range;
            ops.insert_key_impl                = &target_link_insert_key;
            ops.remove_key_impl                = &target_link_remove_key;
            ops.touch_impl                     = &target_link_touch_slots;
            ops.reserve_impl                   = &target_link_reserve_slots;
            ops.subscribe_slot_observer_impl   = &target_link_subscribe_slot_observer;
            ops.unsubscribe_slot_observer_impl = &target_link_unsubscribe_slot_observer;
        }

        template <typename Context>
        void initialise_target_link_context(Context &context,
                                            const TSValueTypeMetaData &schema,
                                            std::size_t storage_offset,
                                            const TSDataBinding &regular_binding)
        {
            context.schema = &schema;
            context.storage_offset = storage_offset;
            context.regular_binding = &regular_binding;
        }

        [[nodiscard]] std::unique_ptr<TSInputTargetLinkContext>
        make_base_target_link_context(const TSValueTypeMetaData &schema,
                                      const MemoryUtils::StoragePlan &,
                                      std::size_t storage_offset,
                                      const TSDataBinding &regular_binding,
                                      const TSDataLayout &regular_layout)
        {
            auto context = std::make_unique<TargetLinkContextFor<TSDataLayout, TSDataOps>>();
            initialise_target_link_context(*context, schema, storage_offset, regular_binding);
            context->layout = regular_layout;
            context->layout.tracking_offset = storage_offset;
            context->ops = target_link_base_ops(*context);
            context->active_layout = &context->layout;
            context->active_ops = &context->ops;
            return context;
        }

        [[nodiscard]] std::unique_ptr<TSInputTargetLinkContext>
        make_set_target_link_context(const TSValueTypeMetaData &schema,
                                     const MemoryUtils::StoragePlan &,
                                     std::size_t storage_offset,
                                     const TSDataBinding &regular_binding,
                                     const TSDataLayout &regular_layout)
        {
            auto context = std::make_unique<TargetLinkContextFor<TSSDataLayout, TSSDataOps>>();
            initialise_target_link_context(*context, schema, storage_offset, regular_binding);
            context->layout = static_cast<const TSSDataLayout &>(regular_layout);
            context->layout.tracking_offset = storage_offset;
            context->ops = TSSDataOps{};
            static_cast<TSDataOps &>(context->ops) = target_link_base_ops(*context);
            configure_target_link_set_ops(context->ops);
            context->active_layout = &context->layout;
            context->active_ops = &context->ops;
            return context;
        }

        [[nodiscard]] std::unique_ptr<TSInputTargetLinkContext>
        make_dict_target_link_context(const TSValueTypeMetaData &schema,
                                      const MemoryUtils::StoragePlan &root_plan,
                                      std::size_t storage_offset,
                                      const TSDataBinding &regular_binding,
                                      const TSDataLayout &regular_layout)
        {
            auto context = std::make_unique<TargetLinkDictContext>();
            initialise_target_link_context(*context, schema, storage_offset, regular_binding);

            context->dict_layout = static_cast<const TSDDataLayout &>(regular_layout);
            context->dict_layout.tracking_offset = storage_offset;

            context->dict_ops = TSDDataOps{};
            static_cast<TSDataOps &>(context->dict_ops) = target_link_base_ops(*context);
            configure_target_link_set_ops(context->dict_ops);
            context->dict_ops.child_at_slot_impl = &target_link_dict_child_at_slot;
            context->dict_ops.slot_modified_impl = &target_link_dict_slot_modified;
            context->dict_ops.make_ts_values_range_impl = &target_link_dict_values_range;
            context->dict_ops.make_valid_keys_range_impl = &target_link_dict_valid_keys_range;
            context->dict_ops.make_valid_ts_values_range_impl = &target_link_dict_valid_values_range;
            context->dict_ops.make_modified_keys_range_impl = &target_link_dict_modified_keys_range;
            context->dict_ops.make_modified_ts_values_range_impl = &target_link_dict_modified_values_range;
            context->dict_ops.make_added_ts_values_range_impl = &target_link_dict_added_values_range;
            context->dict_ops.make_removed_ts_values_range_impl = &target_link_dict_removed_values_range;
            context->dict_ops.make_ts_kv_range_impl = &target_link_dict_items_range;
            context->dict_ops.make_valid_ts_kv_range_impl = &target_link_dict_valid_items_range;
            context->dict_ops.make_modified_ts_kv_range_impl = &target_link_dict_modified_items_range;
            context->dict_ops.make_added_ts_kv_range_impl = &target_link_dict_added_items_range;
            context->dict_ops.make_removed_ts_kv_range_impl = &target_link_dict_removed_items_range;

            context->key_set_ops = TSSDataOps{};
            TSDataOps key_set_base_ops = target_link_base_ops(*context);
            key_set_base_ops.kind = TSTypeKind::TSS;
            static_cast<TSDataOps &>(context->key_set_ops) = key_set_base_ops;
            configure_target_link_set_ops(context->key_set_ops);
            const auto *key_set_schema = TypeRegistry::instance().tss(schema.key_type());
            if (key_set_schema == nullptr)
            {
                throw std::logic_error("TSInput target-link TSD key-set schema is not resolved");
            }
            context->dict_layout.key_set_binding =
                &TSDataBinding::intern(*key_set_schema, root_plan, context->key_set_ops);

            context->active_layout = &context->dict_layout;
            context->active_ops = &context->dict_ops;
            return context;
        }

        [[nodiscard]] std::unique_ptr<TSInputTargetLinkContext>
        make_indexed_target_link_context(const TSValueTypeMetaData &schema,
                                         const MemoryUtils::StoragePlan &,
                                         std::size_t storage_offset,
                                         const TSDataBinding &regular_binding,
                                         const TSDataLayout &regular_layout)
        {
            auto context = std::make_unique<TargetLinkContextFor<TSDataLayout, IndexedTSDataOps>>();
            initialise_target_link_context(*context, schema, storage_offset, regular_binding);
            context->layout = regular_layout;
            context->layout.tracking_offset = storage_offset;
            context->ops = IndexedTSDataOps{};
            static_cast<TSDataOps &>(context->ops) = target_link_base_ops(*context);
            context->ops.size_impl = &target_link_indexed_size;
            context->ops.element_binding_impl = &target_link_indexed_element_binding;
            context->ops.element_memory_impl = &target_link_indexed_element_memory;
            context->ops.mutable_element_memory_impl = &target_link_indexed_mutable_element_memory;
            context->active_layout = &context->layout;
            context->active_ops = &context->ops;
            return context;
        }

        [[nodiscard]] std::unique_ptr<TSInputTargetLinkContext>
        make_window_target_link_context(const TSValueTypeMetaData &schema,
                                        const MemoryUtils::StoragePlan &,
                                        std::size_t storage_offset,
                                        const TSDataBinding &regular_binding,
                                        const TSDataLayout &regular_layout)
        {
            if (schema.is_duration_based())
            {
                auto context = std::make_unique<TargetLinkContextFor<TimeTSWDataLayout, TSWDataOps>>();
                initialise_target_link_context(*context, schema, storage_offset, regular_binding);
                context->layout = static_cast<const TimeTSWDataLayout &>(regular_layout);
                context->layout.tracking_offset = storage_offset;
                context->ops = TSWDataOps{};
                static_cast<TSDataOps &>(context->ops) = target_link_base_ops(*context);
                context->ops.size_impl = &target_link_window_size;
                context->ops.element_at_impl = &target_link_window_element_at;
                context->ops.time_at_impl = &target_link_window_time_at;
                context->ops.time_element_at_impl = &target_link_window_time_element_at;
                context->ops.capacity_impl = &target_link_window_capacity;
                context->ops.full_impl = &target_link_window_full;
                context->ops.push_impl = &target_link_window_push;
                context->active_layout = &context->layout;
                context->active_ops = &context->ops;
                return context;
            }

            auto context = std::make_unique<TargetLinkContextFor<SizeTSWDataLayout, TSWDataOps>>();
            initialise_target_link_context(*context, schema, storage_offset, regular_binding);
            context->layout = static_cast<const SizeTSWDataLayout &>(regular_layout);
            context->layout.tracking_offset = storage_offset;
            context->ops = TSWDataOps{};
            static_cast<TSDataOps &>(context->ops) = target_link_base_ops(*context);
            context->ops.size_impl = &target_link_window_size;
            context->ops.element_at_impl = &target_link_window_element_at;
            context->ops.time_at_impl = &target_link_window_time_at;
            context->ops.time_element_at_impl = &target_link_window_time_element_at;
            context->ops.capacity_impl = &target_link_window_capacity;
            context->ops.full_impl = &target_link_window_full;
            context->ops.push_impl = &target_link_window_push;
            context->active_layout = &context->layout;
            context->active_ops = &context->ops;
            return context;
        }
    }  // namespace

    const TSInputTargetLinkStorage *target_link_storage_at(const TSInputTargetLinkContext &context,
                                                           const void *memory) noexcept
    {
        return MemoryUtils::cast<TSInputTargetLinkStorage>(advance(memory, context.storage_offset));
    }

    TSInputTargetLinkStorage *target_link_storage_at(const TSInputTargetLinkContext &context,
                                                     void *memory) noexcept
    {
        return MemoryUtils::cast<TSInputTargetLinkStorage>(advance(memory, context.storage_offset));
    }

    const TSInputTargetLinkContext *target_link_context_for_ops(const TSDataOps *ops) noexcept
    {
        return ops != nullptr && ops->layout_impl == &target_link_layout
                   ? static_cast<const TSInputTargetLinkContext *>(ops->context)
                   : nullptr;
    }

    const TSInputTargetLinkContextBuilder &target_link_context_builder_for(TSTypeKind kind)
    {
        static constexpr std::size_t kind_count = ts_kind_index(TSTypeKind::SIGNAL) + 1U;
        static const std::array<TSInputTargetLinkContextBuilder, kind_count> table{
            &make_base_target_link_context,
            &make_set_target_link_context,
            &make_dict_target_link_context,
            &make_indexed_target_link_context,
            &make_window_target_link_context,
            &make_indexed_target_link_context,
            &make_base_target_link_context,
            &make_base_target_link_context,
        };

        const auto index = ts_kind_index(kind);
        if (index >= table.size()) { return table.front(); }
        return table[index];
    }
}  // namespace hgraph::detail
