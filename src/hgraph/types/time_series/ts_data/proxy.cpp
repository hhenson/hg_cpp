#include <hgraph/types/time_series/ts_data/proxy.h>

#if HGRAPH_ENABLE_PYTHON_USER_NODES
#include <nanobind/nanobind.h>
#endif

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/utils/value_slot_store.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/util/scope.h>

#include <memory>
#include <mutex>
#include <stdexcept>
#include <unordered_map>
#include <utility>

namespace hgraph
{
    namespace
    {
        enum class TSDProxySetSurface
        {
            Live,
            Added,
            Removed,
        };

        enum class TSDProxyMapSurface
        {
            Live,
            Added,
            Removed,
            Modified,
        };

        [[nodiscard]] TSDProxy &proxy_storage(void *memory)
        {
            if (memory == nullptr) { throw std::logic_error("TSDProxy requires live storage"); }
            return *static_cast<TSDProxy *>(memory);
        }

        [[nodiscard]] const TSDProxy &proxy_storage(const void *memory)
        {
            if (memory == nullptr) { throw std::logic_error("TSDProxy requires live storage"); }
            return *static_cast<const TSDProxy *>(memory);
        }

        struct TSDProxyContextKey
        {
            const TSValueTypeMetaData *schema{nullptr};
            const TSDataBinding      *element_binding{nullptr};

            [[nodiscard]] bool operator==(const TSDProxyContextKey &) const noexcept = default;
        };

        struct TSDProxyContextKeyHash
        {
            [[nodiscard]] std::size_t operator()(const TSDProxyContextKey &key) const noexcept
            {
                std::size_t seed = std::hash<const void *>{}(key.schema);
                const auto  h    = std::hash<const void *>{}(key.element_binding);
                seed ^= h + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
                return seed;
            }
        };

        struct TSDProxyContext
        {
            const TSValueTypeMetaData      *schema{nullptr};
            const MemoryUtils::StoragePlan *plan{nullptr};
            TSDDataLayout                   layout{};
            TSDDataOps                      dict_ops{};
            TSSDataOps                      key_set_ts_ops{};
            SetValueOps                     key_set_value_ops{};
            SetValueOps                     added_set_value_ops{};
            SetValueOps                     removed_set_value_ops{};
            MapValueOps                     value_map_ops{};
            MapValueOps                     modified_map_ops{};
            IndexedValueOps                 delta_bundle_ops{};
            const TSDataBinding            *element_binding{nullptr};
            const ValueTypeBinding         *key_set_value_binding{nullptr};
            const ValueTypeBinding         *added_set_binding{nullptr};
            const ValueTypeBinding         *removed_set_binding{nullptr};
            const ValueTypeBinding         *modified_map_binding{nullptr};

            TSDProxyContext(const TSValueTypeMetaData &schema_, const TSDataBinding &element_binding_)
                : schema(&schema_),
                  plan(&MemoryUtils::plan_for<TSDProxy>()),
                  element_binding(&element_binding_)
            {
                if (schema->kind != TSTypeKind::TSD)
                {
                    throw std::logic_error("TSDProxy context requires a TSD schema");
                }
                if (schema->key_type() == nullptr || schema->element_ts() == nullptr)
                {
                    throw std::logic_error("TSDProxy schema is incomplete");
                }
                if (element_binding->type_meta != schema->element_ts())
                {
                    throw std::logic_error("TSDProxy element binding does not match the TSD element schema");
                }

                const auto &element_ops    = element_binding->ops_ref();
                const auto *element_layout = element_ops.layout_impl(element_ops.context);
                if (element_layout == nullptr)
                {
                    throw std::logic_error("TSDProxy element layout is not resolved");
                }

                layout.key_binding           = ValuePlanFactory::instance().binding_for(schema->key_type());
                layout.element_binding       = element_binding;
                layout.element_layout        = element_layout;
                layout.element_value_binding = element_layout->value_binding;
                layout.element_delta_binding = element_layout->delta_binding;
                layout.tracking_offset       = 0;
                if (layout.key_binding == nullptr || layout.element_value_binding == nullptr ||
                    layout.element_delta_binding == nullptr)
                {
                    throw std::logic_error("TSDProxy value bindings are not resolved");
                }

                configure_value_ops();
                configure_ts_ops();
                bind_surfaces();
            }

          private:
            void configure_value_ops()
            {
                key_set_value_ops     = set_value_ops_for<TSDProxySetSurface::Live>();
                added_set_value_ops   = set_value_ops_for<TSDProxySetSurface::Added>();
                removed_set_value_ops = set_value_ops_for<TSDProxySetSurface::Removed>();
                value_map_ops         = map_value_ops_for<TSDProxyMapSurface::Live>();
                modified_map_ops      = map_value_ops_for<TSDProxyMapSurface::Modified>();
                delta_bundle_ops      = IndexedValueOps{
                    {this, false, nullptr, nullptr, nullptr, nullptr},
                    &delta_size,
                    &delta_element_at,
                    &delta_element_binding,
                    &delta_range,
                    nullptr,
                };
                delta_bundle_ops.owning_binding_impl      = &canonical_value_binding;
                delta_bundle_ops.copy_construct_view_impl = &delta_copy_construct_view;
                delta_bundle_ops.copy_assign_view_impl    = &delta_copy_assign_view;
            }

            void configure_ts_ops()
            {
                key_set_ts_ops = TSSDataOps{};
                TSDataOps &set_base = key_set_ts_ops;
                set_base = TSDataOps{
                    .context                   = this,
                    .kind                      = TSTypeKind::TSS,
                    .allows_mutation           = false,
                    .layout_impl               = &ts_layout,
                    .tracking_impl             = &tracking,
                    .mutable_tracking_impl     = &mutable_tracking,
                    .has_current_value_impl    = &has_current_value,
                    .all_valid_impl            = &all_valid,
                    .value_memory_impl         = &value_memory,
                    .mutable_value_memory_impl = &mutable_value_memory,
                    .delta_memory_impl         = &delta_memory,
                    .mutable_delta_memory_impl = &mutable_delta_memory,
                    .record_child_modified_impl = &record_child_modified,
                    .empty_delta_impl          = &ts_data_detail::empty_delta_tss,
                    .capture_delta_impl        = &ts_data_detail::capture_delta_tss,
                    .delta_has_effect_impl     = &ts_data_detail::delta_has_effect_tss,
                    .apply_delta_impl          = &ts_data_detail::apply_delta_tss,
                };
                key_set_ts_ops.size_impl                      = &set_size<TSDProxySetSurface::Live>;
                key_set_ts_ops.slot_capacity_impl             = &slot_capacity;
                key_set_ts_ops.slot_occupied_impl             = &slot_occupied;
                key_set_ts_ops.slot_live_impl                 = &slot_live;
                key_set_ts_ops.slot_added_impl                = &slot_added;
                key_set_ts_ops.slot_removed_impl              = &slot_removed;
                key_set_ts_ops.key_at_slot_impl               = &key_at_slot;
                key_set_ts_ops.contains_impl                  = &set_contains;
                key_set_ts_ops.find_slot_impl                 = &find_slot;
                key_set_ts_ops.make_values_range_impl         = &set_range<TSDProxySetSurface::Live>;
                key_set_ts_ops.make_added_values_range_impl   = &set_range<TSDProxySetSurface::Added>;
                key_set_ts_ops.make_removed_values_range_impl = &set_range<TSDProxySetSurface::Removed>;
                key_set_ts_ops.subscribe_slot_observer_impl   = &subscribe_slot_observer;
                key_set_ts_ops.unsubscribe_slot_observer_impl = &unsubscribe_slot_observer;

                dict_ops = TSDDataOps{};
                TSSDataOps &dict_set = dict_ops;
                dict_set = key_set_ts_ops;
                TSDataOps &dict_base = dict_ops;
                dict_base.kind = TSTypeKind::TSD;
                dict_base.context = this;
                dict_base.empty_delta_impl = &ts_data_detail::empty_delta_tsd;
                dict_base.capture_delta_impl = &ts_data_detail::capture_delta_tsd;
                dict_base.delta_has_effect_impl = &ts_data_detail::delta_has_effect_tsd;
                dict_base.apply_delta_impl = &ts_data_detail::apply_delta_tsd;
                dict_ops.child_at_slot_impl = &tsd_child_at_slot;
                dict_ops.slot_modified_impl = &slot_modified;
                dict_ops.make_ts_values_range_impl = &ts_value_range<TSDProxyMapSurface::Live>;
                dict_ops.make_valid_keys_range_impl = &set_range<TSDProxySetSurface::Live>;
                dict_ops.make_valid_ts_values_range_impl = &ts_value_range<TSDProxyMapSurface::Live>;
                dict_ops.make_modified_keys_range_impl = &map_key_range<TSDProxyMapSurface::Modified>;
                dict_ops.make_modified_ts_values_range_impl = &ts_value_range<TSDProxyMapSurface::Modified>;
                dict_ops.make_added_ts_values_range_impl = &ts_value_range<TSDProxyMapSurface::Added>;
                dict_ops.make_removed_ts_values_range_impl = &ts_value_range<TSDProxyMapSurface::Removed>;
                dict_ops.make_ts_kv_range_impl = &ts_kv_range<TSDProxyMapSurface::Live>;
                dict_ops.make_valid_ts_kv_range_impl = &ts_kv_range<TSDProxyMapSurface::Live>;
                dict_ops.make_modified_ts_kv_range_impl = &ts_kv_range<TSDProxyMapSurface::Modified>;
                dict_ops.make_added_ts_kv_range_impl = &ts_kv_range<TSDProxyMapSurface::Added>;
                dict_ops.make_removed_ts_kv_range_impl = &ts_kv_range<TSDProxyMapSurface::Removed>;
            }

            void bind_surfaces()
            {
                const auto *set_schema        = TypeRegistry::instance().set(schema->key_type());
                const auto *key_set_ts_schema = TypeRegistry::instance().tss(schema->key_type());
                if (schema->value_schema == nullptr || schema->delta_value_schema == nullptr ||
                    set_schema == nullptr || key_set_ts_schema == nullptr)
                {
                    throw std::logic_error("TSDProxy schemas are not populated");
                }

                layout.value_binding = &ValueTypeBinding::intern(*schema->value_schema, *plan, value_map_ops);

                if (schema->delta_value_schema->kind != ValueTypeKind::Bundle ||
                    schema->delta_value_schema->field_count != 2)
                {
                    throw std::logic_error("TSDProxy delta schema must be Bundle{removed, modified}");
                }
                removed_set_binding = &ValueTypeBinding::intern(*schema->delta_value_schema->fields[0].type,
                                                                *plan,
                                                                removed_set_value_ops);
                modified_map_binding = &ValueTypeBinding::intern(*schema->delta_value_schema->fields[1].type,
                                                                 *plan,
                                                                 modified_map_ops);
                added_set_binding = &ValueTypeBinding::intern(*set_schema, *plan, added_set_value_ops);
                layout.delta_binding = &ValueTypeBinding::intern(*schema->delta_value_schema, *plan, delta_bundle_ops);

                key_set_value_binding = &ValueTypeBinding::intern(*set_schema, *plan, key_set_value_ops);
                layout.key_set_binding = &TSDataBinding::intern(*key_set_ts_schema, *plan, key_set_ts_ops);
            }

            template <TSDProxySetSurface Surface>
            [[nodiscard]] SetValueOps set_value_ops_for()
            {
                SetValueOps ops{
                    {{this, false, nullptr, nullptr, nullptr, nullptr
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                      ,
                      &set_surface_to_python<Surface>,
                      nullptr
#endif
                     },
                     &set_size<Surface>,
                     &set_element_at<Surface>,
                     &set_element_binding,
                     &set_range<Surface>,
                     nullptr},
                    &set_contains_raw,
                };
                ops.owning_binding_impl      = &canonical_value_binding;
                ops.copy_construct_view_impl = &set_copy_construct_view<Surface>;
                ops.copy_assign_view_impl    = &set_copy_assign_view<Surface>;
                return ops;
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] MapValueOps map_value_ops_for()
            {
                MapValueOps ops{
                    {{this, false, nullptr, nullptr, nullptr, nullptr
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                      ,
                      &map_surface_to_python<Surface>,
                      nullptr
#endif
                     },
                     &map_size<Surface>,
                     &map_key_at_index<Surface>,
                     &set_element_binding,
                     &map_key_range<Surface>,
                     nullptr},
                    &map_contains_raw<Surface>,
                    &map_value_at<Surface>,
                    &map_value_at_index<Surface>,
                    &map_value_binding,
                    &map_key_range<Surface>,
                    &map_value_range<Surface>,
                    &map_kv_range<Surface>,
                    &map_key_set,
                };
                ops.owning_binding_impl      = &canonical_value_binding;
                ops.copy_construct_view_impl = &map_copy_construct_view<Surface>;
                ops.copy_assign_view_impl    = &map_copy_assign_view<Surface>;
                return ops;
            }

            [[nodiscard]] static const TSDProxyContext *ctx(const void *context) noexcept
            {
                return static_cast<const TSDProxyContext *>(context);
            }

            [[nodiscard]] static const ValueTypeBinding *
            canonical_value_binding(const void *, const ValueTypeBinding &view_binding)
            {
                const auto *binding = ValuePlanFactory::instance().binding_for(view_binding.type_meta);
                if (binding == nullptr)
                {
                    throw std::logic_error("TSDProxy value surface has no canonical owning binding");
                }
                return binding;
            }

            static void delta_copy_construct_view(const void *context,
                                                  const ValueTypeBinding &binding,
                                                  void *dst,
                                                  const void *memory)
            {
                const auto &plan = binding.checked_plan();
                plan.default_construct(dst);
                auto rollback = make_scope_exit([&]() noexcept { plan.destroy(dst); });
                delta_copy_assign_view(context, binding, dst, memory);
                rollback.release();
            }

            static void delta_copy_assign_view(const void *context,
                                               const ValueTypeBinding &binding,
                                               void *dst,
                                               const void *memory)
            {
                if (binding.type_meta == nullptr || binding.type_meta->kind != ValueTypeKind::Bundle ||
                    binding.type_meta->field_count != 2)
                {
                    throw std::logic_error("TSDProxy delta copy requires canonical Bundle{removed, modified}");
                }

                const auto &plan = binding.checked_plan();
                if (!plan.is_composite() || plan.component_count() < 2)
                {
                    throw std::logic_error("TSDProxy delta copy requires a two-field structured plan");
                }

                auto removed  = Value{ValueView{delta_element_binding(context, memory, 0),
                                                delta_element_at(context, memory, 0)}};
                auto modified = Value{ValueView{delta_element_binding(context, memory, 1),
                                                delta_element_at(context, memory, 1)}};

                BundleBuilder builder{binding};
                builder.set(0, removed.view());
                builder.set(1, modified.view());
                Value bundle = builder.build();
                plan.copy_assign(dst, bundle.view().data());
            }

            template <TSDProxySetSurface Surface>
            static void set_copy_construct_view(const void *context,
                                                const ValueTypeBinding &binding,
                                                void *dst,
                                                const void *memory)
            {
                auto storage = build_set_storage<Surface>(context, binding, memory);
                std::construct_at(static_cast<SetStorage *>(dst), std::move(storage));
            }

            template <TSDProxySetSurface Surface>
            static void set_copy_assign_view(const void *context,
                                             const ValueTypeBinding &binding,
                                             void *dst,
                                             const void *memory)
            {
                *static_cast<SetStorage *>(dst) = build_set_storage<Surface>(context, binding, memory);
            }

            template <TSDProxySetSurface Surface>
            [[nodiscard]] static SetStorage build_set_storage(const void *context,
                                                              const ValueTypeBinding &binding,
                                                              const void *memory)
            {
                const auto *state = ctx(context);
                if (binding.type_meta == nullptr || binding.type_meta->kind != ValueTypeKind::Set)
                {
                    throw std::logic_error("TSDProxy set copy requires a canonical set binding");
                }
                const auto *key_binding = ValuePlanFactory::instance().binding_for(binding.type_meta->element_type);
                if (key_binding == nullptr || key_binding != state->layout.key_binding)
                {
                    throw std::logic_error("TSDProxy set copy key binding is not resolved");
                }

                SetBuilder builder{*key_binding};
                for (const auto key : set_range<Surface>(context, memory))
                {
                    builder.insert_copy(key.data());
                }
                return builder.build_storage();
            }

            template <TSDProxyMapSurface Surface>
            static void map_copy_construct_view(const void *context,
                                                const ValueTypeBinding &binding,
                                                void *dst,
                                                const void *memory)
            {
                auto storage = build_map_storage<Surface>(context, binding, memory);
                std::construct_at(static_cast<MapStorage *>(dst), std::move(storage));
            }

            template <TSDProxyMapSurface Surface>
            static void map_copy_assign_view(const void *context,
                                             const ValueTypeBinding &binding,
                                             void *dst,
                                             const void *memory)
            {
                *static_cast<MapStorage *>(dst) = build_map_storage<Surface>(context, binding, memory);
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static MapStorage build_map_storage(const void *context,
                                                              const ValueTypeBinding &binding,
                                                              const void *memory)
            {
                if (binding.type_meta == nullptr || binding.type_meta->kind != ValueTypeKind::Map)
                {
                    throw std::logic_error("TSDProxy map copy requires a canonical map binding");
                }

                const auto *key_binding = ValuePlanFactory::instance().binding_for(binding.type_meta->key_type);
                const auto *value_binding = ValuePlanFactory::instance().binding_for(binding.type_meta->element_type);
                if (key_binding == nullptr || value_binding == nullptr)
                {
                    throw std::logic_error("TSDProxy map copy bindings are not resolved");
                }

                MapBuilder builder{*key_binding, *value_binding};
                for (const auto item : ts_kv_range<Surface>(context, memory))
                {
                    Value value{item.second.value()};
                    builder.set_item_copy(item.first.data(), value.view().data());
                }
                return builder.build_storage();
            }

            [[nodiscard]] static const TSDataLayout *ts_layout(const void *context) noexcept
            {
                return &ctx(context)->layout;
            }

            [[nodiscard]] static const TSDataTracking *tracking(const void *, const void *memory) noexcept
            {
                return &proxy_storage(memory).tracking();
            }

            [[nodiscard]] static TSDataTracking *mutable_tracking(const void *, void *memory) noexcept
            {
                return &proxy_storage(memory).tracking();
            }

            [[nodiscard]] static bool has_current_value(const void *, const void *memory) noexcept
            {
                return proxy_storage(memory).tracking().last_modified_time != MIN_DT;
            }

            [[nodiscard]] static bool all_valid(const void *context, const void *memory) noexcept
            {
                return fallback_on_exception(false, [&] {
                    if (!has_current_value(context, memory)) { return false; }
                    const auto *state = ctx(context);
                    const auto &ops   = state->element_binding->ops_ref();
                    const auto &store = proxy_storage(memory);
                    auto        dict  = store.source_dict();
                    for (std::size_t slot = 0; slot < dict.slot_capacity(); ++slot)
                    {
                        if (!dict.slot_live(slot)) { continue; }
                        if (!store.has_child(slot)) { return false; }
                        if (!ops.all_valid_impl(ops.context, store.child_at_slot(slot))) { return false; }
                    }
                    return true;
                });
            }

            [[nodiscard]] static const void *value_memory(const void *, const void *memory) noexcept
            {
                return memory;
            }

            [[nodiscard]] static void *mutable_value_memory(const void *, void *memory) noexcept
            {
                return memory;
            }

            [[nodiscard]] static const void *delta_memory(const void *, const void *memory) noexcept
            {
                return memory;
            }

            [[nodiscard]] static void *mutable_delta_memory(const void *, void *memory) noexcept
            {
                return memory;
            }

            static void record_child_modified(const void *, void *memory, std::size_t child_id,
                                              DateTime modified_time)
            {
                proxy_storage(memory).record_child_modified(child_id, modified_time);
            }

            static void subscribe_slot_observer(const void *, void *memory, SlotObserver *observer)
            {
                proxy_storage(memory).subscribe_slot_observer(observer);
            }

            static void unsubscribe_slot_observer(const void *, void *memory, SlotObserver *observer)
            {
                proxy_storage(memory).unsubscribe_slot_observer(observer);
            }

            [[nodiscard]] static TSDDataView source_dict(const void *memory)
            {
                return proxy_storage(memory).source_dict();
            }

            template <TSDProxySetSurface Surface>
            [[nodiscard]] static bool slot_in_set_surface(const void *, const void *memory, std::size_t slot)
            {
                auto dict = source_dict(memory);
                if constexpr (Surface == TSDProxySetSurface::Live) { return dict.slot_live(slot); }
                if constexpr (Surface == TSDProxySetSurface::Added) { return dict.slot_added(slot); }
                return dict.slot_removed(slot);
            }

            [[nodiscard]] static bool slot_modified(const void *context, const void *memory, std::size_t slot)
            {
                const auto &store = proxy_storage(memory);
                if (!store.has_child(slot) || !source_dict(memory).slot_live(slot)) { return false; }
                if (store.child_updated(slot)) { return true; }
                const auto *state          = ctx(context);
                const auto &ops            = state->element_binding->ops_ref();
                const auto *child_tracking = ops.tracking_impl(ops.context, store.child_at_slot(slot));
                return child_tracking != nullptr &&
                       child_tracking->last_modified_time == store.tracking().last_modified_time;
            }

            [[nodiscard]] static const void *tsd_child_at_slot(const void *, const void *memory, std::size_t slot)
            {
                return proxy_storage(memory).child_at_slot(slot);
            }

            [[nodiscard]] static bool slot_occupied(const void *, const void *memory, std::size_t slot)
            {
                return source_dict(memory).slot_occupied(slot);
            }

            [[nodiscard]] static bool slot_live(const void *, const void *memory, std::size_t slot)
            {
                return source_dict(memory).slot_live(slot);
            }

            [[nodiscard]] static bool slot_added(const void *, const void *memory, std::size_t slot)
            {
                return source_dict(memory).slot_added(slot);
            }

            [[nodiscard]] static bool slot_removed(const void *, const void *memory, std::size_t slot)
            {
                return source_dict(memory).slot_removed(slot);
            }

            [[nodiscard]] static std::size_t slot_capacity(const void *, const void *memory)
            {
                return source_dict(memory).slot_capacity();
            }

            template <TSDProxySetSurface Surface>
            [[nodiscard]] static std::size_t set_size(const void *context, const void *memory) noexcept
            {
                return fallback_on_exception<std::size_t>(0, [&] {
                    if constexpr (Surface == TSDProxySetSurface::Live) { return source_dict(memory).size(); }
                    std::size_t count = 0;
                    auto        dict  = source_dict(memory);
                    for (std::size_t slot = 0; slot < dict.slot_capacity(); ++slot)
                    {
                        if (slot_in_set_surface<Surface>(context, memory, slot)) { ++count; }
                    }
                    return count;
                });
            }

            [[nodiscard]] static const void *key_at_slot(const void *, const void *memory, std::size_t slot)
            {
                return source_dict(memory).key_at_slot(slot).data();
            }

            [[nodiscard]] static bool set_contains(const void *context, const void *memory, const ValueView &key)
            {
                if (key.binding() != ctx(context)->layout.key_binding) { return false; }
                return source_dict(memory).contains(key);
            }

            [[nodiscard]] static bool set_contains_raw(const void *context, const void *memory, const void *key)
            {
                return set_contains(context, memory, ValueView{ctx(context)->layout.key_binding, key});
            }

            [[nodiscard]] static std::size_t find_slot(const void *context, const void *memory, const ValueView &key)
            {
                if (key.binding() != ctx(context)->layout.key_binding) { return TS_DATA_NO_CHILD_ID; }
                return source_dict(memory).find_slot(key);
            }

            template <TSDProxySetSurface Surface>
            [[nodiscard]] static const void *set_element_at(const void *context, const void *memory, std::size_t index)
            {
                auto        dict = source_dict(memory);
                std::size_t seen = 0;
                for (std::size_t slot = 0; slot < dict.slot_capacity(); ++slot)
                {
                    if (!slot_in_set_surface<Surface>(context, memory, slot)) { continue; }
                    if (seen++ == index) { return dict.key_at_slot(slot).data(); }
                }
                throw std::out_of_range("TSDProxy set element index out of range");
            }

            [[nodiscard]] static const ValueTypeBinding *set_element_binding(const void *context,
                                                                             const void *,
                                                                             std::size_t) noexcept
            {
                return ctx(context)->layout.key_binding;
            }

            [[nodiscard]] static ValueView key_projector(const void *context, const void *memory, std::size_t slot)
            {
                return ValueView{ctx(context)->layout.key_binding, key_at_slot(context, memory, slot)};
            }

            template <TSDProxySetSurface Surface>
            [[nodiscard]] static Range<ValueView> set_range(const void *context, const void *memory)
            {
                return Range<ValueView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = slot_capacity(context, memory),
                    .predicate = &slot_in_set_surface<Surface>,
                    .projector = &key_projector,
                };
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static bool map_slot_in_surface(const void *context, const void *memory, std::size_t slot)
            {
                if constexpr (Surface == TSDProxyMapSurface::Live)
                {
                    return source_dict(memory).slot_live(slot);
                }
                if constexpr (Surface == TSDProxyMapSurface::Added)
                {
                    return source_dict(memory).slot_added(slot);
                }
                if constexpr (Surface == TSDProxyMapSurface::Removed)
                {
                    return source_dict(memory).slot_removed(slot);
                }
                return slot_modified(context, memory, slot);
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static std::size_t map_size(const void *context, const void *memory) noexcept
            {
                return fallback_on_exception<std::size_t>(0, [&] {
                    if constexpr (Surface == TSDProxyMapSurface::Live) { return source_dict(memory).size(); }
                    std::size_t count = 0;
                    auto        dict  = source_dict(memory);
                    for (std::size_t slot = 0; slot < dict.slot_capacity(); ++slot)
                    {
                        if (map_slot_in_surface<Surface>(context, memory, slot)) { ++count; }
                    }
                    return count;
                });
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static const void *map_key_at_index(const void *context,
                                                              const void *memory,
                                                              std::size_t index)
            {
                auto        dict = source_dict(memory);
                std::size_t seen = 0;
                for (std::size_t slot = 0; slot < dict.slot_capacity(); ++slot)
                {
                    if (!map_slot_in_surface<Surface>(context, memory, slot)) { continue; }
                    if (seen++ == index) { return dict.key_at_slot(slot).data(); }
                }
                throw std::out_of_range("TSDProxy map key index out of range");
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static bool map_contains_raw(const void *context, const void *memory, const void *key)
            {
                const auto slot = source_dict(memory).find_slot(ValueView{ctx(context)->layout.key_binding, key});
                return slot != TS_DATA_NO_CHILD_ID && map_slot_in_surface<Surface>(context, memory, slot);
            }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
            /** Surface read-back for python (type-erasure rule: conversion
                binds to the ops). Children are read through their TS views -
                link-endpoint children resolve to their targets. */
            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static nanobind::object map_surface_to_python(const void *context, const void *memory)
            {
                namespace nb = nanobind;
                const auto *state = ctx(context);
                const auto &proxy = proxy_storage(memory);
                auto        dict  = source_dict(memory);
                nb::dict    result;
                for (std::size_t slot = 0; slot < dict.slot_capacity(); ++slot)
                {
                    if (!map_slot_in_surface<Surface>(context, memory, slot) || !proxy.has_child(slot)) { continue; }
                    auto child = TSDataView{state->element_binding, proxy.child_at_slot(slot)};
                    auto value = child.value();
                    if (!value.valid()) { continue; }
                    auto key = dict.key_at_slot(slot);
                    result[key.binding()->ops_ref().to_python(key.data())] =
                        value.binding()->ops_ref().to_python(value.data());
                }
                return result;
            }

            template <TSDProxySetSurface Surface>
            [[nodiscard]] static nanobind::object set_surface_to_python(const void *context, const void *memory)
            {
                namespace nb = nanobind;
                auto     dict = source_dict(memory);
                nb::list items;
                for (std::size_t slot = 0; slot < dict.slot_capacity(); ++slot)
                {
                    if (!slot_in_set_surface<Surface>(context, memory, slot)) { continue; }
                    auto key = dict.key_at_slot(slot);
                    items.append(key.binding()->ops_ref().to_python(key.data()));
                }
                return nb::steal(PyFrozenSet_New(items.ptr()));
            }
#endif

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static const void *map_value_at(const void *context, const void *memory, const void *key)
            {
                const auto slot = source_dict(memory).find_slot(ValueView{ctx(context)->layout.key_binding, key});
                if (slot == TS_DATA_NO_CHILD_ID || !map_slot_in_surface<Surface>(context, memory, slot))
                {
                    return nullptr;
                }
                return proxy_storage(memory).child_at_slot(slot);
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static const void *map_value_at_index(const void *context,
                                                                const void *memory,
                                                                std::size_t index)
            {
                auto        dict = source_dict(memory);
                std::size_t seen = 0;
                for (std::size_t slot = 0; slot < dict.slot_capacity(); ++slot)
                {
                    if (!map_slot_in_surface<Surface>(context, memory, slot)) { continue; }
                    if (seen++ == index) { return proxy_storage(memory).child_at_slot(slot); }
                }
                throw std::out_of_range("TSDProxy map value index out of range");
            }

            [[nodiscard]] static const ValueTypeBinding *map_value_binding(const void *context,
                                                                           const void *) noexcept
            {
                return ctx(context)->layout.element_value_binding;
            }

            [[nodiscard]] static ValueView map_value_projector(const void *context,
                                                               const void *memory,
                                                               std::size_t slot)
            {
                return ValueView{ctx(context)->layout.element_value_binding,
                                 proxy_storage(memory).child_at_slot(slot)};
            }

            [[nodiscard]] static std::pair<ValueView, ValueView> map_kv_projector(const void *context,
                                                                                  const void *memory,
                                                                                  std::size_t slot)
            {
                return {key_projector(context, memory, slot), map_value_projector(context, memory, slot)};
            }

            [[nodiscard]] static TSDataView ts_value_projector(const void *context,
                                                               const void *memory,
                                                               std::size_t slot)
            {
                return TSDataView{ctx(context)->layout.element_binding, proxy_storage(memory).child_at_slot(slot)};
            }

            [[nodiscard]] static std::pair<ValueView, TSDataView> ts_kv_projector(const void *context,
                                                                                  const void *memory,
                                                                                  std::size_t slot)
            {
                return {key_projector(context, memory, slot), ts_value_projector(context, memory, slot)};
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static Range<ValueView> map_key_range(const void *context, const void *memory)
            {
                return Range<ValueView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = slot_capacity(context, memory),
                    .predicate = &map_slot_in_surface<Surface>,
                    .projector = &key_projector,
                };
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static Range<ValueView> map_value_range(const void *context, const void *memory)
            {
                return Range<ValueView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = slot_capacity(context, memory),
                    .predicate = &map_slot_in_surface<Surface>,
                    .projector = &map_value_projector,
                };
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static KeyValueRange<ValueView, ValueView> map_kv_range(const void *context,
                                                                                  const void *memory)
            {
                return KeyValueRange<ValueView, ValueView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = slot_capacity(context, memory),
                    .predicate = &map_slot_in_surface<Surface>,
                    .projector = &map_kv_projector,
                };
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static Range<TSDataView> ts_value_range(const void *context, const void *memory)
            {
                return Range<TSDataView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = slot_capacity(context, memory),
                    .predicate = &map_slot_in_surface<Surface>,
                    .projector = &ts_value_projector,
                };
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static KeyValueRange<ValueView, TSDataView> ts_kv_range(const void *context,
                                                                                  const void *memory)
            {
                return KeyValueRange<ValueView, TSDataView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = slot_capacity(context, memory),
                    .predicate = &map_slot_in_surface<Surface>,
                    .projector = &ts_kv_projector,
                };
            }

            [[nodiscard]] static SetView map_key_set(const void *context, const ValueTypeBinding *, const void *memory)
            {
                return ValueView{ctx(context)->key_set_value_binding, memory}.as_set();
            }

            [[nodiscard]] static std::size_t delta_size(const void *, const void *) noexcept
            {
                return 2;
            }

            [[nodiscard]] static const void *delta_element_at(const void *, const void *memory, std::size_t index)
            {
                if (index == 0 || index == 1) { return memory; }
                throw std::out_of_range("TSDProxy delta element index out of range");
            }

            [[nodiscard]] static const ValueTypeBinding *delta_element_binding(const void *context,
                                                                               const void *,
                                                                               std::size_t index) noexcept
            {
                if (index == 0) { return ctx(context)->removed_set_binding; }
                if (index == 1) { return ctx(context)->modified_map_binding; }
                return nullptr;
            }

            [[nodiscard]] static Range<ValueView> delta_range(const void *context, const void *memory)
            {
                return Range<ValueView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = 2,
                    .predicate = nullptr,
                    .projector = &delta_projector,
                };
            }

            [[nodiscard]] static ValueView delta_projector(const void *context,
                                                           const void *memory,
                                                           std::size_t index)
            {
                return ValueView{delta_element_binding(context, memory, index),
                                 delta_element_at(context, memory, index)};
            }
        };

        [[nodiscard]] std::unordered_map<TSDProxyContextKey,
                                         std::unique_ptr<TSDProxyContext>,
                                         TSDProxyContextKeyHash> &
        tsd_proxy_contexts() noexcept
        {
            static std::unordered_map<TSDProxyContextKey,
                                      std::unique_ptr<TSDProxyContext>,
                                      TSDProxyContextKeyHash> contexts;
            return contexts;
        }

        [[nodiscard]] std::recursive_mutex &tsd_proxy_context_mutex() noexcept
        {
            static std::recursive_mutex mutex;
            return mutex;
        }

        [[nodiscard]] const TSDProxyContext &tsd_proxy_context_for(const TSValueTypeMetaData &schema,
                                                                   const TSDataBinding      &element_binding)
        {
            std::lock_guard<std::recursive_mutex> lock(tsd_proxy_context_mutex());
            auto &contexts = tsd_proxy_contexts();
            const TSDProxyContextKey key{&schema, &element_binding};
            if (const auto it = contexts.find(key); it != contexts.end()) { return *it->second; }

            auto context = std::make_unique<TSDProxyContext>(schema, element_binding);
            const auto *result = context.get();
            contexts.emplace(key, std::move(context));
            return *result;
        }
    }  // namespace

    TSDProxySlotSync::TSDProxySlotSync(TSDProxy &owner) noexcept
        : owner_(&owner)
    {
    }

    TSDProxySlotSync::~TSDProxySlotSync() = default;

    void TSDProxySlotSync::on_capacity(std::size_t old_capacity, std::size_t new_capacity)
    {
        owner_->on_slot_capacity(old_capacity, new_capacity);
    }

    void TSDProxySlotSync::on_insert(std::size_t slot)
    {
        owner_->on_slot_inserted(slot);
    }

    void TSDProxySlotSync::on_remove(std::size_t slot)
    {
        owner_->on_slot_removed(slot);
    }

    void TSDProxySlotSync::on_erase(std::size_t slot)
    {
        owner_->on_slot_erased(slot);
    }

    void TSDProxySlotSync::on_clear()
    {
        owner_->on_slots_cleared();
    }

    void TSDProxySlotSync::notify(DateTime modified_time)
    {
        owner_->on_source_modified(modified_time);
    }

    TSDProxy::TSDProxy() noexcept
        : source_sync_(*this)
    {
    }

    TSDProxy::~TSDProxy()
    {
        unsubscribe_source();
    }

    void TSDProxy::bind(const TSDataBinding &self_binding,
                        const TSDataBinding &element_binding,
                        const TSDDataView   &source,
                        ValueBuilder         builder,
                        const void          *builder_context,
                        DateTime        modified_time,
                        TSDProxyChildRefresh child_refresh)
    {
        child_refresh_ = child_refresh;
        if (source.schema() == nullptr || source.schema()->kind != TSTypeKind::TSD)
        {
            throw std::invalid_argument("TSDProxy requires a TSD source view");
        }
        if (builder == nullptr)
        {
            throw std::invalid_argument("TSDProxy requires a value builder");
        }

        const auto &element_plan = element_binding.checked_plan();
        const bool reconfigure =
            self_binding_ != &self_binding ||
            element_binding_ != &element_binding ||
            source_storage_.binding() != source.binding() ||
            source_storage_.data() != source.base().data() ||
            value_builder_ != builder ||
            value_builder_context_ != builder_context;

        if (reconfigure)
        {
            unsubscribe_source();
            self_binding_          = &self_binding;
            element_binding_       = &element_binding;
            source_storage_        = TSDDataStorageRef{source.base().storage_ref(), TSTypeKind::TSD};
            value_builder_         = builder;
            value_builder_context_ = builder_context;
            values_.bind_plan(element_plan);
            values_.destroy_all();
        }
        else
        {
            source_storage_ = TSDDataStorageRef{source.base().storage_ref(), TSTypeKind::TSD};
        }

        // The initial sync only FORCES a modified mark when the source has
        // actually ticked - binding over a never-ticked source must not
        // fabricate an empty first tick for the alternative's consumers.
        sync_from_source(modified_time, source_view().has_current_value());
        subscribe_source();
    }

    void TSDProxy::on_slot_capacity(std::size_t old_capacity, std::size_t new_capacity)
    {
        values_.reserve_to(new_capacity);
        if (built_times_.size() < new_capacity) { built_times_.resize(new_capacity, MIN_DT); }
        slot_observers_.notify_capacity(old_capacity, new_capacity);
    }

    void TSDProxy::on_slot_inserted(std::size_t slot)
    {
        construct_child_at_slot(slot);
        if (slot < built_times_.size()) { built_times_[slot] = MIN_DT; }
        slot_observers_.notify_insert(slot);
    }

    void TSDProxy::on_slot_removed(std::size_t slot)
    {
        construct_child_at_slot(slot);
        slot_observers_.notify_remove(slot);
    }

    void TSDProxy::on_slot_erased(std::size_t slot)
    {
        values_.destroy_at(slot);
        if (slot < built_times_.size()) { built_times_[slot] = MIN_DT; }
        slot_observers_.notify_erase(slot);
    }

    void TSDProxy::on_slots_cleared()
    {
        values_.destroy_all();
        built_times_.assign(built_times_.size(), MIN_DT);
        slot_observers_.notify_clear();
    }

    void TSDProxy::on_source_modified(DateTime modified_time)
    {
        if (modified_time == MIN_DT || !source_storage_.valid() || value_builder_ == nullptr) { return; }

        auto dict = source_dict();
        bool touched = false;
        for (std::size_t slot = 0; slot < dict.slot_capacity(); ++slot)
        {
            if (dict.slot_added(slot) || dict.slot_removed(slot))
            {
                ensure_child_at_slot(slot, modified_time);
                touched = true;
                continue;
            }
            // OnChildTick proxies (from-REF): a LIVE slot whose source child
            // ticked re-runs the builder so links rebind on a retarget. The
            // proxy only marks itself modified when the child actually
            // recorded at this time, so same-reference re-publication stays
            // silent. StructureOnly proxies (to-REF) never rebuild on value
            // ticks - the materialised identity did not change.
            if (child_refresh_ == TSDProxyChildRefresh::OnChildTick && dict.slot_live(slot) &&
                dict.slot_modified(slot) && has_child(slot))
            {
                refresh_child_at_slot(slot, modified_time);
                auto child = TSDataView{element_binding_, values_.value_memory(slot)};
                if (child.tracking().last_modified_time == modified_time) { touched = true; }
            }
        }

        if (touched) { mark_modified(modified_time); }
    }

    TSDataView TSDProxy::source_view() const noexcept
    {
        return TSDataView{source_storage_.storage_ref()};
    }

    TSDDataView TSDProxy::source_dict() const
    {
        if (!source_storage_.valid()) { throw std::logic_error("TSDProxy is not bound to a source"); }
        auto source = source_view();
        return source.as_dict();
    }

    TSDataView TSDProxy::source_child_at_slot(std::size_t slot) const
    {
        return source_dict().at_slot(slot);
    }

    TSDataTracking &TSDProxy::tracking() noexcept
    {
        return tracking_;
    }

    const TSDataTracking &TSDProxy::tracking() const noexcept
    {
        return tracking_;
    }

    bool TSDProxy::has_child(std::size_t slot) const noexcept
    {
        return values_.has_slot(slot);
    }

    bool TSDProxy::child_updated(std::size_t slot) const noexcept
    {
        return values_.slot_updated(slot);
    }

    const void *TSDProxy::child_at_slot(std::size_t slot) const
    {
        if (!has_child(slot)) { throw std::out_of_range("TSDProxy child slot is not constructed"); }
        refresh_stale_child(slot);
        return values_.value_memory(slot);
    }

    void *TSDProxy::child_at_slot(std::size_t slot)
    {
        if (!has_child(slot)) { throw std::out_of_range("TSDProxy child slot is not constructed"); }
        refresh_stale_child(slot);
        return values_.value_memory(slot);
    }

    void TSDProxy::subscribe_source()
    {
        if (subscribed_ || !source_storage_.valid()) { return; }
        source_dict().key_set().subscribe_slot_observer(&source_sync_);
        auto rollback_slot_observer = make_scope_exit<true>([&] {
            source_dict().key_set().unsubscribe_slot_observer(&source_sync_);
        });
        source_view().subscribe(&source_sync_);
        rollback_slot_observer.release();
        subscribed_ = true;
    }

    void TSDProxy::unsubscribe_source() noexcept
    {
        if (!subscribed_ || !source_storage_.valid()) { return; }
        FirstExceptionRecorder cleanup_errors;
        cleanup_errors.capture([&] {
            source_view().unsubscribe(&source_sync_);
        });
        cleanup_errors.capture([&] {
            source_dict().key_set().unsubscribe_slot_observer(&source_sync_);
        });
        subscribed_ = false;
    }

    void TSDProxy::sync_from_source(DateTime modified_time, bool force_modified)
    {
        if (element_binding_ == nullptr || !source_storage_.valid() || value_builder_ == nullptr)
        {
            return;
        }

        auto dict = source_dict();
        values_.reserve_to(dict.slot_capacity());
        if (built_times_.size() < dict.slot_capacity()) { built_times_.resize(dict.slot_capacity(), MIN_DT); }

        bool changed = force_modified;
        for (std::size_t slot = 0; slot < dict.slot_capacity(); ++slot)
        {
            if (dict.slot_live(slot) || dict.slot_removed(slot))
            {
                const bool existed = has_child(slot);
                ensure_child_at_slot(slot, modified_time);
                changed = changed || !existed;
                continue;
            }

            if (has_child(slot))
            {
                values_.destroy_at(slot);
                changed = true;
            }
        }

        if (changed) { mark_modified(modified_time); }
    }

    void TSDProxy::construct_child_at_slot(std::size_t slot)
    {
        if (self_binding_ == nullptr || element_binding_ == nullptr)
        {
            throw std::logic_error("TSDProxy is not initialised");
        }

        if (values_.has_slot(slot)) { return; }

        values_.construct_at(slot);
        auto target = TSDataView{element_binding_, values_.value_memory(slot)};
        target.mutable_tracking().parent = TSParentLink{self_binding_, this, slot};
    }

    void TSDProxy::ensure_child_at_slot(std::size_t slot, DateTime modified_time)
    {
        if (value_builder_ == nullptr)
        {
            throw std::logic_error("TSDProxy is not initialised");
        }

        construct_child_at_slot(slot);
        auto target = TSDataView{element_binding_, values_.value_memory(slot)};
        value_builder_(*this, slot, target, source_child_at_slot(slot), modified_time, value_builder_context_);
        stamp_built(slot, modified_time);
    }

    void TSDProxy::refresh_child_at_slot(std::size_t slot, DateTime modified_time)
    {
        if (element_binding_ == nullptr || value_builder_ == nullptr)
        {
            throw std::logic_error("TSDProxy is not initialised");
        }
        auto target = TSDataView{element_binding_, values_.value_memory(slot)};
        value_builder_(*this, slot, target, source_child_at_slot(slot), modified_time, value_builder_context_);
        stamp_built(slot, modified_time);
    }

    void TSDProxy::stamp_built(std::size_t slot, DateTime modified_time)
    {
        // Capacity is managed by the slot-observer callbacks (aligned with
        // ``values_`` and the source keyset); sync_from_source pre-reserves
        // for the initial bind, so this only defends against a stale call.
        if (built_times_.size() <= slot) { built_times_.resize(slot + 1, MIN_DT); }
        built_times_[slot] = modified_time;
    }

    void TSDProxy::refresh_stale_child(std::size_t slot) const
    {
        if (!has_child(slot) || value_builder_ == nullptr || !source_storage_.valid()) { return; }
        auto source_child = source_child_at_slot(slot);
        if (!source_child.valid() || source_child.binding() == nullptr) { return; }
        const auto source_time = source_child.tracking().last_modified_time;
        const auto built_time  = slot < built_times_.size() ? built_times_[slot] : MIN_DT;
        if (source_time == MIN_DT || source_time < built_time) { return; }
        if (source_time == built_time)
        {
            // Same-time writes are indistinguishable by stamp (insert + write
            // in one mutation share the evaluation time). Re-run the builder
            // only for a child that never materialised - the builder saw the
            // pre-write source; materialised children keep run-once semantics.
            auto child = TSDataView{element_binding_, values_.value_memory(slot)};
            if (child.has_current_value()) { return; }
        }
        // Deliberate interior mutability: derived state catching up with its
        // source on read (see refresh_stale_child in the header).
        auto *self = const_cast<TSDProxy *>(this);
        self->refresh_child_at_slot(slot, source_time);
    }

    void TSDProxy::mark_modified(DateTime modified_time)
    {
        if (tracking_.record_modified(modified_time)) { tracking_.parent.notify_child_modified(modified_time); }
    }

    void TSDProxy::record_child_modified(std::size_t slot, DateTime modified_time)
    {
        if (!has_child(slot)) { return; }
        // LAZY delta-window roll: updated bits describe the CURRENT window
        // only - a record at a new time clears the previous window's bits
        // (they otherwise over-report the Modified surface forever).
        if (modified_time != MIN_DT && modified_time != updated_window_)
        {
            for (std::size_t index = 0; index < built_times_.size(); ++index)
            {
                if (values_.has_slot(index)) { values_.clear_updated(index); }
            }
            updated_window_ = modified_time;
        }
        auto dict = source_dict();
        if (dict.slot_live(slot)) { values_.mark_updated(slot); }
    }

    void TSDProxy::subscribe_slot_observer(SlotObserver *observer)
    {
        slot_observers_.add(observer);
    }

    void TSDProxy::unsubscribe_slot_observer(SlotObserver *observer)
    {
        slot_observers_.remove(observer);
    }

    const TSDataBinding &tsd_proxy_binding_for(const TSValueTypeMetaData &schema,
                                               const TSDataBinding      &element_binding)
    {
        const auto &context = tsd_proxy_context_for(schema, element_binding);
        return TSDataBinding::intern(schema, *context.plan, context.dict_ops);
    }

    void clear_tsd_proxy_contexts() noexcept
    {
        std::lock_guard<std::recursive_mutex> lock(tsd_proxy_context_mutex());
        tsd_proxy_contexts().clear();
    }

    void bind_tsd_proxy(const TSDataView       &proxy,
                        const TSDDataView      &source,
                        TSDProxy::ValueBuilder  builder,
                        const void             *builder_context,
                        DateTime modified_time,
                        TSDProxyChildRefresh child_refresh)
    {
        if (!proxy.valid()) { throw std::invalid_argument("bind_tsd_proxy requires a live proxy view"); }
        if (proxy.schema() == nullptr || proxy.schema()->kind != TSTypeKind::TSD)
        {
            throw std::invalid_argument("bind_tsd_proxy requires a TSD proxy schema");
        }
        if (&proxy.binding()->checked_plan() != &MemoryUtils::plan_for<TSDProxy>())
        {
            throw std::invalid_argument("bind_tsd_proxy requires storage backed by TSDProxy");
        }
        if (source.schema() == nullptr || source.schema()->kind != TSTypeKind::TSD ||
            source.schema()->key_type() != proxy.schema()->key_type())
        {
            throw std::invalid_argument("bind_tsd_proxy requires a source TSD with the same key schema");
        }

        const auto &dict   = proxy.as_dict();
        const auto &layout = dict.layout();
        if (layout.element_binding == nullptr)
        {
            throw std::logic_error("bind_tsd_proxy requires an element binding");
        }

        auto &storage = proxy_storage(const_cast<void *>(proxy.data()));
        storage.bind(*proxy.binding(), *layout.element_binding, source, builder, builder_context, modified_time, child_refresh);
    }
}  // namespace hgraph
