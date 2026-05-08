#include <hgraph/types/metadata/ts_data_plan_factory.h>

#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/utils/intern_table.h>

#include <fmt/format.h>

#include <cstddef>
#include <stdexcept>
#include <string>

namespace hgraph
{
    namespace
    {
        struct AtomicTSDataOpsEntry
        {
            TSDataLayout layout{};
            TSDataOps    ops{};

            AtomicTSDataOpsEntry(const ValueTypeBinding &value_binding,
                                 const ValueTypeBinding &delta_binding,
                                 const MemoryUtils::StoragePlan &plan)
            {
                const auto *value_component    = plan.find_component("value");
                const auto *delta_component    = plan.find_component("delta");
                const auto *tracking_component = plan.find_component("tracking");
                if (value_component == nullptr || delta_component == nullptr || tracking_component == nullptr)
                {
                    throw std::logic_error("TSDataPlanFactory: atomic TSData plan is missing required components");
                }

                layout = TSDataLayout{
                    .value_binding   = &value_binding,
                    .delta_binding   = &delta_binding,
                    .value_offset    = value_component->offset,
                    .delta_offset    = delta_component->offset,
                    .tracking_offset = tracking_component->offset,
                };

                ops = TSDataOps{
                    .context                   = &layout,
                    .layout_impl               = &atomic_layout,
                    .tracking_impl             = &atomic_tracking,
                    .mutable_tracking_impl     = &atomic_mutable_tracking,
                    .value_memory_impl         = &atomic_value_memory,
                    .mutable_value_memory_impl = &atomic_mutable_value_memory,
                    .delta_memory_impl         = &atomic_delta_memory,
                    .mutable_delta_memory_impl = &atomic_mutable_delta_memory,
                    .copy_value_from_impl      = &atomic_copy_value_from,
                };
            }

            AtomicTSDataOpsEntry(const AtomicTSDataOpsEntry &other)
                : layout(other.layout), ops(other.ops)
            {
                ops.context = &layout;
            }

            AtomicTSDataOpsEntry(AtomicTSDataOpsEntry &&other) noexcept
                : layout(other.layout), ops(other.ops)
            {
                ops.context = &layout;
            }

            [[nodiscard]] static const TSDataLayout *atomic_layout(const void *context) noexcept
            {
                return static_cast<const TSDataLayout *>(context);
            }

            [[nodiscard]] static const void *advance(const TSDataLayout *layout,
                                                     const void         *memory,
                                                     std::size_t         offset) noexcept
            {
                return static_cast<const std::byte *>(memory) + offset;
            }

            [[nodiscard]] static void *advance(const TSDataLayout *layout, void *memory, std::size_t offset) noexcept
            {
                (void) layout;
                return static_cast<std::byte *>(memory) + offset;
            }

            [[nodiscard]] static const TSDataTracking *atomic_tracking(const void *context,
                                                                       const void *memory) noexcept
            {
                const auto *layout = atomic_layout(context);
                return MemoryUtils::cast<TSDataTracking>(advance(layout, memory, layout->tracking_offset));
            }

            [[nodiscard]] static TSDataTracking *atomic_mutable_tracking(const void *context, void *memory) noexcept
            {
                const auto *layout = atomic_layout(context);
                return MemoryUtils::cast<TSDataTracking>(advance(layout, memory, layout->tracking_offset));
            }

            [[nodiscard]] static const void *atomic_value_memory(const void *context, const void *memory) noexcept
            {
                const auto *layout = atomic_layout(context);
                return advance(layout, memory, layout->value_offset);
            }

            [[nodiscard]] static void *atomic_mutable_value_memory(const void *context, void *memory) noexcept
            {
                const auto *layout = atomic_layout(context);
                return advance(layout, memory, layout->value_offset);
            }

            [[nodiscard]] static const void *atomic_delta_memory(const void *context, const void *memory) noexcept
            {
                const auto *layout = atomic_layout(context);
                return advance(layout, memory, layout->delta_offset);
            }

            [[nodiscard]] static void *atomic_mutable_delta_memory(const void *context, void *memory) noexcept
            {
                const auto *layout = atomic_layout(context);
                return advance(layout, memory, layout->delta_offset);
            }

            static void assign_or_reconstruct(const MemoryUtils::StoragePlan &plan, void *dst, const void *src)
            {
                if (plan.can_copy_assign())
                {
                    plan.copy_assign(dst, src);
                    return;
                }
                if (!plan.can_copy_construct())
                {
                    throw std::logic_error("TSData atomic assignment requires copy assignment or copy construction");
                }
                plan.destroy(dst);
                plan.copy_construct(dst, src);
            }

            static void atomic_copy_value_from(const void *context,
                                               void       *memory,
                                               const ValueView &source,
                                               engine_time_t    modified_time)
            {
                if (memory == nullptr) { throw std::logic_error("TSData atomic copy requires live TSData memory"); }
                if (!source.has_value()) { throw std::invalid_argument("TSData atomic copy requires a live source value"); }

                const auto *layout = atomic_layout(context);
                if (source.binding() != layout->value_binding)
                {
                    throw std::invalid_argument("TSData atomic copy requires the bound value schema and plan");
                }

                const auto &value_plan = layout->value_binding->checked_plan();
                assign_or_reconstruct(value_plan, atomic_mutable_value_memory(context, memory), source.data());

                const auto &delta_plan = layout->delta_binding->checked_plan();
                if (layout->delta_binding == layout->value_binding)
                {
                    assign_or_reconstruct(delta_plan, atomic_mutable_delta_memory(context, memory), source.data());
                }
                else
                {
                    throw std::logic_error("TSData atomic copy currently requires value and delta bindings to match");
                }

                atomic_mutable_tracking(context, memory)->last_modified_time = modified_time;
            }
        };

        struct AtomicTSDataOpsKey
        {
            const ValueTypeBinding         *value_binding{nullptr};
            const ValueTypeBinding         *delta_binding{nullptr};
            const MemoryUtils::StoragePlan *plan{nullptr};

            [[nodiscard]] bool operator==(const AtomicTSDataOpsKey &) const noexcept = default;
        };

        struct AtomicTSDataOpsKeyHash
        {
            [[nodiscard]] std::size_t operator()(const AtomicTSDataOpsKey &key) const noexcept
            {
                std::size_t seed = std::hash<const ValueTypeBinding *>{}(key.value_binding);
                seed ^= std::hash<const ValueTypeBinding *>{}(key.delta_binding) + 0x9e3779b97f4a7c15ULL +
                        (seed << 6U) + (seed >> 2U);
                seed ^= std::hash<const MemoryUtils::StoragePlan *>{}(key.plan) + 0x9e3779b97f4a7c15ULL +
                        (seed << 6U) + (seed >> 2U);
                return seed;
            }
        };

        [[nodiscard]] InternTable<AtomicTSDataOpsKey, AtomicTSDataOpsEntry, AtomicTSDataOpsKeyHash> &
        atomic_ts_data_ops_cache() noexcept
        {
            static InternTable<AtomicTSDataOpsKey, AtomicTSDataOpsEntry, AtomicTSDataOpsKeyHash> cache;
            return cache;
        }

        [[nodiscard]] const TSDataOps &atomic_ts_data_ops(const ValueTypeBinding &value_binding,
                                                          const ValueTypeBinding &delta_binding,
                                                          const MemoryUtils::StoragePlan &plan)
        {
            return atomic_ts_data_ops_cache()
                .emplace(AtomicTSDataOpsKey{&value_binding, &delta_binding, &plan}, value_binding, delta_binding, plan)
                .ops;
        }

        void clear_ts_data_ops() noexcept
        {
            atomic_ts_data_ops_cache().clear();
        }

        [[nodiscard]] bool is_compact_atomic_ts_data(const TSValueTypeMetaData &schema) noexcept
        {
            switch (schema.kind)
            {
                case TSTypeKind::TS:
                case TSTypeKind::REF:
                case TSTypeKind::SIGNAL:
                    return schema.value_schema != nullptr && schema.delta_value_schema != nullptr &&
                           schema.value_schema->kind == ValueTypeKind::Atomic &&
                           schema.delta_value_schema->kind == ValueTypeKind::Atomic;
                default:
                    return false;
            }
        }

        [[noreturn]] void unsupported(const TSValueTypeMetaData *schema)
        {
            const auto kind = schema == nullptr ? -1 : static_cast<int>(schema->kind);
            throw std::logic_error(
                fmt::format("TSDataPlanFactory: slot-oriented TSData storage is not yet ported for kind {}", kind));
        }
    }  // namespace

    TSDataPlanFactory &TSDataPlanFactory::instance()
    {
        static TSDataPlanFactory factory;
        return factory;
    }

    const MemoryUtils::StoragePlan *TSDataPlanFactory::plan_for(const TSValueTypeMetaData *schema)
    {
        if (schema == nullptr) { return nullptr; }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (const auto it = cache_.find(schema); it != cache_.end()) { return it->second; }
        }

        return synthesise(schema);
    }

    const MemoryUtils::StoragePlan *TSDataPlanFactory::find(const TSValueTypeMetaData *schema) const
    {
        if (schema == nullptr) { return nullptr; }

        std::lock_guard<std::mutex> lock(mutex_);
        const auto                  it = cache_.find(schema);
        return it == cache_.end() ? nullptr : it->second;
    }

    void TSDataPlanFactory::reset() noexcept
    {
        std::lock_guard<std::mutex> lock(mutex_);
        cache_.clear();
        binding_cache_.clear();
        clear_ts_data_ops();
    }

    const TSDataBinding *TSDataPlanFactory::binding_for(const TSValueTypeMetaData *schema)
    {
        if (schema == nullptr) { return nullptr; }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (const auto it = binding_cache_.find(schema); it != binding_cache_.end()) { return it->second; }
        }

        return synthesise_binding(schema);
    }

    const TSDataBinding *TSDataPlanFactory::find_binding(const TSValueTypeMetaData *schema) const
    {
        if (schema == nullptr) { return nullptr; }

        std::lock_guard<std::mutex> lock(mutex_);
        const auto                  it = binding_cache_.find(schema);
        return it == binding_cache_.end() ? nullptr : it->second;
    }

    const MemoryUtils::StoragePlan *TSDataPlanFactory::synthesise(const TSValueTypeMetaData *schema)
    {
        if (schema == nullptr) { return nullptr; }
        if (!is_compact_atomic_ts_data(*schema)) { unsupported(schema); }

        const auto *value_plan = ValuePlanFactory::instance().plan_for(schema->value_schema);
        const auto *delta_plan = ValuePlanFactory::instance().plan_for(schema->delta_value_schema);
        if (value_plan == nullptr || delta_plan == nullptr)
        {
            throw std::logic_error("TSDataPlanFactory: atomic TSData value/delta plans are not resolvable");
        }

        auto builder = MemoryUtils::named_tuple();
        builder.reserve(3);
        builder.add_field("value", *value_plan);
        builder.add_field("delta", *delta_plan);
        builder.add_field("tracking", MemoryUtils::plan_for<TSDataTracking>());
        const auto *plan = &builder.build();

        std::lock_guard<std::mutex> lock(mutex_);
        if (const auto it = cache_.find(schema); it != cache_.end()) { return it->second; }
        cache_.emplace(schema, plan);
        return plan;
    }

    const TSDataBinding *TSDataPlanFactory::synthesise_binding(const TSValueTypeMetaData *schema)
    {
        if (schema == nullptr) { return nullptr; }
        if (!is_compact_atomic_ts_data(*schema)) { unsupported(schema); }

        const auto *value_binding = ValuePlanFactory::instance().binding_for(schema->value_schema);
        const auto *delta_binding = ValuePlanFactory::instance().binding_for(schema->delta_value_schema);
        if (value_binding == nullptr || delta_binding == nullptr)
        {
            throw std::logic_error("TSDataPlanFactory: atomic TSData value/delta bindings are not resolvable");
        }

        const auto *plan = plan_for(schema);
        if (plan == nullptr) { throw std::logic_error("TSDataPlanFactory: atomic TSData plan is not resolvable"); }

        const auto &ops = atomic_ts_data_ops(*value_binding, *delta_binding, *plan);
        const auto &binding = TSDataBinding::intern(*schema, *plan, ops);

        std::lock_guard<std::mutex> lock(mutex_);
        if (const auto it = binding_cache_.find(schema); it != binding_cache_.end()) { return it->second; }
        binding_cache_.emplace(schema, &binding);
        if (const auto plan_it = cache_.find(schema); plan_it != cache_.end())
        {
            if (plan_it->second != binding.plan())
            {
                throw std::logic_error("TSDataPlanFactory: synthesised binding does not match cached plan");
            }
        }
        else
        {
            cache_.emplace(schema, binding.plan());
        }
        return &binding;
    }
}  // namespace hgraph
