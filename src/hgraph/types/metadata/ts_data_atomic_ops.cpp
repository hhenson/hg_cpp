#include <hgraph/types/metadata/ts_data_plan_factory_detail.h>

#include <hgraph/types/utils/intern_table.h>

#include <cstddef>
#include <cstdint>
#include <stdexcept>

namespace hgraph::ts_data_plan_factory_detail
{
    struct AtomicTSDataOpsEntry
    {
        TSDataLayout layout{};
        TSDataOps    ops{};

        AtomicTSDataOpsEntry(TSTypeKind kind, const ValueTypeBinding &value_binding,
                             const ValueTypeBinding &delta_binding,
                             std::size_t value_offset, std::size_t tracking_offset)
        {
            layout = TSDataLayout{
                .value_binding   = &value_binding,
                .delta_binding   = &delta_binding,
                .value_offset    = value_offset,
                .tracking_offset = tracking_offset,
            };

            ops = TSDataOps{
                .context                   = &layout,
                .kind                      = kind,
                .allows_mutation           = true,
                .layout_impl               = &atomic_layout,
                .tracking_impl             = &atomic_tracking,
                .mutable_tracking_impl     = &atomic_mutable_tracking,
                .has_current_value_impl    = &atomic_has_current_value,
                .all_valid_impl            = &atomic_has_current_value,
                .value_memory_impl         = &atomic_value_memory,
                .mutable_value_memory_impl = &atomic_mutable_value_memory,
                .delta_memory_impl         = &atomic_delta_memory,
                .mutable_delta_memory_impl = &atomic_mutable_delta_memory,
                .copy_value_from_impl      = &atomic_copy_value_from,
                .empty_delta_impl          = kind == TSTypeKind::REF ? &ts_data_detail::missing_empty_delta
                                                                     : &ts_data_detail::empty_delta_atomic,
                .capture_delta_impl        = kind == TSTypeKind::SIGNAL ? &ts_data_detail::capture_delta_signal
                                              : kind == TSTypeKind::TS     ? &ts_data_detail::capture_delta_ts
                                                                          : &ts_data_detail::missing_capture_delta,
                .delta_has_effect_impl     = kind == TSTypeKind::REF ? &ts_data_detail::missing_delta_has_effect
                                                                     : &ts_data_detail::delta_has_effect_atomic,
                .apply_delta_impl          = kind == TSTypeKind::REF ? &ts_data_detail::missing_apply_delta
                                                                     : &ts_data_detail::apply_delta_atomic,
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                .from_python_impl          = &atomic_from_python,
                .to_python_impl            = &atomic_to_python,
                .delta_to_python_impl      = &atomic_delta_to_python,
#endif
            };
        }

        AtomicTSDataOpsEntry(const AtomicTSDataOpsEntry &other) : layout(other.layout), ops(other.ops)
        {
            ops.context = &layout;
        }

        AtomicTSDataOpsEntry(AtomicTSDataOpsEntry &&other) noexcept : layout(other.layout), ops(other.ops)
        {
            ops.context = &layout;
        }

        [[nodiscard]] static const TSDataLayout *atomic_layout(const void *context) noexcept
        {
            return static_cast<const TSDataLayout *>(context);
        }

        [[nodiscard]] static const void *advance(const void *memory, std::size_t offset) noexcept
        {
            return static_cast<const std::byte *>(memory) + offset;
        }

        [[nodiscard]] static void *advance(void *memory, std::size_t offset) noexcept
        {
            return static_cast<std::byte *>(memory) + offset;
        }

        [[nodiscard]] static const TSDataTracking *atomic_tracking(const void *context, const void *memory) noexcept
        {
            const auto *layout = atomic_layout(context);
            return MemoryUtils::cast<TSDataTracking>(advance(memory, layout->tracking_offset));
        }

        [[nodiscard]] static TSDataTracking *atomic_mutable_tracking(const void *context, void *memory) noexcept
        {
            const auto *layout = atomic_layout(context);
            return MemoryUtils::cast<TSDataTracking>(advance(memory, layout->tracking_offset));
        }

        [[nodiscard]] static bool atomic_has_current_value(const void *context, const void *memory) noexcept
        {
            return atomic_tracking(context, memory)->last_modified_time != MIN_DT;
        }

        [[nodiscard]] static const void *atomic_value_memory(const void *context, const void *memory) noexcept
        {
            const auto *layout = atomic_layout(context);
            return advance(memory, layout->value_offset);
        }

        [[nodiscard]] static void *atomic_mutable_value_memory(const void *context, void *memory) noexcept
        {
            const auto *layout = atomic_layout(context);
            return advance(memory, layout->value_offset);
        }

        [[nodiscard]] static const void *atomic_delta_memory(const void *context, const void *memory) noexcept
        {
            return atomic_value_memory(context, memory);
        }

        [[nodiscard]] static void *atomic_mutable_delta_memory(const void *context, void *memory) noexcept
        {
            return atomic_mutable_value_memory(context, memory);
        }

        static void copy_assign_required(const MemoryUtils::StoragePlan &plan, void *dst, const void *src)
        {
            if (!plan.can_copy_assign())
            {
                throw std::logic_error("TSData atomic assignment requires copy-assignable value storage");
            }
            plan.copy_assign(dst, src);
        }

        [[nodiscard]] static bool atomic_copy_value_from(const void *context, void *memory, const ValueView &source,
                                                         DateTime modified_time)
        {
            if (memory == nullptr)
            {
                throw std::logic_error("TSData atomic copy requires live TSData memory");
            }
            if (!source.has_value())
            {
                throw std::invalid_argument("TSData atomic copy requires a live source value");
            }
            if (modified_time == MIN_DT)
            {
                throw std::invalid_argument("TSData atomic copy requires a concrete evaluation time");
            }

            const auto *layout = atomic_layout(context);
            if (source.binding() != layout->value_binding)
            {
                throw std::invalid_argument("TSData atomic copy requires the bound value schema and plan");
            }

            const auto *tracking       = atomic_tracking(context, memory);
            const bool  first_for_time = tracking->last_modified_time != modified_time;

            const auto &value_plan = layout->value_binding->checked_plan();
            copy_assign_required(value_plan, atomic_mutable_value_memory(context, memory), source.data());
            return first_for_time;
        }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        [[nodiscard]] static bool atomic_from_python(const void *context,
                                                     void       *memory,
                                                     nb::handle  source,
                                                     DateTime modified_time)
        {
            if (memory == nullptr)
            {
                throw std::logic_error("TSData atomic from_python requires live TSData memory");
            }
            if (source.is_none())
            {
                throw std::invalid_argument("TSData atomic from_python requires a non-None source");
            }
            if (modified_time == MIN_DT)
            {
                throw std::invalid_argument("TSData atomic from_python requires a concrete evaluation time");
            }

            const auto *layout = atomic_layout(context);
            const auto *tracking = atomic_tracking(context, memory);
            const bool  first_for_time = tracking->last_modified_time != modified_time;
            layout->value_binding->checked_ops().from_python(
                *layout->value_binding,
                atomic_mutable_value_memory(context, memory),
                source);
            return first_for_time;
        }

        [[nodiscard]] static nb::object atomic_to_python(const void *context, const void *memory)
        {
            const auto *layout = atomic_layout(context);
            return layout->value_binding->checked_ops().to_python(atomic_value_memory(context, memory));
        }

        [[nodiscard]] static nb::object atomic_delta_to_python(const void *context,
                                                               const void *memory,
                                                               DateTime evaluation_time)
        {
            if (atomic_tracking(context, memory)->last_modified_time != evaluation_time) { return nb::none(); }
            const auto *layout = atomic_layout(context);
            return layout->delta_binding->checked_ops().to_python(atomic_delta_memory(context, memory));
        }
#endif
    };

    struct AtomicTSDataOpsKey
    {
        const ValueTypeBinding         *value_binding{nullptr};
        const ValueTypeBinding         *delta_binding{nullptr};
        const MemoryUtils::StoragePlan *plan{nullptr};
        TSTypeKind                      kind{TSTypeKind::TS};
        std::size_t                     value_offset{0};
        std::size_t                     tracking_offset{0};

        [[nodiscard]] bool operator==(const AtomicTSDataOpsKey &) const noexcept = default;
    };

    struct AtomicTSDataOpsKeyHash
    {
        [[nodiscard]] std::size_t operator()(const AtomicTSDataOpsKey &key) const noexcept
        {
            std::size_t seed = std::hash<const ValueTypeBinding *>{}(key.value_binding);
            seed ^= std::hash<const ValueTypeBinding *>{}(key.delta_binding) + 0x9e3779b97f4a7c15ULL + (seed << 6U) +
                    (seed >> 2U);
            seed ^= std::hash<const MemoryUtils::StoragePlan *>{}(key.plan) + 0x9e3779b97f4a7c15ULL + (seed << 6U) +
                    (seed >> 2U);
            seed ^= std::hash<std::uint8_t>{}(static_cast<std::uint8_t>(key.kind)) + 0x9e3779b97f4a7c15ULL +
                    (seed << 6U) + (seed >> 2U);
            seed ^= std::hash<std::size_t>{}(key.value_offset) + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
            seed ^= std::hash<std::size_t>{}(key.tracking_offset) + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
            return seed;
        }
    };

    [[nodiscard]] InternTable<AtomicTSDataOpsKey, AtomicTSDataOpsEntry, AtomicTSDataOpsKeyHash> &
    atomic_ts_data_ops_cache() noexcept
    {
        static InternTable<AtomicTSDataOpsKey, AtomicTSDataOpsEntry, AtomicTSDataOpsKeyHash> cache;
        return cache;
    }

    [[nodiscard]] const TSDataOps &atomic_ts_data_ops(TSTypeKind                     kind,
                                                      const ValueTypeBinding         &value_binding,
                                                      const ValueTypeBinding         &delta_binding,
                                                      const MemoryUtils::StoragePlan &plan, std::size_t value_offset,
                                                      std::size_t tracking_offset)
    {
        return atomic_ts_data_ops_cache()
            .emplace(AtomicTSDataOpsKey{&value_binding, &delta_binding, &plan, kind, value_offset, tracking_offset},
                     kind, value_binding, delta_binding, value_offset, tracking_offset)
            .ops;
    }

    void clear_atomic_ts_data_ops() noexcept
    {
        atomic_ts_data_ops_cache().clear();
    }
} // namespace hgraph::ts_data_plan_factory_detail
