#ifndef HGRAPH_RUNTIME_EVALUATION_CLOCK_H
#define HGRAPH_RUNTIME_EVALUATION_CLOCK_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/metadata/type_binding.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/util/date_time.h>

#include <cstddef>
#include <string_view>

namespace hgraph
{
    /** Schema descriptor for a type-erased evaluation clock provider. */
    struct HGRAPH_EXPORT EvaluationClockTypeMetaData
    {
        const char *display_name{nullptr};

        [[nodiscard]] std::string_view name() const noexcept
        {
            return display_name != nullptr ? std::string_view{display_name} : std::string_view{};
        }
    };

    /** Type-erased read-only clock operations exposed to user-authored nodes. */
    struct HGRAPH_EXPORT EvaluationClockOps
    {
        const void *context{nullptr};

        [[nodiscard]] DateTime (*evaluation_time_impl)(const void *context, const void *memory) noexcept = nullptr;
        [[nodiscard]] DateTime (*now_impl)(const void *context, const void *memory) noexcept = nullptr;
        [[nodiscard]] TimeDelta (*cycle_time_impl)(const void *context, const void *memory) noexcept = nullptr;
        [[nodiscard]] DateTime (*next_cycle_evaluation_time_impl)(const void *context,
                                                                  const void *memory) noexcept = nullptr;
    };

    using EvaluationClockTypeBinding = TypeBinding<EvaluationClockTypeMetaData, EvaluationClockOps>;
    using EvaluationClockStorageRef  = MemoryUtils::StorageRef<EvaluationClockTypeBinding>;

    namespace detail
    {
        [[nodiscard]] inline DateTime default_evaluation_time_impl(const void *, const void *) noexcept
        {
            return MIN_DT;
        }

        [[nodiscard]] inline DateTime default_now_impl(const void *, const void *) noexcept
        {
            return MIN_DT;
        }

        [[nodiscard]] inline TimeDelta default_cycle_time_impl(const void *, const void *) noexcept
        {
            return TimeDelta{0};
        }

        [[nodiscard]] inline DateTime default_next_cycle_evaluation_time_impl(const void *, const void *) noexcept
        {
            return MIN_DT;
        }

        [[nodiscard]] inline const EvaluationClockOps &default_evaluation_clock_ops() noexcept
        {
            static constexpr EvaluationClockOps table{
                .context = nullptr,
                .evaluation_time_impl = &default_evaluation_time_impl,
                .now_impl = &default_now_impl,
                .cycle_time_impl = &default_cycle_time_impl,
                .next_cycle_evaluation_time_impl = &default_next_cycle_evaluation_time_impl,
            };
            return table;
        }

        [[nodiscard]] inline const EvaluationClockTypeBinding &default_evaluation_clock_binding() noexcept
        {
            static const EvaluationClockTypeMetaData meta{.display_name = "default_evaluation_clock"};
            static const EvaluationClockTypeBinding binding{
                .type_meta = &meta,
                .storage_plan = &MemoryUtils::plan_for<std::byte>(),
                .ops = &default_evaluation_clock_ops(),
            };
            return binding;
        }
    }  // namespace detail

    /**
     * Borrowed view over the active evaluation clock.
     *
     * The view owns no state. Runtime storage supplies a borrowed storage ref
     * whose binding carries the operation table; static nodes receive this as
     * a transparent injectable.
     */
    class HGRAPH_EXPORT EvaluationClockView
    {
      public:
        EvaluationClockView() noexcept
            : storage_(EvaluationClockStorageRef::empty(detail::default_evaluation_clock_binding()))
        {
        }

        explicit EvaluationClockView(EvaluationClockStorageRef storage) noexcept
            : storage_(storage.has_value()
                           ? storage
                           : EvaluationClockStorageRef::empty(detail::default_evaluation_clock_binding()))
        {
        }

        [[nodiscard]] bool valid() const noexcept { return storage_.has_value(); }

        [[nodiscard]] DateTime evaluation_time() const noexcept
        {
            const auto &table = ops();
            return table.evaluation_time_impl(table.context, storage_.data());
        }

        [[nodiscard]] DateTime now() const noexcept
        {
            const auto &table = ops();
            return table.now_impl(table.context, storage_.data());
        }

        [[nodiscard]] TimeDelta cycle_time() const noexcept
        {
            const auto &table = ops();
            return table.cycle_time_impl(table.context, storage_.data());
        }

        [[nodiscard]] DateTime next_cycle_evaluation_time() const noexcept
        {
            const auto &table = ops();
            return table.next_cycle_evaluation_time_impl(table.context, storage_.data());
        }

      private:
        [[nodiscard]] const EvaluationClockOps &ops() const noexcept
        {
            return storage_.binding()->ops_ref();
        }

        EvaluationClockStorageRef storage_{EvaluationClockStorageRef::empty(detail::default_evaluation_clock_binding())};
    };

}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_EVALUATION_CLOCK_H
