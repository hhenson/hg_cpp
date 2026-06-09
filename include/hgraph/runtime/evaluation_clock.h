#ifndef HGRAPH_RUNTIME_EVALUATION_CLOCK_H
#define HGRAPH_RUNTIME_EVALUATION_CLOCK_H

#include <hgraph/hgraph_export.h>
#include <hgraph/util/date_time.h>

namespace hgraph
{
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
    }  // namespace detail

    /**
     * Borrowed view over the active evaluation clock.
     *
     * The view owns no state. Runtime storage supplies the operation table and
     * memory pointer; static nodes receive this as a transparent injectable.
     */
    class HGRAPH_EXPORT EvaluationClockView
    {
      public:
        EvaluationClockView() noexcept
            : ops_(&detail::default_evaluation_clock_ops())
        {
        }

        EvaluationClockView(const EvaluationClockOps *ops, const void *memory) noexcept
            : ops_(ops != nullptr && memory != nullptr ? ops : &detail::default_evaluation_clock_ops()),
              memory_(ops != nullptr && memory != nullptr ? memory : nullptr)
        {
        }

        [[nodiscard]] bool valid() const noexcept { return memory_ != nullptr; }

        [[nodiscard]] DateTime evaluation_time() const noexcept
        {
            return ops_->evaluation_time_impl(ops_->context, memory_);
        }

        [[nodiscard]] DateTime now() const noexcept
        {
            return ops_->now_impl(ops_->context, memory_);
        }

        [[nodiscard]] TimeDelta cycle_time() const noexcept
        {
            return ops_->cycle_time_impl(ops_->context, memory_);
        }

        [[nodiscard]] DateTime next_cycle_evaluation_time() const noexcept
        {
            return ops_->next_cycle_evaluation_time_impl(ops_->context, memory_);
        }

      private:
        const EvaluationClockOps *ops_{&detail::default_evaluation_clock_ops()};
        const void               *memory_{nullptr};
    };

}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_EVALUATION_CLOCK_H
