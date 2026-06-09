#ifndef HGRAPH_RUNTIME_EVALUATION_CLOCK_H
#define HGRAPH_RUNTIME_EVALUATION_CLOCK_H

#include <hgraph/hgraph_export.h>
#include <hgraph/util/date_time.h>

#include <stdexcept>

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

    /**
     * Borrowed view over the active evaluation clock.
     *
     * The view owns no state. Runtime storage supplies the operation table and
     * memory pointer; static nodes receive this as a transparent injectable.
     */
    class HGRAPH_EXPORT EvaluationClockView
    {
      public:
        EvaluationClockView() noexcept = default;
        EvaluationClockView(const EvaluationClockOps *ops, const void *memory) noexcept
            : ops_(ops),
              memory_(memory)
        {
        }

        [[nodiscard]] bool valid() const noexcept { return ops_ != nullptr && memory_ != nullptr; }

        [[nodiscard]] DateTime evaluation_time() const noexcept
        {
            return valid() ? ops_->evaluation_time_impl(ops_->context, memory_) : MIN_DT;
        }

        [[nodiscard]] DateTime now() const noexcept
        {
            return valid() ? ops_->now_impl(ops_->context, memory_) : MIN_DT;
        }

        [[nodiscard]] TimeDelta cycle_time() const noexcept
        {
            return valid() ? ops_->cycle_time_impl(ops_->context, memory_) : TimeDelta{0};
        }

        [[nodiscard]] DateTime next_cycle_evaluation_time() const noexcept
        {
            return valid() ? ops_->next_cycle_evaluation_time_impl(ops_->context, memory_) : MIN_DT;
        }

      private:
        const EvaluationClockOps *ops_{nullptr};
        const void               *memory_{nullptr};
    };

}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_EVALUATION_CLOCK_H
