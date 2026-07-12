#ifndef HGRAPH_RUNTIME_CLOCK_TYPE_REF_H
#define HGRAPH_RUNTIME_CLOCK_TYPE_REF_H

#include <hgraph/types/metadata/type_record.h>
#include <hgraph/types/type_pointer.h>

#include <cstddef>
#include <functional>
#include <string_view>
#include <type_traits>

namespace hgraph
{
    struct EvaluationClockOps;
    struct EvaluationClockTypeMetaData;

    inline constexpr std::uint16_t CLOCK_OPS_ABI_VERSION = 1;

    class ClockTypeRef
    {
      public:
        constexpr ClockTypeRef() noexcept = default;
        constexpr ClockTypeRef(std::nullptr_t) noexcept {}

        [[nodiscard]] static ClockTypeRef checked(AnyPtr pointer);

        [[nodiscard]] constexpr const TypeRecord *record() const noexcept { return record_; }
        [[nodiscard]] constexpr bool bound() const noexcept { return record_ != nullptr; }
        [[nodiscard]] bool valid() const noexcept;
        [[nodiscard]] constexpr explicit operator bool() const noexcept { return bound(); }
        [[nodiscard]] const EvaluationClockTypeMetaData *schema() const noexcept;
        [[nodiscard]] constexpr const MemoryUtils::StoragePlan *plan() const noexcept
        {
            return record_ != nullptr ? record_->plan : nullptr;
        }
        [[nodiscard]] const MemoryUtils::StoragePlan &checked_plan() const;
        [[nodiscard]] const EvaluationClockOps *ops() const noexcept;
        [[nodiscard]] const EvaluationClockOps &ops_ref() const;
        [[nodiscard]] constexpr TypeCapabilities capabilities() const noexcept
        {
            return record_ != nullptr ? record_->capabilities : TypeCapabilities::None;
        }

        [[nodiscard]] ClockPtr typed_null() const noexcept;
        [[nodiscard]] ClockPtr read_only(const void *data) const noexcept;

        [[nodiscard]] friend constexpr bool operator==(ClockTypeRef, ClockTypeRef) noexcept = default;

      private:
        friend ClockTypeRef intern_clock_type(const EvaluationClockTypeMetaData &,
                                              const MemoryUtils::StoragePlan &,
                                              const EvaluationClockOps &, std::string_view);
        friend class EvaluationClockView;

        explicit constexpr ClockTypeRef(const TypeRecord *record) noexcept : record_(record) {}

        const TypeRecord *record_{nullptr};
    };

    [[nodiscard]] TypeCapabilities clock_type_capabilities(const MemoryUtils::StoragePlan &plan);
    [[nodiscard]] ClockTypeRef intern_clock_type(const EvaluationClockTypeMetaData &schema,
                                                 const MemoryUtils::StoragePlan &plan,
                                                 const EvaluationClockOps &ops,
                                                 std::string_view implementation_label = {});

    static_assert(sizeof(ClockTypeRef) == sizeof(void *));
    static_assert(alignof(ClockTypeRef) == alignof(void *));
    static_assert(std::is_standard_layout_v<ClockTypeRef>);
    static_assert(std::is_trivially_copyable_v<ClockTypeRef>);
}  // namespace hgraph

template <> struct std::hash<hgraph::ClockTypeRef>
{
    [[nodiscard]] std::size_t operator()(hgraph::ClockTypeRef type) const noexcept
    {
        return std::hash<const hgraph::TypeRecord *>{}(type.record());
    }
};

#endif  // HGRAPH_RUNTIME_CLOCK_TYPE_REF_H
