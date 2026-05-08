#ifndef HGRAPH_CPP_ROOT_TS_DATA_H
#define HGRAPH_CPP_ROOT_TS_DATA_H

#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/metadata/type_binding.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/util/date_time.h>

#include <cstddef>
#include <stdexcept>

namespace hgraph
{
    /**
     * Per-TSData modification stamps.
     *
     * This lives in the TSData payload/delta memory component, not in the
     * surrounding TSState. TSState still owns graph-level notification and
     * parent propagation; TSData owns the timestamps needed to decide whether
     * its local delta payload belongs to a given evaluation time.
     */
    struct TSDataTracking
    {
        engine_time_t last_modified_time{MIN_DT};
    };

    /**
     * Memory offsets for one TSData implementation.
     *
     * Current value bytes, delta bytes, and tracking bytes are separate plan
     * regions. Keeping the offsets in the ops context lets views expose the
     * current value as a contiguous value-layer view while still retaining
     * delta metadata beside it.
     */
    struct TSDataLayout
    {
        const ValueTypeBinding *value_binding{nullptr};
        const ValueTypeBinding *delta_binding{nullptr};
        std::size_t             value_offset{0};
        std::size_t             delta_offset{0};
        std::size_t             tracking_offset{0};
    };

    struct TSDataOps;
    using TSDataBinding = TypeBinding<TSValueTypeMetaData, TSDataOps>;

    /**
     * Type-erased operations over a TSData memory region.
     *
     * These ops only manage the payload/delta component inside a full
     * TSValue. Graph notifications, parent propagation, and subscriber lists
     * are owned by the surrounding TSState layer.
     */
    struct TSDataOps
    {
        const void *context{nullptr};

        const TSDataLayout *(*layout_impl)(const void *context) noexcept = nullptr;
        const TSDataTracking *(*tracking_impl)(const void *context, const void *memory) noexcept = nullptr;
        TSDataTracking *(*mutable_tracking_impl)(const void *context, void *memory) noexcept = nullptr;
        const void *(*value_memory_impl)(const void *context, const void *memory) noexcept = nullptr;
        void *(*mutable_value_memory_impl)(const void *context, void *memory) noexcept = nullptr;
        const void *(*delta_memory_impl)(const void *context, const void *memory) noexcept = nullptr;
        void *(*mutable_delta_memory_impl)(const void *context, void *memory) noexcept = nullptr;
        bool (*copy_value_from_impl)(const void *context, void *memory, const ValueView &source,
                                     engine_time_t modified_time) = nullptr;

        [[nodiscard]] const TSDataLayout &layout() const
        {
            if (layout_impl == nullptr) { throw std::logic_error("TSDataOps is missing layout access"); }
            const auto *result = layout_impl(context);
            if (result == nullptr) { throw std::logic_error("TSDataOps layout access returned null"); }
            return *result;
        }

        [[nodiscard]] const TSDataTracking &tracking(const void *memory) const
        {
            if (memory == nullptr) { throw std::logic_error("TSDataOps::tracking requires live TSData memory"); }
            if (tracking_impl == nullptr) { throw std::logic_error("TSDataOps is missing tracking access"); }
            const auto *result = tracking_impl(context, memory);
            if (result == nullptr) { throw std::logic_error("TSDataOps tracking access returned null"); }
            return *result;
        }

        [[nodiscard]] TSDataTracking &mutable_tracking(void *memory) const
        {
            if (memory == nullptr) { throw std::logic_error("TSDataOps::mutable_tracking requires live TSData memory"); }
            if (mutable_tracking_impl == nullptr)
            {
                throw std::logic_error("TSDataOps is missing mutable tracking access");
            }
            auto *result = mutable_tracking_impl(context, memory);
            if (result == nullptr) { throw std::logic_error("TSDataOps mutable tracking access returned null"); }
            return *result;
        }

        [[nodiscard]] ValueView value_view(const void *memory) const
        {
            if (memory == nullptr) { throw std::logic_error("TSDataOps::value_view requires live TSData memory"); }
            if (value_memory_impl == nullptr) { throw std::logic_error("TSDataOps is missing value access"); }
            const auto &data_layout = layout();
            return ValueView{data_layout.value_binding, value_memory_impl(context, memory)};
        }

        [[nodiscard]] ValueView delta_value_view(const void *memory, engine_time_t evaluation_time) const
        {
            if (memory == nullptr)
            {
                throw std::logic_error("TSDataOps::delta_value_view requires live TSData memory");
            }
            const auto &data_layout = layout();
            if (!has_delta(memory, evaluation_time)) { return ValueView{data_layout.delta_binding, nullptr}; }
            if (delta_memory_impl == nullptr) { throw std::logic_error("TSDataOps is missing delta access"); }
            return ValueView{data_layout.delta_binding, delta_memory_impl(context, memory)};
        }

        [[nodiscard]] engine_time_t last_modified_time(const void *memory) const
        {
            return tracking(memory).last_modified_time;
        }

        [[nodiscard]] bool has_delta(const void *memory, engine_time_t evaluation_time) const
        {
            return evaluation_time != MIN_DT && tracking(memory).last_modified_time == evaluation_time;
        }

        void mark_modified(void *memory, engine_time_t modified_time) const
        {
            mutable_tracking(memory).last_modified_time = modified_time;
        }

        [[nodiscard]] bool copy_value_from(void *memory, const ValueView &source, engine_time_t modified_time) const
        {
            if (copy_value_from_impl == nullptr)
            {
                throw std::logic_error("TSDataOps::copy_value_from is not available for this TSData kind");
            }
            return copy_value_from_impl(context, memory, source, modified_time);
        }
    };

    /**
     * Non-owning view over TSData.
     */
    class TSDataView
    {
      public:
        constexpr TSDataView() noexcept = default;

        TSDataView(const TSDataBinding *binding, void *data) noexcept
            : binding_(binding), data_(data), writable_(data != nullptr)
        {
        }

        TSDataView(const TSDataBinding *binding, const void *data) noexcept
            : binding_(binding), data_(data)
        {
        }

        [[nodiscard]] bool valid() const noexcept { return binding_ != nullptr && data_ != nullptr; }
        explicit operator bool() const noexcept { return valid(); }
        [[nodiscard]] const TSDataBinding *binding() const noexcept { return binding_; }
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept
        {
            return binding_ != nullptr ? binding_->type_meta : nullptr;
        }
        [[nodiscard]] const void *data() const noexcept { return data_; }
        [[nodiscard]] void *mutable_data() const
        {
            if (!valid()) { throw std::logic_error("TSDataView::mutable_data requires a live view"); }
            if (!writable_) { throw std::logic_error("TSDataView::mutable_data requires writable storage"); }
            return const_cast<void *>(data_);
        }

        [[nodiscard]] const TSDataOps &ops() const
        {
            if (binding_ == nullptr) { throw std::logic_error("TSDataView is not bound"); }
            return binding_->checked_ops();
        }

        [[nodiscard]] ValueView value() const { return ops().value_view(data_); }
        [[nodiscard]] ValueView delta_value(engine_time_t evaluation_time) const
        {
            return ops().delta_value_view(data_, evaluation_time);
        }
        [[nodiscard]] engine_time_t last_modified_time() const { return ops().last_modified_time(data_); }
        [[nodiscard]] bool modified(engine_time_t evaluation_time) const { return ops().has_delta(data_, evaluation_time); }

        void mark_modified(engine_time_t modified_time) { ops().mark_modified(mutable_data(), modified_time); }
        [[nodiscard]] bool copy_value_from(const ValueView &source, engine_time_t modified_time)
        {
            return ops().copy_value_from(mutable_data(), source, modified_time);
        }

      private:
        const TSDataBinding *binding_{nullptr};
        const void          *data_{nullptr};
        bool                 writable_{false};
    };

    /**
     * Owning TSData storage handle.
     */
    class TSData
    {
      public:
        using storage_type = MemoryUtils::StorageHandle<MemoryUtils::InlineStoragePolicy<>, TSDataBinding>;

        TSData() noexcept = default;

        explicit TSData(const TSDataBinding &binding)
            : storage_(binding)
        {
        }

        [[nodiscard]] bool has_value() const noexcept { return storage_.has_value(); }
        [[nodiscard]] const TSDataBinding *binding() const noexcept { return storage_.binding(); }
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept
        {
            const auto *bound = binding();
            return bound != nullptr ? bound->type_meta : nullptr;
        }

        [[nodiscard]] TSDataView view() { return TSDataView{binding(), storage_.data()}; }
        [[nodiscard]] TSDataView view() const { return TSDataView{binding(), storage_.data()}; }

      private:
        storage_type storage_{};
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_DATA_H
