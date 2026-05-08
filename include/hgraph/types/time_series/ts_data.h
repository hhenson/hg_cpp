#ifndef HGRAPH_CPP_ROOT_TS_DATA_H
#define HGRAPH_CPP_ROOT_TS_DATA_H

#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/metadata/type_binding.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/util/date_time.h>

#include <cstddef>
#include <exception>
#include <stdexcept>
#include <string>
#include <utility>

namespace hgraph
{
    struct TSDataOps;
    using TSDataBinding = TypeBinding<TSValueTypeMetaData, TSDataOps>;

    class TSDataView;
    class TSDataMutationView;

    inline constexpr std::size_t TS_DATA_NO_CHILD_ID = static_cast<std::size_t>(-1);

    /**
     * Per-TSData modification stamps.
     *
     * This lives in the TSData payload/delta memory component, not in the
     * surrounding TSState. TSDataView owns local parent bubble-up, TSState owns
     * graph-level notification, and TSData owns the timestamps needed to decide
     * whether its local delta payload belongs to a given evaluation time.
     */
    struct TSDataTracking
    {
        engine_time_t last_modified_time{MIN_DT};
        std::size_t   mutation_depth{0};
        bool          delta_dirty{false};
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

    namespace ts_data_detail
    {
        [[noreturn]] inline void missing_ts_data_op(const char *name)
        {
            throw std::logic_error(std::string{"TSDataOps is missing "} + name + " implementation");
        }

        [[nodiscard]] inline const TSDataLayout *missing_layout(const void *)
        {
            missing_ts_data_op("layout");
        }

        [[nodiscard]] inline const TSDataTracking *missing_tracking(const void *, const void *)
        {
            missing_ts_data_op("tracking");
        }

        [[nodiscard]] inline TSDataTracking *missing_mutable_tracking(const void *, void *)
        {
            missing_ts_data_op("mutable tracking");
        }

        [[nodiscard]] inline const void *missing_value_memory(const void *, const void *)
        {
            missing_ts_data_op("value memory");
        }

        [[nodiscard]] inline void *missing_mutable_value_memory(const void *, void *)
        {
            missing_ts_data_op("mutable value memory");
        }

        [[nodiscard]] inline const void *missing_delta_memory(const void *, const void *)
        {
            missing_ts_data_op("delta memory");
        }

        [[nodiscard]] inline void *missing_mutable_delta_memory(const void *, void *)
        {
            missing_ts_data_op("mutable delta memory");
        }

        inline void noop_reset_delta(const void *, void *) {}

        inline void noop_record_child_modified(const void *, void *, std::size_t) {}

        [[nodiscard]] inline bool missing_copy_value_from(const void *,
                                                          void *,
                                                          const ValueView &,
                                                          engine_time_t)
        {
            missing_ts_data_op("copy value");
        }
    }  // namespace ts_data_detail

    /**
     * Type-erased operation table over a TSData memory region.
     *
     * This is intentionally a passive table of function pointers plus context.
     * Generic read, mutation, delta, and parent-propagation policy lives on
     * TSDataView / TSDataMutationView.
     */
    struct TSDataOps
    {
        const void *context{nullptr};

        const TSDataLayout *(*layout_impl)(const void *context) = &ts_data_detail::missing_layout;
        const TSDataTracking *(*tracking_impl)(const void *context,
                                               const void *memory) = &ts_data_detail::missing_tracking;
        TSDataTracking *(*mutable_tracking_impl)(const void *context,
                                                 void *memory) = &ts_data_detail::missing_mutable_tracking;
        const void *(*value_memory_impl)(const void *context,
                                         const void *memory) = &ts_data_detail::missing_value_memory;
        void *(*mutable_value_memory_impl)(const void *context,
                                           void *memory) = &ts_data_detail::missing_mutable_value_memory;
        const void *(*delta_memory_impl)(const void *context,
                                         const void *memory) = &ts_data_detail::missing_delta_memory;
        void *(*mutable_delta_memory_impl)(const void *context,
                                           void *memory) = &ts_data_detail::missing_mutable_delta_memory;
        void (*reset_delta_impl)(const void *context, void *memory) = &ts_data_detail::noop_reset_delta;
        void (*record_child_modified_impl)(const void *context,
                                           void *memory,
                                           std::size_t child_id) = &ts_data_detail::noop_record_child_modified;
        bool (*copy_value_from_impl)(const void *context, void *memory, const ValueView &source,
                                     engine_time_t modified_time) = &ts_data_detail::missing_copy_value_from;
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

        TSDataView(const TSDataBinding *binding, void *data, TSDataView &parent, std::size_t child_id)
            : binding_(binding),
              data_(data),
              parent_(&parent),
              child_id_(child_id),
              writable_(data != nullptr)
        {
            require_parent_view(parent);
        }

        TSDataView(const TSDataBinding *binding, const void *data, TSDataView &parent, std::size_t child_id)
            : binding_(binding), data_(data), parent_(&parent), child_id_(child_id)
        {
            require_parent_view(parent);
        }

        [[nodiscard]] bool valid() const noexcept { return binding_ != nullptr && data_ != nullptr; }
        explicit operator bool() const noexcept { return valid(); }
        [[nodiscard]] const TSDataBinding *binding() const noexcept { return binding_; }
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept
        {
            return binding_ != nullptr ? binding_->type_meta : nullptr;
        }
        [[nodiscard]] const void *data() const noexcept { return data_; }
        [[nodiscard]] std::size_t child_id() const noexcept { return child_id_; }
        [[nodiscard]] bool has_parent() const noexcept { return parent_ != nullptr; }
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

        [[nodiscard]] const TSDataLayout &layout() const
        {
            const auto &table = ops();
            return *table.layout_impl(table.context);
        }

        [[nodiscard]] const TSDataTracking &tracking() const
        {
            require_live("TSDataView::tracking");
            const auto &table = ops();
            return *table.tracking_impl(table.context, data_);
        }

        [[nodiscard]] ValueView value() const
        {
            require_live("TSDataView::value");
            const auto &table = ops();
            return ValueView{layout().value_binding, table.value_memory_impl(table.context, data_)};
        }

        [[nodiscard]] ValueView delta_value(engine_time_t evaluation_time) const
        {
            require_live("TSDataView::delta_value");
            const auto &data_layout = layout();
            if (!modified(evaluation_time)) { return ValueView{data_layout.delta_binding, nullptr}; }

            const auto &table = ops();
            return ValueView{data_layout.delta_binding, table.delta_memory_impl(table.context, data_)};
        }
        [[nodiscard]] engine_time_t last_modified_time() const { return tracking().last_modified_time; }
        [[nodiscard]] std::size_t mutation_depth() const { return tracking().mutation_depth; }
        [[nodiscard]] bool delta_dirty() const { return tracking().delta_dirty; }
        [[nodiscard]] bool modified(engine_time_t evaluation_time) const
        {
            return tracking().last_modified_time == evaluation_time;
        }

        [[nodiscard]] TSDataMutationView begin_mutation(engine_time_t evaluation_time) const;

      private:
        friend class TSDataMutationView;

        static void require_parent_view(const TSDataView &parent)
        {
            if (!parent.valid()) { throw std::logic_error("TSDataView child construction requires a live parent view"); }
            if (!parent.writable_)
            {
                throw std::logic_error("TSDataView child construction requires a writable parent view");
            }
        }

        void require_live(const char *what) const
        {
            if (!valid()) { throw std::logic_error(std::string{what} + " requires a live view"); }
        }

        [[nodiscard]] TSDataTracking &mutable_tracking() const
        {
            const auto &table = ops();
            return *table.mutable_tracking_impl(table.context, mutable_data());
        }

        const TSDataBinding *binding_{nullptr};
        const void          *data_{nullptr};
        TSDataView          *parent_{nullptr};
        std::size_t          child_id_{TS_DATA_NO_CHILD_ID};
        bool                 writable_{false};
    };

    class TSDataMutationView
    {
      public:
        TSDataMutationView(TSDataView view, engine_time_t evaluation_time)
            : view_(view), mutation_time_(evaluation_time)
        {
            begin_scope();
        }

        TSDataMutationView(const TSDataMutationView &) = delete;
        TSDataMutationView &operator=(const TSDataMutationView &) = delete;

        TSDataMutationView(TSDataMutationView &&other) noexcept
            : view_(other.view_),
              mutation_time_(std::exchange(other.mutation_time_, MIN_DT)),
              owns_scope_(std::exchange(other.owns_scope_, false))
        {
        }

        TSDataMutationView &operator=(TSDataMutationView &&) = delete;

        ~TSDataMutationView() noexcept
        {
            if (owns_scope_) { end_scope_noexcept(); }
        }

        [[nodiscard]] const TSDataView &view() const noexcept { return view_; }
        [[nodiscard]] TSDataView &view() noexcept { return view_; }
        [[nodiscard]] ValueView value() const { return view_.value(); }
        [[nodiscard]] ValueView delta_value(engine_time_t evaluation_time) const
        {
            return view_.delta_value(evaluation_time);
        }
        [[nodiscard]] engine_time_t current_mutation_time() const { return mutation_time_; }
        [[nodiscard]] std::size_t mutation_depth() const { return view_.mutation_depth(); }

        void mark_modified()
        {
            if (record_modified_local()) { notify_parent_modified(); }
        }

        [[nodiscard]] bool copy_value_from(const ValueView &source)
        {
            require_active_mutation();

            const auto &table = view_.ops();
            const bool newly_modified =
                table.copy_value_from_impl(table.context, view_.mutable_data(), source, mutation_time_);
            if (newly_modified && !record_modified_local())
            {
                throw std::logic_error(
                    "TSDataMutationView::copy_value_from reported a new modification that was already recorded");
            }
            if (newly_modified) { notify_parent_modified(); }
            return newly_modified;
        }

        void mark_child_modified(std::size_t child_id)
        {
            const auto &table = view_.ops();
            table.record_child_modified_impl(table.context, view_.mutable_data(), child_id);
            if (record_modified_local()) { notify_parent_modified(); }
        }

      private:
        void require_active_mutation() const
        {
            if (view_.tracking().mutation_depth == 0)
            {
                throw std::logic_error("TSData mutation requires an active mutation scope");
            }
        }

        void begin_scope() const
        {
            if (mutation_time_ == MIN_DT)
            {
                throw std::invalid_argument("TSDataMutationView requires a concrete engine time");
            }

            auto       &state = view_.mutable_tracking();
            const auto &table = view_.ops();
            if (state.mutation_depth == 0 && state.delta_dirty && state.last_modified_time < mutation_time_)
            {
                table.reset_delta_impl(table.context, view_.mutable_data());
                state.delta_dirty = false;
            }

            ++state.mutation_depth;
        }

        void end_scope() const
        {
            auto &state = view_.mutable_tracking();
            if (state.mutation_depth == 0) { throw std::logic_error("TSDataMutationView depth underflow"); }

            --state.mutation_depth;
        }

        void end_scope_noexcept() const noexcept
        {
            try
            {
                end_scope();
            }
            catch (...)
            {
                std::terminate();
            }
        }

        [[nodiscard]] bool record_modified_local() const
        {
            require_active_mutation();

            auto &state = view_.mutable_tracking();
            if (state.last_modified_time == mutation_time_) { return false; }

            state.last_modified_time = mutation_time_;
            state.delta_dirty        = true;
            return true;
        }

        void notify_parent_modified() const
        {
            if (view_.parent_ == nullptr) { return; }

            auto parent_mutation = view_.parent_->begin_mutation(mutation_time_);
            parent_mutation.mark_child_modified(view_.child_id_);
        }

        TSDataView    view_{};
        engine_time_t mutation_time_{MIN_DT};
        bool          owns_scope_{true};
    };

    inline TSDataMutationView TSDataView::begin_mutation(engine_time_t evaluation_time) const
    {
        return TSDataMutationView{*this, evaluation_time};
    }

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
        [[nodiscard]] TSDataView view(TSDataView &parent, std::size_t child_id)
        {
            return TSDataView{binding(), storage_.data(), parent, child_id};
        }
        [[nodiscard]] TSDataView view(TSDataView &parent, std::size_t child_id) const
        {
            return TSDataView{binding(), storage_.data(), parent, child_id};
        }

      private:
        storage_type storage_{};
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_DATA_H
