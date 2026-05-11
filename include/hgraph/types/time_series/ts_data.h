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
     * graph-level notification, and TSData owns the timestamp needed to decide
     * whether its local delta view belongs to a given evaluation time.
     */
    struct TSDataTracking
    {
        engine_time_t last_modified_time{MIN_DT};
    };

    /**
     * Memory offsets for one TSData implementation.
     *
     * Current value bytes and tracking bytes are separate plan regions. Some
     * implementations also expose a separate delta memory region; compact
     * atomic TSData aliases ``delta_value(t)`` to the current value region
     * when ``last_modified_time == t``.
     */
    struct TSDataLayout
    {
        const ValueTypeBinding *value_binding{nullptr};
        const ValueTypeBinding *delta_binding{nullptr};
        std::size_t             value_offset{0};
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
        bool        allows_mutation{false};

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
     * Parent-owned identity carried by a child TSData view.
     *
     * The ``child_id`` is meaningful only to the parent view. Keeping it with
     * the parent reference makes that ownership explicit and keeps bubble-up
     * notification logic out of the generic mutation view.
     */
    struct TSDataParentLink
    {
        TSDataView *parent{nullptr};
        std::size_t child_id{TS_DATA_NO_CHILD_ID};

        constexpr TSDataParentLink() noexcept = default;
        TSDataParentLink(TSDataView &parent_view, std::size_t parent_child_id);

        [[nodiscard]] bool has_parent() const noexcept { return parent != nullptr; }
        void notify_child_modified(engine_time_t mutation_time) const;

      private:
        static void require_parent_view(const TSDataView &parent_view);
    };

    /**
     * Non-owning view over TSData.
     */
    class TSDataView
    {
      public:
        constexpr TSDataView() noexcept = default;

        TSDataView(const TSDataBinding *binding, void *data) noexcept
            : binding_(binding), data_(data)
        {
        }

        TSDataView(const TSDataBinding *binding, const void *data) noexcept
            : binding_(binding), data_(data)
        {
        }

        TSDataView(const TSDataBinding *binding, void *data, TSDataView &parent, std::size_t child_id)
            : binding_(binding),
              data_(data),
              parent_link_(parent, child_id)
        {
        }

        TSDataView(const TSDataBinding *binding, const void *data, TSDataView &parent, std::size_t child_id)
            : binding_(binding), data_(data), parent_link_(parent, child_id)
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
        [[nodiscard]] const TSDataParentLink &parent_link() const noexcept { return parent_link_; }
        [[nodiscard]] std::size_t child_id() const noexcept { return parent_link_.child_id; }
        [[nodiscard]] bool has_parent() const noexcept { return parent_link_.has_parent(); }
        [[nodiscard]] void *mutable_data() const
        {
            if (!valid()) { throw std::logic_error("TSDataView::mutable_data requires a live view"); }
            if (!ops().allows_mutation) { throw std::logic_error("TSDataView::mutable_data requires mutable TSData ops"); }
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
        [[nodiscard]] bool modified(engine_time_t evaluation_time) const
        {
            return tracking().last_modified_time == evaluation_time;
        }

        [[nodiscard]] TSDataMutationView begin_mutation(engine_time_t evaluation_time) const;

      private:
        friend class TSDataMutationView;

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
        TSDataParentLink     parent_link_{};
    };

    class TSDataMutationView
    {
      public:
        TSDataMutationView(TSDataView view, engine_time_t evaluation_time)
            : view_(view), mutation_time_(evaluation_time)
        {
            validate_mutation_view();
        }

        TSDataMutationView(const TSDataMutationView &) = delete;
        TSDataMutationView &operator=(const TSDataMutationView &) = delete;

        TSDataMutationView(TSDataMutationView &&other) noexcept
            : view_(other.view_),
              mutation_time_(std::exchange(other.mutation_time_, MIN_DT))
        {
        }

        TSDataMutationView &operator=(TSDataMutationView &&) = delete;

        ~TSDataMutationView() noexcept = default;

        [[nodiscard]] const TSDataView &view() const noexcept { return view_; }
        [[nodiscard]] TSDataView &view() noexcept { return view_; }
        [[nodiscard]] ValueView value() const { return view_.value(); }
        [[nodiscard]] ValueView delta_value(engine_time_t evaluation_time) const
        {
            return view_.delta_value(evaluation_time);
        }
        [[nodiscard]] engine_time_t current_mutation_time() const { return mutation_time_; }

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
            if (mutation_time_ == MIN_DT)
            {
                throw std::logic_error("TSData mutation requires an active mutation scope");
            }
            (void)view_.mutable_data();
        }

        void validate_mutation_view() const
        {
            if (mutation_time_ == MIN_DT)
            {
                throw std::invalid_argument("TSDataMutationView requires a concrete engine time");
            }
            (void)view_.mutable_data();
        }

        [[nodiscard]] bool record_modified_local() const
        {
            require_active_mutation();

            auto &state = view_.mutable_tracking();
            if (state.last_modified_time == mutation_time_) { return false; }

            state.last_modified_time = mutation_time_;
            return true;
        }

        void notify_parent_modified() const
        {
            view_.parent_link_.notify_child_modified(mutation_time_);
        }

        TSDataView    view_{};
        engine_time_t mutation_time_{MIN_DT};
    };

    inline TSDataParentLink::TSDataParentLink(TSDataView &parent_view, std::size_t parent_child_id)
        : parent(&parent_view), child_id(parent_child_id)
    {
        require_parent_view(parent_view);
    }

    inline void TSDataParentLink::require_parent_view(const TSDataView &parent_view)
    {
        if (!parent_view.valid())
        {
            throw std::logic_error("TSDataParentLink requires a live parent view");
        }
        if (!parent_view.ops().allows_mutation)
        {
            throw std::logic_error("TSDataParentLink requires mutable parent TSData ops");
        }
    }

    inline void TSDataParentLink::notify_child_modified(engine_time_t mutation_time) const
    {
        if (parent == nullptr) { return; }

        auto parent_mutation = parent->begin_mutation(mutation_time);
        parent_mutation.mark_child_modified(child_id);
    }

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
