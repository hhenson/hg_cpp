#include <hgraph/types/time_series/ts_data.h>

#include <stdexcept>
#include <string>
#include <utility>

namespace hgraph
{
    TSDataView::TSDataView(const TSDataBinding *binding, void *data) noexcept
        : binding_(binding),
          data_(data)
    {
    }

    TSDataView::TSDataView(const TSDataBinding *binding, const void *data) noexcept
        : binding_(binding),
          data_(data)
    {
    }

    bool TSDataView::valid() const noexcept
    {
        return binding_ != nullptr && data_ != nullptr;
    }

    TSDataView::operator bool() const noexcept
    {
        return valid();
    }

    const TSDataBinding *TSDataView::binding() const noexcept
    {
        return binding_;
    }

    const TSValueTypeMetaData *TSDataView::schema() const noexcept
    {
        return binding_ != nullptr ? binding_->type_meta : nullptr;
    }

    const void *TSDataView::data() const noexcept
    {
        return data_;
    }

    const TSDataParentLink &TSDataView::parent_link() const
    {
        return tracking().parent;
    }

    std::size_t TSDataView::child_id() const
    {
        if (!valid()) { return TS_DATA_NO_CHILD_ID; }
        const auto &link = tracking().parent;
        return link.has_parent() ? link.child_id : TS_DATA_NO_CHILD_ID;
    }

    bool TSDataView::has_parent() const
    {
        return valid() && tracking().parent.has_parent();
    }

    std::vector<std::size_t> TSDataView::path_from_root() const
    {
        return valid() ? tracking().parent.path_from_root() : std::vector<std::size_t>{};
    }

    TSDataView TSDataView::root_view() const
    {
        if (!valid()) { return *this; }
        const auto &link = tracking().parent;
        return link.has_ts_data_parent() ? link.root_view() : *this;
    }

    void *TSDataView::mutable_data() const
    {
        if (!valid()) { throw std::logic_error("TSDataView::mutable_data requires a live view"); }
        if (!ops().allows_mutation) { throw std::logic_error("TSDataView::mutable_data requires mutable TSData ops"); }
        return const_cast<void *>(data_);
    }

    const TSDataOps &TSDataView::ops() const
    {
        if (binding_ == nullptr) { throw std::logic_error("TSDataView is not bound"); }
        return binding_->checked_ops();
    }

    const TSDataLayout &TSDataView::layout() const
    {
        const auto &table = ops();
        return *table.layout_impl(table.context);
    }

    const TSDataTracking &TSDataView::tracking() const
    {
        require_live("TSDataView::tracking");
        const auto &table = ops();
        return *table.tracking_impl(table.context, data_);
    }

    ValueView TSDataView::value() const
    {
        require_live("TSDataView::value");
        const auto &table = ops();
        return ValueView{layout().value_binding, table.value_memory_impl(table.context, data_)};
    }

    ValueView TSDataView::delta_value(engine_time_t evaluation_time) const
    {
        require_live("TSDataView::delta_value");
        const auto &data_layout = layout();
        if (!modified(evaluation_time)) { return ValueView{data_layout.delta_binding, nullptr}; }

        const auto &table = ops();
        return ValueView{data_layout.delta_binding, table.delta_memory_impl(table.context, data_)};
    }

    engine_time_t TSDataView::last_modified_time() const
    {
        return tracking().last_modified_time;
    }

    bool TSDataView::modified(engine_time_t evaluation_time) const
    {
        return tracking().last_modified_time == evaluation_time;
    }

    bool TSDataView::has_current_value() const
    {
        require_live("TSDataView::has_current_value");
        const auto &table = ops();
        return table.has_current_value_impl(table.context, data_);
    }

    bool TSDataView::all_valid() const
    {
        require_live("TSDataView::all_valid");
        if (!has_current_value()) { return false; }
        const auto &table = ops();
        return table.all_valid_impl(table.context, data_);
    }

    void TSDataView::cleanup_delta(engine_time_t modified_time) const
    {
        require_live("TSDataView::cleanup_delta");
        if (modified_time == MIN_DT || tracking().last_modified_time != modified_time) { return; }

        const auto &table = ops();
        table.cleanup_delta_impl(table.context, mutable_data(), modified_time);
    }

    TSSDataView TSDataView::as_set() &
    {
        return TSSDataView{*this};
    }

    TSSDataView TSDataView::as_set() const &
    {
        return TSSDataView{const_cast<TSDataView &>(*this)};
    }

    TSDDataView TSDataView::as_dict() &
    {
        return TSDDataView{*this};
    }

    TSDDataView TSDataView::as_dict() const &
    {
        return TSDDataView{const_cast<TSDataView &>(*this)};
    }

    TSBDataView TSDataView::as_bundle() &
    {
        return TSBDataView{*this};
    }

    TSBDataView TSDataView::as_bundle() const &
    {
        return TSBDataView{const_cast<TSDataView &>(*this)};
    }

    TSLDataView TSDataView::as_list() &
    {
        return TSLDataView{*this};
    }

    TSLDataView TSDataView::as_list() const &
    {
        return TSLDataView{const_cast<TSDataView &>(*this)};
    }

    TSWDataView TSDataView::as_window() &
    {
        return TSWDataView{*this};
    }

    TSWDataView TSDataView::as_window() const &
    {
        return TSWDataView{const_cast<TSDataView &>(*this)};
    }

    TSDataMutationView TSDataView::begin_mutation(engine_time_t evaluation_time) const
    {
        return TSDataMutationView{*this, evaluation_time};
    }

    TSDataView::TSDataView(const TSDataBinding *binding, void *data, TSDataView &parent, std::size_t child_id)
        : binding_(binding),
          data_(data)
    {
        bind_parent(parent, child_id);
    }

    TSDataView::TSDataView(const TSDataBinding *binding, const void *data, TSDataView &parent, std::size_t child_id)
        : binding_(binding),
          data_(data)
    {
        bind_parent(parent, child_id);
    }

    void TSDataView::require_live(const char *what) const
    {
        if (!valid()) { throw std::logic_error(std::string{what} + " requires a live view"); }
    }

    TSDataTracking &TSDataView::mutable_tracking() const
    {
        const auto &table = ops();
        return *table.mutable_tracking_impl(table.context, mutable_data());
    }

    void TSDataView::bind_parent(const TSDataView &parent, std::size_t child_id) const
    {
        if (!valid()) { throw std::logic_error("TSDataView child requires a live view"); }
        if (!parent.valid()) { throw std::logic_error("TSDataView child requires a live parent view"); }
        if (!parent.ops().allows_mutation)
        {
            throw std::logic_error("TSDataView child requires mutable parent TSData ops");
        }
        mutable_tracking().parent = TSDataParentLink{parent.binding(), parent.data(), child_id};
    }

    void TSDataView::bind_parent(TSDataParent &parent, std::size_t child_id) const
    {
        if (!valid()) { throw std::logic_error("TSDataView child requires a live view"); }
        mutable_tracking().parent = TSDataParentLink{parent, child_id};
    }

    TSDataMutationView::TSDataMutationView(TSDataView view, engine_time_t evaluation_time)
        : view_(view),
          mutation_time_(evaluation_time)
    {
        validate_mutation_view();
    }

    TSDataMutationView::TSDataMutationView(TSDataMutationView &&other) noexcept
        : view_(other.view_),
          mutation_time_(std::exchange(other.mutation_time_, MIN_DT))
    {
    }

    TSDataMutationView::~TSDataMutationView() noexcept = default;

    const TSDataView &TSDataMutationView::view() const noexcept
    {
        return view_;
    }

    TSDataView &TSDataMutationView::view() noexcept
    {
        return view_;
    }

    const TSDataOps &TSDataMutationView::ops() const
    {
        return view_.ops();
    }

    void *TSDataMutationView::mutable_data() const
    {
        require_active_mutation();
        return view_.mutable_data();
    }

    ValueView TSDataMutationView::value() const
    {
        return view_.value();
    }

    ValueView TSDataMutationView::delta_value(engine_time_t evaluation_time) const
    {
        return view_.delta_value(evaluation_time);
    }

    engine_time_t TSDataMutationView::current_mutation_time() const
    {
        return mutation_time_;
    }

    bool TSDataMutationView::modified(engine_time_t evaluation_time) const
    {
        return view_.modified(evaluation_time);
    }

    void TSDataMutationView::mark_modified()
    {
        if (record_modified_local()) { notify_parent_modified(); }
    }

    bool TSDataMutationView::copy_value_from(const ValueView &source)
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
        if (newly_modified)
        {
            notify_parent_modified();
        }
        return newly_modified;
    }

    void TSDataMutationView::require_active_mutation() const
    {
        if (mutation_time_ == MIN_DT)
        {
            throw std::logic_error("TSData mutation requires an active mutation scope");
        }
        (void)view_.mutable_data();
    }

    void TSDataMutationView::validate_mutation_view() const
    {
        if (mutation_time_ == MIN_DT)
        {
            throw std::invalid_argument("TSDataMutationView requires a concrete engine time");
        }
        (void)view_.mutable_data();
    }

    bool TSDataMutationView::record_modified_local() const
    {
        require_active_mutation();

        auto &state = view_.mutable_tracking();
        if (state.last_modified_time == mutation_time_) { return false; }

        state.last_modified_time = mutation_time_;
        return true;
    }

    void TSDataMutationView::notify_parent_modified() const
    {
        view_.parent_link().notify_child_modified(mutation_time_);
    }

    void apply_slot_mutation_result(TSDataMutationView &mutation, const SlotTSDataMutationResult &result)
    {
        if (!result.changed) { return; }
        mutation.mark_modified();
    }
}  // namespace hgraph
