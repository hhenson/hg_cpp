#include <hgraph/types/time_series/ts_data.h>

#include <hgraph/runtime/graph.h>
#include <hgraph/runtime/node.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/value/value.h>

#include <stdexcept>
#include <string>
#include <utility>

namespace hgraph
{
    TSDataView::TSDataView(const TSDataBinding *binding, void *data) noexcept
        : storage_(binding, data)
    {
    }

    TSDataView::TSDataView(const TSDataBinding *binding, const void *data) noexcept
        : storage_(binding, data)
    {
    }

    TSDataView::TSDataView(TSStorageTypeRef type, void *data) noexcept
        : storage_(type, data)
    {
    }

    TSDataView::TSDataView(TSStorageTypeRef type, const void *data) noexcept
        : storage_(type, data)
    {
    }

    TSDataView::TSDataView(TSRoleTypeRef type, void *data) noexcept
        : storage_(type, data)
    {
    }

    TSDataView::TSDataView(TSDataStorageRef<> storage) noexcept
        : storage_(storage)
    {
    }

    TSDataView TSDataView::borrowed_ref() const noexcept
    {
        return TSDataView{storage_};
    }

    TSDataStorageRef<> TSDataView::storage_ref() const noexcept
    {
        return storage_;
    }

    bool TSDataView::valid() const noexcept
    {
        return storage_.has_value();
    }

    TSDataView::operator bool() const noexcept
    {
        return valid();
    }

    const TSDataBinding *TSDataView::binding() const noexcept
    {
        return storage_.binding();
    }

    TSStorageTypeRef TSDataView::storage_type() const noexcept
    {
        return storage_.storage_type();
    }

    TSRoleTypeRef TSDataView::type_ref() const noexcept
    {
        return storage_.type_ref();
    }

    const TSValueTypeMetaData *TSDataView::schema() const noexcept
    {
        return storage_.schema();
    }

    const void *TSDataView::data() const noexcept
    {
        return storage_.data();
    }

    const TSParentLink &TSDataView::parent_link() const
    {
        return tracking().parent;
    }

    std::size_t TSDataView::child_id() const
    {
        const auto &link = tracking().parent;
        return link.has_parent() ? link.child_id : TS_DATA_NO_CHILD_ID;
    }

    bool TSDataView::has_parent() const
    {
        return tracking().parent.has_parent();
    }

    std::vector<std::size_t> TSDataView::path_from_root() const
    {
        return tracking().parent.path_from_root();
    }

    TSDataView TSDataView::root_view() const
    {
        const auto &link = tracking().parent;
        return link.has_ts_data_parent() ? link.root_view() : borrowed_ref();
    }

    TSParentLink TSDataView::root_endpoint_owner() const noexcept
    {
        if (!valid()) { return {}; }

        auto current = borrowed_ref();
        for (std::size_t depth = 0; current.valid() && depth < 256; ++depth)
        {
            const auto &link = current.parent_link();
            if (link.has_endpoint_parent()) { return link; }
            if (!link.has_ts_data_parent()) { return {}; }
            current = TSDataView{link.parent_storage_type(), link.parent_data()};
        }
        return {};
    }

    NodeView TSDataView::owner_node() const
    {
        return root_endpoint_owner().parent_node();
    }

    GraphView TSDataView::owner_graph() const
    {
        return root_endpoint_owner().parent_graph();
    }

    void *TSDataView::mutable_data() const
    {
        const auto type = type_ref();
        if (type && !has_capability(type.capabilities(), TypeCapabilities::Mutable))
            throw std::logic_error("TSDataView::mutable_data requires a mutable time-series role");
        if (!ops().allows_mutation) { throw std::logic_error("TSDataView::mutable_data requires mutable TSData ops"); }
        return storage_.data();
    }

    const TSDataOps &TSDataView::ops() const
    {
        return storage_.ops();
    }

    const TSDataLayout &TSDataView::layout() const
    {
        const auto &table = ops();
        return *table.layout_impl(table.context);
    }

    const TSDataTracking &TSDataView::tracking() const
    {
        const auto &table = ops();
        return *table.tracking_impl(table.context, data());
    }

    ValueView TSDataView::value() const
    {
        const auto &table = ops();
        return ValueView{layout().value_binding, table.value_memory_impl(table.context, data())};
    }

    ValueView TSDataView::delta_value(DateTime evaluation_time) const
    {
        const auto &data_layout = layout();
        if (!modified(evaluation_time)) { return ValueView{data_layout.delta_binding, nullptr}; }

        const auto &table = ops();
        return ValueView{data_layout.delta_binding, table.delta_memory_impl(table.context, data())};
    }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
    nb::object TSDataView::value_to_python() const
    {
        if (!has_current_value()) { return nb::none(); }

        const auto &table = ops();
        return table.to_python_impl(table.context, data());
    }

    nb::object TSDataView::delta_value_to_python(DateTime evaluation_time) const
    {
        if (!modified(evaluation_time)) { return nb::none(); }

        const auto &table = ops();
        return table.delta_to_python_impl(table.context, data(), evaluation_time);
    }
#endif

    DateTime TSDataView::last_modified_time() const
    {
        return tracking().last_modified_time;
    }

    bool TSDataView::modified(DateTime evaluation_time) const
    {
        return evaluation_time != MIN_DT && tracking().last_modified_time == evaluation_time;
    }

    void TSDataView::subscribe(Notifiable *observer) const
    {
        require_live("TSDataView::subscribe");
        mutable_tracking().observers.subscribe(observer);
    }

    void TSDataView::unsubscribe(Notifiable *observer) const
    {
        require_live("TSDataView::unsubscribe");
        mutable_tracking().observers.unsubscribe(observer);
    }

    void TSDataView::replace_observer(Notifiable *observer, Notifiable *replacement) const
    {
        require_live("TSDataView::replace_observer");
        mutable_tracking().observers.replace(observer, replacement);
    }

    bool TSDataView::has_observers() const
    {
        return !tracking().observers.empty();
    }

    std::size_t TSDataView::observer_count() const
    {
        return tracking().observers.size();
    }

    bool TSDataView::has_current_value() const
    {
        const auto &table = ops();
        return table.has_current_value_impl(table.context, data());
    }

    bool TSDataView::all_valid() const
    {
        if (!has_current_value()) { return false; }
        const auto &table = ops();
        return table.all_valid_impl(table.context, data());
    }

    std::size_t TSDataView::indexed_child_count() const
    {
        const auto &table = ops();
        return table.indexed_child_count_impl(table.context, data());
    }

    TSDataView TSDataView::indexed_child_at(std::size_t index) const
    {
        const auto &table = ops();
        if (index >= table.indexed_child_count_impl(table.context, data()))
        {
            throw std::out_of_range("TSDataView::indexed_child_at index out of range");
        }

        const auto *element_binding = table.indexed_child_binding_impl(table.context, data(), index);
        if (element_binding == nullptr)
        {
            throw std::logic_error("TSDataView::indexed_child_at element binding is not resolved");
        }

        const auto *element_memory = table.indexed_child_memory_impl(table.context, data(), index);
        if (element_memory == nullptr) { return TSDataView{element_binding, element_memory}; }

        auto parent = borrowed_ref();
        if (!parent.ops().allows_mutation) { return TSDataView{element_binding, element_memory}; }
        return TSDataView{element_binding, element_memory, parent, index};
    }

    TSDataView TSDataView::ensure_indexed_child_at(std::size_t index) const
    {
        const auto &table = ops();
        if (index >= table.indexed_child_count_impl(table.context, data()))
        {
            if (!table.indexed_child_growth)
            {
                throw std::out_of_range("TSDataView::ensure_indexed_child_at index out of range");
            }
            static_cast<void>(table.mutable_indexed_child_memory_impl(table.context, mutable_data(), index));
        }
        return indexed_child_at(index);
    }

    bool TSDataView::clear_collection(DateTime modified_time) const
    {
        const auto &table = ops();
        return table.clear_collection_impl(borrowed_ref(), modified_time);
    }

    TSSDataView TSDataView::as_set() &
    {
        return TSSDataView{borrowed_ref()};
    }

    TSSDataView TSDataView::as_set() const &
    {
        return TSSDataView{borrowed_ref()};
    }

    TSDDataView TSDataView::as_dict() &
    {
        return TSDDataView{borrowed_ref()};
    }

    TSDDataView TSDataView::as_dict() const &
    {
        return TSDDataView{borrowed_ref()};
    }

    TSBDataView TSDataView::as_bundle() &
    {
        return TSBDataView{borrowed_ref()};
    }

    TSBDataView TSDataView::as_bundle() const &
    {
        return TSBDataView{borrowed_ref()};
    }

    TSLDataView TSDataView::as_list() &
    {
        return TSLDataView{borrowed_ref()};
    }

    TSLDataView TSDataView::as_list() const &
    {
        return TSLDataView{borrowed_ref()};
    }

    TSWDataView TSDataView::as_window() &
    {
        return TSWDataView{borrowed_ref()};
    }

    TSWDataView TSDataView::as_window() const &
    {
        return TSWDataView{borrowed_ref()};
    }

    TSDataMutationView TSDataView::begin_mutation(DateTime evaluation_time) const
    {
        return TSDataMutationView{borrowed_ref(), evaluation_time};
    }

    TSDataView::TSDataView(const TSDataBinding *binding, void *data, const TSDataView &parent, std::size_t child_id)
        : storage_(binding, data)
    {
        bind_parent(parent, child_id);
    }

    TSDataView::TSDataView(const TSDataBinding *binding, const void *data, const TSDataView &parent, std::size_t child_id)
        : storage_(binding, data)
    {
        bind_parent(parent, child_id);
    }

    TSDataView::TSDataView(TSStorageTypeRef type, void *data, const TSDataView &parent, std::size_t child_id)
        : storage_(type, data)
    {
        bind_parent(parent, child_id);
    }

    TSDataView::TSDataView(TSStorageTypeRef type, const void *data, const TSDataView &parent, std::size_t child_id)
        : storage_(type, data)
    {
        bind_parent(parent, child_id);
    }

    void TSDataView::require_live(const char *what) const
    {
        if (!valid()) { throw std::logic_error(std::string{what} + " requires a live view"); }
    }

    TSDataTracking &TSDataView::mutable_tracking() const
    {
        if (!valid()) { throw std::logic_error("TSDataView::mutable_tracking requires a live view"); }
        const auto &table = ops();
        return *table.mutable_tracking_impl(table.context, storage_.data());
    }

    void TSDataView::bind_parent(const TSDataView &parent, std::size_t child_id) const
    {
        if (!valid()) { throw std::logic_error("TSDataView child requires a live view"); }
        if (!parent.valid()) { throw std::logic_error("TSDataView child requires a live parent view"); }
        if (!parent.ops().allows_mutation)
        {
            throw std::logic_error("TSDataView child requires mutable parent TSData ops");
        }
        mutable_tracking().parent = TSParentLink{parent.storage_type(), parent.data(), child_id};
    }

    void TSDataView::bind_parent(const NodeView &parent, TSEndpointOwnerPort port) const
    {
        if (!valid()) { throw std::logic_error("TSDataView child requires a live view"); }
        if (!parent.valid()) { throw std::logic_error("TSDataView node endpoint requires a live node view"); }
        mutable_tracking().parent = TSParentLink{parent.binding(), parent.data(), port};
    }

    void TSDataView::bind_parent(TSInput &parent, std::size_t child_id) const
    {
        if (!valid()) { throw std::logic_error("TSDataView child requires a live view"); }
        mutable_tracking().parent = TSParentLink{parent, child_id};
    }

    void TSDataView::bind_parent(TSOutput &parent, std::size_t child_id) const
    {
        if (!valid()) { throw std::logic_error("TSDataView child requires a live view"); }
        mutable_tracking().parent = TSParentLink{parent, child_id};
    }

    TSDataMutationView::TSDataMutationView(TSDataView view, DateTime evaluation_time)
        : storage_(view.storage_ref()),
          mutation_time_(evaluation_time)
    {
        validate_mutation_view();
    }

    TSDataMutationView::TSDataMutationView(TSDataMutationView &&other) noexcept
        : storage_(std::exchange(other.storage_, TSDataStorageRef<>{})),
          mutation_time_(std::exchange(other.mutation_time_, MIN_DT))
    {
    }

    TSDataMutationView::~TSDataMutationView() noexcept = default;

    TSDataView TSDataMutationView::view() const noexcept
    {
        return TSDataView{storage_};
    }

    const TSDataOps &TSDataMutationView::ops() const
    {
        return view().ops();
    }

    void *TSDataMutationView::mutable_data() const
    {
        require_active_mutation();
        return view().mutable_data();
    }

    ValueView TSDataMutationView::value() const
    {
        return view().value();
    }

    ValueView TSDataMutationView::delta_value(DateTime evaluation_time) const
    {
        return view().delta_value(evaluation_time);
    }

    DateTime TSDataMutationView::current_mutation_time() const
    {
        return mutation_time_;
    }

    bool TSDataMutationView::modified(DateTime evaluation_time) const
    {
        return view().modified(evaluation_time);
    }

    void TSDataMutationView::mark_modified()
    {
        if (record_modified_local()) { notify_parent_modified(); }
    }

    bool TSDataMutationView::copy_value_from(const ValueView &source)
    {
        require_active_mutation();

        auto        current = view();
        const auto &table   = current.ops();
        const bool newly_modified =
            table.copy_value_from_impl(table.context, current.mutable_data(), source, mutation_time_);
        if (newly_modified)
        {
            // The modification may already be recorded for this cycle — e.g.
            // the storage was structurally created earlier in the same dict
            // mutation (which marks it), or a write-through forwarding link
            // landed on pre-marked storage. Recording is idempotent; parents
            // are notified only by the first recording.
            if (record_modified_local()) { notify_parent_modified(); }
        }
        return newly_modified;
    }

    bool TSDataMutationView::move_value_from(Value &&source)
    {
        require_active_mutation();

        auto        current = view();
        const auto &table   = current.ops();
        const bool newly_modified =
            table.move_value_from_impl(table.context, current.mutable_data(), std::move(source), mutation_time_);
        if (newly_modified)
        {
            // The modification may already be recorded for this cycle — e.g.
            // the storage was structurally created earlier in the same dict
            // mutation (which marks it), or a write-through forwarding link
            // landed on pre-marked storage. Recording is idempotent; parents
            // are notified only by the first recording.
            if (record_modified_local()) { notify_parent_modified(); }
        }
        return newly_modified;
    }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
    bool TSDataMutationView::from_python(nb::handle source)
    {
        require_active_mutation();
        if (source.is_none()) { return false; }

        auto        current = view();
        const auto &table   = current.ops();
        const bool  newly_modified =
            table.from_python_impl(table.context, current.mutable_data(), source, mutation_time_);
        if (newly_modified && !record_modified_local())
        {
            throw std::logic_error(
                "TSDataMutationView::from_python reported a new modification that was already recorded");
        }
        if (newly_modified)
        {
            notify_parent_modified();
        }
        return newly_modified;
    }

#endif

    void TSDataMutationView::require_active_mutation() const
    {
        if (mutation_time_ == MIN_DT)
        {
            throw std::logic_error("TSData mutation requires an active mutation scope");
        }
        (void)view().mutable_data();
    }

    void TSDataMutationView::validate_mutation_view() const
    {
        if (mutation_time_ == MIN_DT)
        {
            throw std::invalid_argument("TSDataMutationView requires a concrete evaluation time");
        }
        (void)view().mutable_data();
    }

    bool TSDataMutationView::record_modified_local() const
    {
        require_active_mutation();

        auto  current = view();
        auto &state   = current.mutable_tracking();
        return state.record_modified(mutation_time_);
    }

    void TSDataMutationView::notify_parent_modified() const
    {
        view().parent_link().notify_child_modified(mutation_time_);
    }

    void apply_slot_mutation_result(TSDataMutationView &mutation, const SlotTSDataMutationResult &result)
    {
        if (!result.changed) { return; }
        mutation.mark_modified();
    }
}  // namespace hgraph
