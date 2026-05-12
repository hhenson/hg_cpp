#include <hgraph/types/time_series/ts_data.h>

#include <stdexcept>

namespace hgraph
{
    TSWDataView::TSWDataView(TSDataView view)
        : view_(view)
    {
        validate_kind(view_);
    }

    const TSDataView &TSWDataView::base() const noexcept
    {
        return view_;
    }

    TSDataView &TSWDataView::base() noexcept
    {
        return view_;
    }

    const TSDataBinding *TSWDataView::binding() const noexcept
    {
        return view_.binding();
    }

    const TSValueTypeMetaData *TSWDataView::schema() const noexcept
    {
        return view_.schema();
    }

    const TSWDataLayout &TSWDataView::layout() const
    {
        return static_cast<const TSWDataLayout &>(view_.layout());
    }

    const SizeTSWDataLayout &TSWDataView::size_layout() const
    {
        if (duration_based()) { throw std::logic_error("TSWDataView::size_layout requires a size-based TSW"); }
        return static_cast<const SizeTSWDataLayout &>(view_.layout());
    }

    const TimeTSWDataLayout &TSWDataView::time_layout() const
    {
        if (!duration_based()) { throw std::logic_error("TSWDataView::time_layout requires a time-based TSW"); }
        return static_cast<const TimeTSWDataLayout &>(view_.layout());
    }

    ValueView TSWDataView::value() const
    {
        return view_.value();
    }

    ValueView TSWDataView::delta_value(engine_time_t evaluation_time) const
    {
        return view_.delta_value(evaluation_time);
    }

    engine_time_t TSWDataView::last_modified_time() const
    {
        return view_.last_modified_time();
    }

    bool TSWDataView::modified(engine_time_t evaluation_time) const
    {
        return view_.modified(evaluation_time);
    }

    void TSWDataView::subscribe(Notifiable *observer) const
    {
        view_.subscribe(observer);
    }

    void TSWDataView::unsubscribe(Notifiable *observer) const
    {
        view_.unsubscribe(observer);
    }

    bool TSWDataView::has_observers() const
    {
        return view_.has_observers();
    }

    std::size_t TSWDataView::observer_count() const
    {
        return view_.observer_count();
    }

    bool TSWDataView::duration_based() const noexcept
    {
        return schema()->is_duration_based();
    }

    bool TSWDataView::size_based() const noexcept
    {
        return !duration_based();
    }

    bool TSWDataView::time_based() const noexcept
    {
        return duration_based();
    }

    std::size_t TSWDataView::period() const
    {
        return size_layout().period;
    }

    std::size_t TSWDataView::min_period() const
    {
        return size_layout().min_period;
    }

    engine_time_delta_t TSWDataView::time_range() const
    {
        return time_layout().time_range;
    }

    engine_time_delta_t TSWDataView::min_time_range() const
    {
        return time_layout().min_time_range;
    }

    std::size_t TSWDataView::capacity() const
    {
        const auto &ops = window_ops();
        return ops.capacity_impl(ops.context, view_.data());
    }

    std::size_t TSWDataView::size() const
    {
        const auto &ops = window_ops();
        return ops.size_impl(ops.context, view_.data());
    }

    bool TSWDataView::empty() const
    {
        return size() == 0;
    }

    bool TSWDataView::full() const
    {
        const auto &ops = window_ops();
        return ops.full_impl(ops.context, view_.data());
    }

    bool TSWDataView::all_valid() const
    {
        return view_.all_valid();
    }

    engine_time_t TSWDataView::first_modified_time() const
    {
        return empty() ? MIN_DT : time_at(0);
    }

    engine_time_t TSWDataView::time_at(std::size_t index) const
    {
        const auto &ops = window_ops();
        if (index >= size()) { throw std::out_of_range("TSWDataView::time_at: index out of range"); }
        return ops.time_at_impl(ops.context, view_.data(), index);
    }

    ValueView TSWDataView::time_value_at(std::size_t index) const
    {
        const auto &ops = window_ops();
        if (index >= size()) { throw std::out_of_range("TSWDataView::time_value_at: index out of range"); }
        return ValueView{layout().time_binding, ops.time_element_at_impl(ops.context, view_.data(), index)};
    }

    ValueView TSWDataView::at(std::size_t index) const
    {
        const auto &ops = window_ops();
        if (index >= size()) { throw std::out_of_range("TSWDataView::at: index out of range"); }
        return ValueView{layout().element_binding, ops.element_at_impl(ops.context, view_.data(), index)};
    }

    ValueView TSWDataView::operator[](std::size_t index) const
    {
        return at(index);
    }

    ValueView TSWDataView::front() const
    {
        if (empty()) { throw std::out_of_range("TSWDataView::front on empty window"); }
        return at(0);
    }

    ValueView TSWDataView::back() const
    {
        if (empty()) { throw std::out_of_range("TSWDataView::back on empty window"); }
        return at(size() - 1);
    }

    Range<ValueView> TSWDataView::values() const
    {
        return Range<ValueView>{
            .context   = this,
            .memory    = nullptr,
            .limit     = size(),
            .predicate = nullptr,
            .projector = &project_value,
        };
    }

    Range<ValueView> TSWDataView::time_values() const
    {
        return Range<ValueView>{
            .context   = this,
            .memory    = nullptr,
            .limit     = size(),
            .predicate = nullptr,
            .projector = &project_time_value,
        };
    }

    Range<engine_time_t> TSWDataView::value_times() const
    {
        return Range<engine_time_t>{
            .context   = this,
            .memory    = nullptr,
            .limit     = size(),
            .predicate = nullptr,
            .projector = &project_time,
        };
    }

    Range<ValueView>::iterator TSWDataView::begin() const
    {
        return values().begin();
    }

    Range<ValueView>::iterator TSWDataView::end() const
    {
        return values().end();
    }

    TSWDataMutationView TSWDataView::begin_mutation(engine_time_t evaluation_time) const
    {
        return TSWDataMutationView{view_, evaluation_time};
    }

    void TSWDataView::validate_kind(const TSDataView &view)
    {
        if (!view.valid()) { throw std::logic_error("TSWDataView requires a live view"); }
        const auto *schema = view.schema();
        if (schema == nullptr || schema->kind != TSTypeKind::TSW)
        {
            throw std::invalid_argument("TSWDataView requires a TSW TSData kind");
        }
        (void)static_cast<const TSWDataOps &>(view.ops());
    }

    const TSWDataOps &TSWDataView::window_ops() const
    {
        return static_cast<const TSWDataOps &>(view_.ops());
    }

    ValueView TSWDataView::project_value(const void *context, const void *, std::size_t index)
    {
        return static_cast<const TSWDataView *>(context)->at(index);
    }

    ValueView TSWDataView::project_time_value(const void *context, const void *, std::size_t index)
    {
        return static_cast<const TSWDataView *>(context)->time_value_at(index);
    }

    engine_time_t TSWDataView::project_time(const void *context, const void *, std::size_t index)
    {
        return static_cast<const TSWDataView *>(context)->time_at(index);
    }

    TSWDataMutationView::TSWDataMutationView(TSDataView view, engine_time_t evaluation_time)
        : TSWDataView(view),
          mutation_(view.begin_mutation(evaluation_time))
    {
        if (view.schema() == nullptr || view.schema()->kind != TSTypeKind::TSW)
        {
            throw std::invalid_argument("TSWDataMutationView requires a TSW TSData kind");
        }
    }

    TSWDataMutationView::TSWDataMutationView(TSWDataMutationView &&) noexcept = default;

    TSWDataMutationView::~TSWDataMutationView() noexcept = default;

    TSWDataView TSWDataMutationView::view()
    {
        return TSWDataView{base()};
    }

    engine_time_t TSWDataMutationView::current_mutation_time() const
    {
        return mutation_.current_mutation_time();
    }

    void TSWDataMutationView::push(const ValueView &source)
    {
        if (mutation_.modified(current_mutation_time()))
        {
            throw std::logic_error("TSWDataMutationView::push allows only one window tick per engine time");
        }
        const auto &ops = static_cast<const TSWDataOps &>(mutation_.ops());
        if (ops.push_impl == nullptr)
        {
            throw std::logic_error("TSWDataMutationView::push is not supported by this TSW ops");
        }
        ops.push_impl(ops.context, mutation_.mutable_data(), source, current_mutation_time());
        mutation_.mark_modified();
    }

    bool TSWDataMutationView::copy_value_from(const ValueView &source)
    {
        return mutation_.copy_value_from(source);
    }
}  // namespace hgraph
