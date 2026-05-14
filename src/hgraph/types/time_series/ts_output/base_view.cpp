#include <hgraph/types/time_series/ts_output/base_view.h>

#include <hgraph/types/time_series/ts_output/view_common.h>

#include <utility>

namespace hgraph
{
    TSOutputHandle::TSOutputHandle() noexcept = default;

    TSOutputHandle::TSOutputHandle(const TSOutput *output, TSDataView data) noexcept
        : output_(output),
          data_(data)
    {
    }

    TSOutputHandle::TSOutputHandle(const TSOutputView &view) noexcept
        : output_(view.output()),
          data_(view.data_view())
    {
    }

    const TSOutput *TSOutputHandle::output() const noexcept
    {
        return output_;
    }

    const TSDataView &TSOutputHandle::data_view() const noexcept
    {
        return data_;
    }

    TSDataView &TSOutputHandle::data_view() noexcept
    {
        return data_;
    }

    const TSDataBinding *TSOutputHandle::binding() const noexcept
    {
        return data_.binding();
    }

    const TSValueTypeMetaData *TSOutputHandle::schema() const noexcept
    {
        return data_.schema();
    }

    bool TSOutputHandle::bound() const noexcept
    {
        return output_ != nullptr && data_.valid();
    }

    bool TSOutputHandle::same_as(const TSOutputHandle &other) const noexcept
    {
        return output_ == other.output_ && data_.binding() == other.data_.binding() &&
               data_.data() == other.data_.data();
    }

    TSOutputView TSOutputHandle::view(engine_time_t evaluation_time) const noexcept
    {
        return TSOutputView{*this, evaluation_time};
    }

    void TSOutputHandle::reset() noexcept
    {
        output_ = nullptr;
        data_ = {};
    }

    TSOutputView::TSOutputView() noexcept = default;

    TSOutputView::TSOutputView(const TSOutput *output, TSDataView data, engine_time_t evaluation_time) noexcept
        : output_(output),
          data_(data),
          evaluation_time_(evaluation_time)
    {
    }

    TSOutputView::TSOutputView(TSOutputHandle handle, engine_time_t evaluation_time) noexcept
        : output_(handle.output()),
          data_(handle.data_view()),
          evaluation_time_(evaluation_time)
    {
    }

    const TSOutput *TSOutputView::output() const noexcept
    {
        return output_;
    }

    TSOutputHandle TSOutputView::handle() const noexcept
    {
        return TSOutputHandle{output_, data_};
    }

    const TSDataView &TSOutputView::data_view() const noexcept
    {
        return data_;
    }

    TSDataView &TSOutputView::data_view() noexcept
    {
        return data_;
    }

    engine_time_t TSOutputView::evaluation_time() const noexcept
    {
        return evaluation_time_;
    }

    const TSDataBinding *TSOutputView::binding() const noexcept
    {
        return data_.binding();
    }

    const TSValueTypeMetaData *TSOutputView::schema() const noexcept
    {
        return data_.schema();
    }

    bool TSOutputView::bound() const noexcept
    {
        return output_ != nullptr && data_.valid();
    }

    ValueView TSOutputView::value() const
    {
        return data_.valid() ? data_.value() : ValueView{};
    }

    ValueView TSOutputView::delta_value() const
    {
        return data_.valid() ? data_.delta_value(evaluation_time_) : ValueView{};
    }

    engine_time_t TSOutputView::last_modified_time() const
    {
        return data_.valid() ? data_.last_modified_time() : MIN_DT;
    }

    bool TSOutputView::modified() const
    {
        return evaluation_time_ != MIN_DT && data_.valid() && data_.modified(evaluation_time_);
    }

    bool TSOutputView::valid() const
    {
        return data_.valid() && data_.has_current_value();
    }

    bool TSOutputView::all_valid() const
    {
        return data_.valid() && data_.all_valid();
    }

    void TSOutputView::subscribe(Notifiable *observer) const
    {
        if (!data_.valid()) { throw std::logic_error("TSOutputView::subscribe requires a bound view"); }
        data_.subscribe(observer);
    }

    void TSOutputView::unsubscribe(Notifiable *observer) const
    {
        if (!data_.valid()) { throw std::logic_error("TSOutputView::unsubscribe requires a bound view"); }
        data_.unsubscribe(observer);
    }

    TSDataMutationView TSOutputView::begin_mutation(engine_time_t evaluation_time) const
    {
        if (!data_.valid()) { throw std::logic_error("TSOutputView::begin_mutation requires a bound view"); }
        return data_.begin_mutation(evaluation_time);
    }

    TSSOutputView TSOutputView::as_set() &
    {
        return TSSOutputView{*this};
    }

    TSSOutputView TSOutputView::as_set() const &
    {
        return TSSOutputView{*this};
    }

    TSDOutputView TSOutputView::as_dict() &
    {
        return TSDOutputView{*this};
    }

    TSDOutputView TSOutputView::as_dict() const &
    {
        return TSDOutputView{*this};
    }

    TSBOutputView TSOutputView::as_bundle() &
    {
        return TSBOutputView{*this};
    }

    TSBOutputView TSOutputView::as_bundle() const &
    {
        return TSBOutputView{*this};
    }

    TSLOutputView TSOutputView::as_list() &
    {
        return TSLOutputView{*this};
    }

    TSLOutputView TSOutputView::as_list() const &
    {
        return TSLOutputView{*this};
    }

    TSWOutputView TSOutputView::as_window() &
    {
        return TSWOutputView{*this};
    }

    TSWOutputView TSOutputView::as_window() const &
    {
        return TSWOutputView{*this};
    }

}  // namespace hgraph
