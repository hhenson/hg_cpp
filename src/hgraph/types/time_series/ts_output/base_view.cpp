#include <hgraph/types/time_series/ts_output/base_view.h>

#include "view_common.h"

#include <utility>

namespace hgraph
{
    TSOutputView::TSOutputView() noexcept = default;

    TSOutputView::TSOutputView(const TSOutput *output, TSDataView data, engine_time_t evaluation_time) noexcept
        : output_(output),
          data_(data),
          evaluation_time_(evaluation_time)
    {
    }

    const TSOutput *TSOutputView::output() const noexcept
    {
        return output_;
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
