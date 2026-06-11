#include <hgraph/types/time_series/ts_output/base_view.h>

#include <hgraph/types/time_series/ts_input/target_link.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_output/view_common.h>

#include <utility>

namespace hgraph
{
    TSOutputHandle::TSOutputHandle() noexcept = default;

    TSOutputHandle::TSOutputHandle(const TSOutput *output, const TSDataView &data) noexcept
        : output_(output),
          data_(data.storage_ref())
    {
    }

    TSOutputHandle::TSOutputHandle(const TSOutputView &view) noexcept
        : output_(view.output()),
          data_(view.data_view().storage_ref())
    {
    }

    const TSOutput *TSOutputHandle::output() const noexcept
    {
        return output_;
    }

    TSDataView TSOutputHandle::data_view() const noexcept
    {
        return TSDataView{data_};
    }

    const TSDataBinding *TSOutputHandle::binding() const noexcept
    {
        return data_.binding();
    }

    const TSValueTypeMetaData *TSOutputHandle::schema() const noexcept
    {
        const auto *bound = data_.binding();
        return bound != nullptr ? bound->type_meta : nullptr;
    }

    bool TSOutputHandle::bound() const noexcept
    {
        return output_ != nullptr && data_.has_value();
    }

    bool TSOutputHandle::same_as(const TSOutputHandle &other) const noexcept
    {
        return output_ == other.output_ && data_.binding() == other.data_.binding() &&
               data_.data() == other.data_.data();
    }

    TSOutputView TSOutputHandle::view(DateTime evaluation_time) const noexcept
    {
        return TSOutputView{*this, evaluation_time};
    }

    void TSOutputHandle::reset() noexcept
    {
        output_ = nullptr;
        data_.reset();
    }

    TSOutputView::TSOutputView() noexcept = default;

    TSOutputView::TSOutputView(const TSOutput *output, const TSDataView &data, DateTime evaluation_time) noexcept
        : output_(output),
          data_(data.borrowed_ref()),
          evaluation_time_(evaluation_time)
    {
    }

    TSOutputView::TSOutputView(TSOutputHandle handle, DateTime evaluation_time) noexcept
        : output_(handle.output()),
          data_(handle.data_view()),
          evaluation_time_(evaluation_time)
    {
    }

    TSOutputView TSOutputView::borrowed_ref() const noexcept
    {
        return TSOutputView{output_, data_.borrowed_ref(), evaluation_time_};
    }

    const TSOutput *TSOutputView::output() const noexcept
    {
        return output_;
    }

    TSOutputHandle TSOutputView::handle() const noexcept
    {
        return TSOutputHandle{output_, data_.borrowed_ref()};
    }

    const TSDataView &TSOutputView::data_view() const noexcept
    {
        return data_;
    }

    TSDataView &TSOutputView::data_view() noexcept
    {
        return data_;
    }

    DateTime TSOutputView::evaluation_time() const noexcept
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
        return data_.value();
    }

    ValueView TSOutputView::delta_value() const
    {
        return data_.delta_value(evaluation_time_);
    }

    DateTime TSOutputView::last_modified_time() const
    {
        return data_.last_modified_time();
    }

    bool TSOutputView::modified() const
    {
        return evaluation_time_ != MIN_DT && data_.modified(evaluation_time_);
    }

    bool TSOutputView::valid() const
    {
        return data_.has_current_value();
    }

    bool TSOutputView::all_valid() const
    {
        return data_.all_valid();
    }

    bool TSOutputView::forwarding() const noexcept
    {
        return detail::is_target_link_view(data_);
    }

    bool TSOutputView::forwarding_bound() const noexcept
    {
        return detail::target_link_bound(data_);
    }

    TSOutputHandle TSOutputView::forwarding_target() const noexcept
    {
        const auto *link = detail::target_link_storage(data_);
        return link != nullptr ? link->target_output() : TSOutputHandle{};
    }

    void TSOutputView::bind_forwarding_target(const TSOutputView &source) const
    {
        if (!forwarding())
        {
            throw std::logic_error("TSOutputView::bind_forwarding_target requires a forwarding output view");
        }
        const TSOutputHandle previous = forwarding_target();
        detail::bind_target_link(data_, source);
        if (evaluation_time_ != MIN_DT && previous.bound() && !previous.same_as(forwarding_target()))
        {
            detail::mutable_target_link_storage(data_)->record_target_modified(evaluation_time_);
        }
    }

    void TSOutputView::clear_forwarding_target() const
    {
        if (!forwarding())
        {
            throw std::logic_error("TSOutputView::clear_forwarding_target requires a forwarding output view");
        }
        const TSOutputHandle previous = forwarding_target();
        detail::unbind_target_link(data_);
        if (evaluation_time_ != MIN_DT && previous.bound())
        {
            detail::mutable_target_link_storage(data_)->record_target_modified(evaluation_time_);
        }
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

    TSOutputHandle TSOutputView::binding_for(const TSValueTypeMetaData &requested_schema) const
    {
        if (output_ == nullptr || !data_.valid())
        {
            throw std::logic_error("TSOutputView::binding_for requires a bound output view");
        }
        return output_->binding_for(*this, requested_schema);
    }

    TSDataMutationView TSOutputView::begin_mutation(DateTime evaluation_time) const
    {
        if (!data_.valid()) { throw std::logic_error("TSOutputView::begin_mutation requires a bound view"); }
        return data_.begin_mutation(evaluation_time);
    }

    TSSOutputView TSOutputView::as_set() &
    {
        return TSSOutputView{borrowed_ref()};
    }

    TSSOutputView TSOutputView::as_set() const &
    {
        return TSSOutputView{borrowed_ref()};
    }

    TSDOutputView TSOutputView::as_dict() &
    {
        return TSDOutputView{borrowed_ref()};
    }

    TSDOutputView TSOutputView::as_dict() const &
    {
        return TSDOutputView{borrowed_ref()};
    }

    TSBOutputView TSOutputView::as_bundle() &
    {
        return TSBOutputView{borrowed_ref()};
    }

    TSBOutputView TSOutputView::as_bundle() const &
    {
        return TSBOutputView{borrowed_ref()};
    }

    TSLOutputView TSOutputView::as_list() &
    {
        return TSLOutputView{borrowed_ref()};
    }

    TSLOutputView TSOutputView::as_list() const &
    {
        return TSLOutputView{borrowed_ref()};
    }

    TSWOutputView TSOutputView::as_window() &
    {
        return TSWOutputView{borrowed_ref()};
    }

    TSWOutputView TSOutputView::as_window() const &
    {
        return TSWOutputView{borrowed_ref()};
    }

}  // namespace hgraph
