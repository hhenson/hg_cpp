#include <hgraph/types/time_series/ts_input/base_view.h>

#include <hgraph/types/time_series/ts_input/view_common.h>

#include <stdexcept>
#include <string>
#include <utility>

namespace hgraph
{
    TSInputView::TSInputView() noexcept = default;

    TSInputView::InputDataCursor::InputDataCursor(TSDataView value_data_,
                                                  TSDataView raw_data_,
                                                  detail::TSInputTargetActiveNode *target_node_) noexcept
        : value_data(std::move(value_data_)),
          raw_data(raw_data_.valid() ? std::move(raw_data_) : value_data.borrowed_ref()),
          target_node(target_node_)
    {
    }

    TSInputView::InputDataCursor TSInputView::InputDataCursor::borrowed_ref() const noexcept
    {
        return InputDataCursor{value_data.borrowed_ref(), raw_data.borrowed_ref(), target_node};
    }

    bool TSInputView::InputDataCursor::has_storage() const noexcept
    {
        return value_data.valid() || raw_data.valid();
    }

    bool TSInputView::InputDataCursor::is_target_position() const noexcept
    {
        return detail::is_target_link_view(raw_data);
    }

    bool TSInputView::InputDataCursor::target_bound() const noexcept
    {
        return detail::target_link_bound(raw_data);
    }

    const TSDataBinding *TSInputView::InputDataCursor::binding() const noexcept
    {
        if (is_target_position())
        {
            const auto &target = resolved_value_data();
            if (target.valid()) { return target.binding(); }
            return detail::regular_ts_data_binding_for(target_path_schema());
        }
        return value_data.binding();
    }

    const TSValueTypeMetaData *TSInputView::InputDataCursor::schema() const noexcept
    {
        if (is_target_position()) { return target_path_schema(); }
        return value_data.schema();
    }

    const TSValueTypeMetaData *TSInputView::InputDataCursor::target_path_schema() const noexcept
    {
        if (!is_target_position()) { return nullptr; }
        return detail::target_path_schema(raw_data, target_node);
    }

    const TSDataView &TSInputView::InputDataCursor::resolved_value_data() const noexcept
    {
        if (is_target_position()) { value_data = detail::target_link_resolve(raw_data, target_node); }
        return value_data.valid() ? value_data : detail::empty_ts_data_view();
    }

    bool TSInputView::InputDataCursor::value_live() const noexcept
    {
        return resolved_value_data().valid();
    }

    engine_time_t TSInputView::InputDataCursor::last_modified_time() const
    {
        const auto &data = resolved_value_data();
        if (is_target_position() && !data.valid()) { return raw_data.last_modified_time(); }
        return data.valid() ? data.last_modified_time() : MIN_DT;
    }

    bool TSInputView::InputDataCursor::modified(engine_time_t evaluation_time) const
    {
        if (evaluation_time == MIN_DT) { return false; }
        const auto &data = resolved_value_data();
        if (is_target_position() && !data.valid()) { return raw_data.modified(evaluation_time); }
        return data.valid() && data.modified(evaluation_time);
    }

    TSDataView &TSInputView::InputDataCursor::checked_value_data(const char *what) const
    {
        if (value_live()) { return value_data; }
        throw std::logic_error(std::string{what} + " requires a bound peered input view");
    }

    TSInputView::InputDataCursor TSInputView::InputDataCursor::target_child(TSDataView child,
                                                                            std::size_t index) const
    {
        auto *child_node = detail::target_link_child_node(raw_data, target_node, index);
        return InputDataCursor{std::move(child), raw_data.borrowed_ref(), child_node};
    }

    void TSInputView::InputDataCursor::bind_target(const TSOutputView &output)
    {
        detail::bind_target_link(raw_data, output);
        value_data = detail::target_link_resolve(raw_data, target_node);
    }

    void TSInputView::InputDataCursor::unbind_target()
    {
        detail::unbind_target_link(raw_data);
        value_data = {};
    }

    void TSInputView::InputDataCursor::make_active(TSInput *input,
                                                   Notifiable *scheduling_notifier) const
    {
        if (is_target_position())
        {
                detail::make_target_link_active(raw_data,
                                            target_node,
                                            value_live() ? value_data.borrowed_ref() : TSDataView{},
                                            scheduling_notifier);
        }
        else if (input != nullptr && value_data.valid())
        {
            input->make_active(value_data.path_from_root(), value_data.borrowed_ref(), scheduling_notifier);
        }
    }

    void TSInputView::InputDataCursor::make_passive(TSInput *input) const
    {
        if (is_target_position())
        {
            detail::make_target_link_passive(raw_data, target_node);
            return;
        }
        if (input != nullptr && value_data.valid()) { input->make_passive(value_data.path_from_root()); }
    }

    bool TSInputView::InputDataCursor::active(const TSInput *input) const
    {
        if (is_target_position()) { return detail::target_link_active(raw_data, target_node); }
        return input != nullptr && value_data.valid() && input->active(value_data.path_from_root());
    }

    TSInputView::TSInputView(TSInput                  *input,
                             TSDataView               value_data,
                             TSDataView               raw_data,
                             detail::TSInputTargetActiveNode *target_node,
                             Notifiable              *scheduling_notifier,
                             engine_time_t            evaluation_time) noexcept
        : input_(input),
          data_(std::move(value_data), std::move(raw_data), target_node),
          scheduling_notifier_(scheduling_notifier),
          evaluation_time_(evaluation_time)
    {
    }

    TSInputView TSInputView::borrowed_ref() const noexcept
    {
        auto cursor = data_.borrowed_ref();
        return TSInputView{input_,
                           std::move(cursor.value_data),
                           std::move(cursor.raw_data),
                           cursor.target_node,
                           scheduling_notifier_,
                           evaluation_time_};
    }

    namespace detail
    {
        void TSInputViewOps::make_active(TSInputView &view) const
        {
            view.data_.make_active(view.input_, view.scheduling_notifier_);
            if (view.scheduling_notifier_ != nullptr && view.evaluation_time_ != MIN_DT && view.modified())
            {
                view.scheduling_notifier_->notify(view.evaluation_time_);
            }
        }

        void TSInputViewOps::make_passive(TSInputView &view) const
        {
            view.data_.make_passive(view.input_);
        }

        bool TSInputViewOps::active(const TSInputView &view) const
        {
            return view.data_.active(view.input_);
        }

        const TSInputViewOps &input_view_ops() noexcept
        {
            static const TSInputViewOps ops{};
            return ops;
        }
    }  // namespace detail

    engine_time_t TSInputView::evaluation_time() const noexcept
    {
        return evaluation_time_;
    }

    const TSDataBinding *TSInputView::binding() const noexcept
    {
        return data_.binding();
    }

    const TSValueTypeMetaData *TSInputView::schema() const noexcept
    {
        return data_.schema();
    }

    const TSDataView &TSInputView::data_view() const noexcept
    {
        return data_.resolved_value_data();
    }

    bool TSInputView::bound() const noexcept
    {
        if (!data_.has_storage()) { return false; }
        if (!is_bindable()) { return true; }
        return data_.target_bound();
    }

    bool TSInputView::is_bindable() const noexcept
    {
        return is_target_position();
    }

    bool TSInputView::valid() const
    {
        const auto &data = data_view();
        return data.valid() && data.has_current_value();
    }

    bool TSInputView::all_valid() const
    {
        const auto &data = data_view();
        return data.valid() && data.all_valid();
    }

    engine_time_t TSInputView::last_modified_time() const
    {
        return data_.last_modified_time();
    }

    bool TSInputView::modified() const
    {
        return data_.modified(evaluation_time_);
    }

    ValueView TSInputView::value() const
    {
        const auto &data = data_view();
        if (data.valid()) { return data.value(); }
        throw std::logic_error("TSInputView::value requires a live input view");
    }

    ValueView TSInputView::delta_value() const
    {
        const auto &data = data_view();
        if (data.valid()) { return data.delta_value(evaluation_time_); }
        throw std::logic_error("TSInputView::delta_value requires a live input view");
    }

    TimeSeriesReference TSInputView::reference() const
    {
        const auto *view_schema = schema();
        if (view_schema == nullptr)
        {
            throw std::logic_error("TSInputView::reference requires a typed input view");
        }

        if (is_target_position())
        {
            const auto *target_schema = detail::target_link_schema(data_.raw_data);
            const auto *link = detail::target_link_storage(data_.raw_data);
            if (target_schema == nullptr || link == nullptr)
            {
                throw std::logic_error("TSInputView::reference requires target-link storage");
            }

            auto target = link->target_output_at_path(*target_schema, data_.target_node);
            if (!target.bound()) { return TimeSeriesReference::empty(view_schema); }
            return TimeSeriesReference{std::move(target)};
        }

        const auto &ops = detail::input_endpoint_ops_for(view_schema);
        if (ops.reference == nullptr) { return TimeSeriesReference::empty(view_schema); }
        return ops.reference(*this);
    }

    void TSInputView::bind_output(const TSOutputView &output)
    {
        if (!is_bindable())
        {
            throw std::logic_error("TSInputView::bind_output requires a peered target-link input view");
        }
        data_.bind_target(output);
        if (scheduling_notifier_ != nullptr && evaluation_time_ != MIN_DT && modified())
        {
            scheduling_notifier_->notify(evaluation_time_);
        }
    }

    void TSInputView::unbind_output()
    {
        if (!is_bindable())
        {
            throw std::logic_error("TSInputView::unbind_output requires a peered target-link input view");
        }
        const bool was_valid = valid();
        data_.unbind_target();
        if (was_valid && scheduling_notifier_ != nullptr && evaluation_time_ != MIN_DT)
        {
            scheduling_notifier_->notify(evaluation_time_);
        }
    }

    void TSInputView::make_active()
    {
        detail::input_view_ops().make_active(*this);
    }

    void TSInputView::make_passive()
    {
        detail::input_view_ops().make_passive(*this);
    }

    bool TSInputView::active() const
    {
        return detail::input_view_ops().active(*this);
    }

    TSSInputView TSInputView::as_set() &
    {
        return TSSInputView{borrowed_ref()};
    }

    TSSInputView TSInputView::as_set() const &
    {
        return TSSInputView{borrowed_ref()};
    }

    TSDInputView TSInputView::as_dict() &
    {
        return TSDInputView{borrowed_ref()};
    }

    TSDInputView TSInputView::as_dict() const &
    {
        return TSDInputView{borrowed_ref()};
    }

    TSBInputView TSInputView::as_bundle() &
    {
        return TSBInputView{borrowed_ref()};
    }

    TSBInputView TSInputView::as_bundle() const &
    {
        return TSBInputView{borrowed_ref()};
    }

    TSLInputView TSInputView::as_list() &
    {
        return TSLInputView{borrowed_ref()};
    }

    TSLInputView TSInputView::as_list() const &
    {
        return TSLInputView{borrowed_ref()};
    }

    TSWInputView TSInputView::as_window() &
    {
        return TSWInputView{borrowed_ref()};
    }

    TSWInputView TSInputView::as_window() const &
    {
        return TSWInputView{borrowed_ref()};
    }

    bool TSInputView::is_target_position() const noexcept
    {
        return data_.is_target_position();
    }

    const TSValueTypeMetaData *TSInputView::target_path_schema() const noexcept
    {
        return data_.target_path_schema();
    }

    TSDataView TSInputView::resolve_target_data_view() const noexcept
    {
        return data_.resolved_value_data().borrowed_ref();
    }

    bool TSInputView::target_view_live() const noexcept
    {
        return data_.value_live();
    }

    TSDataView &TSInputView::checked_target_data_view(const char *what) const
    {
        return data_.checked_value_data(what);
    }

    TSInputView TSInputView::child_from_target(TSDataView child, std::size_t index) const
    {
        auto cursor = data_.target_child(std::move(child), index);
        return TSInputView{input_, std::move(cursor.value_data), std::move(cursor.raw_data), cursor.target_node,
                           scheduling_notifier_, evaluation_time_};
    }

    TSInputView TSInputView::child_from_input(std::size_t index) const
    {
        auto parent = data_.value_data.borrowed_ref();
        auto projection = detail::input_child_projection(parent, index);
        if (projection.target_link.valid())
        {
            projection.target_link = TSDataView{projection.target_link.binding(), projection.target_link.data(),
                                                parent, index};
        }
        else if (projection.visible.valid())
        {
            projection.visible = TSDataView{projection.visible.binding(), projection.visible.data(), parent, index};
        }
        return child_from_projection(std::move(projection), index);
    }

    TSInputView TSInputView::child_from_projection(detail::TSInputChildProjection projection,
                                                   std::size_t index) const noexcept
    {
        static_cast<void>(index);
        return TSInputView{input_, std::move(projection.visible), std::move(projection.target_link), nullptr,
                           scheduling_notifier_, evaluation_time_};
    }

}  // namespace hgraph
