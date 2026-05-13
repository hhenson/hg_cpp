#include <hgraph/types/time_series/ts_input/base_view.h>

#include "view_common.h"

#include <stdexcept>
#include <string>
#include <utility>

namespace hgraph
{
    TSInputView::TSInputView() noexcept = default;

    TSInputView::TSInputView(TSInput                  *input,
                             detail::TSInputNode     *node,
                             TSDataView               target_view,
                             std::vector<std::size_t> target_path,
                             Notifiable              *scheduling_notifier,
                             engine_time_t            evaluation_time) noexcept
        : input_(input),
          node_(node),
          data_view_(target_view),
          target_path_(std::move(target_path)),
          scheduling_notifier_(scheduling_notifier),
          evaluation_time_(evaluation_time)
    {
        if (!data_view_.valid() && node_ != nullptr && node_->role == TSEndpointRole::NonPeered)
        {
            data_view_ = TSDataView{node_->data_binding, node_};
        }
    }

    engine_time_t TSInputView::evaluation_time() const noexcept
    {
        return evaluation_time_;
    }

    const TSDataBinding *TSInputView::binding() const noexcept
    {
        if (target_view_live()) { return data_view_.binding(); }
        if (node_ != nullptr && node_->role == TSEndpointRole::Peered && node_->target.bound())
        {
            return node_->target.binding();
        }
        return node_ != nullptr ? node_->data_binding : nullptr;
    }

    const TSValueTypeMetaData *TSInputView::schema() const noexcept
    {
        if (target_view_live()) { return data_view_.schema(); }
        return node_ != nullptr ? node_->schema : nullptr;
    }

    const TSDataView &TSInputView::data_view() const noexcept
    {
        if (target_view_live()) { return data_view_; }
        if (node_ == nullptr) { return detail::empty_ts_data_view(); }
        if (node_->role == TSEndpointRole::Peered)
        {
            return node_->target.bound() ? node_->target.data_view() : detail::empty_ts_data_view();
        }
        return data_view_;
    }

    bool TSInputView::bound() const noexcept
    {
        if (node_ == nullptr) { return false; }
        if (!is_bindable()) { return true; }
        return node_->target.bound();
    }

    bool TSInputView::is_bindable() const noexcept
    {
        return node_ != nullptr && node_->role == TSEndpointRole::Peered;
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
        const auto &data = data_view();
        return data.valid() ? data.last_modified_time() : MIN_DT;
    }

    bool TSInputView::modified() const
    {
        if (evaluation_time_ == MIN_DT) { return false; }
        const auto &data = data_view();
        return data.valid() && data.modified(evaluation_time_);
    }

    ValueView TSInputView::value() const
    {
        auto data = data_view();
        if (data.valid()) { return data.value(); }
        throw std::logic_error("TSInputView::value requires a live input view");
    }

    ValueView TSInputView::delta_value() const
    {
        auto data = data_view();
        if (data.valid()) { return data.delta_value(evaluation_time_); }
        throw std::logic_error("TSInputView::delta_value requires a live input view");
    }

    void TSInputView::bind_output(const TSOutputView &output)
    {
        if (node_ == nullptr) { throw std::logic_error("TSInputView::bind_output requires a live input view"); }
        if (!is_bindable())
        {
            throw std::logic_error("TSInputView::bind_output requires a peered target-link input view");
        }
        node_->bind_target(output);
        data_view_ = node_->target.bound()
                         ? (target_path_.empty() ? node_->target.data_view()
                                                 : node_->target_child_at_path(target_path_))
                         : TSDataView{};
        if (scheduling_notifier_ != nullptr && evaluation_time_ != MIN_DT && modified())
        {
            scheduling_notifier_->notify(evaluation_time_);
        }
    }

    void TSInputView::unbind_output()
    {
        if (node_ == nullptr) { return; }
        if (!is_bindable())
        {
            throw std::logic_error("TSInputView::unbind_output requires a peered target-link input view");
        }
        const bool was_valid = valid();
        node_->unbind_target();
        data_view_ = {};
        if (was_valid && scheduling_notifier_ != nullptr && evaluation_time_ != MIN_DT)
        {
            scheduling_notifier_->notify(evaluation_time_);
        }
    }

    void TSInputView::make_active()
    {
        if (node_ == nullptr) { return; }
        if (!target_path_.empty())
        {
            node_->make_target_active(target_path_, target_view_live() ? data_view_ : TSDataView{}, scheduling_notifier_);
            if (scheduling_notifier_ != nullptr && evaluation_time_ != MIN_DT && modified())
            {
                scheduling_notifier_->notify(evaluation_time_);
            }
            return;
        }
        node_->make_local_active(scheduling_notifier_);
        if (scheduling_notifier_ != nullptr && evaluation_time_ != MIN_DT && modified())
        {
            scheduling_notifier_->notify(evaluation_time_);
        }
    }

    void TSInputView::make_passive()
    {
        if (node_ == nullptr) { return; }
        if (!target_path_.empty())
        {
            node_->make_target_passive(target_path_);
            return;
        }
        node_->make_local_passive();
    }

    bool TSInputView::active() const
    {
        if (node_ == nullptr) { return false; }
        if (!target_path_.empty()) { return node_->target_active(target_path_); }
        return node_->local_active();
    }

    TSSInputView TSInputView::as_set() &
    {
        return TSSInputView{*this};
    }

    TSSInputView TSInputView::as_set() const &
    {
        return TSSInputView{*this};
    }

    TSDInputView TSInputView::as_dict() &
    {
        return TSDInputView{*this};
    }

    TSDInputView TSInputView::as_dict() const &
    {
        return TSDInputView{*this};
    }

    TSBInputView TSInputView::as_bundle() &
    {
        return TSBInputView{*this};
    }

    TSBInputView TSInputView::as_bundle() const &
    {
        return TSBInputView{*this};
    }

    TSLInputView TSInputView::as_list() &
    {
        return TSLInputView{*this};
    }

    TSLInputView TSInputView::as_list() const &
    {
        return TSLInputView{*this};
    }

    TSWInputView TSInputView::as_window() &
    {
        return TSWInputView{*this};
    }

    TSWInputView TSInputView::as_window() const &
    {
        return TSWInputView{*this};
    }

    bool TSInputView::is_target_position() const noexcept
    {
        return target_view_live();
    }

    bool TSInputView::target_view_live() const noexcept
    {
        return node_ != nullptr && node_->role == TSEndpointRole::Peered && node_->target.bound() &&
               data_view_.valid();
    }

    TSDataView &TSInputView::checked_target_data_view(const char *what) const
    {
        if (target_view_live()) { return const_cast<TSDataView &>(data_view_); }
        if (node_ != nullptr && node_->role == TSEndpointRole::Peered && node_->target.bound())
        {
            return node_->target.data_view();
        }
        throw std::logic_error(std::string{what} + " requires a bound peered input view");
    }

    TSInputView TSInputView::child_from_target(TSDataView child, std::size_t index) const
    {
        auto path = target_path_;
        path.push_back(index);
        return TSInputView{input_, node_, child, std::move(path), scheduling_notifier_, evaluation_time_};
    }

    TSInputView TSInputView::child_from_node(detail::TSInputNode *child) const noexcept
    {
        TSDataView target_view{};
        if (child != nullptr && child->role == TSEndpointRole::Peered && child->target.bound())
        {
            target_view = child->target.data_view();
        }
        return TSInputView{input_, child, target_view, {}, scheduling_notifier_, evaluation_time_};
    }

}  // namespace hgraph
