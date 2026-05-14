#include <hgraph/types/time_series/ts_output/list_view.h>

#include <hgraph/types/time_series/ts_output/view_common.h>

#include <utility>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] bool tsl_output_valid_child(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSLOutputView *>(context)->at(index).valid();
        }

        [[nodiscard]] bool tsl_output_modified_child(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSLOutputView *>(context)->at(index).modified();
        }

        [[nodiscard]] TSOutputView tsl_output_project_value(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSLOutputView *>(context)->at(index);
        }

        [[nodiscard]] std::pair<std::size_t, TSOutputView> tsl_output_project_item(
            const void *context,
            const void *,
            std::size_t index)
        {
            return {index, static_cast<const TSLOutputView *>(context)->at(index)};
        }

    }  // namespace

    TSLOutputView::TSLOutputView(TSOutputView view)
        : TSOutputTypedView<TSLOutputView>(std::move(view))
    {
        detail::validate_output_view_kind(schema(), TSTypeKind::TSL, "TSLOutputView");
    }

    TSLDataView TSLOutputView::data_view() const
    {
        return view_.data_view().as_list();
    }

    std::size_t TSLOutputView::size() const
    {
        return data_view().size();
    }

    bool TSLOutputView::empty() const
    {
        return data_view().empty();
    }

    Range<TSOutputView> TSLOutputView::values() const
    {
        return Range<TSOutputView>{.context = this, .memory = nullptr, .limit = size(), .predicate = nullptr,
                                   .projector = &tsl_output_project_value};
    }

    Range<TSOutputView> TSLOutputView::valid_values() const
    {
        return Range<TSOutputView>{.context = this, .memory = nullptr, .limit = size(),
                                   .predicate = &tsl_output_valid_child,
                                   .projector = &tsl_output_project_value};
    }

    Range<TSOutputView> TSLOutputView::modified_values() const
    {
        return Range<TSOutputView>{.context = this, .memory = nullptr, .limit = size(),
                                   .predicate = &tsl_output_modified_child,
                                   .projector = &tsl_output_project_value};
    }

    KeyValueRange<std::size_t, TSOutputView> TSLOutputView::items() const
    {
        return KeyValueRange<std::size_t, TSOutputView>{.context = this,
                                                        .memory = nullptr,
                                                        .limit = size(),
                                                        .predicate = nullptr,
                                                        .projector = &tsl_output_project_item};
    }

    KeyValueRange<std::size_t, TSOutputView> TSLOutputView::valid_items() const
    {
        return KeyValueRange<std::size_t, TSOutputView>{.context = this,
                                                        .memory = nullptr,
                                                        .limit = size(),
                                                        .predicate = &tsl_output_valid_child,
                                                        .projector = &tsl_output_project_item};
    }

    KeyValueRange<std::size_t, TSOutputView> TSLOutputView::modified_items() const
    {
        return KeyValueRange<std::size_t, TSOutputView>{.context = this,
                                                        .memory = nullptr,
                                                        .limit = size(),
                                                        .predicate = &tsl_output_modified_child,
                                                        .projector = &tsl_output_project_item};
    }

    TSOutputView TSLOutputView::at(std::size_t index) &
    {
        auto data = data_view();
        return TSOutputView{view_.output(), data.at(index), view_.evaluation_time()};
    }

    TSOutputView TSLOutputView::at(std::size_t index) const &
    {
        return const_cast<TSLOutputView *>(this)->at(index);
    }

    TSOutputView TSLOutputView::operator[](std::size_t index) &
    {
        return at(index);
    }

    TSOutputView TSLOutputView::operator[](std::size_t index) const &
    {
        return at(index);
    }

}  // namespace hgraph
