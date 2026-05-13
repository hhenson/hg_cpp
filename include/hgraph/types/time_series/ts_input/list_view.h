#ifndef HGRAPH_CPP_ROOT_TS_INPUT_LIST_VIEW_H
#define HGRAPH_CPP_ROOT_TS_INPUT_LIST_VIEW_H

#include <hgraph/types/time_series/ts_input/typed_view.h>

namespace hgraph
{
    namespace detail
    {
        struct TSInputNode;
    }

    class TSInput;
    class TSInputView;
    class TSBInputView;
    class TSLInputView;
    class TSSInputView;
    class TSDInputView;
    class TSWInputView;

    class TSLInputView : public TSInputTypedView<TSLInputView>
    {
      public:
        explicit TSLInputView(TSInputView view);

        [[nodiscard]] std::size_t size() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] TSLDataView data_view() const;

        /** Child views in index order. */
        [[nodiscard]] Range<TSInputView> values() const;

        /** Child views filtered by current validity or modification time. */
        [[nodiscard]] Range<TSInputView> valid_values() const;
        [[nodiscard]] Range<TSInputView> modified_values() const;

        /** ``index -> child`` pairs in index order, optionally filtered. */
        [[nodiscard]] KeyValueRange<std::size_t, TSInputView> items() const;
        [[nodiscard]] KeyValueRange<std::size_t, TSInputView> valid_items() const;
        [[nodiscard]] KeyValueRange<std::size_t, TSInputView> modified_items() const;

        [[nodiscard]] TSInputView at(std::size_t index) &;
        [[nodiscard]] TSInputView at(std::size_t index) const &;
        TSInputView at(std::size_t) && = delete;
        [[nodiscard]] TSInputView operator[](std::size_t index) &;
        [[nodiscard]] TSInputView operator[](std::size_t index) const &;
        TSInputView operator[](std::size_t) && = delete;
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_INPUT_LIST_VIEW_H
