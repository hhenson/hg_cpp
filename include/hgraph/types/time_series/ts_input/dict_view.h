#ifndef HGRAPH_CPP_ROOT_TS_INPUT_DICT_VIEW_H
#define HGRAPH_CPP_ROOT_TS_INPUT_DICT_VIEW_H

#include <hgraph/types/time_series/ts_input/typed_view.h>

namespace hgraph
{
    class TSInput;
    class TSInputView;
    class TSBInputView;
    class TSLInputView;
    class TSSInputView;
    class TSDInputView;
    class TSWInputView;

    class TSDInputView : public TSInputTypedView<TSDInputView>
    {
      public:
        explicit TSDInputView(TSInputView view);

        [[nodiscard]] TSDDataView data_view() const;
        [[nodiscard]] std::size_t size() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] std::size_t slot_capacity() const;
        [[nodiscard]] bool slot_occupied(std::size_t slot) const;
        [[nodiscard]] bool slot_live(std::size_t slot) const;
        [[nodiscard]] bool slot_added(std::size_t slot) const;
        [[nodiscard]] bool slot_removed(std::size_t slot) const;
        [[nodiscard]] bool slot_modified(std::size_t slot) const;
        [[nodiscard]] ValueView key_at_slot(std::size_t slot) const;
        [[nodiscard]] TSInputView at_slot(std::size_t slot) const;
        [[nodiscard]] bool contains(const ValueView &key) const;
        [[nodiscard]] std::size_t find_slot(const ValueView &key) const;
        [[nodiscard]] TSInputView at(const ValueView &key) const;
        [[nodiscard]] TSInputView operator[](const ValueView &key) const;
        [[nodiscard]] Range<ValueView> keys() const;
        [[nodiscard]] Range<TSInputView> values() const;
        [[nodiscard]] KeyValueRange<ValueView, TSInputView> items() const;
        [[nodiscard]] Range<ValueView> valid_keys() const;
        [[nodiscard]] Range<TSInputView> valid_values() const;
        [[nodiscard]] KeyValueRange<ValueView, TSInputView> valid_items() const;
        [[nodiscard]] Range<ValueView> modified_keys() const;
        [[nodiscard]] Range<TSInputView> modified_values() const;
        [[nodiscard]] KeyValueRange<ValueView, TSInputView> modified_items() const;
        [[nodiscard]] Range<ValueView> added_keys() const;
        [[nodiscard]] Range<TSInputView> added_values() const;
        [[nodiscard]] KeyValueRange<ValueView, TSInputView> added_items() const;
        [[nodiscard]] Range<ValueView> removed_keys() const;
        [[nodiscard]] Range<TSInputView> removed_values() const;
        [[nodiscard]] KeyValueRange<ValueView, TSInputView> removed_items() const;
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_INPUT_DICT_VIEW_H
