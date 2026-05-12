#include <hgraph/types/time_series/ts_data.h>

#include <algorithm>
#include <stdexcept>
#include <vector>

namespace hgraph
{
    bool TSDataParentLink::has_parent() const noexcept
    {
        return parent_binding != nullptr && parent_data != nullptr;
    }

    const TSDataTracking &TSDataParentLink::parent_tracking() const
    {
        if (!has_parent()) { throw std::logic_error("TSDataParentLink requires a parent"); }
        const auto &table = parent_binding->checked_ops();
        return *table.tracking_impl(table.context, parent_data);
    }

    TSDataTracking &TSDataParentLink::mutable_parent_tracking() const
    {
        if (!has_parent()) { throw std::logic_error("TSDataParentLink requires a parent"); }
        const auto &table = parent_binding->checked_ops();
        auto       *memory = const_cast<void *>(parent_data);
        return *table.mutable_tracking_impl(table.context, memory);
    }

    void TSDataParentLink::notify_child_modified(engine_time_t mutation_time) const
    {
        if (!has_parent()) { return; }

        const auto &table = parent_binding->checked_ops();
        auto       *memory = const_cast<void *>(parent_data);
        table.record_child_modified_impl(table.context, memory, child_id, mutation_time);

        auto &state = mutable_parent_tracking();
        if (state.last_modified_time == mutation_time) { return; }

        state.last_modified_time = mutation_time;
        state.parent.notify_child_modified(mutation_time);
    }

    std::vector<std::size_t> TSDataParentLink::path_from_root() const
    {
        std::vector<std::size_t> reversed_path;
        auto                     current = *this;
        while (current.has_parent())
        {
            reversed_path.push_back(current.child_id);
            const auto &next = current.parent_tracking().parent;
            if (!next.has_parent()) { break; }
            current = next;
        }

        std::reverse(reversed_path.begin(), reversed_path.end());
        return reversed_path;
    }

    TSDataView TSDataParentLink::root_view() const
    {
        if (!has_parent()) { return TSDataView{}; }

        const TSDataBinding *root_binding = parent_binding;
        const void          *root_data    = parent_data;
        auto                 current      = *this;
        while (current.has_parent())
        {
            root_binding = current.parent_binding;
            root_data    = current.parent_data;
            const auto &next = current.parent_tracking().parent;
            if (!next.has_parent()) { break; }
            current = next;
        }
        return TSDataView{root_binding, root_data};
    }

    const ValueTypeBinding *FixedTSDataFieldLayout::value_binding() const noexcept
    {
        return layout != nullptr ? layout->value_binding : nullptr;
    }

    const ValueTypeBinding *FixedTSDataFieldLayout::delta_binding() const noexcept
    {
        return layout != nullptr ? layout->delta_binding : nullptr;
    }

    std::size_t FixedTSBDataLayout::size() const noexcept
    {
        return fields.size();
    }

    const FixedTSDataFieldLayout &FixedTSBDataLayout::field(std::size_t index) const
    {
        return fields.at(index);
    }

    std::size_t FixedTSLDataLayout::size() const noexcept
    {
        return element_count;
    }

    std::size_t FixedTSLDataLayout::element_value_offset(std::size_t index) const
    {
        if (index >= element_count) { throw std::out_of_range("FixedTSLDataLayout element index out of range"); }
        return value_offset + index * element_value_stride;
    }

    std::size_t FixedTSLDataLayout::element_auxiliary_offset_at(std::size_t index) const
    {
        if (index >= element_count) { throw std::out_of_range("FixedTSLDataLayout element index out of range"); }
        return element_auxiliary_offset + index * element_auxiliary_stride;
    }
}  // namespace hgraph
