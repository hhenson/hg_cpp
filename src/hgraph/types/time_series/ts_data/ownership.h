#ifndef HGRAPH_CPP_TS_DATA_OWNERSHIP_H
#define HGRAPH_CPP_TS_DATA_OWNERSHIP_H

#include <hgraph/types/time_series/ts_data/types.h>

#include <cstddef>

namespace hgraph::detail
{
    struct TSDataOwnedChild
    {
        TSStorageTypeRef type{};
        void            *data{nullptr};
    };

    struct TSDataOwnershipOps
    {
        using child_count_fn = std::size_t (*)(const void *context) noexcept;
        using child_at_fn = TSDataOwnedChild (*)(const void *context, void *memory, std::size_t index) noexcept;

        child_count_fn child_count{nullptr};
        child_at_fn    child_at{nullptr};
    };

    [[nodiscard]] const TSDataOwnershipOps *composed_input_ownership_ops_for(const TSDataOps *ops) noexcept;
    [[nodiscard]] const TSDataOwnershipOps *fixed_ts_data_ownership_ops_for(const TSDataOps *ops) noexcept;
}  // namespace hgraph::detail

#endif  // HGRAPH_CPP_TS_DATA_OWNERSHIP_H
