#ifndef HGRAPH_CPP_TS_DATA_STORAGE_H
#define HGRAPH_CPP_TS_DATA_STORAGE_H

#include <hgraph/types/time_series/ts_data/base_view.h>
#include <hgraph/types/utils/memory_utils.h>

namespace hgraph
{
    /** Owning storage for one TSData root allocation. */
    class TSData
    {
      public:
        using storage_type = MemoryUtils::StorageHandle<MemoryUtils::InlineStoragePolicy<>, TSDataBinding>;

        TSData() noexcept;
        explicit TSData(const TSDataBinding &binding);

        /** True when the storage owns a bound TSData allocation. */
        [[nodiscard]] bool has_value() const noexcept;

        /** Binding and schema for the owned allocation, or null when empty. */
        [[nodiscard]] const TSDataBinding *binding() const noexcept;
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept;

        /** Borrowed root TSData view over the owned allocation. */
        [[nodiscard]] TSDataView view();
        [[nodiscard]] TSDataView view() const;

      private:
        storage_type storage_{};
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_TS_DATA_STORAGE_H
