#include <hgraph/types/time_series/ts_data/storage.h>

namespace hgraph
{
    TSData::TSData() noexcept = default;

    TSData::TSData(const TSDataBinding &binding)
        : storage_(binding)
    {
    }

    bool TSData::has_value() const noexcept
    {
        return storage_.has_value();
    }

    const TSDataBinding *TSData::binding() const noexcept
    {
        return storage_.binding();
    }

    const TSValueTypeMetaData *TSData::schema() const noexcept
    {
        const auto *bound = binding();
        return bound != nullptr ? bound->type_meta : nullptr;
    }

    TSDataView TSData::view()
    {
        return TSDataView{binding(), storage_.data()};
    }

    TSDataView TSData::view() const
    {
        return TSDataView{binding(), storage_.data()};
    }
}  // namespace hgraph
