#include <hgraph/types/metadata/ts_value_plan_factory.h>

#include <stdexcept>

namespace hgraph
{
    TSValuePlanFactory &TSValuePlanFactory::instance()
    {
        static TSValuePlanFactory factory;
        return factory;
    }

    const MemoryUtils::StoragePlan *TSValuePlanFactory::plan_for(const TSValueTypeMetaData *schema)
    {
        if (schema == nullptr) { return nullptr; }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (const auto it = cache_.find(schema); it != cache_.end()) { return it->second; }
        }

        unsupported(schema);
    }

    const MemoryUtils::StoragePlan *TSValuePlanFactory::find(const TSValueTypeMetaData *schema) const
    {
        if (schema == nullptr) { return nullptr; }

        std::lock_guard<std::mutex> lock(mutex_);
        const auto                  it = cache_.find(schema);
        return it == cache_.end() ? nullptr : it->second;
    }

    void TSValuePlanFactory::unsupported(const TSValueTypeMetaData *)
    {
        throw std::logic_error(
            "TSValuePlanFactory: time-series plan synthesis requires value-layer + TS-layer storage support "
            "(not yet ported)");
    }
}  // namespace hgraph
