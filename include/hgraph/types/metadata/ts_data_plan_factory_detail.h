#ifndef HGRAPH_CPP_ROOT_TS_DATA_PLAN_FACTORY_DETAIL_H
#define HGRAPH_CPP_ROOT_TS_DATA_PLAN_FACTORY_DETAIL_H

#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/time_series/ts_data.h>
#include <hgraph/types/utils/memory_utils.h>

#include <cstddef>
#include <vector>

namespace hgraph::ts_data_plan_factory_detail
{
    [[nodiscard]] const TSDataOps &atomic_ts_data_ops(TSTypeKind                     kind,
                                                      const ValueTypeRef         &value_binding,
                                                      const ValueTypeRef         &delta_binding,
                                                      const MemoryUtils::StoragePlan &plan, std::size_t value_offset,
                                                      std::size_t tracking_offset);

    void clear_atomic_ts_data_ops() noexcept;

    [[nodiscard]] bool is_compact_atomic_ts_data(const TSValueTypeMetaData &schema) noexcept;
    [[nodiscard]] bool is_fixed_structured_ts_data(const TSValueTypeMetaData &schema) noexcept;
    [[nodiscard]] bool is_dynamic_list_ts_data(const TSValueTypeMetaData &schema) noexcept;
    [[nodiscard]] bool is_window_ts_data(const TSValueTypeMetaData &schema) noexcept;
    [[nodiscard]] bool is_slot_ts_data(const TSValueTypeMetaData &schema) noexcept;

    [[nodiscard]] const MemoryUtils::StoragePlan &ts_data_aux_plan(const TSValueTypeMetaData &schema);
    [[nodiscard]] const MemoryUtils::StoragePlan *synthesise_fixed_plan(const TSValueTypeMetaData &schema);
    [[nodiscard]] const MemoryUtils::StoragePlan *synthesise_dynamic_list_plan(const TSValueTypeMetaData &schema);
    [[nodiscard]] const MemoryUtils::StoragePlan *synthesise_window_plan(const TSValueTypeMetaData &schema);
    [[nodiscard]] const MemoryUtils::StoragePlan *synthesise_slot_plan(const TSValueTypeMetaData &schema);
    [[nodiscard]] const MemoryUtils::StoragePlan *synthesise_slot_tsd_plan(
        const TSValueTypeMetaData &schema,
        const TSDataBinding       &element_binding);

    [[nodiscard]] const TSDataBinding *embedded_ts_data_binding(const TSValueTypeMetaData      &schema,
                                                                const MemoryUtils::StoragePlan &root_plan,
                                                                std::size_t value_offset, std::size_t aux_offset);

    [[nodiscard]] const TSDataOps &fixed_structured_ts_data_ops(const TSValueTypeMetaData      &schema,
                                                                const MemoryUtils::StoragePlan &plan,
                                                                std::size_t value_offset, std::size_t aux_offset,
                                                                std::size_t tracking_offset,
                                                                std::vector<const TSDataBinding *> element_bindings,
                                                                std::vector<std::size_t> element_data_offsets);

    [[nodiscard]] const TSDataOps &dynamic_list_ts_data_ops(const TSValueTypeMetaData      &schema,
                                                            const MemoryUtils::StoragePlan &plan,
                                                            std::size_t storage_offset);

    [[nodiscard]] const TSDataOps &window_ts_data_ops(const TSValueTypeMetaData      &schema,
                                                      const MemoryUtils::StoragePlan &plan,
                                                      std::size_t value_offset,
                                                      std::size_t tracking_offset);

    [[nodiscard]] const TSDataOps &slot_ts_data_ops(const TSValueTypeMetaData      &schema,
                                                    const MemoryUtils::StoragePlan &plan,
                                                    std::size_t storage_offset);
    [[nodiscard]] const TSDataOps &slot_tsd_ts_data_ops(const TSValueTypeMetaData      &schema,
                                                        const MemoryUtils::StoragePlan &plan,
                                                        std::size_t storage_offset,
                                                        const TSDataBinding            &element_binding);

    void clear_fixed_ts_data_contexts() noexcept;
    void clear_dynamic_list_ts_data_contexts() noexcept;
    void clear_window_ts_data_contexts() noexcept;
    void clear_slot_ts_data_contexts() noexcept;
} // namespace hgraph::ts_data_plan_factory_detail

#endif // HGRAPH_CPP_ROOT_TS_DATA_PLAN_FACTORY_DETAIL_H
