#include <hgraph/types/metadata/ts_data_plan_factory_detail.h>

#include <hgraph/types/metadata/value_plan_factory.h>

#include <fmt/format.h>

#include <cstddef>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace hgraph::ts_data_plan_factory_detail
{
    [[nodiscard]] std::string field_component_name(std::size_t index)
    {
        return fmt::format("field_{}", index);
    }

    [[nodiscard]] constexpr std::string_view tsl_elements_component_name() noexcept
    {
        return "elements";
    }

    [[nodiscard]] bool is_compact_atomic_ts_data(const TSValueTypeMetaData &schema) noexcept
    {
        switch (schema.kind)
        {
        case TSTypeKind::TS:
        case TSTypeKind::REF:
        case TSTypeKind::SIGNAL:
            return schema.value_schema != nullptr && schema.delta_value_schema != nullptr &&
                   schema.value_schema == schema.delta_value_schema &&
                   schema.value_schema->kind == ValueTypeKind::Atomic &&
                   schema.delta_value_schema->kind == ValueTypeKind::Atomic;
        default:
            return false;
        }
    }

    [[nodiscard]] bool is_fixed_structured_ts_data(const TSValueTypeMetaData &schema) noexcept
    {
        switch (schema.kind)
        {
        case TSTypeKind::TSB:
            return schema.value_schema != nullptr && schema.delta_value_schema != nullptr &&
                   (schema.field_count() == 0 || schema.fields() != nullptr);
        case TSTypeKind::TSL:
            return schema.fixed_size() != 0 && schema.element_ts() != nullptr && schema.value_schema != nullptr &&
                   schema.delta_value_schema != nullptr;
        default:
            return false;
        }
    }

    [[nodiscard]] std::size_t fixed_element_count(const TSValueTypeMetaData &schema) noexcept
    {
        switch (schema.kind)
        {
        case TSTypeKind::TSB:
            return schema.field_count();
        case TSTypeKind::TSL:
            return schema.fixed_size();
        default:
            return 0;
        }
    }

    [[nodiscard]] const TSValueTypeMetaData *fixed_element_schema(const TSValueTypeMetaData &schema, std::size_t index)
    {
        switch (schema.kind)
        {
        case TSTypeKind::TSB:
            if (index >= schema.field_count())
            {
                throw std::out_of_range("TSB field index out of range");
            }
            return schema.fields()[index].type;
        case TSTypeKind::TSL:
            if (index >= schema.fixed_size())
            {
                throw std::out_of_range("TSL element index out of range");
            }
            return schema.element_ts();
        default:
            return nullptr;
        }
    }

    [[nodiscard]] const MemoryUtils::StoragePlan &ts_data_aux_plan(const TSValueTypeMetaData &schema)
    {
        auto builder = MemoryUtils::named_tuple();
        if (is_compact_atomic_ts_data(schema))
        {
            builder.reserve(1);
            builder.add_field("tracking", MemoryUtils::plan_for<TSDataTracking>());
            return builder.build();
        }
        if (schema.kind == TSTypeKind::TSS)
        {
            return *synthesise_slot_plan(schema);
        }
        if (is_slot_ts_data(schema))
        {
            throw std::logic_error("TSDataPlanFactory: embedded TSD slot storage is not implemented");
        }
        if (!is_fixed_structured_ts_data(schema))
        {
            throw std::logic_error(
                fmt::format("TSDataPlanFactory: slot-oriented TSData storage is not implemented for kind {}",
                            static_cast<int>(schema.kind)));
        }

        if (schema.kind == TSTypeKind::TSB)
        {
            const auto count = fixed_element_count(schema);
            builder.reserve(count + 1);
            for (std::size_t index = 0; index < count; ++index)
            {
                const auto *element_schema = fixed_element_schema(schema, index);
                if (element_schema == nullptr)
                {
                    throw std::logic_error("TSDataPlanFactory: fixed TSData element schema is not resolved");
                }
                builder.add_field(field_component_name(index), ts_data_aux_plan(*element_schema));
            }
            builder.add_field("tracking", MemoryUtils::plan_for<TSDataTracking>());
            return builder.build();
        }

        const auto *element_schema = schema.element_ts();
        if (element_schema == nullptr)
        {
            throw std::logic_error("TSDataPlanFactory: fixed TSL element schema is not resolved");
        }
        const auto &element_aux_plan = ts_data_aux_plan(*element_schema);
        builder.reserve(2);
        builder.add_field(tsl_elements_component_name(), MemoryUtils::array_plan(element_aux_plan, schema.fixed_size()));
        builder.add_field("tracking", MemoryUtils::plan_for<TSDataTracking>());
        return builder.build();
    }

    [[nodiscard]] std::size_t fixed_element_value_offset(const TSValueTypeMetaData      &schema,
                                                         const MemoryUtils::StoragePlan &value_plan, std::size_t index)
    {
        switch (schema.kind)
        {
        case TSTypeKind::TSB:
            return value_plan.component(index).offset;
        case TSTypeKind::TSL:
            return value_plan.element_offset(index);
        default:
            throw std::logic_error("TSDataPlanFactory: fixed element value offset requires TSB or TSL");
        }
    }

    [[nodiscard]] std::size_t tracking_offset_in_aux(const MemoryUtils::StoragePlan &aux_plan)
    {
        const auto *tracking_component = aux_plan.find_component("tracking");
        if (tracking_component == nullptr)
        {
            throw std::logic_error("TSDataPlanFactory: TSData auxiliary plan is missing tracking");
        }
        return tracking_component->offset;
    }

    [[nodiscard]] std::size_t fixed_element_aux_offset(const TSValueTypeMetaData      &schema,
                                                       const MemoryUtils::StoragePlan &aux_plan, std::size_t index)
    {
        switch (schema.kind)
        {
        case TSTypeKind::TSB:
        {
            const auto *child_aux_component = aux_plan.find_component(field_component_name(index));
            if (child_aux_component == nullptr)
            {
                throw std::logic_error("TSDataPlanFactory: fixed TSB auxiliary plan is missing a field");
            }
            return child_aux_component->offset;
        }
        case TSTypeKind::TSL:
        {
            const auto *elements_component = aux_plan.find_component(tsl_elements_component_name());
            if (elements_component == nullptr || elements_component->plan == nullptr ||
                !elements_component->plan->is_array())
            {
                throw std::logic_error("TSDataPlanFactory: fixed TSL auxiliary plan is missing element array");
            }
            return elements_component->offset + elements_component->plan->element_offset(index);
        }
        default:
            throw std::logic_error("TSDataPlanFactory: fixed element auxiliary offset requires TSB or TSL");
        }
    }

    [[nodiscard]] const TSDataBinding *embedded_ts_data_binding(const TSValueTypeMetaData      &schema,
                                                                const MemoryUtils::StoragePlan &root_plan,
                                                                std::size_t value_offset, std::size_t aux_offset)
    {
        if (schema.kind == TSTypeKind::TSS)
        {
            const auto *plan = synthesise_slot_plan(schema);
            if (plan == nullptr)
            {
                throw std::logic_error("TSDataPlanFactory: slot TSData plan is not resolved");
            }
            const auto &ops = slot_ts_data_ops(schema, *plan, 0);
            return &TSDataBinding::intern(schema, *plan, ops);
        }
        if (is_slot_ts_data(schema))
        {
            throw std::logic_error("TSDataPlanFactory: embedded TSD slot storage is not implemented");
        }

        const auto &aux_plan        = ts_data_aux_plan(schema);
        const auto  tracking_offset = aux_offset + tracking_offset_in_aux(aux_plan);

        if (is_compact_atomic_ts_data(schema))
        {
            const auto *value_binding = ValuePlanFactory::instance().binding_for(schema.value_schema);
            const auto *delta_binding = ValuePlanFactory::instance().binding_for(schema.delta_value_schema);
            if (value_binding == nullptr || delta_binding == nullptr)
            {
                throw std::logic_error("TSDataPlanFactory: embedded atomic bindings are not resolved");
            }
            const auto &ops = atomic_ts_data_ops(schema.kind, *value_binding, *delta_binding, root_plan, value_offset,
                                                 tracking_offset);
            return &TSDataBinding::intern(schema, root_plan, ops);
        }

        if (!is_fixed_structured_ts_data(schema))
        {
            throw std::logic_error(
                fmt::format("TSDataPlanFactory: slot-oriented TSData storage is not implemented for kind {}",
                            static_cast<int>(schema.kind)));
        }

        const auto *value_binding = ValuePlanFactory::instance().binding_for(schema.value_schema);
        if (value_binding == nullptr)
        {
            throw std::logic_error("TSDataPlanFactory: embedded fixed value binding is not resolved");
        }

        const auto                        &value_plan = value_binding->checked_plan();
        const auto                         count      = fixed_element_count(schema);
        std::vector<const TSDataBinding *> element_bindings;
        std::vector<std::size_t>           element_data_offsets;
        element_bindings.reserve(count);
        element_data_offsets.reserve(count);
        for (std::size_t index = 0; index < count; ++index)
        {
            const auto *element_schema = fixed_element_schema(schema, index);
            if (element_schema == nullptr)
            {
                throw std::logic_error("TSDataPlanFactory: embedded fixed element schema is not resolved");
            }

            const auto child_value_offset = value_offset + fixed_element_value_offset(schema, value_plan, index);
            const auto child_aux_offset   = aux_offset + fixed_element_aux_offset(schema, aux_plan, index);
            const auto *child_binding =
                embedded_ts_data_binding(*element_schema, root_plan, child_value_offset, child_aux_offset);
            if (child_binding == nullptr)
            {
                throw std::logic_error("TSDataPlanFactory: embedded fixed element binding is not resolved");
            }
            element_bindings.push_back(child_binding);
            element_data_offsets.push_back(child_binding->plan() == &root_plan ? 0 : child_aux_offset);
        }

        const auto &ops = fixed_structured_ts_data_ops(schema, root_plan, value_offset, aux_offset, tracking_offset,
                                                       std::move(element_bindings), std::move(element_data_offsets));
        return &TSDataBinding::intern(schema, root_plan, ops);
    }

    [[nodiscard]] const MemoryUtils::StoragePlan *synthesise_fixed_plan(const TSValueTypeMetaData &schema)
    {
        const auto *value_plan = ValuePlanFactory::instance().plan_for(schema.value_schema);
        if (value_plan == nullptr)
        {
            throw std::logic_error("TSDataPlanFactory: fixed TSData value plan is not resolved");
        }
        auto builder = MemoryUtils::named_tuple();
        builder.reserve(2);
        builder.add_field("value", *value_plan);
        builder.add_field("aux", ts_data_aux_plan(schema));
        return &builder.build();
    }

} // namespace hgraph::ts_data_plan_factory_detail
