#include <hgraph/types/metadata/ts_data_plan_factory.h>

#include <hgraph/types/metadata/ts_data_plan_factory_detail.h>

#include <hgraph/types/metadata/value_plan_factory.h>

#include <fmt/format.h>

#include <stdexcept>

namespace hgraph
{
    namespace plan_detail = ts_data_plan_factory_detail;

    namespace
    {
        [[noreturn]] void unsupported(const TSValueTypeMetaData *schema)
        {
            const auto kind = schema == nullptr ? -1 : static_cast<int>(schema->kind);
            throw std::logic_error(
                fmt::format("TSDataPlanFactory: slot-oriented TSData storage is not implemented for kind {}", kind));
        }
    } // namespace

    TSDataPlanFactory &TSDataPlanFactory::instance()
    {
        static TSDataPlanFactory factory;
        return factory;
    }

    const MemoryUtils::StoragePlan *TSDataPlanFactory::plan_for(const TSValueTypeMetaData *schema)
    {
        if (schema == nullptr)
        {
            return nullptr;
        }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (const auto it = cache_.find(schema); it != cache_.end())
            {
                return it->second;
            }
        }

        return synthesise(schema);
    }

    const MemoryUtils::StoragePlan *TSDataPlanFactory::find(const TSValueTypeMetaData *schema) const
    {
        if (schema == nullptr)
        {
            return nullptr;
        }

        std::lock_guard<std::mutex> lock(mutex_);
        const auto                  it = cache_.find(schema);
        return it == cache_.end() ? nullptr : it->second;
    }

    void TSDataPlanFactory::reset() noexcept
    {
        std::lock_guard<std::mutex> lock(mutex_);
        cache_.clear();
        binding_cache_.clear();
        plan_detail::clear_atomic_ts_data_ops();
        plan_detail::clear_fixed_ts_data_contexts();
        plan_detail::clear_window_ts_data_contexts();
        plan_detail::clear_slot_ts_data_contexts();
    }

    const TSDataBinding *TSDataPlanFactory::binding_for(const TSValueTypeMetaData *schema)
    {
        if (schema == nullptr)
        {
            return nullptr;
        }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (const auto it = binding_cache_.find(schema); it != binding_cache_.end())
            {
                return it->second;
            }
        }

        return synthesise_binding(schema);
    }

    const TSDataBinding *TSDataPlanFactory::find_binding(const TSValueTypeMetaData *schema) const
    {
        if (schema == nullptr)
        {
            return nullptr;
        }

        std::lock_guard<std::mutex> lock(mutex_);
        const auto                  it = binding_cache_.find(schema);
        return it == binding_cache_.end() ? nullptr : it->second;
    }

    const MemoryUtils::StoragePlan *TSDataPlanFactory::synthesise(const TSValueTypeMetaData *schema)
    {
        if (schema == nullptr)
        {
            return nullptr;
        }
        if (plan_detail::is_fixed_structured_ts_data(*schema))
        {
            const auto *plan = plan_detail::synthesise_fixed_plan(*schema);

            std::lock_guard<std::mutex> lock(mutex_);
            if (const auto it = cache_.find(schema); it != cache_.end())
            {
                return it->second;
            }
            cache_.emplace(schema, plan);
            return plan;
        }
        if (plan_detail::is_window_ts_data(*schema))
        {
            const auto *plan = plan_detail::synthesise_window_plan(*schema);

            std::lock_guard<std::mutex> lock(mutex_);
            if (const auto it = cache_.find(schema); it != cache_.end())
            {
                return it->second;
            }
            cache_.emplace(schema, plan);
            return plan;
        }
        if (plan_detail::is_slot_ts_data(*schema))
        {
            const auto *plan = plan_detail::synthesise_slot_plan(*schema);

            std::lock_guard<std::mutex> lock(mutex_);
            if (const auto it = cache_.find(schema); it != cache_.end())
            {
                return it->second;
            }
            cache_.emplace(schema, plan);
            return plan;
        }
        if (!plan_detail::is_compact_atomic_ts_data(*schema))
        {
            unsupported(schema);
        }

        const auto *value_plan = ValuePlanFactory::instance().plan_for(schema->value_schema);
        if (value_plan == nullptr)
        {
            throw std::logic_error("TSDataPlanFactory: atomic TSData value plan is not resolvable");
        }

        auto builder = MemoryUtils::named_tuple();
        builder.reserve(2);
        builder.add_field("value", *value_plan);
        builder.add_field("tracking", MemoryUtils::plan_for<TSDataTracking>());
        const auto *plan = &builder.build();

        std::lock_guard<std::mutex> lock(mutex_);
        if (const auto it = cache_.find(schema); it != cache_.end())
        {
            return it->second;
        }
        cache_.emplace(schema, plan);
        return plan;
    }

    const TSDataBinding *TSDataPlanFactory::synthesise_binding(const TSValueTypeMetaData *schema)
    {
        if (schema == nullptr)
        {
            return nullptr;
        }
        if (plan_detail::is_fixed_structured_ts_data(*schema))
        {
            const auto *plan = plan_for(schema);
            if (plan == nullptr)
            {
                throw std::logic_error("TSDataPlanFactory: fixed TSData plan is not resolvable");
            }

            const auto *value_component = plan->find_component("value");
            const auto *aux_component   = plan->find_component("aux");
            if (value_component == nullptr || aux_component == nullptr)
            {
                throw std::logic_error("TSDataPlanFactory: fixed TSData plan is missing value or auxiliary region");
            }

            const auto *binding =
                plan_detail::embedded_ts_data_binding(*schema, *plan, value_component->offset, aux_component->offset);

            std::lock_guard<std::mutex> lock(mutex_);
            if (const auto it = binding_cache_.find(schema); it != binding_cache_.end())
            {
                return it->second;
            }
            binding_cache_.emplace(schema, binding);
            if (const auto plan_it = cache_.find(schema); plan_it != cache_.end())
            {
                if (plan_it->second != binding->plan())
                {
                    throw std::logic_error("TSDataPlanFactory: synthesised binding does not match cached plan");
                }
            }
            else
            {
                cache_.emplace(schema, binding->plan());
            }
            return binding;
        }
        if (plan_detail::is_window_ts_data(*schema))
        {
            const auto *plan = plan_for(schema);
            if (plan == nullptr)
            {
                throw std::logic_error("TSDataPlanFactory: TSW TSData plan is not resolvable");
            }

            const auto *window_component   = plan->find_component("window");
            const auto *tracking_component = plan->find_component("tracking");
            if (window_component == nullptr || tracking_component == nullptr)
            {
                throw std::logic_error("TSDataPlanFactory: TSW TSData plan is missing required components");
            }

            const auto &ops =
                plan_detail::window_ts_data_ops(*schema, *plan, window_component->offset, tracking_component->offset);
            const auto &binding = TSDataBinding::intern(*schema, *plan, ops);

            std::lock_guard<std::mutex> lock(mutex_);
            if (const auto it = binding_cache_.find(schema); it != binding_cache_.end())
            {
                return it->second;
            }
            binding_cache_.emplace(schema, &binding);
            if (const auto plan_it = cache_.find(schema); plan_it != cache_.end())
            {
                if (plan_it->second != binding.plan())
                {
                    throw std::logic_error("TSDataPlanFactory: synthesised binding does not match cached plan");
                }
            }
            else
            {
                cache_.emplace(schema, binding.plan());
            }
            return &binding;
        }
        if (plan_detail::is_slot_ts_data(*schema))
        {
            const auto *plan = plan_for(schema);
            if (plan == nullptr)
            {
                throw std::logic_error("TSDataPlanFactory: slot TSData plan is not resolvable");
            }

            const auto &ops = plan_detail::slot_ts_data_ops(*schema, *plan, 0);
            const auto &binding = TSDataBinding::intern(*schema, *plan, ops);

            std::lock_guard<std::mutex> lock(mutex_);
            if (const auto it = binding_cache_.find(schema); it != binding_cache_.end())
            {
                return it->second;
            }
            binding_cache_.emplace(schema, &binding);
            if (const auto plan_it = cache_.find(schema); plan_it != cache_.end())
            {
                if (plan_it->second != binding.plan())
                {
                    throw std::logic_error("TSDataPlanFactory: synthesised binding does not match cached plan");
                }
            }
            else
            {
                cache_.emplace(schema, binding.plan());
            }
            return &binding;
        }
        if (!plan_detail::is_compact_atomic_ts_data(*schema))
        {
            unsupported(schema);
        }

        const auto *value_binding = ValuePlanFactory::instance().binding_for(schema->value_schema);
        const auto *delta_binding = ValuePlanFactory::instance().binding_for(schema->delta_value_schema);
        if (value_binding == nullptr || delta_binding == nullptr)
        {
            throw std::logic_error("TSDataPlanFactory: atomic TSData value/delta bindings are not resolvable");
        }

        const auto *plan = plan_for(schema);
        if (plan == nullptr)
        {
            throw std::logic_error("TSDataPlanFactory: atomic TSData plan is not resolvable");
        }

        const auto *value_component    = plan->find_component("value");
        const auto *tracking_component = plan->find_component("tracking");
        if (value_component == nullptr || tracking_component == nullptr)
        {
            throw std::logic_error("TSDataPlanFactory: atomic TSData plan is missing required components");
        }

        const auto &ops     = plan_detail::atomic_ts_data_ops(schema->kind, *value_binding, *delta_binding, *plan,
                                                              value_component->offset, tracking_component->offset);
        const auto &binding = TSDataBinding::intern(*schema, *plan, ops);

        std::lock_guard<std::mutex> lock(mutex_);
        if (const auto it = binding_cache_.find(schema); it != binding_cache_.end())
        {
            return it->second;
        }
        binding_cache_.emplace(schema, &binding);
        if (const auto plan_it = cache_.find(schema); plan_it != cache_.end())
        {
            if (plan_it->second != binding.plan())
            {
                throw std::logic_error("TSDataPlanFactory: synthesised binding does not match cached plan");
            }
        }
        else
        {
            cache_.emplace(schema, binding.plan());
        }
        return &binding;
    }
} // namespace hgraph
