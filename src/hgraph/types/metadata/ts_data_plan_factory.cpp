#include <hgraph/types/metadata/ts_data_plan_factory.h>

#include <hgraph/types/metadata/ts_data_plan_factory_detail.h>

#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/time_series/ts_output/alternative.h>

#include <fmt/format.h>

#include <stdexcept>

namespace hgraph
{
    namespace plan_detail = ts_data_plan_factory_detail;

    namespace
    {
        [[nodiscard]] bool fixed_root(const TSValueTypeMetaData *schema) noexcept
        {
            return schema != nullptr &&
                   (schema->kind == TSTypeKind::TSB ||
                    (schema->kind == TSTypeKind::TSL && schema->fixed_size() != 0));
        }

        [[nodiscard]] bool slot_root(const TSValueTypeMetaData *schema) noexcept
        {
            return schema != nullptr && (schema->kind == TSTypeKind::TSS || schema->kind == TSTypeKind::TSD);
        }

        [[nodiscard]] std::string_view root_label(TSTypeKind kind, TypeRole role)
        {
            if (kind == TSTypeKind::TSS)
                return role == TypeRole::Data ? "ts.tss.data.root" : "ts.tss.output.root";
            if (kind == TSTypeKind::TSD)
                return role == TypeRole::Data ? "ts.tsd.data.root" : "ts.tsd.output.root";
            if (kind == TSTypeKind::REF)
                return role == TypeRole::Data ? "ts.ref.data.root" : "ts.ref.output.root";
            return {};
        }

        [[nodiscard]] bool migrated_root(const TSValueTypeMetaData *schema) noexcept
        {
            return is_migrated_ts_root_schema(schema);
        }

        [[noreturn]] void unsupported(const TSValueTypeMetaData *schema)
        {
            const auto kind = schema == nullptr ? -1 : static_cast<int>(schema->kind);
            throw std::logic_error(
                fmt::format("TSDataPlanFactory: slot-oriented TSData storage is not implemented for kind {}", kind));
        }
    } // namespace

    TSDataPlanFactory &TSDataPlanFactory::instance()
    {
        // Immortal (see OperatorRegistry::instance).
        static TSDataPlanFactory *factory = new TSDataPlanFactory();
        return *factory;
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
        data_type_cache_.clear();
        output_type_cache_.clear();
        plan_detail::clear_atomic_ts_data_ops();
        plan_detail::clear_fixed_ts_data_contexts();
        plan_detail::clear_dynamic_list_ts_data_contexts();
        plan_detail::clear_window_ts_data_contexts();
        plan_detail::clear_slot_ts_data_contexts();
        clear_tsd_proxy_contexts();
        detail::clear_ts_output_alternative_type_cache();
    }

    const TSDataBinding *TSDataPlanFactory::binding_for(const TSValueTypeMetaData *schema)
    {
        if (schema == nullptr) return nullptr;
        if (migrated_root(schema))
        {
            throw std::logic_error("TSDataPlanFactory: migrated root identity is a TypeRecord, not TSDataBinding");
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

    const TSDataBinding *TSDataPlanFactory::legacy_binding_for(const TSValueTypeMetaData *schema)
    {
        if (schema == nullptr) return nullptr;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (const auto it = binding_cache_.find(schema); it != binding_cache_.end()) return it->second;
        }
        return synthesise_binding(schema);
    }

    TSDataTypeRef TSDataPlanFactory::data_type_for(const TSValueTypeMetaData *schema)
    {
        if (!migrated_root(schema)) throw std::invalid_argument("data_type_for requires a migrated TS schema");
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (const auto it = data_type_cache_.find(schema); it != data_type_cache_.end()) return it->second;
        }
        const auto *plan = plan_for(schema);
        if (slot_root(schema))
        {
            if (plan == nullptr) throw std::logic_error("data_type_for could not resolve keyed storage");
            const auto &ops = plan_detail::slot_ts_data_ops(*schema, *plan, 0, TypeRole::Data);
            const auto type = TSDataTypeRef::checked(intern_ts_type(
                *schema, TypeRole::Data, *plan, ops, root_label(schema->kind, TypeRole::Data)));
            std::lock_guard<std::mutex> lock(mutex_);
            return data_type_cache_.try_emplace(schema, type).first->second;
        }
        if (fixed_root(schema))
        {
            const auto *value = plan != nullptr ? plan->find_component("value") : nullptr;
            const auto *aux = plan != nullptr ? plan->find_component("aux") : nullptr;
            if (plan == nullptr || value == nullptr || aux == nullptr)
                throw std::logic_error("data_type_for could not resolve fixed storage");
            const auto storage_type = plan_detail::embedded_ts_storage_type(
                *schema, TypeRole::Data, *plan, value->offset, aux->offset, true);
            const auto type = TSDataTypeRef::checked(storage_type.type_ref());
            std::lock_guard<std::mutex> lock(mutex_);
            return data_type_cache_.try_emplace(schema, type).first->second;
        }
        const auto value_type = ValuePlanFactory::instance().type_for(schema->value_schema);
        const auto delta_type = ValuePlanFactory::instance().type_for(schema->delta_value_schema);
        const auto *value = plan != nullptr ? plan->find_component("value") : nullptr;
        const auto *tracking = plan != nullptr ? plan->find_component("tracking") : nullptr;
        if (plan == nullptr || !value_type || !delta_type || value == nullptr || tracking == nullptr)
            throw std::logic_error("data_type_for could not resolve scalar storage");
        const auto &ops = plan_detail::atomic_ts_data_ops(schema->kind, value_type, delta_type, *plan,
                                                          value->offset, tracking->offset);
        const auto type = checked_ts_role_type(intern_ts_type(
                                                   *schema, TypeRole::Data, *plan, ops,
                                                   root_label(schema->kind, TypeRole::Data)),
                                               std::integral_constant<TypeRole, TypeRole::Data>{});
        std::lock_guard<std::mutex> lock(mutex_);
        return data_type_cache_.try_emplace(schema, type).first->second;
    }

    TSOutputTypeRef TSDataPlanFactory::output_type_for(const TSValueTypeMetaData *schema)
    {
        if (!migrated_root(schema)) throw std::invalid_argument("output_type_for requires a migrated TS schema");
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (const auto it = output_type_cache_.find(schema); it != output_type_cache_.end()) return it->second;
        }
        if (fixed_root(schema))
        {
            const auto *plan = plan_for(schema);
            const auto *value = plan != nullptr ? plan->find_component("value") : nullptr;
            const auto *aux = plan != nullptr ? plan->find_component("aux") : nullptr;
            if (plan == nullptr || value == nullptr || aux == nullptr)
                throw std::logic_error("output_type_for could not resolve fixed storage");
            const auto storage_type = plan_detail::embedded_ts_storage_type(
                *schema, TypeRole::Output, *plan, value->offset, aux->offset, true);
            const auto type = TSOutputTypeRef::checked(storage_type.type_ref());
            std::lock_guard<std::mutex> lock(mutex_);
            return output_type_cache_.try_emplace(schema, type).first->second;
        }
        if (slot_root(schema))
        {
            const auto *plan = plan_for(schema);
            if (plan == nullptr) throw std::logic_error("output_type_for could not resolve keyed storage");
            const auto &ops = plan_detail::slot_ts_data_ops(*schema, *plan, 0, TypeRole::Output);
            const auto type = TSOutputTypeRef::checked(intern_ts_type(
                *schema, TypeRole::Output, *plan, ops, root_label(schema->kind, TypeRole::Output)));
            std::lock_guard<std::mutex> lock(mutex_);
            return output_type_cache_.try_emplace(schema, type).first->second;
        }
        const auto data_type = data_type_for(schema);
        const auto type = checked_ts_role_type(
            intern_ts_type(*schema, TypeRole::Output, data_type.checked_plan(), data_type.ops_ref(),
                           root_label(schema->kind, TypeRole::Output)),
            std::integral_constant<TypeRole, TypeRole::Output>{});
        std::lock_guard<std::mutex> lock(mutex_);
        return output_type_cache_.try_emplace(schema, type).first->second;
    }

    TSDataTypeRef TSDataPlanFactory::find_data_type(const TSValueTypeMetaData *schema) const noexcept
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto it = data_type_cache_.find(schema);
        return it == data_type_cache_.end() ? TSDataTypeRef{} : it->second;
    }

    TSOutputTypeRef TSDataPlanFactory::find_output_type(const TSValueTypeMetaData *schema) const noexcept
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto it = output_type_cache_.find(schema);
        return it == output_type_cache_.end() ? TSOutputTypeRef{} : it->second;
    }

    const TSDataBinding *TSDataPlanFactory::find_binding(const TSValueTypeMetaData *schema) const
    {
        if (schema == nullptr || migrated_root(schema))
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
        if (plan_detail::is_dynamic_list_ts_data(*schema))
        {
            const auto *plan = plan_detail::synthesise_dynamic_list_plan(*schema);

            std::lock_guard<std::mutex> lock(mutex_);
            if (const auto it = cache_.find(schema); it != cache_.end())
            {
                return it->second;
            }
            cache_.emplace(schema, plan);
            return plan;
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

        const auto value_plan = ValuePlanFactory::instance().plan_for(schema->value_schema);
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
        if (plan_detail::is_dynamic_list_ts_data(*schema))
        {
            const auto *plan = plan_for(schema);
            if (plan == nullptr)
            {
                throw std::logic_error("TSDataPlanFactory: dynamic TSL plan is not resolvable");
            }

            const auto &ops = plan_detail::dynamic_list_ts_data_ops(*schema, *plan, 0);
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

        const auto value_binding = ValuePlanFactory::instance().type_for(schema->value_schema);
        const auto delta_binding = ValuePlanFactory::instance().type_for(schema->delta_value_schema);
        if (!value_binding || !delta_binding)
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

        const auto &ops     = plan_detail::atomic_ts_data_ops(schema->kind, value_binding, delta_binding, *plan,
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
