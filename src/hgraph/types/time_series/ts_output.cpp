#include <hgraph/types/time_series/ts_output.h>

#include <hgraph/runtime/graph.h>
#include <hgraph/runtime/node.h>
#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/time_series/endpoint_schema.h>
#include <hgraph/types/time_series/ts_input/detail.h>
#include <hgraph/types/time_series/ts_output/alternative.h>

#include <stdexcept>
#include <utility>

namespace hgraph
{
    TSOutput::TSOutput() noexcept = default;

    TSOutput::TSOutput(TSOutputTypeRef type)
        : data_(type)
    {
        attach_root_parent();
    }

    TSOutput::TSOutput(const TSValueTypeMetaData &schema)
        : data_(checked_data_for(&schema))
    {
        attach_root_parent();
    }

    TSOutput::TSOutput(const TSValueTypeMetaData *schema)
        : data_(checked_data_for(schema))
    {
        attach_root_parent();
    }

    TSOutput::TSOutput(const TSEndpointSchema &endpoint_schema)
        : data_(checked_data_for(endpoint_schema))
    {
        attach_root_parent();
    }

    TSOutput::~TSOutput() noexcept
    {
        invalidate_observers();
    }

    TSOutput::TSOutput(const TSOutput &other)
        : data_(copyable_data(other))
    {
        attach_root_parent();
    }

    TSOutput &TSOutput::operator=(const TSOutput &other)
    {
        if (this != &other)
        {
            invalidate_observers();
            alternatives_.reset();
            data_ = other.data_;
            attach_root_parent();
        }
        return *this;
    }

    TSOutput::TSOutput(TSOutput &&other) noexcept
    {
        other.invalidate_observers();
        other.alternatives_.reset();
        data_ = std::move(other.data_);
        attach_root_parent();
    }

    TSOutput &TSOutput::operator=(TSOutput &&other) noexcept
    {
        if (this != &other)
        {
            invalidate_observers();
            other.invalidate_observers();
            alternatives_.reset();
            other.alternatives_.reset();
            data_ = std::move(other.data_);
            attach_root_parent();
        }
        return *this;
    }

    bool TSOutput::has_value() const noexcept
    {
        return data_.has_value();
    }

    TSOutputTypeRef TSOutput::type_ref() const
    {
        const auto type = data_.type_ref();
        return type ? TSOutputTypeRef::checked(type) : TSOutputTypeRef{};
    }

    const TSValueTypeMetaData *TSOutput::schema() const noexcept
    {
        return data_.schema();
    }

    TSDataView TSOutput::data_view()
    {
        return data_.view();
    }

    TSDataView TSOutput::data_view() const
    {
        return data_.view();
    }

    void TSOutput::subscribe(Notifiable *observer)
    {
        if (!has_value()) { throw std::logic_error("TSOutput::subscribe requires a bound output"); }
        data_view().subscribe(observer);
    }

    void TSOutput::unsubscribe(Notifiable *observer)
    {
        if (!has_value()) { throw std::logic_error("TSOutput::unsubscribe requires a bound output"); }
        data_view().unsubscribe(observer);
    }

    TSOutputView TSOutput::view(DateTime evaluation_time)
    {
        return TSOutputView{this, data_view(), evaluation_time};
    }

    TSOutputView TSOutput::view(DateTime evaluation_time) const
    {
        return TSOutputView{this, data_view(), evaluation_time};
    }

    TSOutputHandle TSOutput::binding_for(const TSOutputView &source,
                                         const TSValueTypeMetaData &requested_schema) const
    {
        if (source.output() != this)
        {
            throw std::invalid_argument("TSOutput::binding_for requires a view owned by this output");
        }
        const auto *source_schema = source.schema();
        if (source_schema == nullptr)
        {
            throw std::invalid_argument("TSOutput::binding_for requires a typed output view");
        }

        if (time_series_schema_equivalent(source_schema, &requested_schema)) { return source.handle(); }

        auto &registry = TypeRegistry::instance();
        if (!time_series_schema_equivalent(registry.dereference(source_schema),
                                           registry.dereference(&requested_schema)))
        {
            throw std::invalid_argument("TSOutput alternative binding requires dereference-compatible schemas");
        }

        if (!alternatives_) { alternatives_ = std::make_unique<detail::TSOutputAlternativeStore>(); }
        return alternatives_->binding_for(source, requested_schema);
    }

    void TSOutput::release_alternative_subscriptions(DateTime release_time) const noexcept
    {
        if (alternatives_) { alternatives_->release_subscriptions(release_time); }
    }

    TSData TSOutput::checked_data_for(const TSValueTypeMetaData *schema)
    {
        if (schema == nullptr) { throw std::invalid_argument("TSOutput requires a time-series schema"); }
        if (const auto *snapshot = active_type_realization();
            snapshot != nullptr && schema->kind == TSTypeKind::TS && schema->value_schema != nullptr)
        {
            const auto realized = snapshot->type_for(schema->value_schema);
            if (realized != ValuePlanFactory::instance().type_for(schema->value_schema))
            {
                return TSData{TSDataPlanFactory::instance().output_type_for(schema, realized)};
            }
        }
        return TSData{TSDataPlanFactory::instance().output_type_for(schema)};
    }

    TSData TSOutput::checked_data_for(const TSEndpointSchema &endpoint_schema)
    {
        if (endpoint_schema.empty() || endpoint_schema.schema() == nullptr)
        {
            throw std::invalid_argument("TSOutput requires a non-empty output endpoint schema");
        }
        if (const auto *schema = endpoint_schema.schema(); endpoint_schema.is_owned())
        {
            return checked_data_for(schema);
        }
        const auto type = detail::output_data_storage_type_for(endpoint_schema);
        if (!type) { throw std::logic_error("TSOutput could not resolve output endpoint storage"); }
        return TSData{type};
    }

    const TSData &TSOutput::copyable_data(const TSOutput &other)
    {
        return other.data_;
    }

    void TSOutput::invalidate_observers() noexcept
    {
        if (!data_.has_value()) { return; }
        detail::invalidate_owned_ts_data_tree(data_.view());
    }

    void TSOutput::attach_root_parent()
    {
        if (has_value())
        {
            auto root = data_view();
            root.bind_parent(*this, TS_DATA_NO_CHILD_ID);
            detail::attach_owned_ts_data_parents(root.borrowed_ref());
        }
    }

    void TSOutput::record_child_modified(std::size_t, DateTime)
    {
        // Root outputs have no parent to notify and no eager delta state to mark;
        // delta visibility is read-gated and reclamation is lazy (next mutation).
    }

    NodeView TSOutput::owner_node() const
    {
        return has_value() ? data_view().owner_node() : NodeView{};
    }

    GraphView TSOutput::owner_graph() const
    {
        return has_value() ? data_view().owner_graph() : GraphView{};
    }

    void TSOutput::bind_node_parent(const NodeView &node, TSEndpointOwnerPort port)
    {
        if (has_value()) { data_view().bind_parent(node, port); }
    }

    void TSOutput::clear_node_parent()
    {
        attach_root_parent();
    }

    TSOutputMutationView TSOutput::begin_mutation(DateTime evaluation_time)
    {
        return TSOutputMutationView{*this, evaluation_time};
    }

}  // namespace hgraph
