#ifndef HGRAPH_RUNTIME_REDUCE_OUTPUT_BINDING_H
#define HGRAPH_RUNTIME_REDUCE_OUTPUT_BINDING_H

#include <hgraph/runtime/nested_bindings.h>

#include <stdexcept>
#include <utility>
#include <vector>

namespace hgraph::runtime_detail
{
    [[nodiscard]] inline TSEndpointSchema reduce_output_endpoint_schema(
        const TSValueTypeMetaData *schema)
    {
        if (schema == nullptr)
        {
            throw std::invalid_argument("reduce_ output endpoint requires a schema");
        }

        std::vector<TSEndpointSchema> children;
        if (schema->kind == TSTypeKind::TSB)
        {
            children.reserve(schema->field_count());
            for (std::size_t index = 0; index < schema->field_count(); ++index)
            {
                children.push_back(reduce_output_endpoint_schema(schema->fields()[index].type));
            }
            return TSEndpointSchema::non_peered(schema, std::move(children));
        }
        if (schema->kind == TSTypeKind::TSL && schema->fixed_size() != 0)
        {
            children.reserve(schema->fixed_size());
            for (std::size_t index = 0; index < schema->fixed_size(); ++index)
            {
                children.push_back(reduce_output_endpoint_schema(schema->element_ts()));
            }
            return TSEndpointSchema::non_peered(schema, std::move(children));
        }
        return TSEndpointSchema::peered(schema);
    }

    inline void clear_reduce_output(TSOutputView target)
    {
        if (target.forwarding())
        {
            if (target.forwarding_bound()) { target.clear_forwarding_target_sampled(); }
            return;
        }

        const auto *schema = target.schema();
        const std::size_t child_count = schema != nullptr && schema->kind == TSTypeKind::TSB
                                            ? schema->field_count()
                                            : schema != nullptr && schema->kind == TSTypeKind::TSL
                                                  ? schema->fixed_size()
                                                  : 0;
        if (child_count == 0)
        {
            throw std::logic_error("reduce_ output has a non-forwarding leaf endpoint");
        }
        for (std::size_t index = 0; index < child_count; ++index)
        {
            clear_reduce_output(target.indexed_child_at(index));
        }
    }

    inline void bind_reduce_output(TSOutputView target, const TSOutputView &source,
                                   DateTime evaluation_time)
    {
        if (target.forwarding())
        {
            TSOutputView resolved_source = resolve_forwarding_source(source.borrowed_ref());
            if (!resolved_source.bound())
            {
                if (target.forwarding_bound()) { target.clear_forwarding_target_sampled(); }
                return;
            }

            const TSOutputHandle before = target.forwarding_target();
            if (!before.same_as(resolved_source.handle()))
            {
                target.bind_forwarding_target_sampled(resolved_source);
            }
            return;
        }

        if (!source.bound())
        {
            clear_reduce_output(std::move(target));
            return;
        }
        if (!time_series_schema_equivalent(target.schema(), source.schema()))
        {
            throw std::logic_error("reduce_ output source schema does not match its forwarding endpoint");
        }

        const auto *schema = target.schema();
        const std::size_t child_count = schema != nullptr && schema->kind == TSTypeKind::TSB
                                            ? schema->field_count()
                                            : schema != nullptr && schema->kind == TSTypeKind::TSL
                                                  ? schema->fixed_size()
                                                  : 0;
        if (child_count == 0)
        {
            throw std::logic_error("reduce_ output has a non-forwarding leaf endpoint");
        }
        for (std::size_t index = 0; index < child_count; ++index)
        {
            bind_reduce_output(target.indexed_child_at(index), source.indexed_child_at(index), evaluation_time);
        }
    }
}  // namespace hgraph::runtime_detail

#endif  // HGRAPH_RUNTIME_REDUCE_OUTPUT_BINDING_H
