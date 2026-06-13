#include <hgraph/runtime/try_except_node.h>

#include <hgraph/runtime/nested_bindings.h>
#include <hgraph/runtime/node_error.h>
#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/time_series/ts_output/bundle_view.h>

#include <exception>
#include <string>
#include <utility>

namespace hgraph
{
    namespace
    {
        // Write a NodeError to the node's exception output: the "exception" field
        // of a TSB output, or the whole output for a sink (bare TS<NodeError>).
        void write_try_except_error(const NodeView &view, DateTime evaluation_time, std::string error_msg)
        {
            const NodeTypeMetaData *schema = view.schema();
            NodeErrorFields         fields;
            fields.signature_name =
                schema != nullptr && schema->display_name != nullptr ? std::string{schema->display_name} : std::string{};
            fields.label       = std::string{view.label()};
            fields.wiring_path = fields.label.empty() ? fields.signature_name : fields.label;
            fields.error_msg   = std::move(error_msg);

            Value error_value = make_node_error_value(fields);
            auto  output      = view.output(evaluation_time);

            const bool is_bundle = schema != nullptr && schema->output_schema != nullptr &&
                                   schema->output_schema->kind == TSTypeKind::TSB;
            if (is_bundle)
            {
                auto bundle   = output.as_bundle();
                auto target   = bundle.field("exception");
                auto mutation = target.begin_mutation(evaluation_time);
                (void)mutation.copy_value_from(error_value.view());
            }
            else
            {
                auto mutation = output.begin_mutation(evaluation_time);
                (void)mutation.copy_value_from(error_value.view());
            }
        }

        // Copy the child's terminal output into the bundle's ``out`` field when it
        // ticked this cycle. The output is owned (not forwarded), so this is a
        // whole-value copy — tactical, mirroring map_'s first-cut output merge;
        // zero-copy sub-path forwarding is a recorded refinement.
        void copy_child_output(const SingleNestedGraphNodeView &nested, const NodeView &view, DateTime evaluation_time)
        {
            const auto &binding = nested.context().spec.output_binding;
            if (!binding.has_value()) { return; }

            auto source = walk_ts_path(
                nested.child_graph().node_at(binding->source.node).output(evaluation_time), binding->source.path);
            if (!source.valid() || !source.modified()) { return; }

            auto target   = walk_ts_path(view.output(evaluation_time), binding->target_path);
            auto mutation = target.begin_mutation(evaluation_time);
            (void)mutation.copy_value_from(source.value());
        }

        void try_except_start(const NodeView &view, DateTime evaluation_time)
        {
            auto nested = view.as<SingleNestedGraphNodeView>();
            nested.ensure_child_graph();
            single_nested_graph_bind_inputs(nested, evaluation_time);
            if (nested.context().options.start_child_on_start) { nested.child_graph().start(evaluation_time); }
            single_nested_graph_propagate_schedule(nested);
        }

        void try_except_evaluate(const NodeView &view, DateTime evaluation_time)
        {
            if (!view.started()) { return; }

            auto nested = view.as<SingleNestedGraphNodeView>();
            nested.ensure_child_graph();
            single_nested_graph_bind_inputs(nested, evaluation_time);
            try
            {
                nested.child_graph().evaluate(evaluation_time);
                copy_child_output(nested, view, evaluation_time);
            }
            catch (const std::exception &error)
            {
                write_try_except_error(view, evaluation_time, error.what());
            }
            catch (...)
            {
                write_try_except_error(view, evaluation_time, "unknown error");
            }
            single_nested_graph_propagate_schedule(nested);
        }

        void try_except_evaluate_impl(const void *, const NodeView &view, DateTime evaluation_time)
        {
            try_except_evaluate(view, evaluation_time);
        }
    }  // namespace

    NodeBuilder try_except_node(NodeTypeMetaData meta, SingleNestedGraphNodeSpec spec,
                                SingleNestedGraphNodeOptions options)
    {
        options.manage_output_externally = true;
        NodeTypeDescriptor descriptor =
            single_nested_graph_node_descriptor(std::move(meta), std::move(spec), options);
        descriptor.callbacks.start   = &try_except_start;
        descriptor.ops.evaluate_impl = &try_except_evaluate_impl;
        return NodeBuilder::from_descriptor(std::move(descriptor));
    }
}  // namespace hgraph
