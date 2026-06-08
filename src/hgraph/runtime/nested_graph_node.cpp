#include <hgraph/runtime/nested_graph_node.h>

#include <hgraph/types/time_series/ts_delta.h>

#include <array>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace
    {
        constexpr std::string_view child_graph_field_name{"single_nested_graph"};

        struct SingleNestedGraphNodeStorage
        {
            GraphValue graph{};
        };

        [[nodiscard]] TSInputView input_at_path(TSInputView view, const std::vector<std::size_t> &path)
        {
            for (const std::size_t component : path)
            {
                const auto *schema = view.schema();
                if (schema == nullptr) { throw std::logic_error("Nested graph input path requires a typed input view"); }
                switch (schema->kind)
                {
                    case TSTypeKind::TSB:
                    {
                        auto bundle = view.as_bundle();
                        view = bundle[component];
                        break;
                    }
                    case TSTypeKind::TSL:
                    {
                        auto list = view.as_list();
                        view = list[component];
                        break;
                    }
                    default:
                        throw std::invalid_argument(
                            "Nested graph input path can only traverse indexed time-series structures");
                }
            }
            return view;
        }

        [[nodiscard]] TSOutputView output_at_path(TSOutputView view, const std::vector<std::size_t> &path)
        {
            for (const std::size_t component : path)
            {
                const auto *schema = view.schema();
                if (schema == nullptr) { throw std::logic_error("Nested graph output path requires a typed output view"); }
                switch (schema->kind)
                {
                    case TSTypeKind::TSB:
                    {
                        auto bundle = view.as_bundle();
                        view = bundle[component];
                        break;
                    }
                    case TSTypeKind::TSL:
                    {
                        auto list = view.as_list();
                        view = list[component];
                        break;
                    }
                    default:
                        throw std::invalid_argument(
                            "Nested graph output path can only traverse indexed time-series structures");
                }
            }
            return view;
        }

        [[nodiscard]] std::vector<std::unique_ptr<SingleNestedGraphNodeContext>> &single_nested_graph_contexts() noexcept
        {
            // Node contexts are type-level metadata referenced by interned ops tables.
            static auto *contexts = new std::vector<std::unique_ptr<SingleNestedGraphNodeContext>>;
            return *contexts;
        }

        [[nodiscard]] const SingleNestedGraphNodeContext &register_single_nested_graph_context(
            SingleNestedGraphNodeSpec spec,
            SingleNestedGraphNodeOptions options,
            std::size_t graph_storage_offset)
        {
            auto context = std::make_unique<SingleNestedGraphNodeContext>(
                SingleNestedGraphNodeContext{
                    .spec = std::move(spec),
                    .options = options,
                    .graph_storage_offset = graph_storage_offset,
                });
            const auto *result = context.get();
            single_nested_graph_contexts().push_back(std::move(context));
            return *result;
        }

        [[nodiscard]] SingleNestedGraphNodeView checked_nested_view(const NodeView &view)
        {
            return view.as<SingleNestedGraphNodeView>();
        }

        void single_nested_graph_evaluate_impl(const void *, const NodeView &view, engine_time_t evaluation_time, bool)
        {
            single_nested_graph_evaluate(view, evaluation_time);
        }
    }  // namespace

    const void *SingleNestedGraphNodeView::node_view_type_id() noexcept
    {
        static const char token{};
        return &token;
    }

    SingleNestedGraphNodeView SingleNestedGraphNodeView::from_node(NodeView view, const void *context)
    {
        if (context == nullptr) { throw std::logic_error("SingleNestedGraphNodeView requires a typed view context"); }
        const auto &typed_context = *static_cast<const SingleNestedGraphNodeContext *>(context);
        auto       *storage = MemoryUtils::cast<SingleNestedGraphNodeStorage>(
            MemoryUtils::advance(view.data(), typed_context.graph_storage_offset));
        return SingleNestedGraphNodeView{std::move(view), typed_context, storage->graph};
    }

    const NodeView &SingleNestedGraphNodeView::node() const noexcept
    {
        return view_;
    }

    const SingleNestedGraphNodeContext &SingleNestedGraphNodeView::context() const noexcept
    {
        return *context_;
    }

    GraphValue &SingleNestedGraphNodeView::child_graph_value() const noexcept
    {
        return *child_graph_;
    }

    GraphView SingleNestedGraphNodeView::child_graph() const
    {
        return child_graph_->view();
    }

    void SingleNestedGraphNodeView::ensure_child_graph() const
    {
        if (!child_graph_->has_value()) { *child_graph_ = GraphValue{context_->spec.graph_builder}; }
    }

    SingleNestedGraphNodeView::SingleNestedGraphNodeView(NodeView view,
                                                         const SingleNestedGraphNodeContext &context,
                                                         GraphValue &child_graph) noexcept
        : view_(std::move(view)),
          context_(&context),
          child_graph_(&child_graph)
    {
    }

    NodeBuilder nested_graph_boundary_source(const TSValueTypeMetaData *schema, const char *label)
    {
        if (schema == nullptr) { throw std::invalid_argument("nested_graph_boundary_source requires an output schema"); }

        NodeTypeMetaData meta;
        meta.display_name  = (label != nullptr && label[0] != '\0') ? label : "nested_graph_boundary_source";
        meta.output_schema = schema;
        meta.node_kind     = NodeKind::PullSource;
        return NodeBuilder::native(std::move(meta));
    }

    NodeTypeDescriptor single_nested_graph_node_descriptor(
        NodeTypeMetaData meta,
        SingleNestedGraphNodeSpec spec,
        SingleNestedGraphNodeOptions options)
    {
        meta.node_kind = NodeKind::Nested;

        NodeTypeDescriptor descriptor;
        descriptor.schema = std::move(meta);

        const std::array fields{NodeStorageField{
            .name = child_graph_field_name,
            .plan = &MemoryUtils::plan_for<SingleNestedGraphNodeStorage>(),
        }};
        descriptor.storage_plan = &node_storage_plan_for(descriptor.schema, fields);

        const auto &context = register_single_nested_graph_context(
            std::move(spec),
            options,
            descriptor.storage_plan->component(child_graph_field_name).offset);

        descriptor.callbacks.start = &single_nested_graph_start;
        descriptor.callbacks.stop  = &single_nested_graph_stop;
        descriptor.ops.evaluate_impl = &single_nested_graph_evaluate_impl;
        descriptor.ops.extended_view_type_id = SingleNestedGraphNodeView::node_view_type_id();
        descriptor.ops.extended_view_context = &context;

        return descriptor;
    }

    NodeBuilder single_nested_graph_node(
        NodeTypeMetaData meta,
        SingleNestedGraphNodeSpec spec,
        SingleNestedGraphNodeOptions options)
    {
        return NodeBuilder::from_descriptor(
            single_nested_graph_node_descriptor(std::move(meta), std::move(spec), options));
    }

    void single_nested_graph_start(const NodeView &view, engine_time_t evaluation_time)
    {
        auto nested = checked_nested_view(view);
        nested.ensure_child_graph();
        if (nested.context().options.start_child_on_start)
        {
            nested.child_graph().start(evaluation_time);
        }
        single_nested_graph_propagate_schedule(nested);
    }

    void single_nested_graph_stop(const NodeView &view, engine_time_t)
    {
        auto nested = checked_nested_view(view);
        if (nested.context().options.stop_child_on_stop && nested.child_graph_value().has_value())
        {
            nested.child_graph().stop();
        }
    }

    void single_nested_graph_evaluate(const NodeView &view, engine_time_t evaluation_time)
    {
        if (!view.started()) { return; }

        auto nested = checked_nested_view(view);
        nested.ensure_child_graph();

        if (nested.context().options.copy_inputs_on_evaluate)
        {
            single_nested_graph_copy_inputs(nested, evaluation_time);
        }
        nested.child_graph().evaluate(evaluation_time);
        if (nested.context().options.copy_output_on_evaluate)
        {
            single_nested_graph_copy_output(nested, evaluation_time);
        }
        single_nested_graph_propagate_schedule(nested);
    }

    void single_nested_graph_copy_inputs(const SingleNestedGraphNodeView &nested,
                                         engine_time_t evaluation_time)
    {
        const auto &bindings = nested.context().spec.input_bindings;
        if (bindings.empty()) { return; }

        auto root_input = nested.node().input(evaluation_time);
        auto child_graph = nested.child_graph();

        for (const NestedGraphInputBinding &binding : bindings)
        {
            auto source = input_at_path(root_input.borrowed_ref(), binding.source_path);
            if (!source.valid()) { continue; }

            auto target = output_at_path(
                child_graph.node_at(binding.target.node).output(evaluation_time),
                binding.target.path);
            if (!target.valid()) { apply_current_value(target, source.value()); }
            else if (source.modified()) { apply_delta(target, source.delta_value()); }
        }
    }

    void single_nested_graph_copy_output(const SingleNestedGraphNodeView &nested,
                                         engine_time_t evaluation_time)
    {
        const auto &binding = nested.context().spec.output_binding;
        if (!binding.has_value()) { return; }

        auto source = output_at_path(
            nested.child_graph().node_at(binding->source.node).output(evaluation_time),
            binding->source.path);
        if (!source.valid() || !source.modified()) { return; }

        auto target = output_at_path(nested.node().output(evaluation_time), binding->target_path);
        apply_current_value(target, source.value());
    }

    void single_nested_graph_propagate_schedule(const SingleNestedGraphNodeView &nested)
    {
        if (!nested.context().options.propagate_child_schedule || !nested.child_graph_value().has_value()) { return; }

        const engine_time_t next = nested.child_graph().next_scheduled_time();
        if (next != MAX_DT && nested.node().graph_value() != nullptr)
        {
            nested.node().graph_value()->schedule_node(nested.node().node_index(), next, true);
        }
    }
}  // namespace hgraph
