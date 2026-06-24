#include <hgraph/runtime/nested_bindings.h>
#include <hgraph/runtime/switch_node.h>

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
        constexpr std::string_view switch_storage_field_name{"switch"};

        struct SwitchNodeStorage
        {
            GraphValue                       active{};
            std::vector<GraphValue>          retired{};
            Value                            active_key{};
            const SingleNestedGraphNodeSpec *active_spec{nullptr};
        };

        struct SwitchNodeContext
        {
            SwitchNodeSpec spec{};
            std::size_t    storage_offset{0};
        };

        // Program-lifetime, intentionally-leaked context storage — same rationale
        // as single_nested_graph_contexts (see nested_graph_node.cpp): contexts
        // own GraphBuilders referencing interned registries, and a static
        // container's destruction order against those registries is undefined.
        [[nodiscard]] std::vector<std::unique_ptr<SwitchNodeContext>> &switch_node_contexts() noexcept
        {
            static auto *contexts = new std::vector<std::unique_ptr<SwitchNodeContext>>;
            return *contexts;
        }

        [[nodiscard]] const SwitchNodeContext &register_switch_node_context(SwitchNodeSpec spec,
                                                                            std::size_t storage_offset)
        {
            auto context = std::make_unique<SwitchNodeContext>(SwitchNodeContext{
                .spec           = std::move(spec),
                .storage_offset = storage_offset,
            });
            const auto *result = context.get();
            switch_node_contexts().push_back(std::move(context));
            return *result;
        }

        void bind_branch_inputs(const NodeView &view, const SingleNestedGraphNodeSpec &spec, GraphView child,
                                DateTime evaluation_time)
        {
            if (spec.input_bindings.empty()) { return; }
            auto root_input = view.input(evaluation_time);
            for (const NestedGraphInputBinding &binding : spec.input_bindings)
            {
                auto source = walk_source_to_output(root_input.borrowed_ref(), binding.source_path);
                auto target = walk_ts_path(child.node_at(binding.target.node).input(evaluation_time),
                                           binding.target.path);
                bind_input_to_source(std::move(target), source);
            }
        }

        void clear_collection_output_if_valid(const TSOutputView &output, DateTime evaluation_time)
        {
            TSOutputView target = resolve_forwarding_source(output.borrowed_ref());
            if (!target.bound() || target.forwarding() || !target.valid()) { return; }

            const auto *schema = target.schema();
            if (schema == nullptr) { return; }

            switch (schema->kind)
            {
                case TSTypeKind::TSD:
                {
                    auto dict     = target.as_dict();
                    auto mutation = dict.begin_mutation(evaluation_time);
                    mutation.clear();
                    break;
                }
                case TSTypeKind::TSS:
                {
                    auto set      = target.as_set();
                    auto mutation = set.begin_mutation(evaluation_time);
                    mutation.clear();
                    break;
                }
                default:
                    break;
            }
        }

        void copy_source_to_switch_output(const NodeView &view, const NestedGraphOutputBinding &binding,
                                          const TSOutputView &source, DateTime evaluation_time)
        {
            auto output = walk_ts_path(view.output(evaluation_time), binding.target_path);
            if (!source.bound() || !source.valid())
            {
                clear_collection_output_if_valid(output, evaluation_time);
                return;
            }

            auto mutation = output.begin_mutation(evaluation_time);
            (void)mutation.copy_value_from(source.value());
        }

        void bind_branch_output(const NodeView &view, const SingleNestedGraphNodeSpec &spec, GraphView child,
                                DateTime evaluation_time)
        {
            if (!spec.output_binding.has_value()) { return; }

            const NestedGraphOutputBinding &binding = *spec.output_binding;
            auto switch_output = walk_ts_path(view.output(evaluation_time), binding.target_path);

            if (binding.kind == NestedGraphOutputBinding::Kind::ParentInput)
            {
                auto root_input = view.input(evaluation_time);
                auto source     = walk_source_to_output(root_input.borrowed_ref(), binding.parent_source_path);
                copy_source_to_switch_output(view, binding, source, evaluation_time);
                return;
            }

            auto branch_terminal = walk_ts_path(child.node_at(binding.source.node).output(evaluation_time),
                                                binding.source.path);
            bind_forwarding_output_to_source(branch_terminal, switch_output);
        }

        void clear_branch_output(const NodeView &view, const SingleNestedGraphNodeSpec &spec,
                                 DateTime evaluation_time)
        {
            if (!spec.output_binding.has_value()) { return; }

            const NestedGraphOutputBinding &binding = *spec.output_binding;
            if (binding.kind == NestedGraphOutputBinding::Kind::ChildOutput)
            {
                auto switch_view = view.as<SwitchNodeView>();
                auto &storage = *MemoryUtils::cast<SwitchNodeStorage>(switch_view.internal_storage());
                if (storage.active.has_value())
                {
                    auto branch_terminal = walk_ts_path(
                        storage.active.view().node_at(binding.source.node).output(evaluation_time),
                        binding.source.path);
                    if (branch_terminal.forwarding_bound()) { branch_terminal.clear_forwarding_target(); }
                }
            }

            auto output = walk_ts_path(view.output(evaluation_time), binding.target_path);
            clear_collection_output_if_valid(output, evaluation_time);
        }

        [[nodiscard]] const SingleNestedGraphNodeSpec *select_branch(const SwitchNodeContext &context,
                                                                     const Value &key)
        {
            for (const SwitchBranch &branch : context.spec.branches)
            {
                if (branch.key.equals(key)) { return &branch.spec; }
            }
            if (context.spec.default_branch.has_value()) { return &*context.spec.default_branch; }
            return nullptr;
        }

        void switch_teardown(const NodeView &view, SwitchNodeStorage &storage, DateTime evaluation_time)
        {
            if (!storage.active.has_value()) { return; }
            if (storage.active_spec != nullptr) { clear_branch_output(view, *storage.active_spec, evaluation_time); }
            storage.active.view().stop();
            storage.retired.push_back(std::move(storage.active));
            storage.active      = GraphValue{};
            storage.active_key  = Value{};
            storage.active_spec = nullptr;
        }

        // Single active child: propagate its pause directly. On resume, re-binding is
        // idempotent and the active child resumes from its own cursor.
        bool switch_evaluate(const NodeView &view, DateTime evaluation_time)
        {
            if (!view.started()) { return true; }

            auto        switch_view = view.as<SwitchNodeView>();
            const auto &context = *static_cast<const SwitchNodeContext *>(switch_view.internal_context());
            auto       &storage = *MemoryUtils::cast<SwitchNodeStorage>(switch_view.internal_storage());

            storage.retired.clear();

            auto root_input   = view.input(evaluation_time);
            auto root_bundle  = root_input.as_bundle();
            auto key_input    = root_bundle[0];

            if (key_input.valid() && (key_input.modified() || !storage.active.has_value()))
            {
                Value      key_value{key_input.value()};
                const bool same_key =
                    storage.active.has_value() && storage.active_key.has_value() &&
                    key_value.equals(storage.active_key);

                if (!storage.active.has_value() || context.spec.reload_on_ticked || !same_key)
                {
                    switch_teardown(view, storage, evaluation_time);

                    const SingleNestedGraphNodeSpec *spec = select_branch(context, key_value);
                    if (spec == nullptr)
                    {
                        throw std::runtime_error(
                            "switch_: no branch is registered for the key value (and no default branch)");
                    }

                    storage.active = spec->graph_builder.make_nested_graph(
                        NodeStorageRef{view.binding(), view.data()});
                    storage.active_key  = std::move(key_value);
                    storage.active_spec = spec;

                    bind_branch_inputs(view, *spec, storage.active.view(), evaluation_time);
                    bind_branch_output(view, *spec, storage.active.view(), evaluation_time);
                    storage.active.view().start(evaluation_time);

                    // Sampled rebind (the sampled-runtime contract): binding an
                    // active child input to an already-valid source schedules the
                    // child through the input notification path.
                }
            }

            if (storage.active.has_value())
            {
                // Re-bind boundaries each cycle (cheap no-op when stable; absorbs
                // upstream REF re-points). The nested graph evaluator propagates
                // its cached next scheduled time back to this node before returning.
                const SingleNestedGraphNodeSpec &spec = *storage.active_spec;
                bind_branch_inputs(view, spec, storage.active.view(), evaluation_time);
                bind_branch_output(view, spec, storage.active.view(), evaluation_time);
                return storage.active.view().evaluate(evaluation_time);
            }
            return true;
        }

        bool switch_evaluate_impl(const void *, const NodeView &view, DateTime evaluation_time)
        {
            return switch_evaluate(view, evaluation_time);
        }

        void switch_node_stop(const NodeView &view, DateTime)
        {
            auto  switch_view = view.as<SwitchNodeView>();
            auto &storage     = *MemoryUtils::cast<SwitchNodeStorage>(switch_view.internal_storage());
            // The active branch terminal's forwarding link is intentionally
            // left intact: the child lives in this node's storage, so the last
            // value it wrote into the switch output remains observable after a
            // run. Branch swaps clear the retired terminal link explicitly.
            if (storage.active.has_value()) { storage.active.view().stop(); }
            storage.retired.clear();
        }
    }  // namespace

    const void *SwitchNodeView::node_view_type_id() noexcept
    {
        static const char token{};
        return &token;
    }

    SwitchNodeView SwitchNodeView::from_node(NodeView view, const void *context)
    {
        if (context == nullptr) { throw std::logic_error("SwitchNodeView requires a typed view context"); }
        const auto &typed_context = *static_cast<const SwitchNodeContext *>(context);
        void       *storage = MemoryUtils::advance(view.data(), typed_context.storage_offset);
        return SwitchNodeView{std::move(view), context, storage};
    }

    const NodeView &SwitchNodeView::node() const noexcept { return view_; }

    bool SwitchNodeView::has_active_branch() const noexcept
    {
        return MemoryUtils::cast<SwitchNodeStorage>(storage_)->active.has_value();
    }

    GraphValue &SwitchNodeView::active_graph_value() const noexcept
    {
        return MemoryUtils::cast<SwitchNodeStorage>(storage_)->active;
    }

    const Value &SwitchNodeView::active_key() const noexcept
    {
        return MemoryUtils::cast<SwitchNodeStorage>(storage_)->active_key;
    }

    SwitchNodeView::SwitchNodeView(NodeView view, const void *context, void *storage) noexcept
        : view_(std::move(view)),
          context_(context),
          storage_(storage)
    {
    }

    NodeBuilder switch_node(NodeTypeMetaData meta, SwitchNodeSpec spec)
    {
        if (spec.branches.empty() && !spec.default_branch.has_value())
        {
            throw std::invalid_argument("switch_node requires at least one branch");
        }

        const auto validate_branch = [&](const SingleNestedGraphNodeSpec &branch) {
            if (branch.output_binding.has_value() != (meta.output_schema != nullptr))
            {
                throw std::invalid_argument(
                    "switch_node branches must all produce an output exactly when the node has an output schema");
            }
            if (branch.output_binding.has_value() && !branch.output_binding->target_path.empty())
            {
                throw std::invalid_argument("switch_node currently supports forwarding only at the output root");
            }
        };
        for (const SwitchBranch &branch : spec.branches) { validate_branch(branch.spec); }
        if (spec.default_branch.has_value()) { validate_branch(*spec.default_branch); }

        meta.node_kind = NodeKind::Nested;

        NodeTypeDescriptor descriptor;
        descriptor.schema = std::move(meta);

        const std::array fields{NodeStorageField{
            .name = switch_storage_field_name,
            .plan = &MemoryUtils::plan_for<SwitchNodeStorage>(),
        }};
        descriptor.storage_plan = &node_storage_plan_for(descriptor.schema, {}, fields);

        const auto &context = register_switch_node_context(
            std::move(spec), descriptor.storage_plan->component(switch_storage_field_name).offset);

        descriptor.callbacks.stop            = &switch_node_stop;
        descriptor.ops.evaluate_impl         = &switch_evaluate_impl;
        descriptor.ops.extended_view_type_id = SwitchNodeView::node_view_type_id();
        descriptor.ops.extended_view_context = &context;

        return NodeBuilder::from_descriptor(std::move(descriptor));
    }
}  // namespace hgraph
