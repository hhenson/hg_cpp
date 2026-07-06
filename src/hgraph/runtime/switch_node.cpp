#include <hgraph/runtime/nested_bindings.h>
#include <hgraph/runtime/switch_node.h>

#include <array>
#include <memory>
#include <optional>
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
            // The switch output may be re-homed by an enclosing map_/mesh_.
            // This backing output is used only when the switch owns its target.
            std::optional<TSOutput>          backing_output{};
            // REF-shaped switch with VALUE branches (mixed-branch mode):
            // value terminals forward into this value-schema'd backing and
            // the switch output publishes a peered reference to it.
            std::optional<TSOutput>          value_backing{};
            GraphValue                       active{};
            std::vector<GraphValue>          retired{};
            Value                            active_key{};
            const SingleNestedGraphNodeSpec *active_spec{nullptr};
            // Last forwarding target installed by switch_ itself; any other
            // current target is externally owned and must be preserved.
            TSOutputHandle                   owned_output_target{};
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

        void ensure_backing_output(SwitchNodeStorage &storage, const TSValueTypeMetaData *schema)
        {
            if (schema == nullptr) { return; }
            if (!storage.backing_output.has_value()) { storage.backing_output.emplace(*schema); }
        }

        [[nodiscard]] TSOutputView backing_output_view(SwitchNodeStorage &storage,
                                                       const NodeView &view,
                                                       DateTime evaluation_time)
        {
            const NodeTypeMetaData *schema = view.schema();
            ensure_backing_output(storage, schema != nullptr ? schema->output_schema : nullptr);
            return storage.backing_output.has_value()
                       ? storage.backing_output->view(evaluation_time)
                       : TSOutputView{};
        }

        void clear_collection_output_if_valid(const TSOutputView &output, DateTime evaluation_time)
        {
            if (!output.bound() || output.forwarding() || !output.valid()) { return; }

            static_cast<void>(output.data_view().clear_collection(evaluation_time));
        }

        [[nodiscard]] TSOutputView switch_output_forwarding_view(const NodeView &view,
                                                                 const NestedGraphOutputBinding &binding,
                                                                 DateTime evaluation_time)
        {
            auto output = walk_forwarding_target_path(view.output(evaluation_time), binding.target_path);
            if (!output.forwarding())
            {
                throw std::logic_error("switch_ output must be a forwarding endpoint");
            }
            return output;
        }

        void bind_switch_output_to_source(const NodeView &view, SwitchNodeStorage &storage,
                                          const NestedGraphOutputBinding &binding,
                                          const TSOutputView &source, DateTime evaluation_time)
        {
            auto output = switch_output_forwarding_view(view, binding, evaluation_time);
            bind_forwarding_output_to_source(output, source);
            storage.owned_output_target = output.forwarding_target();
        }

        void bind_switch_output_to_backing(const NodeView &view, SwitchNodeStorage &storage,
                                           const NestedGraphOutputBinding &binding,
                                           DateTime evaluation_time)
        {
            auto output = switch_output_forwarding_view(view, binding, evaluation_time);
            auto current = output.forwarding_target();
            if (current.bound() && !current.same_as(storage.owned_output_target))
            {
                // An enclosing map_/mesh_ already bound this switch terminal to
                // its element; branch terminals should resolve through it.
                storage.owned_output_target.reset();
                return;
            }

            auto backing = backing_output_view(storage, view, evaluation_time);
            if (!backing.bound()) { return; }
            bind_forwarding_output_to_source(output, backing);
            storage.owned_output_target = output.forwarding_target();
        }

        void bind_branch_output(const NodeView &view, const SingleNestedGraphNodeSpec &spec, GraphView child,
                                DateTime evaluation_time)
        {
            if (!spec.output_binding.has_value()) { return; }

            auto &storage = *MemoryUtils::cast<SwitchNodeStorage>(
                view.as<SwitchNodeView>().internal_storage());

            const NestedGraphOutputBinding &binding = *spec.output_binding;

            if (binding.kind == NestedGraphOutputBinding::Kind::ParentInput)
            {
                auto root_input = view.input(evaluation_time);
                auto source     = walk_source_to_output(root_input.borrowed_ref(), binding.parent_source_path);
                bind_switch_output_to_source(view, storage, binding, source, evaluation_time);
                return;
            }

            bind_switch_output_to_backing(view, storage, binding, evaluation_time);
            auto switch_output = walk_ts_path(view.output(evaluation_time), binding.target_path);

            auto branch_terminal = walk_ts_path(child.node_at(binding.source.node).output(evaluation_time),
                                                binding.source.path);
            // A VALUE branch feeding a REFERENCE-shaped switch output (the
            // branches differ only in REF-ness) publishes a peered REFERENCE
            // to its terminal instead of forwarding - consumers dereference
            // through the sampled-rebind contract.
            const auto *out_schema      = switch_output.schema();
            const auto *terminal_schema = branch_terminal.schema();
            if (out_schema != nullptr && out_schema->kind == TSTypeKind::REF && terminal_schema != nullptr &&
                terminal_schema->kind != TSTypeKind::REF)
            {
                // A VALUE branch under a REFERENCE-shaped switch output: the
                // terminal is a forwarding output BY CONSTRUCTION, so it
                // forwards into the switch's BACKING output (value-schema'd),
                // and the switch output publishes a peered reference TO the
                // backing. Branch swaps re-point the forwarding; the
                // reference (and consumers' bindings) stay stable.
                if (!storage.value_backing.has_value()) { storage.value_backing.emplace(*terminal_schema); }
                auto backing = storage.value_backing->view(evaluation_time);
                bind_forwarding_output_to_source(branch_terminal, backing);

                Value reference{TimeSeriesReference::peered(backing)};
                if (switch_output.data_view().has_current_value() &&
                    switch_output.data_view().value().checked_as<TimeSeriesReference>() ==
                        reference.view().checked_as<TimeSeriesReference>())
                {
                    return;   // same-reference dedup
                }
                auto mutation = switch_output.begin_mutation(evaluation_time);
                static_cast<void>(mutation.move_value_from(std::move(reference)));
                return;
            }
            bind_forwarding_output_to_source(branch_terminal, switch_output);
        }

        void clear_branch_output(const NodeView &view, const SingleNestedGraphNodeSpec &spec,
                                 DateTime evaluation_time)
        {
            if (!spec.output_binding.has_value()) { return; }

            const NestedGraphOutputBinding &binding = *spec.output_binding;
            auto switch_view = view.as<SwitchNodeView>();
            auto &storage = *MemoryUtils::cast<SwitchNodeStorage>(switch_view.internal_storage());

            if (binding.kind == NestedGraphOutputBinding::Kind::ChildOutput)
            {
                if (storage.active.has_value())
                {
                    auto branch_terminal = walk_ts_path(
                        storage.active.view().node_at(binding.source.node).output(evaluation_time),
                        binding.source.path);
                    if (branch_terminal.forwarding_bound()) { branch_terminal.clear_forwarding_target(); }
                }
            }

            if (storage.backing_output.has_value())
            {
                clear_collection_output_if_valid(storage.backing_output->view(evaluation_time),
                                                 evaluation_time);
            }
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
            // Mixed-branch REF mode: the branch is GONE, so consumers must
            // observe the reset - publish the EMPTY reference (the
            // synchronous notify unbinds from-REF consumers from the value
            // backing) and destroy the stale backing; the next branch bind
            // recreates it invalid.
            if (storage.value_backing.has_value() && storage.active_spec != nullptr &&
                storage.active_spec->output_binding.has_value())
            {
                auto switch_output = walk_ts_path(view.output(evaluation_time),
                                                  storage.active_spec->output_binding->target_path);
                if (switch_output.schema() != nullptr && switch_output.schema()->kind == TSTypeKind::REF)
                {
                    Value empty{TimeSeriesReference{}};
                    auto  mutation = switch_output.begin_mutation(evaluation_time);
                    static_cast<void>(mutation.move_value_from(std::move(empty)));
                }
                storage.value_backing.reset();
            }
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
        if (meta.output_schema != nullptr)
        {
            meta.output_endpoint_schema = TSEndpointSchema::peered(meta.output_schema);
        }

        NodeTypeDescriptor descriptor;
        descriptor.schema = std::move(meta);

        const std::array fields{NodeStorageField{
            .name = switch_storage_field_name,
            .plan = &MemoryUtils::plan_for<SwitchNodeStorage>(),
        }};
        // The node output target-link may point into SwitchNodeStorage::backing_output,
        // so the output must be destroyed before this storage field.
        descriptor.storage_plan = &node_storage_plan_for(descriptor.schema, fields);

        const auto &context = register_switch_node_context(
            std::move(spec), descriptor.storage_plan->component(switch_storage_field_name).offset);

        descriptor.callbacks.stop            = &switch_node_stop;
        descriptor.ops.evaluate_impl         = &switch_evaluate_impl;
        descriptor.ops.extended_view_type_id = SwitchNodeView::node_view_type_id();
        descriptor.ops.extended_view_context = &context;

        return NodeBuilder::from_descriptor(std::move(descriptor));
    }
}  // namespace hgraph
