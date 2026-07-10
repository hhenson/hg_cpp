#include <hgraph/runtime/nested_bindings.h>
#include <hgraph/runtime/switch_node.h>
#include <hgraph/types/utils/stable_slot_storage.h>
#include <hgraph/util/scope.h>

#include <algorithm>
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
            // Raw graph storage must outlive the GraphValue handles that own
            // the constructed payloads placed in its two slots.
            StableSlotStorage                graph_memory{};
            std::array<GraphValue, 2>         graphs{};
            std::optional<std::size_t>        active_slot{};
            std::optional<std::size_t>        previous_slot{};
            Value                            active_key{};
            const SingleNestedGraphNodeSpec *active_spec{nullptr};

            [[nodiscard]] GraphValue *active_graph() noexcept
            {
                return active_slot.has_value() ? &graphs[*active_slot] : nullptr;
            }
        };

        struct SwitchNodeContext
        {
            SwitchNodeSpec spec{};
            std::size_t    storage_offset{0};
            MemoryUtils::StorageLayout graph_slot_layout{};
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
            MemoryUtils::StorageLayout graph_slot_layout{.size = 1, .alignment = 1};
            const auto include_layout = [&](const SingleNestedGraphNodeSpec &branch) {
                const auto layout = branch.graph_builder.nested_storage_layout();
                graph_slot_layout.size = std::max(graph_slot_layout.size, layout.size);
                graph_slot_layout.alignment = std::max(graph_slot_layout.alignment, layout.alignment);
            };
            for (const SwitchBranch &branch : spec.branches) { include_layout(branch.spec); }
            if (spec.default_branch.has_value()) { include_layout(*spec.default_branch); }

            auto context = std::make_unique<SwitchNodeContext>(SwitchNodeContext{
                .spec              = std::move(spec),
                .storage_offset    = storage_offset,
                .graph_slot_layout = graph_slot_layout,
            });
            const auto *result = context.get();
            switch_node_contexts().push_back(std::move(context));
            return *result;
        }

        void bind_branch_inputs(const NodeView &view, const SingleNestedGraphNodeSpec &spec, const GraphView &child,
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

        void bind_branch_output(const NodeView &view, const SingleNestedGraphNodeSpec &spec, const GraphView &child,
                                DateTime evaluation_time)
        {
            if (!spec.output_binding.has_value()) { return; }

            const NestedGraphOutputBinding &binding = *spec.output_binding;

            if (binding.kind != NestedGraphOutputBinding::Kind::ChildOutput)
            {
                throw std::logic_error("switch_ branches must terminate at a child output");
            }

            auto branch_terminal = walk_ts_path(child.node_at(binding.source.node).output(evaluation_time),
                                                binding.source.path);
            auto switch_output = walk_ts_path(view.output(evaluation_time), binding.target_path);

            if (switch_output.schema() != nullptr && switch_output.schema()->kind == TSTypeKind::REF &&
                branch_terminal.schema() != nullptr && branch_terminal.schema()->kind != TSTypeKind::REF)
            {
                const TimeSeriesReference reference = TimeSeriesReference::peered(branch_terminal);
                if (switch_output.valid() &&
                    switch_output.value().checked_as<TimeSeriesReference>() == reference)
                {
                    return;
                }

                Value value{reference};
                static_cast<void>(
                    switch_output.begin_mutation(evaluation_time).move_value_from(std::move(value)));
                return;
            }

            bind_forwarding_output_to_source(branch_terminal, switch_output);
        }

        void clear_branch_output(const NodeView &view, const SingleNestedGraphNodeSpec &spec,
                                 DateTime evaluation_time)
        {
            if (!spec.output_binding.has_value()) { return; }

            const NestedGraphOutputBinding &binding = *spec.output_binding;
            if (binding.kind != NestedGraphOutputBinding::Kind::ChildOutput) { return; }

            auto switch_view = view.as<SwitchNodeView>();
            auto &storage = *MemoryUtils::cast<SwitchNodeStorage>(switch_view.internal_storage());
            GraphValue *active = storage.active_graph();
            if (active == nullptr || !active->has_value()) { return; }

            auto terminal = walk_ts_path(active->view().node_at(binding.source.node).output(evaluation_time),
                                         binding.source.path);
            if (terminal.forwarding_bound()) { terminal.clear_forwarding_target(); }
        }

        void reset_switch_output(const NodeView &view, DateTime evaluation_time)
        {
            auto output = view.output(evaluation_time);
            if (!output.bound()) { return; }

            if (output.schema() != nullptr && output.schema()->kind == TSTypeKind::REF)
            {
                Value empty{TimeSeriesReference{}};
                auto mutation = output.begin_mutation(evaluation_time);
                static_cast<void>(mutation.move_value_from(std::move(empty)));
                return;
            }
            static_cast<void>(output.data_view().clear_collection(evaluation_time));
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
            GraphValue *active = storage.active_graph();
            if (active == nullptr || !active->has_value()) { return; }
            if (storage.active_spec != nullptr) { clear_branch_output(view, *storage.active_spec, evaluation_time); }
            active->view().stop();
            reset_switch_output(view, evaluation_time);
            storage.previous_slot = storage.active_slot;
            storage.active_slot.reset();
            storage.active_key  = Value{};
            storage.active_spec = nullptr;
        }

        void activate_branch(const NodeView &view, const SwitchNodeContext &context,
                             SwitchNodeStorage &storage, const SingleNestedGraphNodeSpec &spec,
                             Value key, DateTime evaluation_time)
        {
            if (storage.graph_memory.slot_capacity() == 0)
            {
                storage.graph_memory.reserve_to(2, context.graph_slot_layout.size,
                                                context.graph_slot_layout.alignment);
            }

            const std::size_t next_slot = storage.active_slot.has_value() ? 1U - *storage.active_slot : 0U;

            // This slot contains the graph retired on the previous switch. Its
            // stop phase has already completed, so it can now be destructed and
            // the same fixed memory reused for the new branch.
            if (storage.previous_slot.has_value() && *storage.previous_slot != next_slot)
            {
                throw std::logic_error("switch_ previous graph does not occupy the reusable slot");
            }
            storage.graphs[next_slot] = GraphValue{};
            storage.previous_slot.reset();
            storage.graphs[next_slot] = spec.graph_builder.make_nested_graph(
                NodeStorageRef{view.binding(), view.data()},
                storage.graph_memory.slot_data(next_slot),
                context.graph_slot_layout);

            auto next = storage.graphs[next_slot].view();
            auto construction_rollback = UnwindCleanupGuard([&] {
                storage.graphs[next_slot] = GraphValue{};
            });
            bind_branch_inputs(view, spec, next, evaluation_time);
            construction_rollback.release();

            switch_teardown(view, storage, evaluation_time);
            storage.active_slot = next_slot;
            storage.active_key  = std::move(key);
            storage.active_spec = &spec;

            bind_branch_output(view, spec, next, evaluation_time);
            next.start(evaluation_time);
        }

        // Single active child: propagate its pause directly. On resume, re-binding is
        // idempotent and the active child resumes from its own cursor.
        bool switch_evaluate(const NodeView &view, DateTime evaluation_time)
        {
            if (!view.started()) { return true; }

            auto        switch_view = view.as<SwitchNodeView>();
            const auto &context = *static_cast<const SwitchNodeContext *>(switch_view.internal_context());
            auto       &storage = *MemoryUtils::cast<SwitchNodeStorage>(switch_view.internal_storage());

            auto root_input   = view.input(evaluation_time);
            auto root_bundle  = root_input.as_bundle();
            auto key_input    = root_bundle[0];

            if (key_input.valid() && (key_input.modified() || !storage.active_slot.has_value()))
            {
                Value      key_value{key_input.value()};
                const bool same_key =
                    storage.active_slot.has_value() && storage.active_key.has_value() &&
                    key_value.equals(storage.active_key);

                if (!storage.active_slot.has_value() || context.spec.reload_on_ticked || !same_key)
                {
                    const SingleNestedGraphNodeSpec *spec = select_branch(context, key_value);
                    if (spec == nullptr)
                    {
                        throw std::runtime_error(
                            "switch_: no branch is registered for the key value (and no default branch)");
                    }

                    activate_branch(view, context, storage, *spec, std::move(key_value), evaluation_time);

                    // Sampled rebind (the sampled-runtime contract): binding an
                    // active child input to an already-valid source schedules the
                    // child through the input notification path.
                }
            }

            if (GraphValue *active = storage.active_graph(); active != nullptr && active->has_value())
            {
                // Re-bind boundaries each cycle (cheap no-op when stable; absorbs
                // upstream REF re-points). The nested graph evaluator propagates
                // its cached next scheduled time back to this node before returning.
                const SingleNestedGraphNodeSpec &spec = *storage.active_spec;
                bind_branch_inputs(view, spec, active->view(), evaluation_time);
                bind_branch_output(view, spec, active->view(), evaluation_time);
                return active->view().evaluate(evaluation_time);
            }
            return true;
        }

        bool switch_evaluate_impl(const void *, const NodeView &view, DateTime evaluation_time)
        {
            return switch_evaluate(view, evaluation_time);
        }

        void switch_node_stop(const NodeView &view, DateTime evaluation_time)
        {
            auto  switch_view = view.as<SwitchNodeView>();
            auto &storage     = *MemoryUtils::cast<SwitchNodeStorage>(switch_view.internal_storage());
            switch_teardown(view, storage, evaluation_time);
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
        return MemoryUtils::cast<SwitchNodeStorage>(storage_)->active_slot.has_value();
    }

    GraphValue &SwitchNodeView::active_graph_value() const noexcept
    {
        auto &storage = *MemoryUtils::cast<SwitchNodeStorage>(storage_);
        return storage.graphs[*storage.active_slot];
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
            meta.output_endpoint_schema = TSEndpointSchema::owned(meta.output_schema);
        }

        NodeTypeDescriptor descriptor;
        descriptor.schema = std::move(meta);

        const std::array fields{NodeStorageField{
            .name = switch_storage_field_name,
            .plan = &MemoryUtils::plan_for<SwitchNodeStorage>(),
        }};
        descriptor.storage_plan = &node_storage_plan_for(descriptor.schema, fields);

        descriptor.callbacks.stop            = &switch_node_stop;
        descriptor.ops.evaluate_impl         = &switch_evaluate_impl;
        descriptor.ops.extended_view_type_id = SwitchNodeView::node_view_type_id();
        descriptor.ops.extended_view_context = &register_switch_node_context(
            std::move(spec), descriptor.storage_plan->component(switch_storage_field_name).offset);

        return NodeBuilder::from_descriptor(std::move(descriptor));
    }
}  // namespace hgraph
