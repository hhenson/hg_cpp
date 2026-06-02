#include <hgraph/runtime/node.h>

#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/runtime/graph.h>
#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <cassert>
#include <deque>
#include <stdexcept>
#include <utility>

namespace hgraph
{
    namespace
    {
        void schedule_node_from_storage(GraphValue *graph, std::size_t node_index, engine_time_t modified_time);

        struct NodeRuntimeStorage final : Notifiable
        {
            NodeRuntimeStorage(const NodeTypeMetaData &schema, std::string runtime_label)
                : label(std::move(runtime_label))
            {
                if (label.empty() && schema.display_name != nullptr) { label = schema.display_name; }
            }

            void notify(engine_time_t modified_time) override
            {
                schedule_node_from_storage(graph, node_index, modified_time);
            }

            GraphValue   *graph{nullptr};
            std::size_t   node_index{0};
            bool          started{false};
            bool          starting{false};
            std::string   label{};
        };

        void schedule_node_from_storage(GraphValue *graph, std::size_t node_index, engine_time_t modified_time)
        {
            if (graph == nullptr) { return; }
            const engine_time_t when =
                modified_time != MIN_DT ? std::max(modified_time, graph->view().evaluation_time()) : graph->view().evaluation_time();
            graph->schedule_node(node_index, when);
        }

        struct NodeRuntimeLayout
        {
            static constexpr std::size_t npos = static_cast<std::size_t>(-1);

            std::size_t storage_offset{0};
            std::size_t input_offset{npos};
            std::size_t output_offset{npos};
            std::size_t state_offset{npos};
            std::size_t scalars_offset{npos};
            std::size_t scheduler_offset{npos};
            std::size_t error_output_offset{npos};
            std::size_t recordable_state_offset{npos};

            [[nodiscard]] bool has_input() const noexcept { return input_offset != npos; }
            [[nodiscard]] bool has_output() const noexcept { return output_offset != npos; }
            [[nodiscard]] bool has_state() const noexcept { return state_offset != npos; }
            [[nodiscard]] bool has_scalars() const noexcept { return scalars_offset != npos; }
            [[nodiscard]] bool has_scheduler() const noexcept { return scheduler_offset != npos; }
            [[nodiscard]] bool has_error_output() const noexcept { return error_output_offset != npos; }
            [[nodiscard]] bool has_recordable_state() const noexcept { return recordable_state_offset != npos; }
        };

        struct NodeRuntimeContext
        {
            NodeCallbacks                   callbacks{};
            NodeRuntimeLayout               layout{};
            const MemoryUtils::StoragePlan *plan{nullptr};
        };

        [[nodiscard]] const NodeRuntimeContext &runtime_context(const void *context)
        {
            if (context == nullptr) { throw std::logic_error("Node runtime context is null"); }
            return *static_cast<const NodeRuntimeContext *>(context);
        }

        [[nodiscard]] void *node_component(void *memory, std::size_t offset)
        {
            if (memory == nullptr) { throw std::logic_error("Node storage is null"); }
            if (offset == NodeRuntimeLayout::npos) { throw std::logic_error("Node component is not present"); }
            return MemoryUtils::advance(memory, offset);
        }

        [[nodiscard]] const void *node_component(const void *memory, std::size_t offset)
        {
            if (memory == nullptr) { throw std::logic_error("Node storage is null"); }
            if (offset == NodeRuntimeLayout::npos) { throw std::logic_error("Node component is not present"); }
            return MemoryUtils::advance(memory, offset);
        }

        [[nodiscard]] NodeRuntimeStorage &node_storage(const NodeRuntimeContext &context, void *memory)
        {
            return *MemoryUtils::cast<NodeRuntimeStorage>(node_component(memory, context.layout.storage_offset));
        }

        [[nodiscard]] const NodeRuntimeStorage &node_storage(const NodeRuntimeContext &context, const void *memory)
        {
            return *MemoryUtils::cast<NodeRuntimeStorage>(node_component(memory, context.layout.storage_offset));
        }

        [[nodiscard]] TSInput &node_input(const NodeRuntimeContext &context, void *memory)
        {
            return *MemoryUtils::cast<TSInput>(node_component(memory, context.layout.input_offset));
        }

        [[nodiscard]] TSOutput &node_output(const NodeRuntimeContext &context, void *memory)
        {
            return *MemoryUtils::cast<TSOutput>(node_component(memory, context.layout.output_offset));
        }

        [[nodiscard]] Value &node_state(const NodeRuntimeContext &context, void *memory)
        {
            return *MemoryUtils::cast<Value>(node_component(memory, context.layout.state_offset));
        }

        [[nodiscard]] Value &node_scalars(const NodeRuntimeContext &context, void *memory)
        {
            return *MemoryUtils::cast<Value>(node_component(memory, context.layout.scalars_offset));
        }

        [[nodiscard]] NodeSchedulerState &node_scheduler_state(const NodeRuntimeContext &context, void *memory)
        {
            return *MemoryUtils::cast<NodeSchedulerState>(node_component(memory, context.layout.scheduler_offset));
        }

        [[nodiscard]] TSOutput &node_error_output(const NodeRuntimeContext &context, void *memory)
        {
            return *MemoryUtils::cast<TSOutput>(node_component(memory, context.layout.error_output_offset));
        }

        [[nodiscard]] TSOutput &node_recordable_state(const NodeRuntimeContext &context, void *memory)
        {
            return *MemoryUtils::cast<TSOutput>(node_component(memory, context.layout.recordable_state_offset));
        }

        [[nodiscard]] const NodeCallbacks &callbacks(const void *context)
        {
            return runtime_context(context).callbacks;
        }

        [[nodiscard]] TSEndpointSchema default_input_endpoint(const TSValueTypeMetaData &schema)
        {
            if (schema.kind != TSTypeKind::TSB)
            {
                return TSEndpointSchema::peered(&schema);
            }

            std::vector<TSEndpointSchema> children;
            children.reserve(schema.field_count());
            for (std::size_t index = 0; index < schema.field_count(); ++index)
            {
                children.push_back(TSEndpointSchema::peered(schema.fields()[index].type));
            }
            return TSEndpointSchema::non_peered(&schema, std::move(children));
        }

        [[nodiscard]] const TSInputBuilder &input_builder_for(const TSValueTypeMetaData &schema,
                                                              TSEndpointSchema endpoint)
        {
            if (endpoint.empty()) { endpoint = default_input_endpoint(schema); }
            return TSInputBuilderFactory::checked_builder_for(schema, endpoint);
        }

        [[nodiscard]] const ValueTypeBinding &state_binding_for(const ValueTypeMetaData *schema)
        {
            if (schema == nullptr) { throw std::logic_error("Node state schema is null"); }
            const auto *binding = ValuePlanFactory::instance().binding_for(schema);
            if (binding == nullptr)
            {
                throw std::logic_error("Node state schema has no value binding");
            }
            return *binding;
        }

        void destroy_constructed_prefix(const MemoryUtils::StoragePlan &plan, void *memory, std::size_t constructed) noexcept
        {
            const auto components = plan.components();
            for (std::size_t index = constructed; index > 0; --index)
            {
                const auto &component = components[index - 1];
                component.plan->destroy(MemoryUtils::advance(memory, component.offset));
            }
        }

        void construct_node_storage(const NodeRuntimeContext &context,
                                    const NodeTypeMetaData   &schema,
                                    TSEndpointSchema          input_endpoint,
                                    std::string               runtime_label,
                                    const Value              &scalars,
                                    void                     *memory)
        {
            if (context.plan == nullptr) { throw std::logic_error("Node runtime context has no storage plan"); }

            std::size_t constructed = 0;
            auto rollback = make_scope_exit([&]() noexcept {
                destroy_constructed_prefix(*context.plan, memory, constructed);
            });

            std::construct_at(MemoryUtils::cast<NodeRuntimeStorage>(
                                  MemoryUtils::advance(memory, context.layout.storage_offset)),
                              schema, std::move(runtime_label));
            ++constructed;

            if (context.layout.has_input())
            {
                const auto &builder = input_builder_for(*schema.input_schema, std::move(input_endpoint));
                std::construct_at(MemoryUtils::cast<TSInput>(
                                      MemoryUtils::advance(memory, context.layout.input_offset)),
                                  builder);
                ++constructed;
            }

            if (context.layout.has_output())
            {
                std::construct_at(MemoryUtils::cast<TSOutput>(
                                      MemoryUtils::advance(memory, context.layout.output_offset)),
                                  *schema.output_schema);
                ++constructed;
            }

            if (context.layout.has_state())
            {
                std::construct_at(MemoryUtils::cast<Value>(
                                      MemoryUtils::advance(memory, context.layout.state_offset)),
                                  state_binding_for(schema.state_schema));
                ++constructed;
            }

            if (context.layout.has_scalars())
            {
                if (!scalars.has_value())
                {
                    throw std::logic_error("Node has a scalar schema but no scalar configuration value was provided");
                }
                std::construct_at(MemoryUtils::cast<Value>(
                                      MemoryUtils::advance(memory, context.layout.scalars_offset)),
                                  scalars);   // copy the per-instance scalar configuration
                ++constructed;
            }

            if (context.layout.has_scheduler())
            {
                std::construct_at(MemoryUtils::cast<NodeSchedulerState>(
                                      MemoryUtils::advance(memory, context.layout.scheduler_offset)));
                ++constructed;
            }

            if (context.layout.has_error_output())
            {
                std::construct_at(MemoryUtils::cast<TSOutput>(
                                      MemoryUtils::advance(memory, context.layout.error_output_offset)),
                                  *schema.error_output_schema);
                ++constructed;
            }

            if (context.layout.has_recordable_state())
            {
                std::construct_at(MemoryUtils::cast<TSOutput>(
                                      MemoryUtils::advance(memory, context.layout.recordable_state_offset)),
                                  *schema.recordable_state_schema);
                ++constructed;
            }

            rollback.release();
        }

        [[nodiscard]] NodeRuntimeLayout layout_for(const MemoryUtils::StoragePlan &plan)
        {
            NodeRuntimeLayout layout;

            const auto &storage_component = plan.component("runtime_storage");
            layout.storage_offset = storage_component.offset;

            if (const auto *component = plan.find_component("input"); component != nullptr)
            {
                layout.input_offset = component->offset;
            }
            if (const auto *component = plan.find_component("output"); component != nullptr)
            {
                layout.output_offset = component->offset;
            }
            if (const auto *component = plan.find_component("state"); component != nullptr)
            {
                layout.state_offset = component->offset;
            }
            if (const auto *component = plan.find_component("scalars"); component != nullptr)
            {
                layout.scalars_offset = component->offset;
            }
            if (const auto *component = plan.find_component("scheduler"); component != nullptr)
            {
                layout.scheduler_offset = component->offset;
            }
            if (const auto *component = plan.find_component("error_output"); component != nullptr)
            {
                layout.error_output_offset = component->offset;
            }
            if (const auto *component = plan.find_component("recordable_state"); component != nullptr)
            {
                layout.recordable_state_offset = component->offset;
            }

            return layout;
        }

        [[nodiscard]] const MemoryUtils::StoragePlan &node_storage_plan_for(const NodeTypeMetaData &schema)
        {
            auto builder = MemoryUtils::named_tuple();
            builder.add_field("runtime_storage", MemoryUtils::plan_for<NodeRuntimeStorage>());
            if (schema.input_schema != nullptr) { builder.add_field("input", MemoryUtils::plan_for<TSInput>()); }
            if (schema.output_schema != nullptr) { builder.add_field("output", MemoryUtils::plan_for<TSOutput>()); }
            if (schema.state_schema != nullptr) { builder.add_field("state", MemoryUtils::plan_for<Value>()); }
            if (schema.scalar_schema != nullptr) { builder.add_field("scalars", MemoryUtils::plan_for<Value>()); }
            if (schema.uses_scheduler)
            {
                builder.add_field("scheduler", MemoryUtils::plan_for<NodeSchedulerState>());
            }
            if (schema.error_output_schema != nullptr)
            {
                builder.add_field("error_output", MemoryUtils::plan_for<TSOutput>());
            }
            if (schema.recordable_state_schema != nullptr)
            {
                builder.add_field("recordable_state", MemoryUtils::plan_for<TSOutput>());
            }
            return builder.build();
        }

        void activate_input_slots(const NodeView &view, engine_time_t evaluation_time)
        {
            if (!view.has_input()) { return; }

            const auto *schema = view.schema()->input_schema;
            auto        input  = view.input(evaluation_time);
            if (schema == nullptr || schema->kind != TSTypeKind::TSB)
            {
                input.make_active();
                return;
            }

            auto bundle = input.as_bundle();
            const auto &slots = view.schema()->active_inputs;
            if (slots.empty())
            {
                for (std::size_t slot = 0; slot < schema->field_count(); ++slot) { bundle[slot].make_active(); }
                return;
            }

            for (const std::size_t slot : slots)
            {
                if (slot >= schema->field_count()) { throw std::out_of_range("Node active input selector is out of range"); }
                bundle[slot].make_active();
            }
        }

        void deactivate_input_slots(const NodeView &view, engine_time_t evaluation_time)
        {
            if (!view.has_input()) { return; }

            const auto *schema = view.schema()->input_schema;
            auto        input  = view.input(evaluation_time);
            if (schema == nullptr || schema->kind != TSTypeKind::TSB)
            {
                input.make_passive();
                return;
            }

            auto bundle = input.as_bundle();
            const auto &slots = view.schema()->active_inputs;
            if (slots.empty())
            {
                for (std::size_t slot = 0; slot < schema->field_count(); ++slot) { bundle[slot].make_passive(); }
                return;
            }

            for (const std::size_t slot : slots)
            {
                if (slot >= schema->field_count()) { throw std::out_of_range("Node active input selector is out of range"); }
                bundle[slot].make_passive();
            }
        }

        [[nodiscard]] TSInputView input_slot(const NodeView &view, std::size_t slot, engine_time_t evaluation_time)
        {
            const auto *schema = view.schema()->input_schema;
            if (schema == nullptr || schema->kind != TSTypeKind::TSB)
            {
                throw std::logic_error("Node input selectors require a TSB root input schema");
            }
            if (slot >= schema->field_count()) { throw std::out_of_range("Node input selector is out of range"); }
            auto root = view.input(evaluation_time);
            auto input = root.as_bundle();
            return input[slot];
        }

        [[nodiscard]] bool ready_to_evaluate(const NodeView &view, engine_time_t evaluation_time)
        {
            if (!view.has_input()) { return true; }

            const auto *schema = view.schema()->input_schema;
            const auto &valid_slots = view.schema()->valid_inputs;
            if (!valid_slots.empty())
            {
                for (const std::size_t slot : valid_slots)
                {
                    if (!input_slot(view, slot, evaluation_time).valid()) { return false; }
                }
            }
            else if (schema != nullptr && schema->kind == TSTypeKind::TSB)
            {
                for (std::size_t slot = 0; slot < schema->field_count(); ++slot)
                {
                    if (!input_slot(view, slot, evaluation_time).valid()) { return false; }
                }
            }
            else if (!view.input(evaluation_time).valid())
            {
                return false;
            }

            for (const std::size_t slot : view.schema()->all_valid_inputs)
            {
                if (!input_slot(view, slot, evaluation_time).all_valid()) { return false; }
            }

            return true;
        }

        [[nodiscard]] bool active_input_modified(const NodeView &view, engine_time_t evaluation_time)
        {
            if (!view.has_input()) { return false; }

            const auto *schema = view.schema()->input_schema;
            if (schema == nullptr || schema->kind != TSTypeKind::TSB)
            {
                auto input = view.input(evaluation_time);
                return input.active() && input.modified();
            }

            const auto &slots = view.schema()->active_inputs;
            if (!slots.empty())
            {
                for (const std::size_t slot : slots)
                {
                    auto child = input_slot(view, slot, evaluation_time);
                    if (child.active() && child.modified()) { return true; }
                }
                return false;
            }

            for (std::size_t slot = 0; slot < schema->field_count(); ++slot)
            {
                auto child = input_slot(view, slot, evaluation_time);
                if (child.active() && child.modified()) { return true; }
            }
            return false;
        }

        void attach_graph_impl(const void *context, void *memory, GraphValue *graph, std::size_t node_index)
        {
            auto &state = node_storage(runtime_context(context), memory);
            state.graph = graph;
            state.node_index = node_index;
        }

        GraphValue *graph_impl(const void *context, const void *memory) noexcept
        {
            return memory != nullptr ? node_storage(runtime_context(context), memory).graph : nullptr;
        }

        std::size_t node_index_impl(const void *context, const void *memory) noexcept
        {
            return memory != nullptr ? node_storage(runtime_context(context), memory).node_index : 0;
        }

        bool started_impl(const void *context, const void *memory) noexcept
        {
            return memory != nullptr && node_storage(runtime_context(context), memory).started;
        }

        bool has_input_impl(const void *context, const void *memory) noexcept
        {
            return memory != nullptr && runtime_context(context).layout.has_input();
        }

        bool has_output_impl(const void *context, const void *memory) noexcept
        {
            return memory != nullptr && runtime_context(context).layout.has_output();
        }

        bool has_state_impl(const void *context, const void *memory) noexcept
        {
            return memory != nullptr && runtime_context(context).layout.has_state();
        }

        bool has_scalars_impl(const void *context, const void *memory) noexcept
        {
            return memory != nullptr && runtime_context(context).layout.has_scalars();
        }

        bool has_scheduler_impl(const void *context, const void *memory) noexcept
        {
            return memory != nullptr && runtime_context(context).layout.has_scheduler();
        }

        bool has_error_output_impl(const void *context, const void *memory) noexcept
        {
            return memory != nullptr && runtime_context(context).layout.has_error_output();
        }

        bool has_recordable_state_impl(const void *context, const void *memory) noexcept
        {
            return memory != nullptr && runtime_context(context).layout.has_recordable_state();
        }

        TSInputView input_view_impl(const void *context, void *memory, engine_time_t evaluation_time)
        {
            const auto &runtime = runtime_context(context);
            if (!runtime.layout.has_input()) { throw std::logic_error("Node has no input"); }
            return node_input(runtime, memory).view(&node_storage(runtime, memory), evaluation_time);
        }

        TSOutputView output_view_impl(const void *context, void *memory, engine_time_t evaluation_time)
        {
            const auto &runtime = runtime_context(context);
            if (!runtime.layout.has_output()) { throw std::logic_error("Node has no output"); }
            return node_output(runtime, memory).view(evaluation_time);
        }

        ValueView state_view_impl(const void *context, void *memory)
        {
            const auto &runtime = runtime_context(context);
            if (!runtime.layout.has_state()) { throw std::logic_error("Node has no state"); }
            return node_state(runtime, memory).view();
        }

        ValueView scalars_view_impl(const void *context, void *memory)
        {
            const auto &runtime = runtime_context(context);
            if (!runtime.layout.has_scalars()) { throw std::logic_error("Node has no scalar configuration"); }
            return node_scalars(runtime, memory).view();
        }

        NodeSchedulerState *scheduler_state_impl(const void *context, void *memory)
        {
            const auto &runtime = runtime_context(context);
            if (!runtime.layout.has_scheduler()) { throw std::logic_error("Node has no scheduler"); }
            return &node_scheduler_state(runtime, memory);
        }

        TSOutputView error_output_view_impl(const void *context, void *memory, engine_time_t evaluation_time)
        {
            const auto &runtime = runtime_context(context);
            if (!runtime.layout.has_error_output()) { throw std::logic_error("Node has no error output"); }
            return node_error_output(runtime, memory).view(evaluation_time);
        }

        TSOutputView recordable_state_view_impl(const void *context, void *memory, engine_time_t evaluation_time)
        {
            const auto &runtime = runtime_context(context);
            if (!runtime.layout.has_recordable_state()) { throw std::logic_error("Node has no recordable state output"); }
            return node_recordable_state(runtime, memory).view(evaluation_time);
        }

        void start_impl(const void *context, const NodeView &view, engine_time_t evaluation_time)
        {
            auto &state = node_storage(runtime_context(context), view.data());
            if (state.started) { return; }

            std::size_t activated = 0;
            auto rollback = UnwindCleanupGuard([&] {
                static_cast<void>(activated);
                deactivate_input_slots(view, evaluation_time);
            });

            state.starting = true;
            auto clear_starting = make_scope_exit([&] noexcept { state.starting = false; });
            activate_input_slots(view, evaluation_time);
            activated = 1;

            if (callbacks(context).start) { callbacks(context).start(view, evaluation_time); }
            state.started = true;
            rollback.release();

            // Declarative self-scheduling: a node with ``schedule_on_start`` is
            // marked to evaluate in the current cycle (the framework equivalent of
            // doing schedule_now() in a start hook). Done after the user start so
            // the node is fully started before it can be evaluated.
            if (view.schema() != nullptr && view.schema()->schedule_on_start && state.graph != nullptr)
            {
                state.graph->schedule_node(state.node_index, evaluation_time);
            }
        }

        void stop_impl(const void *context, const NodeView &view, engine_time_t evaluation_time)
        {
            auto &state = node_storage(runtime_context(context), view.data());
            if (!state.started) { return; }

            auto mark_stopped = make_scope_exit([&] noexcept { state.started = false; });
            auto deactivate = UnwindCleanupGuard([&] { deactivate_input_slots(view, evaluation_time); });
            if (callbacks(context).stop) { callbacks(context).stop(view, evaluation_time); }
            deactivate.complete();
        }

        void evaluate_impl(const void *context, const NodeView &view, engine_time_t evaluation_time, bool force)
        {
            if (!view.started()) { return; }

            // Mirror the authoritative Python NodeImpl.eval: capture whether the
            // node fired on a scheduler event *before* evaluating, gate the
            // evaluation, then do the scheduler bookkeeping at the end regardless
            // of whether the evaluation actually ran.
            const auto         &runtime      = runtime_context(context);
            const bool          has_scheduler = runtime.layout.has_scheduler();
            NodeSchedulerState *scheduler     = has_scheduler ? &node_scheduler_state(runtime, view.data()) : nullptr;
            const bool          scheduled_now = scheduler != nullptr && !scheduler->events.empty() &&
                                       scheduler->events.begin()->first == evaluation_time;

            bool do_eval = ready_to_evaluate(view, evaluation_time);
            if (do_eval && !force && view.has_input())
            {
                const bool modified = active_input_modified(view, evaluation_time);
                // A scheduler-bearing node also evaluates when its timer fires,
                // even if no input ticked; a plain node only on input changes.
                if (has_scheduler) { do_eval = scheduled_now || modified; }
                else { do_eval = modified; }
            }

            if (do_eval && callbacks(context).evaluate) { callbacks(context).evaluate(view, evaluation_time); }

            if (has_scheduler)
            {
                NodeScheduler sched{*scheduler, view.graph_value(), view.node_index(), evaluation_time};
                if (scheduled_now)
                {
                    sched.advance();  // consume the fired event(s) and re-arm the next
                }
                else if (sched.is_scheduled() && view.graph_value() != nullptr)
                {
                    // Ran for another reason (an input ticked): just re-arm the timer.
                    view.graph_value()->schedule_node(view.node_index(), sched.next_scheduled_time());
                }
            }
        }

        void cleanup_delta_impl(const void *context, const NodeView &view)
        {
            const auto &runtime = runtime_context(context);
            if (runtime.layout.has_output()) { node_output(runtime, view.data()).cleanup_delta(); }
            if (runtime.layout.has_error_output()) { node_error_output(runtime, view.data()).cleanup_delta(); }
            if (runtime.layout.has_recordable_state()) { node_recordable_state(runtime, view.data()).cleanup_delta(); }
        }

        struct NodeRuntimeRegistry
        {
            const NodeTypeBinding &make_binding(NodeTypeMetaData schema, NodeCallbacks callbacks)
            {
                names.push_back(std::make_unique<std::string>(
                    schema.display_name != nullptr ? std::string{schema.display_name} : std::string{}));
                if (!names.back()->empty()) { schema.display_name = names.back()->c_str(); }

                const auto &plan = node_storage_plan_for(schema);
                contexts.push_back(NodeRuntimeContext{
                    .callbacks = std::move(callbacks),
                    .layout = layout_for(plan),
                    .plan = &plan,
                });
                schemas.push_back(std::move(schema));
                ops_storage.push_back(NodeOps{
                    .context = &contexts.back(),
                    .attach_graph_impl = &attach_graph_impl,
                    .graph_impl = &graph_impl,
                    .node_index_impl = &node_index_impl,
                    .started_impl = &started_impl,
                    .start_impl = &start_impl,
                    .stop_impl = &stop_impl,
                    .evaluate_impl = &evaluate_impl,
                    .cleanup_delta_impl = &cleanup_delta_impl,
                    .has_input_impl = &has_input_impl,
                    .has_output_impl = &has_output_impl,
                    .has_state_impl = &has_state_impl,
                    .has_scalars_impl = &has_scalars_impl,
                    .has_scheduler_impl = &has_scheduler_impl,
                    .has_error_output_impl = &has_error_output_impl,
                    .has_recordable_state_impl = &has_recordable_state_impl,
                    .input_view_impl = &input_view_impl,
                    .output_view_impl = &output_view_impl,
                    .state_view_impl = &state_view_impl,
                    .scalars_view_impl = &scalars_view_impl,
                    .scheduler_state_impl = &scheduler_state_impl,
                    .error_output_view_impl = &error_output_view_impl,
                    .recordable_state_view_impl = &recordable_state_view_impl,
                });

                return NodeTypeBinding::intern(schemas.back(), plan, ops_storage.back());
            }

            std::deque<NodeTypeMetaData>                 schemas{};
            std::deque<NodeRuntimeContext>               contexts{};
            std::deque<NodeOps>                          ops_storage{};
            std::vector<std::unique_ptr<std::string>>    names{};
        };

        NodeRuntimeRegistry &node_runtime_registry()
        {
            static NodeRuntimeRegistry registry;
            return registry;
        }
    }  // namespace

    std::string_view NodeTypeMetaData::name() const noexcept
    {
        return display_name != nullptr ? std::string_view{display_name} : std::string_view{};
    }

    bool NodeTypeMetaData::has_input() const noexcept { return input_schema != nullptr; }
    bool NodeTypeMetaData::has_output() const noexcept { return output_schema != nullptr; }
    bool NodeTypeMetaData::has_state() const noexcept { return state_schema != nullptr; }
    bool NodeTypeMetaData::has_scalars() const noexcept { return scalar_schema != nullptr; }
    bool NodeTypeMetaData::has_error_output() const noexcept { return error_output_schema != nullptr; }
    bool NodeTypeMetaData::has_recordable_state() const noexcept { return recordable_state_schema != nullptr; }

    NodeView::NodeView() noexcept = default;

    NodeView::NodeView(const NodeTypeBinding *binding, void *memory) noexcept
        : storage_(binding, memory)
    {
    }

    bool NodeView::valid() const noexcept { return storage_.has_value(); }
    const NodeTypeBinding *NodeView::binding() const noexcept { return storage_.binding(); }
    const NodeTypeMetaData *NodeView::schema() const noexcept
    {
        const auto *bound = binding();
        return bound != nullptr ? bound->type_meta : nullptr;
    }
    void *NodeView::data() const noexcept { return storage_.data(); }

    std::string_view NodeView::label() const noexcept
    {
        if (!valid()) { return {}; }
        const auto &state = node_storage(runtime_context(ops().context), data());
        return state.label.empty() ? schema()->name() : std::string_view{state.label};
    }

    NodeKind NodeView::node_kind() const noexcept
    {
        return schema() != nullptr ? schema()->node_kind : NodeKind::Compute;
    }

    bool NodeView::started() const noexcept
    {
        return valid() && ops().started_impl(ops().context, data());
    }

    std::size_t NodeView::node_index() const noexcept
    {
        return valid() ? ops().node_index_impl(ops().context, data()) : 0;
    }

    GraphValue *NodeView::graph_value() const noexcept
    {
        return valid() ? ops().graph_impl(ops().context, data()) : nullptr;
    }

    GraphView NodeView::graph() const
    {
        auto *graph = graph_value();
        return graph != nullptr ? graph->view() : GraphView{};
    }

    bool NodeView::has_input() const noexcept
    {
        return valid() && ops().has_input_impl(ops().context, data());
    }

    bool NodeView::has_output() const noexcept
    {
        return valid() && ops().has_output_impl(ops().context, data());
    }

    bool NodeView::has_state() const noexcept
    {
        return valid() && ops().has_state_impl(ops().context, data());
    }

    bool NodeView::has_scalars() const noexcept
    {
        return valid() && ops().has_scalars_impl(ops().context, data());
    }

    bool NodeView::has_scheduler() const noexcept
    {
        return valid() && ops().has_scheduler_impl(ops().context, data());
    }

    bool NodeView::has_error_output() const noexcept
    {
        return valid() && ops().has_error_output_impl(ops().context, data());
    }

    bool NodeView::has_recordable_state() const noexcept
    {
        return valid() && ops().has_recordable_state_impl(ops().context, data());
    }

    TSInputView NodeView::input(engine_time_t evaluation_time) const
    {
        return ops().input_view_impl(ops().context, data(), evaluation_time);
    }

    TSOutputView NodeView::output(engine_time_t evaluation_time) const
    {
        return ops().output_view_impl(ops().context, data(), evaluation_time);
    }

    ValueView NodeView::state() const
    {
        return ops().state_view_impl(ops().context, data());
    }

    ValueView NodeView::scalars() const
    {
        return ops().scalars_view_impl(ops().context, data());
    }

    NodeSchedulerState &NodeView::scheduler_state() const
    {
        return *ops().scheduler_state_impl(ops().context, data());
    }

    TSOutputView NodeView::error_output(engine_time_t evaluation_time) const
    {
        return ops().error_output_view_impl(ops().context, data(), evaluation_time);
    }

    TSOutputView NodeView::recordable_state(engine_time_t evaluation_time) const
    {
        return ops().recordable_state_view_impl(ops().context, data(), evaluation_time);
    }

    void NodeView::start(engine_time_t evaluation_time) const { ops().start_impl(ops().context, *this, evaluation_time); }
    void NodeView::stop(engine_time_t evaluation_time) const { ops().stop_impl(ops().context, *this, evaluation_time); }
    void NodeView::evaluate(engine_time_t evaluation_time, bool force) const
    {
        ops().evaluate_impl(ops().context, *this, evaluation_time, force);
    }
    void NodeView::cleanup_delta() const { ops().cleanup_delta_impl(ops().context, *this); }

    const NodeOps &NodeView::ops() const
    {
        if (!valid()) { throw std::logic_error("NodeView requires a live node"); }
        return binding()->checked_ops();
    }

    NodeValue::NodeValue() noexcept = default;

    NodeValue::NodeValue(const NodeBuilder &builder, std::size_t node_index)
    {
        const auto &binding = builder.binding();
        const auto &runtime = runtime_context(binding.checked_ops().context);
        storage_ = storage_type::owning_constructed(binding, [&](void *dst) {
            construct_node_storage(runtime, *binding.type_meta, builder.input_endpoint(), std::string{builder.label()},
                                   builder.scalars(), dst);
        });
        attach_graph(nullptr, node_index);
    }

    NodeValue::~NodeValue() = default;

    NodeValue::NodeValue(NodeValue &&other) noexcept
        : storage_(std::move(other.storage_))
    {
    }

    NodeValue &NodeValue::operator=(NodeValue &&other) noexcept
    {
        if (this != &other) { storage_ = std::move(other.storage_); }
        return *this;
    }

    bool NodeValue::has_value() const noexcept { return storage_.has_value(); }
    const NodeTypeBinding *NodeValue::binding() const noexcept { return storage_.binding(); }
    const NodeTypeMetaData *NodeValue::schema() const noexcept
    {
        return binding() != nullptr ? binding()->type_meta : nullptr;
    }

    NodeView NodeValue::view()
    {
        return NodeView{binding(), storage_.data()};
    }

    NodeView NodeValue::view() const
    {
        return NodeView{binding(), const_cast<void *>(storage_.data())};
    }

    void NodeValue::attach_graph(GraphValue *graph, std::size_t node_index)
    {
        if (!has_value()) { return; }
        const auto &table = binding()->checked_ops();
        table.attach_graph_impl(table.context, storage_.data(), graph, node_index);
    }

    NodeBuilder::NodeBuilder() = default;

    NodeBuilder NodeBuilder::native(NodeTypeMetaData schema,
                                    NodeCallbacks callbacks,
                                    TSEndpointSchema input_endpoint)
    {
        if (schema.input_schema != nullptr && !input_endpoint.empty() &&
            !time_series_schema_equivalent(schema.input_schema, input_endpoint.schema()))
        {
            throw std::invalid_argument("NodeBuilder input endpoint schema does not match node input schema");
        }
        const auto &binding = node_runtime_registry().make_binding(std::move(schema), std::move(callbacks));
        return NodeBuilder{binding, std::move(input_endpoint)};
    }

    NodeBuilder::NodeBuilder(const NodeTypeBinding &binding, TSEndpointSchema input_endpoint)
        : binding_(&binding),
          input_endpoint_(std::move(input_endpoint))
    {
    }

    NodeBuilder &NodeBuilder::label(std::string label)
    {
        label_ = std::move(label);
        return *this;
    }

    std::string_view NodeBuilder::label() const noexcept
    {
        return label_;
    }

    NodeBuilder &NodeBuilder::scalars(Value scalars)
    {
        scalars_ = std::move(scalars);
        return *this;
    }

    const Value &NodeBuilder::scalars() const noexcept
    {
        return scalars_;
    }

    const NodeTypeBinding &NodeBuilder::binding() const
    {
        if (binding_ == nullptr) { throw std::logic_error("NodeBuilder has no binding"); }
        return *binding_;
    }

    const TSEndpointSchema &NodeBuilder::input_endpoint() const noexcept
    {
        return input_endpoint_;
    }

    NodeValue NodeBuilder::make_node(std::size_t node_index) const
    {
        return NodeValue{*this, node_index};
    }

}  // namespace hgraph
