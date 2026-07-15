#include <hgraph/runtime/nested_bindings.h>
#include <hgraph/runtime/nested_graph_storage.h>
#include <hgraph/runtime/ordered_reduce_node.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/util/scope.h>

#include "reduce_output_binding.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace
    {
        constexpr std::string_view ordered_reduce_storage_field_name{"ordered_reduce"};

        struct OrderedReduceEntry
        {
            GraphValue graph{};
        };

        struct OrderedReduceStorage
        {
            OrderedReduceStorage() = default;
            OrderedReduceStorage(const OrderedReduceStorage &) = delete;
            OrderedReduceStorage &operator=(const OrderedReduceStorage &) = delete;
            OrderedReduceStorage(OrderedReduceStorage &&) = delete;
            OrderedReduceStorage &operator=(OrderedReduceStorage &&) = delete;

            ~OrderedReduceStorage()
            {
                destroy_previous_generation();
                destroy_current_generation();
            }

            void initialise(MemoryUtils::StorageLayout graph_layout)
            {
                for (auto &bank : banks) { bank.bind_graph_layout(graph_layout); }
            }

            void stop_generation(std::size_t bank_index, std::size_t count) noexcept
            {
                auto &bank = banks[bank_index];
                for (std::size_t index = count; index-- > 0;)
                {
                    auto *entry = bank.entry_at(index);
                    if (entry == nullptr || !entry->graph.has_value() || !entry->graph.view().started()) { continue; }
                    static_cast<void>(fallback_on_exception(false, [&] {
                        entry->graph.view().stop();
                        return true;
                    }));
                }
            }

            void destroy_generation(std::size_t bank_index, std::size_t count) noexcept
            {
                auto &bank = banks[bank_index];
                for (std::size_t index = count; index-- > 0;) { bank.destroy_at(index); }
            }

            void destroy_current_generation() noexcept
            {
                destroy_generation(current_bank, live_count);
                live_count = 0;
            }

            void destroy_previous_generation() noexcept
            {
                if (previous_count == 0) { return; }
                destroy_generation(1U - current_bank, previous_count);
                previous_count = 0;
                previous_time = MIN_DT;
            }

            void destroy_previous_generation_before(DateTime evaluation_time) noexcept
            {
                if (previous_count != 0 && previous_time < evaluation_time)
                {
                    destroy_previous_generation();
                }
            }

            std::array<InPlaceGraphSlotStore<OrderedReduceEntry>, 2> banks{};
            std::size_t current_bank{0};
            std::size_t live_count{0};
            std::size_t previous_count{0};
            DateTime previous_time{MIN_DT};
            bool primed{false};
            bool published{false};
            std::size_t resume_index_plus_one{0};
        };

        struct OrderedReduceContext
        {
            OrderedReduceNodeSpec spec{};
            std::size_t storage_offset{0};
            MemoryUtils::StorageLayout graph_layout{};
        };

        [[nodiscard]] std::vector<std::unique_ptr<OrderedReduceContext>> &ordered_reduce_contexts() noexcept
        {
            static auto *contexts = new std::vector<std::unique_ptr<OrderedReduceContext>>;
            return *contexts;
        }

        [[nodiscard]] const OrderedReduceContext &register_ordered_reduce_context(
            OrderedReduceNodeSpec spec,
            std::size_t storage_offset,
            MemoryUtils::StorageLayout graph_layout)
        {
            auto context = std::make_unique<OrderedReduceContext>(OrderedReduceContext{
                .spec = std::move(spec),
                .storage_offset = storage_offset,
                .graph_layout = graph_layout,
            });
            const auto *result = context.get();
            ordered_reduce_contexts().push_back(std::move(context));
            return *result;
        }

        [[nodiscard]] TSOutputView child_output(
            const OrderedReduceContext &context,
            const OrderedReduceEntry &entry,
            DateTime evaluation_time)
        {
            const auto &binding = *context.spec.child.output_binding;
            return walk_ts_path(
                entry.graph.view().node_at(binding.source.node).output(evaluation_time),
                binding.source.path);
        }

        [[nodiscard]] std::size_t ordered_input_size(const TSDInputView &dict)
        {
            std::size_t count = 0;
            std::size_t max_index = 0;
            for (const ValueView &key : dict.keys())
            {
                const Int value = key.checked_as<Int>();
                if (value < 0)
                {
                    throw std::invalid_argument("ordered reduce requires non-negative integer keys");
                }
                const auto unsigned_value = static_cast<std::uint64_t>(value);
                if (unsigned_value > std::numeric_limits<std::size_t>::max())
                {
                    throw std::overflow_error("ordered reduce key does not fit in size_t");
                }
                max_index = std::max(max_index, static_cast<std::size_t>(unsigned_value));
                ++count;
            }
            if (count == 0) { return 0; }
            if (max_index == std::numeric_limits<std::size_t>::max() || count != max_index + 1)
            {
                throw std::invalid_argument("ordered reduce requires contiguous integer keys from zero");
            }
            return count;
        }

        [[nodiscard]] TSOutputView dictionary_element_output(
            const NodeView &view,
            std::size_t index,
            DateTime evaluation_time)
        {
            auto source = view.input(evaluation_time).indexed_child_at(0).bound_output();
            if (!source.bound()) { return {}; }
            Value key{static_cast<Int>(index)};
            auto dict = source.as_dict();
            return dict.contains(key.view()) ? dict.at(key.view()) : TSOutputView{};
        }

        void bind_child_inputs(
            const NodeView &view,
            const OrderedReduceContext &context,
            InPlaceGraphSlotStore<OrderedReduceEntry> &bank,
            std::size_t index,
            DateTime evaluation_time)
        {
            auto *entry = bank.entry_at(index);
            if (entry == nullptr || !entry->graph.has_value())
            {
                throw std::logic_error("ordered reduce child entry is not constructed");
            }

            auto child = entry->graph.view();
            for (const NestedGraphInputBinding &binding : context.spec.child.input_bindings)
            {
                TSOutputView source;
                if (binding.source_path[0] == 0)
                {
                    source = index == 0
                                 ? view.input(evaluation_time).indexed_child_at(1).bound_output()
                                 : child_output(context, *bank.entry_at(index - 1), evaluation_time);
                }
                else
                {
                    source = dictionary_element_output(view, index, evaluation_time);
                }
                if (binding.source_path.size() > 1 && source.bound())
                {
                    source = walk_ts_path(
                        std::move(source),
                        std::span<const std::size_t>{binding.source_path}.subspan(1));
                }
                auto target = walk_ts_path(
                    child.node_at(binding.target.node).input(evaluation_time),
                    binding.target.path);
                bind_input_to_source(std::move(target), source);
            }
        }

        void publish_tail(
            const NodeView &view,
            const OrderedReduceContext &context,
            const InPlaceGraphSlotStore<OrderedReduceEntry> &bank,
            std::size_t count,
            DateTime evaluation_time)
        {
            TSOutputView source = view.input(evaluation_time).indexed_child_at(1).bound_output();
            if (count != 0)
            {
                const auto *entry = bank.entry_at(count - 1);
                if (entry == nullptr) { throw std::logic_error("ordered reduce tail entry is missing"); }
                source = child_output(context, *entry, evaluation_time);
            }
            runtime_detail::bind_reduce_output(view.output(evaluation_time), source, evaluation_time);
        }

        void rebuild_chain(
            const NodeView &view,
            const OrderedReduceContext &context,
            OrderedReduceStorage &storage,
            std::size_t next_count,
            DateTime evaluation_time)
        {
            const std::size_t old_bank = storage.current_bank;
            const std::size_t old_count = storage.live_count;
            const std::size_t next_bank = 1U - old_bank;
            auto &bank = storage.banks[next_bank];
            if (bank.has_entries())
            {
                throw std::logic_error("ordered reduce inactive graph bank is still occupied");
            }
            bank.reserve_to(next_count);

            std::size_t created = 0;
            auto rollback = UnwindCleanupGuard([&] {
                storage.stop_generation(next_bank, created);
                storage.destroy_generation(next_bank, created);
            });

            for (std::size_t index = 0; index < next_count; ++index)
            {
                auto &entry = bank.construct_at(index);
                ++created;
                entry.graph = context.spec.child.graph_builder.make_nested_graph(
                    view.pointer(), bank.graph_memory(index), context.graph_layout);
                bind_child_inputs(view, context, bank, index, evaluation_time);
                entry.graph.view().start(evaluation_time);
                schedule_sampled_input_consumers(
                    entry.graph.view(), evaluation_time, context.spec.child.input_bindings);
            }

            publish_tail(view, context, bank, next_count, evaluation_time);
            storage.stop_generation(old_bank, old_count);
            storage.current_bank = next_bank;
            storage.live_count = next_count;
            storage.previous_count = old_count;
            storage.previous_time = old_count == 0 ? MIN_DT : evaluation_time;
            storage.published = true;
            rollback.release();
        }

        void refresh_chain_bindings(
            const NodeView &view,
            const OrderedReduceContext &context,
            OrderedReduceStorage &storage,
            DateTime evaluation_time)
        {
            auto &bank = storage.banks[storage.current_bank];
            for (std::size_t index = 0; index < storage.live_count; ++index)
            {
                bind_child_inputs(view, context, bank, index, evaluation_time);
            }
            publish_tail(view, context, bank, storage.live_count, evaluation_time);
            storage.published = true;
        }

        void reconcile_chain(
            const NodeView &view,
            const OrderedReduceContext &context,
            OrderedReduceStorage &storage,
            DateTime evaluation_time)
        {
            auto root = view.input(evaluation_time);
            auto ts = root.indexed_child_at(0);
            const std::size_t next_count = ts.valid() ? ordered_input_size(ts.as_dict()) : 0;
            if (!storage.primed || next_count != storage.live_count)
            {
                rebuild_chain(view, context, storage, next_count, evaluation_time);
            }
            else if (ts.modified() || !storage.published || root.indexed_child_at(1).modified())
            {
                refresh_chain_bindings(view, context, storage, evaluation_time);
            }
            storage.primed = ts.valid();
        }

        bool ordered_reduce_evaluate(const NodeView &view, DateTime evaluation_time)
        {
            if (!view.started()) { return true; }

            auto typed = view.as<OrderedReduceNodeView>();
            const auto &context = *static_cast<const OrderedReduceContext *>(typed.internal_context());
            auto &storage = *MemoryUtils::cast<OrderedReduceStorage>(typed.internal_storage());
            storage.initialise(context.graph_layout);

            const bool resuming = storage.resume_index_plus_one != 0;
            if (!resuming)
            {
                storage.destroy_previous_generation_before(evaluation_time);
                reconcile_chain(view, context, storage, evaluation_time);
            }

            const std::size_t start = resuming ? storage.resume_index_plus_one - 1 : 0;
            auto &bank = storage.banks[storage.current_bank];
            for (std::size_t index = start; index < storage.live_count; ++index)
            {
                auto *entry = bank.entry_at(index);
                if (entry == nullptr || !entry->graph.has_value()) { continue; }
                if (!entry->graph.view().evaluate(evaluation_time))
                {
                    storage.resume_index_plus_one = index + 1;
                    return false;
                }
            }
            storage.resume_index_plus_one = 0;
            return true;
        }

        bool ordered_reduce_evaluate_impl(const void *, const NodeView &view, DateTime evaluation_time)
        {
            return ordered_reduce_evaluate(view, evaluation_time);
        }

        void ordered_reduce_stop(const NodeView &view, DateTime)
        {
            auto typed = view.as<OrderedReduceNodeView>();
            auto &storage = *MemoryUtils::cast<OrderedReduceStorage>(typed.internal_storage());
            storage.stop_generation(storage.current_bank, storage.live_count);
        }

        void validate_ordered_reduce_spec(
            const NodeTypeMetaData &meta,
            const OrderedReduceNodeSpec &spec)
        {
            if (!spec.child.output_binding.has_value())
            {
                throw std::invalid_argument("ordered_reduce_node requires a combiner output binding");
            }
            if (meta.input_schema == nullptr || meta.input_schema->kind != TSTypeKind::TSB ||
                meta.input_schema->field_count() != 2)
            {
                throw std::invalid_argument("ordered_reduce_node requires input schema [ts, zero]");
            }
            const auto *fields = meta.input_schema->fields();
            const auto *tsd = fields[0].type;
            if (tsd == nullptr || tsd->kind != TSTypeKind::TSD ||
                tsd->key_type() != scalar_descriptor<Int>::value_meta())
            {
                throw std::invalid_argument("ordered_reduce_node requires TSD[int, E] input");
            }
            if (meta.output_schema == nullptr ||
                !time_series_schema_equivalent(meta.output_schema, fields[1].type))
            {
                throw std::invalid_argument("ordered_reduce_node output must match its zero input");
            }

            const std::size_t child_node_count = spec.child.graph_builder.node_count();
            const auto &output_binding = *spec.child.output_binding;
            if (output_binding.kind != NestedGraphOutputBinding::Kind::ChildOutput ||
                !output_binding.target_path.empty() || output_binding.source.node >= child_node_count)
            {
                throw std::invalid_argument(
                    "ordered_reduce_node requires a root child-output combiner terminal");
            }
            for (const NestedGraphInputBinding &binding : spec.child.input_bindings)
            {
                if (binding.source_path.empty() || binding.source_path[0] > 1 ||
                    binding.target.node >= child_node_count)
                {
                    throw std::invalid_argument(
                        "ordered_reduce_node combiner inputs must be sourced from lhs or rhs");
                }
            }
        }
    }  // namespace

    const void *OrderedReduceNodeView::node_view_type_id() noexcept
    {
        static const char token{};
        return &token;
    }

    OrderedReduceNodeView OrderedReduceNodeView::from_node(NodeView view, const void *context)
    {
        if (context == nullptr)
        {
            throw std::logic_error("OrderedReduceNodeView requires a typed view context");
        }
        const auto &typed_context = *static_cast<const OrderedReduceContext *>(context);
        void *storage = MemoryUtils::advance(view.data(), typed_context.storage_offset);
        return OrderedReduceNodeView{std::move(view), context, storage};
    }

    const NodeView &OrderedReduceNodeView::node() const noexcept { return view_; }

    std::size_t OrderedReduceNodeView::child_graph_count() const noexcept
    {
        return MemoryUtils::cast<OrderedReduceStorage>(storage_)->live_count;
    }

    bool OrderedReduceNodeView::child_graphs_use_in_place_storage() const noexcept
    {
        const auto &storage = *MemoryUtils::cast<OrderedReduceStorage>(storage_);
        const auto &bank = storage.banks[storage.current_bank];
        for (std::size_t index = 0; index < storage.live_count; ++index)
        {
            const auto *entry = bank.entry_at(index);
            if (entry != nullptr && entry->graph.has_value() && !entry->graph.uses_external_storage())
            {
                return false;
            }
        }
        return true;
    }

    OrderedReduceNodeView::OrderedReduceNodeView(
        NodeView view,
        const void *context,
        void *storage) noexcept
        : view_(std::move(view)), context_(context), storage_(storage)
    {
    }

    NodeBuilder ordered_reduce_node(NodeTypeMetaData meta, OrderedReduceNodeSpec spec)
    {
        validate_ordered_reduce_spec(meta, spec);

        meta.node_kind = NodeKind::Nested;
        meta.valid_inputs = std::vector<std::size_t>{};
        meta.output_endpoint_schema = runtime_detail::reduce_output_endpoint_schema(meta.output_schema);

        NodeTypeDescriptor descriptor;
        descriptor.schema = std::move(meta);
        const std::array fields{NodeStorageField{
            .name = ordered_reduce_storage_field_name,
            .plan = &MemoryUtils::plan_for<OrderedReduceStorage>(),
        }};
        descriptor.storage_plan = &node_storage_plan_for(descriptor.schema, fields);

        const MemoryUtils::StorageLayout graph_layout = spec.child.graph_builder.nested_storage_layout();
        descriptor.callbacks.stop = &ordered_reduce_stop;
        descriptor.ops.evaluate_impl = &ordered_reduce_evaluate_impl;
        descriptor.ops.extended_view_type_id = OrderedReduceNodeView::node_view_type_id();
        descriptor.ops.extended_view_context = &register_ordered_reduce_context(
            std::move(spec),
            descriptor.storage_plan->component(ordered_reduce_storage_field_name).offset,
            graph_layout);
        return NodeBuilder::from_descriptor(std::move(descriptor));
    }
}  // namespace hgraph
