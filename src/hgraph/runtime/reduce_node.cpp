#include <hgraph/runtime/nested_bindings.h>
#include <hgraph/runtime/reduce_node.h>
#include <hgraph/util/scope.h>

#include <ankerl/unordered_dense.h>

#include <array>
#include <bit>
#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace
    {
        constexpr std::string_view reduce_storage_field_name{"reduce"};

        struct CombinerEntry
        {
            GraphValue graph{};
        };

        struct ValueKeyHash
        {
            using is_transparent = void;
            [[nodiscard]] std::size_t operator()(const Value &value) const
            {
                return value.has_value() ? value.hash() : 0;
            }
        };

        struct ValueKeyEqual
        {
            using is_transparent = void;
            [[nodiscard]] bool operator()(const Value &lhs, const Value &rhs) const
            {
                if (lhs.has_value() != rhs.has_value()) { return false; }
                return !lhs.has_value() || lhs.equals(rhs);
            }
        };

        struct ReduceNodeStorage
        {
            ReduceNodeStorage()                                     = default;
            ReduceNodeStorage(const ReduceNodeStorage &)            = delete;
            ReduceNodeStorage &operator=(const ReduceNodeStorage &) = delete;
            ReduceNodeStorage(ReduceNodeStorage &&) noexcept        = default;
            ReduceNodeStorage &operator=(ReduceNodeStorage &&)      = default;

            ~ReduceNodeStorage()
            {
                destroy_combiners();
                destroy_previous_generation();
            }

            // Root-first (ascending heap index) teardown: a parent combiner's
            // inputs link into its children's outputs — the subscriber must
            // unbind before its target dies. (A plain vector destructor would
            // destroy back-to-front: children first. See *Nested Graphs*.)
            void destroy_combiners() noexcept
            {
                for (auto &combiner : combiners) { combiner.reset(); }
            }

            void destroy_previous_generation() noexcept
            {
                for (auto &combiner : previous_generation) { combiner.reset(); }
                previous_generation.clear();
                previous_generation_time = MIN_DT;
            }

            void destroy_previous_generation_before(DateTime evaluation_time) noexcept
            {
                if (!previous_generation.empty() && previous_generation_time < evaluation_time)
                {
                    destroy_previous_generation();
                }
            }

            /** dense leaf -> key (the key↔leaf mappings; leaves are dense ``0..n-1``). */
            std::vector<Value> dense_to_key{};
            ankerl::unordered_dense::map<Value, std::size_t, ValueKeyHash, ValueKeyEqual> key_to_leaf{};

            /** Power-of-two tree width (monotonic; 0 until the first key). */
            std::size_t leaf_capacity{0};
            /** Heap-indexed internal combine points (size ``leaf_capacity - 1``); null = no live combiner. */
            std::vector<std::unique_ptr<CombinerEntry>> combiners{};
            /** Stopped root-first combiner generation retained through its retirement engine cycle. */
            std::vector<std::unique_ptr<CombinerEntry>> previous_generation{};
            DateTime                                   previous_generation_time{MIN_DT};

            bool primed{false};
            bool published{false};

            // Pause/resume cursor: the combiner position a child paused on (+1 so 0 is a
            // clean "not mid-pause" sentinel), or 0 when not resuming. On resume the
            // structure reconciliation is skipped and the descending combiner loop
            // continues from this position; reset to 0 on completion.
            std::size_t resume_position_plus_one{0};
        };

        struct ReduceNodeContext
        {
            ReduceNodeSpec spec{};
            std::size_t    storage_offset{0};
        };

        // Program-lifetime, intentionally-leaked context storage — same rationale
        // as single_nested_graph_contexts (see nested_graph_node.cpp).
        [[nodiscard]] std::vector<std::unique_ptr<ReduceNodeContext>> &reduce_node_contexts() noexcept
        {
            static auto *contexts = new std::vector<std::unique_ptr<ReduceNodeContext>>;
            return *contexts;
        }

        [[nodiscard]] const ReduceNodeContext &register_reduce_node_context(ReduceNodeSpec spec,
                                                                            std::size_t storage_offset)
        {
            auto context = std::make_unique<ReduceNodeContext>(ReduceNodeContext{
                .spec           = std::move(spec),
                .storage_offset = storage_offset,
            });
            const auto *result = context.get();
            reduce_node_contexts().push_back(std::move(context));
            return *result;
        }

        /** Where an aggregate currently comes from, without materialising it. */
        struct Aggregate
        {
            enum class Kind : std::uint8_t { Empty, Leaf, Node };
            Kind        kind{Kind::Empty};
            std::size_t index{0};
        };

        // Tree layout: internal nodes are heap positions 0..capacity-2 (root =
        // 0, children of i at 2i+1 / 2i+2); leaves are logical positions
        // internal_count + dense_leaf.
        [[nodiscard]] std::size_t internal_count(const ReduceNodeStorage &storage) noexcept
        {
            return storage.leaf_capacity > 1 ? storage.leaf_capacity - 1 : 0;
        }

        [[nodiscard]] Aggregate resolve_aggregate(const ReduceNodeStorage &storage, std::size_t position)
        {
            const std::size_t internals = internal_count(storage);
            if (position >= internals)
            {
                const std::size_t leaf = position - internals;
                if (leaf < storage.dense_to_key.size()) { return {Aggregate::Kind::Leaf, leaf}; }
                return {Aggregate::Kind::Empty, 0};
            }
            const Aggregate left  = resolve_aggregate(storage, 2 * position + 1);
            const Aggregate right = resolve_aggregate(storage, 2 * position + 2);
            if (left.kind == Aggregate::Kind::Empty) { return right; }
            if (right.kind == Aggregate::Kind::Empty) { return left; }
            return {Aggregate::Kind::Node, position};
        }

        [[nodiscard]] Aggregate root_aggregate(const ReduceNodeStorage &storage)
        {
            if (storage.leaf_capacity == 0) { return {Aggregate::Kind::Empty, 0}; }
            return resolve_aggregate(storage, 0);
        }

        void stop_combiner_noexcept(std::unique_ptr<CombinerEntry> &entry) noexcept
        {
            if (entry == nullptr || !entry->graph.has_value()) { return; }
            static_cast<void>(fallback_on_exception(false, [&] {
                entry->graph.view().stop();
                return true;
            }));
        }

        void reset_combiner_noexcept(std::unique_ptr<CombinerEntry> &entry) noexcept
        {
            stop_combiner_noexcept(entry);
            entry.reset();
        }

        /** Resolve an aggregate to the output it aliases. */
        [[nodiscard]] TSOutputView aggregate_output(const NodeView &view, const ReduceNodeContext &context,
                                                    const ReduceNodeStorage &storage, const Aggregate &aggregate,
                                                    DateTime evaluation_time)
        {
            switch (aggregate.kind)
            {
                case Aggregate::Kind::Leaf:
                {
                    auto tsd_source = walk_ts_path(view.input(evaluation_time).borrowed_ref(),
                                                   std::vector<std::size_t>{0})
                                          .bound_output();
                    if (!tsd_source.bound()) { return TSOutputView{}; }
                    auto dict = tsd_source.as_dict();
                    const Value &key = storage.dense_to_key[aggregate.index];
                    if (!dict.contains(key.view())) { return TSOutputView{}; }
                    return dict.at(key.view());
                }
                case Aggregate::Kind::Node:
                {
                    const auto &entry = storage.combiners[aggregate.index];
                    if (entry == nullptr || !entry->graph.has_value()) { return TSOutputView{}; }
                    const auto &output_binding = context.spec.child.output_binding;
                    return walk_ts_path(
                        entry->graph.view().node_at(output_binding->source.node).output(evaluation_time),
                        output_binding->source.path);
                }
                case Aggregate::Kind::Empty: break;
            }
            return TSOutputView{};
        }

        void bind_combiner_inputs(const NodeView &view, const ReduceNodeContext &context,
                                  const ReduceNodeStorage &storage, CombinerEntry &entry, const Aggregate &left,
                                  const Aggregate &right, DateTime evaluation_time)
        {
            auto child = entry.graph.view();
            for (const NestedGraphInputBinding &binding : context.spec.child.input_bindings)
            {
                const Aggregate &side = binding.source_path[0] == 0 ? left : right;
                auto source = aggregate_output(view, context, storage, side, evaluation_time);
                if (!binding.source_path.empty() && binding.source_path.size() > 1 && source.bound())
                {
                    source = walk_ts_path(std::move(source),
                                          std::vector<std::size_t>{binding.source_path.begin() + 1,
                                                                   binding.source_path.end()});
                }
                auto target = walk_ts_path(child.node_at(binding.target.node).input(evaluation_time),
                                           binding.target.path);
                bind_input_to_source(std::move(target), source);
            }
        }

        void remove_leaf_at(ReduceNodeStorage &storage, std::size_t leaf)
        {
            const std::size_t last = storage.dense_to_key.size() - 1;
            storage.key_to_leaf.erase(storage.dense_to_key[leaf]);
            if (leaf != last)
            {
                storage.dense_to_key[leaf]                      = std::move(storage.dense_to_key[last]);
                storage.key_to_leaf[storage.dense_to_key[leaf]] = leaf;
            }
            storage.dense_to_key.pop_back();
        }

        void clear_leaf_state(ReduceNodeStorage &storage)
        {
            storage.dense_to_key.clear();
            storage.key_to_leaf.clear();
        }

        [[nodiscard]] bool reconcile_leaf_state(ReduceNodeStorage &storage, const TSDInputView &dict_input)
        {
            bool structural = false;

            std::vector<Value> stale_keys;
            for (const auto &[key, leaf] : storage.key_to_leaf)
            {
                static_cast<void>(leaf);
                if (!dict_input.contains(key.view())) { stale_keys.push_back(key); }
            }
            for (const Value &key : stale_keys)
            {
                auto it = storage.key_to_leaf.find(key);
                if (it == storage.key_to_leaf.end()) { continue; }
                remove_leaf_at(storage, it->second);
                structural = true;
            }

            for (const ValueView &key : dict_input.keys())
            {
                Value typed_key{key};
                if (storage.key_to_leaf.find(typed_key) != storage.key_to_leaf.end()) { continue; }
                storage.key_to_leaf.emplace(typed_key, storage.dense_to_key.size());
                storage.dense_to_key.push_back(std::move(typed_key));
                structural = true;
            }

            return structural;
        }

        /**
         * Recompute the internal structure after a structural event (key
         * add/remove or capacity growth) — value ticks never come through
         * here; the standing bindings carry them. Three phases, ordered so a
         * link is never bound to or unbound from a dead target:
         *
         * 1. **create** newly-needed combiners, deepest-first, and set aside
         *    no-longer-needed ones (alive, not yet destroyed);
         * 2. **bind** every live combiner's inputs (a same-handle bind is a
         *    no-op; a re-point samples) and publish the root — every old
         *    target is still alive while subscribers move off it;
         * 3. **retire** the set-aside combiners root-first (a doomed parent
         *    unbinds from its still-alive doomed child).
         */
        void rebuild_structure(const NodeView &view, const ReduceNodeContext &context, ReduceNodeStorage &storage,
                               DateTime evaluation_time)
        {
            const std::size_t old_capacity = storage.leaf_capacity;
            const std::size_t live = storage.dense_to_key.size();
            std::size_t       capacity = storage.leaf_capacity;
            if (live > 0) { capacity = std::max({capacity, std::size_t{1}, std::bit_ceil(live)}); }

            std::vector<std::unique_ptr<CombinerEntry>>                 retired_shape{};
            std::vector<std::pair<std::size_t, std::unique_ptr<CombinerEntry>>> retired{};
            std::vector<std::size_t> created{};
            if (capacity != storage.leaf_capacity)
            {
                // Capacity growth re-shapes the tree wholesale: every existing
                // combiner retires and the needed set is rebuilt (the recorded
                // v1 simplification; capacity is monotonic, shrink is a
                // refinement).
                retired_shape = std::move(storage.combiners);
                storage.combiners.clear();
                storage.combiners.resize(capacity > 1 ? capacity - 1 : 0);
                storage.leaf_capacity = capacity;
            }
            auto rollback = UnwindCleanupGuard([&] {
                if (storage.leaf_capacity != old_capacity)
                {
                    for (auto &entry : storage.combiners) { reset_combiner_noexcept(entry); }
                    storage.combiners    = std::move(retired_shape);
                    storage.leaf_capacity = old_capacity;
                    return;
                }

                for (const std::size_t position : created)
                {
                    if (position < storage.combiners.size()) { reset_combiner_noexcept(storage.combiners[position]); }
                }
                for (auto &entry : retired)
                {
                    if (entry.first < storage.combiners.size() && storage.combiners[entry.first] == nullptr)
                    {
                        storage.combiners[entry.first] = std::move(entry.second);
                    }
                }
            });

            // Phase 1 — deepest-first so parents can reference fresh children.
            for (std::size_t position = storage.combiners.size(); position-- > 0;)
            {
                const Aggregate left   = resolve_aggregate(storage, 2 * position + 1);
                const Aggregate right  = resolve_aggregate(storage, 2 * position + 2);
                const bool      needed = left.kind != Aggregate::Kind::Empty &&
                                         right.kind != Aggregate::Kind::Empty;
                auto &entry = storage.combiners[position];

                if (needed && entry == nullptr)
                {
                    entry        = std::make_unique<CombinerEntry>();
                    entry->graph = context.spec.child.graph_builder.make_nested_graph(
                        NodeStorageRef{view.binding(), view.data()});
                    created.push_back(position);
                }
                else if (!needed && entry != nullptr)
                {
                    retired.emplace_back(position, std::move(entry));
                }
            }

            // Phase 2 — bind live combiners (ascending; order is free here),
            // start the fresh ones, then publish the root.
            for (std::size_t position = 0; position < storage.combiners.size(); ++position)
            {
                auto &entry = storage.combiners[position];
                if (entry == nullptr) { continue; }
                const Aggregate left  = resolve_aggregate(storage, 2 * position + 1);
                const Aggregate right = resolve_aggregate(storage, 2 * position + 2);
                bind_combiner_inputs(view, context, storage, *entry, left, right, evaluation_time);
            }
            for (auto it = created.rbegin(); it != created.rend(); ++it)
            {
                storage.combiners[*it]->graph.view().start(evaluation_time);
            }

            // Root publication: Empty -> the zero input's bound output (or
            // unbound), Leaf -> that element, Node -> the combiner output. A
            // re-point to an already-valid source is a tick of this output at
            // the publication time (the sampled contract): the aliased VALUE
            // changed even though the new target did not write this cycle.
            const Aggregate root   = root_aggregate(storage);
            TSOutputView    source = aggregate_output(view, context, storage, root, evaluation_time);
            if (root.kind == Aggregate::Kind::Empty)
            {
                source = walk_ts_path(view.input(evaluation_time).borrowed_ref(), std::vector<std::size_t>{1})
                             .bound_output();
            }
            auto                 output = view.output(evaluation_time);
            const TSOutputHandle before = output.forwarding_target();
            if (source.bound()) { bind_forwarding_output_to_source(output, source); }
            else if (output.forwarding_bound()) { output.clear_forwarding_target(); }
            if (source.bound() && source.valid() && !output.forwarding_target().same_as(before))
            {
                output.begin_mutation(evaluation_time).mark_modified();
            }
            storage.published = true;

            // Phase 3 — stop root-first, but keep the retired graph storage
            // through this engine cycle. Dynamic target links may still read
            // the old aggregate later in the same cycle.
            storage.previous_generation.reserve(storage.previous_generation.size() + retired.size() +
                                                retired_shape.size());
            for (auto it = retired.rbegin(); it != retired.rend(); ++it)
            {
                stop_combiner_noexcept(it->second);
                storage.previous_generation.push_back(std::move(it->second));
            }
            for (auto &entry : retired_shape)
            {
                stop_combiner_noexcept(entry);
                if (entry != nullptr) { storage.previous_generation.push_back(std::move(entry)); }
            }
            if (!storage.previous_generation.empty()) { storage.previous_generation_time = evaluation_time; }
            rollback.release();
        }

        // Reconcile the leaf state + rebuild the combiner tree for this cycle (the
        // "setup"). Skipped on a pause/resume re-entry (the structure does not change
        // mid-cycle).
        void reduce_reconcile(const NodeView &view, const ReduceNodeContext &context,
                              ReduceNodeStorage &storage, DateTime evaluation_time)
        {
            auto root_input = view.input(evaluation_time);
            auto tsd_input  = walk_ts_path(root_input.borrowed_ref(), std::vector<std::size_t>{0});

            bool structural = false;
            bool refresh_bindings = false;
            if (tsd_input.valid())
            {
                auto dict_input = tsd_input.as_dict();
                if (tsd_input.modified() || !storage.primed)
                {
                    structural = reconcile_leaf_state(storage, dict_input) || structural;
                    refresh_bindings = true;
                    storage.primed  = true;
                }
            }
            else if (storage.primed || !storage.dense_to_key.empty())
            {
                clear_leaf_state(storage);
                structural = true;
                refresh_bindings = true;
                storage.primed = false;
            }

            if (structural || refresh_bindings || !storage.published)
            {
                rebuild_structure(view, context, storage, evaluation_time);
            }
        }

        // Evaluates the combiner tree, supporting pause/resume. A combiner that pauses (a
        // mesh nested in the reduce function needs a sibling) propagates the pause: save the
        // descending-position cursor and return false so the enclosing mesh resolves it and
        // re-evaluates us. On resume the reconciliation is skipped and the descending loop
        // continues from the saved position; completion resets the cursor.
        bool reduce_evaluate(const NodeView &view, DateTime evaluation_time)
        {
            if (!view.started()) { return true; }

            auto        reduce_view = view.as<ReduceNodeView>();
            const auto &context     = *static_cast<const ReduceNodeContext *>(reduce_view.internal_context());
            auto       &storage     = *MemoryUtils::cast<ReduceNodeStorage>(reduce_view.internal_storage());

            const bool resuming = storage.resume_position_plus_one != 0;
            if (!resuming)
            {
                storage.destroy_previous_generation_before(evaluation_time);
                reduce_reconcile(view, context, storage, evaluation_time);
            }

            // Evaluate combiners deepest-first (descending heap index): a leaf tick notifies
            // its combiner directly; that combiner's output tick notifies its parent (lower
            // index, processed later), so the cascade settles in one pass. Evaluation is
            // unconditional (an idle child evaluate is a cheap scan). On resume, continue
            // from the saved position downward (positions above it already ran this cycle).
            const std::size_t start = resuming ? storage.resume_position_plus_one : storage.combiners.size();
            for (std::size_t position = start; position-- > 0;)
            {
                const auto &entry = storage.combiners[position];
                if (entry == nullptr || !entry->graph.has_value()) { continue; }
                if (!entry->graph.view().evaluate(evaluation_time))
                {
                    storage.resume_position_plus_one = position + 1;  // resume from this position
                    return false;
                }
            }
            storage.resume_position_plus_one = 0;  // completed the cycle
            return true;
        }

        bool reduce_evaluate_impl(const void *, const NodeView &view, DateTime evaluation_time)
        {
            return reduce_evaluate(view, evaluation_time);
        }

        void reduce_node_stop(const NodeView &view, DateTime)
        {
            auto  reduce_view = view.as<ReduceNodeView>();
            auto &storage     = *MemoryUtils::cast<ReduceNodeStorage>(reduce_view.internal_storage());
            for (const auto &entry : storage.combiners)
            {
                if (entry != nullptr && entry->graph.has_value()) { entry->graph.view().stop(); }
            }
        }

        void validate_reduce_node_spec(const NodeTypeMetaData &meta, const ReduceNodeSpec &spec)
        {
            if (!spec.child.output_binding.has_value())
            {
                throw std::invalid_argument("reduce_node requires a combiner output binding");
            }
            if (meta.input_schema == nullptr || meta.input_schema->kind != TSTypeKind::TSB ||
                meta.input_schema->field_count() != 2)
            {
                throw std::invalid_argument("reduce_node requires input schema [ts, zero]");
            }
            const auto *fields = meta.input_schema->fields();
            if (fields[0].type == nullptr || fields[0].type->kind != TSTypeKind::TSD)
            {
                throw std::invalid_argument("reduce_node first input must be a TSD");
            }
            if (meta.output_schema == nullptr)
            {
                throw std::invalid_argument("reduce_node requires an output schema");
            }

            const std::size_t child_node_count = spec.child.graph_builder.node_count();
            const auto       &output_binding   = *spec.child.output_binding;
            if (output_binding.kind != NestedGraphOutputBinding::Kind::ChildOutput)
            {
                throw std::invalid_argument(
                    "reduce_node requires the combiner output to be a real child output, not a parent-input alias");
            }
            if (!output_binding.target_path.empty())
            {
                throw std::invalid_argument("reduce_node combiner output binding must target the output root");
            }
            if (output_binding.source.node >= child_node_count)
            {
                throw std::invalid_argument("reduce_node combiner output source node is out of range");
            }

            for (const NestedGraphInputBinding &binding : spec.child.input_bindings)
            {
                if (binding.source_path.empty() || binding.source_path[0] > 1)
                {
                    throw std::invalid_argument("reduce_node combiner inputs must be sourced from lhs or rhs");
                }
                if (binding.target.node >= child_node_count)
                {
                    throw std::invalid_argument("reduce_node combiner input target node is out of range");
                }
            }
        }
    }  // namespace

    const void *ReduceNodeView::node_view_type_id() noexcept
    {
        static const char token{};
        return &token;
    }

    ReduceNodeView ReduceNodeView::from_node(NodeView view, const void *context)
    {
        if (context == nullptr) { throw std::logic_error("ReduceNodeView requires a typed view context"); }
        const auto &typed_context = *static_cast<const ReduceNodeContext *>(context);
        void       *storage = MemoryUtils::advance(view.data(), typed_context.storage_offset);
        return ReduceNodeView{std::move(view), context, storage};
    }

    const NodeView &ReduceNodeView::node() const noexcept { return view_; }

    std::size_t ReduceNodeView::leaf_count() const noexcept
    {
        return MemoryUtils::cast<ReduceNodeStorage>(storage_)->dense_to_key.size();
    }

    std::size_t ReduceNodeView::combiner_count() const noexcept
    {
        const auto &combiners = MemoryUtils::cast<ReduceNodeStorage>(storage_)->combiners;
        std::size_t count     = 0;
        for (const auto &entry : combiners)
        {
            if (entry != nullptr) { ++count; }
        }
        return count;
    }

    ReduceNodeView::ReduceNodeView(NodeView view, const void *context, void *storage) noexcept
        : view_(std::move(view)),
          context_(context),
          storage_(storage)
    {
    }

    NodeBuilder reduce_node(NodeTypeMetaData meta, ReduceNodeSpec spec)
    {
        validate_reduce_node_spec(meta, spec);

        meta.node_kind              = NodeKind::Nested;
        meta.valid_inputs          = std::vector<std::size_t>{};
        meta.output_endpoint_schema = TSEndpointSchema::peered(meta.output_schema);

        NodeTypeDescriptor descriptor;
        descriptor.schema = std::move(meta);

        const std::array fields{NodeStorageField{
            .name = reduce_storage_field_name,
            .plan = &MemoryUtils::plan_for<ReduceNodeStorage>(),
        }};
        // Default (before-output) placement: the node's forwarding output
        // links INTO field-held combiner outputs — the output (the link) must
        // destroy before the field (the nested_/switch_ direction).
        descriptor.storage_plan = &node_storage_plan_for(descriptor.schema, fields);

        descriptor.callbacks.stop            = &reduce_node_stop;
        descriptor.ops.evaluate_impl         = &reduce_evaluate_impl;
        descriptor.ops.extended_view_type_id = ReduceNodeView::node_view_type_id();
        descriptor.ops.extended_view_context = &register_reduce_node_context(
            std::move(spec), descriptor.storage_plan->component(reduce_storage_field_name).offset);

        return NodeBuilder::from_descriptor(std::move(descriptor));
    }
}  // namespace hgraph
