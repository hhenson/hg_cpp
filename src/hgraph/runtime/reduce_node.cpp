#include <hgraph/runtime/nested_bindings.h>
#include <hgraph/runtime/nested_graph_storage.h>
#include <hgraph/runtime/reduce_node.h>
#include <hgraph/types/wired_fn.h>
#include <hgraph/types/utils/slot_bitmap.h>
#include <hgraph/util/scope.h>

#include "reduce_output_binding.h"

#include <ankerl/unordered_dense.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
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
        constexpr std::string_view reduce_storage_field_name{"reduce"};

        using runtime_detail::bind_reduce_output;
        using runtime_detail::reduce_output_endpoint_schema;

        struct CombinerEntry
        {
            GraphValue     graph{};
            TSOutputHandle output{};
        };

        struct ValueKeyHash
        {
            using is_transparent = void;
            [[nodiscard]] std::size_t operator()(const Value &value) const
            {
                return value.has_value() ? value.hash() : 0;
            }
            [[nodiscard]] std::size_t operator()(const ValueView &value) const noexcept
            {
                return value.valid() ? value.hash() : 0;
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
            [[nodiscard]] bool operator()(const Value &lhs, const ValueView &rhs) const noexcept
            {
                return lhs.has_value() == rhs.valid() && (!lhs.has_value() || lhs.equals(rhs));
            }
            [[nodiscard]] bool operator()(const ValueView &lhs, const Value &rhs) const noexcept
            {
                return (*this)(rhs, lhs);
            }
        };

        struct ReduceNodeStorage
        {
            ReduceNodeStorage()                                     = default;
            ReduceNodeStorage(const ReduceNodeStorage &)            = delete;
            ReduceNodeStorage &operator=(const ReduceNodeStorage &) = delete;
            ReduceNodeStorage(ReduceNodeStorage &&)                 = delete;
            ReduceNodeStorage &operator=(ReduceNodeStorage &&)      = delete;

            ~ReduceNodeStorage()
            {
                // A stopped retired parent may still retain a target handle to
                // a surviving current combiner output. Destroy retired
                // subscribers before current producers; each generation is
                // still destroyed root-first internally.
                destroy_previous_generation();
                destroy_combiners();
            }

            void initialise(MemoryUtils::StorageLayout graph_layout)
            {
                for (auto &bank : combiner_banks) { bank.bind_graph_layout(graph_layout); }
            }

            // Root-first (ascending heap index) teardown: a parent combiner's
            // inputs link into its children's outputs — the subscriber must
            // unbind before its target dies. (A plain vector destructor would
            // destroy back-to-front: children first. See *Nested Graphs*.)
            void destroy_combiners() noexcept
            {
                auto &bank = combiner_banks[current_bank];
                for (std::size_t position = 0; position < combiners.size(); ++position)
                {
                    if (combiners[position] == nullptr) { continue; }
                    bank.destroy_at(position);
                    combiners[position] = nullptr;
                }
            }

            void destroy_previous_generation() noexcept
            {
                for (const auto &combiner : previous_generation)
                {
                    combiner_banks[combiner.bank].destroy_at(combiner.position);
                }
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
            /** dense leaf -> source collection slot, avoiding repeated key lookup while binding the tree. */
            std::vector<std::size_t> dense_to_source_slot{};
            ankerl::unordered_dense::map<Value, std::size_t, ValueKeyHash, ValueKeyEqual> key_to_leaf{};

            /** Power-of-two tree width (monotonic; 0 until the first key). */
            std::size_t leaf_capacity{0};
            /** Heap-indexed internal combine points (size ``leaf_capacity - 1``); null = no live combiner. */
            std::vector<CombinerEntry *> combiners{};

            struct RetiredCombiner
            {
                std::size_t bank{0};
                std::size_t position{0};
            };

            // Capacity growth reshapes the entire tree. Build into the other
            // bank while retaining the stopped old bank through this cycle.
            std::array<InPlaceGraphSlotStore<CombinerEntry>, 2> combiner_banks{};
            std::size_t                                         current_bank{0};
            std::vector<RetiredCombiner>                        previous_generation{};
            DateTime                                            previous_generation_time{MIN_DT};

            bool primed{false};
            bool published{false};
            bool source_handles_initialised{false};
            TSOutputHandle collection_source{};
            TSOutputHandle zero_source{};

            // Ordinary collection ticks populate only the affected leaf-to-root
            // paths. Full scans remain the conservative path for structural
            // changes, repoints, and independently scheduled child graphs.
            std::vector<std::size_t> evaluation_positions{};
            SlotBitmap              evaluation_candidates{};
            std::vector<std::size_t> modified_leaves{};
            std::vector<std::size_t> structural_leaves{};
            std::vector<std::size_t> structural_positions{};
            std::size_t              resume_candidate_plus_one{0};
            bool                     has_future_combiner_schedule{false};
        };

        struct ReduceCollectionOps
        {
            bool (*reconcile)(ReduceNodeStorage &storage, TSInputView &input, bool full);
            bool (*structure_modified)(const TSInputView &input, DateTime evaluation_time);
            void (*append_modified_leaves)(const ReduceNodeStorage &storage,
                                           TSInputView &input,
                                           std::vector<std::size_t> &leaves);
            TSOutputView (*leaf_output)(TSOutputView source, std::size_t leaf, const Value &key,
                                        std::size_t source_slot);
        };

        [[nodiscard]] bool reconcile_dict_collection(ReduceNodeStorage &storage, TSInputView &input, bool full);
        [[nodiscard]] bool reconcile_list_collection(ReduceNodeStorage &storage, TSInputView &input, bool full);
        void append_modified_dict_leaves(const ReduceNodeStorage &storage, TSInputView &input,
                                         std::vector<std::size_t> &leaves);
        void append_modified_list_leaves(const ReduceNodeStorage &storage, TSInputView &input,
                                         std::vector<std::size_t> &leaves);
        [[nodiscard]] TSOutputView dict_leaf_output(TSOutputView source, std::size_t leaf, const Value &key,
                                                    std::size_t source_slot);
        [[nodiscard]] TSOutputView list_leaf_output(TSOutputView source, std::size_t leaf, const Value &key,
                                                    std::size_t source_slot);

        struct ReduceNodeContext
        {
            ReduceNodeSpec spec{};
            std::size_t    storage_offset{0};
            MemoryUtils::StorageLayout graph_layout{};
            const ReduceCollectionOps *collection_ops{nullptr};
        };

        // Program-lifetime, intentionally-leaked context storage — same rationale
        // as single_nested_graph_contexts (see nested_graph_node.cpp).
        [[nodiscard]] std::vector<std::unique_ptr<ReduceNodeContext>> &reduce_node_contexts() noexcept
        {
            static auto *contexts = new std::vector<std::unique_ptr<ReduceNodeContext>>;
            return *contexts;
        }

        [[nodiscard]] const ReduceNodeContext &register_reduce_node_context(
            ReduceNodeSpec spec, std::size_t storage_offset, MemoryUtils::StorageLayout graph_layout,
            const ReduceCollectionOps &collection_ops)
        {
            auto context = std::make_unique<ReduceNodeContext>(ReduceNodeContext{
                .spec           = std::move(spec),
                .storage_offset = storage_offset,
                .graph_layout   = graph_layout,
                .collection_ops = &collection_ops,
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

            // Live leaves occupy a dense prefix. Derive this heap position's
            // leaf interval directly instead of recursively walking its entire
            // subtree on every structural reconciliation.
            const std::size_t level_start = std::bit_floor(position + 1);
            const std::size_t depth = std::bit_width(level_start) - 1;
            std::size_t span = storage.leaf_capacity >> depth;
            const std::size_t first_leaf = (position + 1 - level_start) * span;
            const std::size_t live = storage.dense_to_key.size();
            if (first_leaf >= live) { return {Aggregate::Kind::Empty, 0}; }
            const std::size_t live_in_subtree = std::min(span, live - first_leaf);
            if (live_in_subtree == 1)
            {
                return {Aggregate::Kind::Leaf, first_leaf};
            }

            // A partially populated subtree may carry its aggregate through a
            // left descendant (for example, two live leaves in a capacity-four
            // root resolve to the left child combiner). Descend only that one
            // frontier; full subtrees resolve immediately.
            std::size_t aggregate_position = position;
            while (live_in_subtree <= span / 2)
            {
                aggregate_position = 2 * aggregate_position + 1;
                span /= 2;
            }
            return {Aggregate::Kind::Node, aggregate_position};
        }

        [[nodiscard]] Aggregate root_aggregate(const ReduceNodeStorage &storage)
        {
            if (storage.leaf_capacity == 0) { return {Aggregate::Kind::Empty, 0}; }
            return resolve_aggregate(storage, 0);
        }

        void stop_combiner_noexcept(CombinerEntry *entry) noexcept
        {
            if (entry == nullptr || !entry->graph.has_value()) { return; }
            static_cast<void>(fallback_on_exception(false, [&] {
                entry->graph.view().stop();
                return true;
            }));
        }

        void reset_combiner_noexcept(InPlaceGraphSlotStore<CombinerEntry> &bank,
                                     std::size_t position) noexcept
        {
            CombinerEntry *entry = bank.entry_at(position);
            stop_combiner_noexcept(entry);
            bank.destroy_at(position);
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
                    auto collection = storage.collection_source.view(evaluation_time);
                    if (!collection.bound()) { return TSOutputView{}; }
                    const Value &key = storage.dense_to_key[aggregate.index];
                    return context.collection_ops->leaf_output(
                        std::move(collection), aggregate.index, key,
                        storage.dense_to_source_slot[aggregate.index]);
                }
                case Aggregate::Kind::Node:
                {
                    const auto &output_binding = context.spec.child.output_binding;
                    if (output_binding->kind == NestedGraphOutputBinding::Kind::ParentInput)
                    {
                        const std::size_t side = output_binding->parent_source_path[0];
                        Aggregate source = resolve_aggregate(storage, 2 * aggregate.index + 1 + side);
                        auto output = aggregate_output(view, context, storage, source, evaluation_time);
                        if (output.bound() && output_binding->parent_source_path.size() > 1)
                        {
                            output = walk_ts_path(
                                std::move(output),
                                std::span<const std::size_t>{output_binding->parent_source_path}.subspan(1));
                        }
                        return output;
                    }
                    const auto *entry = storage.combiners[aggregate.index];
                    if (entry == nullptr || !entry->graph.has_value()) { return TSOutputView{}; }
                    if (entry->output.bound()) { return entry->output.view(evaluation_time); }
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
                    source = walk_ts_path(
                        std::move(source),
                        std::span<const std::size_t>{binding.source_path}.subspan(1));
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
                storage.dense_to_source_slot[leaf]              = storage.dense_to_source_slot[last];
                storage.key_to_leaf[storage.dense_to_key[leaf]] = leaf;
            }
            storage.dense_to_key.pop_back();
            storage.dense_to_source_slot.pop_back();
        }

        void clear_leaf_state(ReduceNodeStorage &storage)
        {
            storage.dense_to_key.clear();
            storage.dense_to_source_slot.clear();
            storage.key_to_leaf.clear();
        }

        void record_removed_leaf_paths(ReduceNodeStorage &storage, std::size_t leaf)
        {
            const std::size_t last = storage.dense_to_key.size() - 1;
            storage.structural_leaves.push_back(leaf);
            if (leaf != last) { storage.structural_leaves.push_back(last); }
        }

        [[nodiscard]] bool reconcile_leaf_state(ReduceNodeStorage &storage, const TSDDataView &dict, bool full)
        {
            if (full)
            {
                clear_leaf_state(storage);
                storage.dense_to_key.reserve(dict.size());
                storage.dense_to_source_slot.reserve(dict.size());
                storage.key_to_leaf.reserve(dict.size());
                for (std::size_t slot = 0; slot < dict.slot_capacity(); ++slot)
                {
                    if (!dict.slot_live(slot)) { continue; }
                    Value key{dict.key_at_slot(slot)};
                    storage.key_to_leaf.emplace(key, storage.dense_to_key.size());
                    storage.dense_to_key.push_back(std::move(key));
                    storage.dense_to_source_slot.push_back(slot);
                }
                return true;
            }

            bool structural = false;
            for (std::size_t slot = dict.next_removed_slot(); slot != TS_DATA_NO_CHILD_ID;
                 slot = dict.next_removed_slot(slot))
            {
                const auto found = storage.key_to_leaf.find(dict.key_at_slot(slot));
                if (found == storage.key_to_leaf.end()) { continue; }
                record_removed_leaf_paths(storage, found->second);
                remove_leaf_at(storage, found->second);
                structural = true;
            }

            for (std::size_t slot = dict.next_added_slot(); slot != TS_DATA_NO_CHILD_ID;
                 slot = dict.next_added_slot(slot))
            {
                Value typed_key{dict.key_at_slot(slot)};
                if (storage.key_to_leaf.find(typed_key) != storage.key_to_leaf.end()) { continue; }
                storage.structural_leaves.push_back(storage.dense_to_key.size());
                storage.key_to_leaf.emplace(typed_key, storage.dense_to_key.size());
                storage.dense_to_key.push_back(std::move(typed_key));
                storage.dense_to_source_slot.push_back(slot);
                structural = true;
            }

            return structural;
        }

        [[nodiscard]] bool reconcile_leaf_state(ReduceNodeStorage &storage, const TSLInputView &list_input)
        {
            bool structural = false;
            while (storage.dense_to_key.size() > list_input.size())
            {
                record_removed_leaf_paths(storage, storage.dense_to_key.size() - 1);
                remove_leaf_at(storage, storage.dense_to_key.size() - 1);
                structural = true;
            }
            while (storage.dense_to_key.size() < list_input.size())
            {
                const std::size_t index = storage.dense_to_key.size();
                storage.structural_leaves.push_back(index);
                Value key{static_cast<Int>(index)};
                storage.key_to_leaf.emplace(key, index);
                storage.dense_to_key.push_back(std::move(key));
                storage.dense_to_source_slot.push_back(index);
                structural = true;
            }
            return structural;
        }

        [[nodiscard]] bool reconcile_dict_collection(ReduceNodeStorage &storage, TSInputView &, bool full)
        {
            auto data = storage.collection_source.data_view();
            auto dict = data.as_dict();
            return reconcile_leaf_state(storage, dict, full);
        }

        [[nodiscard]] bool reconcile_list_collection(ReduceNodeStorage &storage, TSInputView &input, bool)
        {
            return reconcile_leaf_state(storage, input.as_list());
        }

        [[nodiscard]] bool dict_structure_modified(const TSInputView &input, DateTime evaluation_time)
        {
            return input.as_dict().data_view().key_set().modified(evaluation_time);
        }

        [[nodiscard]] bool list_structure_modified(const TSInputView &input, DateTime)
        {
            return input.modified();
        }

        void append_modified_dict_leaves(const ReduceNodeStorage &storage, TSInputView &input,
                                         std::vector<std::size_t> &leaves)
        {
            static_cast<void>(input);
            auto collection = storage.collection_source.data_view();
            auto dict = collection.as_dict();
            for (std::size_t slot = dict.next_modified_slot(); slot != TS_DATA_NO_CHILD_ID;
                 slot = dict.next_modified_slot(slot))
            {
                const auto found = storage.key_to_leaf.find(dict.key_at_slot(slot));
                if (found != storage.key_to_leaf.end()) { leaves.push_back(found->second); }
            }
        }

        void append_modified_list_leaves(const ReduceNodeStorage &, TSInputView &input,
                                         std::vector<std::size_t> &leaves)
        {
            auto list = input.as_list();
            for (auto &&[index, child] : list.modified_items())
            {
                static_cast<void>(child);
                leaves.push_back(index);
            }
        }

        [[nodiscard]] TSOutputView dict_leaf_output(TSOutputView source, std::size_t,
                                                    const Value &, std::size_t source_slot)
        {
            auto dict = source.as_dict();
            return source_slot < dict.slot_capacity() && dict.slot_live(source_slot)
                       ? dict.at_slot(source_slot)
                       : TSOutputView{};
        }

        [[nodiscard]] TSOutputView list_leaf_output(TSOutputView source, std::size_t leaf,
                                                    const Value &, std::size_t)
        {
            auto list = source.as_list();
            return leaf < list.size() ? list.at(leaf) : TSOutputView{};
        }

        [[nodiscard]] const ReduceCollectionOps &reduce_collection_ops_for(
            const TSValueTypeMetaData &schema)
        {
            static const ReduceCollectionOps dict_ops{
                .reconcile = &reconcile_dict_collection,
                .structure_modified = &dict_structure_modified,
                .append_modified_leaves = &append_modified_dict_leaves,
                .leaf_output = &dict_leaf_output,
            };
            static const ReduceCollectionOps list_ops{
                .reconcile = &reconcile_list_collection,
                .structure_modified = &list_structure_modified,
                .append_modified_leaves = &append_modified_list_leaves,
                .leaf_output = &list_leaf_output,
            };
            return schema.kind == TSTypeKind::TSD ? dict_ops : list_ops;
        }

        [[nodiscard]] TSOutputHandle effective_output_handle(TSOutputView source)
        {
            if (!source.bound()) { return {}; }

            TSOutputHandle current = source.handle();
            while (source.forwarding())
            {
                TSOutputHandle target = source.forwarding_target();
                if (!target.bound() || target.same_as(current)) { break; }
                current = target;
                source = target.view(source.evaluation_time());
            }
            return current;
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
        void append_structural_leaf_path(const ReduceNodeStorage &storage, std::size_t leaf,
                                         std::vector<std::size_t> &positions)
        {
            std::size_t position = internal_count(storage) + leaf;
            while (position > 0)
            {
                position = (position - 1) / 2;
                if (position < storage.combiners.size()) { positions.push_back(position); }
            }
        }

        void rebuild_structure(const NodeView &view, const ReduceNodeContext &context, ReduceNodeStorage &storage,
                               DateTime evaluation_time, bool full_structure)
        {
            const std::size_t old_capacity = storage.leaf_capacity;
            const std::size_t live = storage.dense_to_key.size();
            std::size_t       capacity = storage.leaf_capacity;
            if (live > 0) { capacity = std::max({capacity, std::size_t{1}, std::bit_ceil(live)}); }

            const std::size_t old_bank = storage.current_bank;
            std::vector<CombinerEntry *> retired_shape{};
            std::vector<std::pair<std::size_t, CombinerEntry *>> retired{};
            std::vector<std::size_t> created{};
            if (capacity != storage.leaf_capacity)
            {
                full_structure = true;
                // Capacity growth re-shapes the tree wholesale: every existing
                // combiner retires and the needed set is rebuilt (the recorded
                // v1 simplification; capacity is monotonic, shrink is a
                // refinement).
                const std::size_t next_bank = 1U - storage.current_bank;
                auto &new_bank = storage.combiner_banks[next_bank];
                if (new_bank.has_entries())
                {
                    throw std::logic_error("reduce_ inactive combiner bank is still occupied");
                }
                const std::size_t next_combiner_count = capacity > 1 ? capacity - 1 : 0;
                new_bank.reserve_to(next_combiner_count);
                std::vector<CombinerEntry *> next_combiners(next_combiner_count, nullptr);

                retired_shape = std::move(storage.combiners);
                storage.combiners = std::move(next_combiners);
                storage.current_bank = next_bank;
                storage.leaf_capacity = capacity;
            }
            else
            {
                storage.combiner_banks[storage.current_bank].reserve_to(storage.combiners.size());
            }

            storage.structural_positions.clear();
            if (full_structure)
            {
                storage.structural_positions.reserve(storage.combiners.size());
                for (std::size_t position = storage.combiners.size(); position-- > 0;)
                {
                    storage.structural_positions.push_back(position);
                }
            }
            else
            {
                for (const std::size_t leaf : storage.structural_leaves)
                {
                    append_structural_leaf_path(storage, leaf, storage.structural_positions);
                }
                std::ranges::sort(storage.structural_positions, std::greater{});
                const auto unique_end = std::ranges::unique(storage.structural_positions).begin();
                storage.structural_positions.erase(unique_end, storage.structural_positions.end());
            }
            auto rollback = UnwindCleanupGuard([&] {
                auto &current_bank = storage.combiner_banks[storage.current_bank];
                if (storage.leaf_capacity != old_capacity)
                {
                    for (const std::size_t position : created)
                    {
                        reset_combiner_noexcept(current_bank, position);
                    }
                    storage.combiners    = std::move(retired_shape);
                    storage.leaf_capacity = old_capacity;
                    storage.current_bank = old_bank;
                    return;
                }

                for (const std::size_t position : created)
                {
                    if (position < storage.combiners.size())
                    {
                        reset_combiner_noexcept(current_bank, position);
                        storage.combiners[position] = nullptr;
                    }
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
            for (const std::size_t position : storage.structural_positions)
            {
                const Aggregate left   = resolve_aggregate(storage, 2 * position + 1);
                const Aggregate right  = resolve_aggregate(storage, 2 * position + 2);
                const bool      needed = left.kind != Aggregate::Kind::Empty &&
                                         right.kind != Aggregate::Kind::Empty;
                auto &entry = storage.combiners[position];

                if (needed && entry == nullptr)
                {
                    auto &bank = storage.combiner_banks[storage.current_bank];
                    entry = &bank.construct_at(position);
                    created.push_back(position);
                    entry->graph = context.spec.child.graph_builder.make_nested_graph(
                        view.pointer(),
                        bank.graph_memory(position), context.graph_layout);
                    if (context.spec.child.output_binding->kind != NestedGraphOutputBinding::Kind::ParentInput)
                    {
                        entry->output = walk_ts_path(
                            entry->graph.view()
                                .node_at(context.spec.child.output_binding->source.node)
                                .output(evaluation_time),
                            context.spec.child.output_binding->source.path).handle();
                    }
                }
                else if (!needed && entry != nullptr)
                {
                    retired.emplace_back(position, std::exchange(entry, nullptr));
                }
            }

            // Phase 2 — generic combiners need their child-graph inputs bound
            // and graphs started. A lifted scalar capability reads aggregate
            // outputs directly, so activating the otherwise-unused child
            // inputs would only create redundant subscription/scheduler work.
            if (context.spec.lifted_kernel == nullptr)
            {
                for (auto position_it = storage.structural_positions.rbegin();
                     position_it != storage.structural_positions.rend(); ++position_it)
                {
                    const std::size_t position = *position_it;
                    auto &entry = storage.combiners[position];
                    if (entry == nullptr) { continue; }
                    const Aggregate left  = resolve_aggregate(storage, 2 * position + 1);
                    const Aggregate right = resolve_aggregate(storage, 2 * position + 2);
                    bind_combiner_inputs(view, context, storage, *entry, left, right, evaluation_time);
                }
                for (auto it = created.rbegin(); it != created.rend(); ++it)
                {
                    auto child = storage.combiners[*it]->graph.view();
                    child.start(evaluation_time);
                    schedule_sampled_input_consumers(
                        child, evaluation_time, context.spec.child.input_bindings);
                }
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
                source = view.input(evaluation_time).indexed_child_at(1).bound_output();
            }
            bind_reduce_output(view.output(evaluation_time), source, evaluation_time);
            storage.published = true;

            // Phase 3 — stop root-first, but keep the retired graph storage
            // through this engine cycle. Dynamic target links may still read
            // the old aggregate later in the same cycle.
            const std::size_t retired_shape_count = static_cast<std::size_t>(
                std::count_if(retired_shape.begin(), retired_shape.end(),
                              [](const CombinerEntry *entry) { return entry != nullptr; }));
            storage.previous_generation.reserve(storage.previous_generation.size() + retired.size() +
                                                retired_shape_count);
            for (auto it = retired.rbegin(); it != retired.rend(); ++it)
            {
                stop_combiner_noexcept(it->second);
                storage.previous_generation.push_back({storage.current_bank, it->first});
            }
            for (std::size_t position = 0; position < retired_shape.size(); ++position)
            {
                CombinerEntry *entry = retired_shape[position];
                stop_combiner_noexcept(entry);
                if (entry != nullptr) { storage.previous_generation.push_back({old_bank, position}); }
            }
            if (!storage.previous_generation.empty()) { storage.previous_generation_time = evaluation_time; }
            rollback.release();
        }

        // Reconcile the leaf state + rebuild the combiner tree for this cycle (the
        // "setup"). Skipped on a pause/resume re-entry (the structure does not change
        // mid-cycle).
        [[nodiscard]] bool reduce_reconcile(const NodeView &view, const ReduceNodeContext &context,
                                            ReduceNodeStorage &storage, DateTime evaluation_time)
        {
            auto root_input = view.input(evaluation_time);
            auto collection_input = root_input.indexed_child_at(0);
            auto zero_input = root_input.indexed_child_at(1);

            TSOutputHandle collection_source = effective_output_handle(collection_input.bound_output());
            TSOutputHandle zero_source = effective_output_handle(zero_input.bound_output());
            const bool collection_repointed = storage.source_handles_initialised &&
                                              !collection_source.same_as(storage.collection_source);
            const bool zero_repointed = storage.source_handles_initialised &&
                                        !zero_source.same_as(storage.zero_source);
            storage.collection_source = collection_source;
            storage.zero_source = zero_source;
            storage.source_handles_initialised = true;

            storage.structural_leaves.clear();
            storage.structural_positions.clear();
            bool structural = false;
            bool full_structure = collection_repointed || !storage.published;
            if (collection_input.valid())
            {
                if (!storage.primed || collection_repointed ||
                    context.collection_ops->structure_modified(collection_input, evaluation_time))
                {
                    structural = context.collection_ops->reconcile(
                                     storage, collection_input, !storage.primed || collection_repointed) ||
                                 structural;
                    full_structure = full_structure || !storage.primed;
                    storage.primed  = true;
                }
            }
            else if (storage.primed || !storage.dense_to_key.empty())
            {
                clear_leaf_state(storage);
                structural = true;
                full_structure = true;
                storage.primed = false;
            }

            if (structural || collection_repointed || zero_repointed || !storage.published)
            {
                rebuild_structure(view, context, storage, evaluation_time, full_structure);
                return true;
            }
            return false;
        }

        void append_leaf_path(const ReduceNodeStorage &storage, std::size_t leaf,
                              SlotBitmap &positions)
        {
            std::size_t position = internal_count(storage) + leaf;
            while (position > 0)
            {
                position = (position - 1) / 2;
                if (position < storage.combiners.size() && storage.combiners[position] != nullptr)
                {
                    positions.set(position);
                }
            }
        }

        void materialize_descending(const SlotBitmap &candidates,
                                    std::vector<std::size_t> &positions)
        {
            for (std::size_t word_index = candidates.word_count(); word_index-- > 0;)
            {
                std::uint64_t word = candidates.words[word_index];
                while (word != 0)
                {
                    const auto bit = static_cast<std::size_t>(63U - std::countl_zero(word));
                    const std::size_t position = word_index * SlotBitmap::bits_per_word + bit;
                    if (position < candidates.size()) { positions.push_back(position); }
                    word &= ~(std::uint64_t{1} << bit);
                }
            }
        }

        void prepare_reduce_evaluation_positions(const NodeView &view, const ReduceNodeContext &context,
                                                  ReduceNodeStorage &storage, DateTime evaluation_time,
                                                  bool rebuilt)
        {
            storage.evaluation_positions.clear();
            storage.resume_candidate_plus_one = 0;
            storage.evaluation_candidates.resize(storage.combiners.size());
            storage.evaluation_candidates.reset();

            auto root_input = view.input(evaluation_time);
            auto collection_input = root_input.indexed_child_at(0);
            auto zero_input = root_input.indexed_child_at(1);
            const bool collection_event = collection_input.modified();
            const bool input_event = collection_event || zero_input.modified();
            bool full_scan = storage.has_future_combiner_schedule || (!rebuilt && !input_event);

            if (rebuilt && !full_scan)
            {
                storage.evaluation_positions.assign(storage.structural_positions.begin(),
                                                    storage.structural_positions.end());
            }

            if (!rebuilt && !full_scan && collection_event && collection_input.valid())
            {
                storage.modified_leaves.clear();
                context.collection_ops->append_modified_leaves(
                    storage, collection_input, storage.modified_leaves);
                for (const std::size_t leaf : storage.modified_leaves)
                {
                    append_leaf_path(storage, leaf, storage.evaluation_candidates);
                }
            }

            if (full_scan)
            {
                storage.evaluation_positions.reserve(storage.combiners.size());
                for (std::size_t position = storage.combiners.size(); position-- > 0;)
                {
                    if (storage.combiners[position] != nullptr)
                    {
                        storage.evaluation_positions.push_back(position);
                    }
                }
            }
            else
            {
                materialize_descending(storage.evaluation_candidates, storage.evaluation_positions);
            }
            storage.has_future_combiner_schedule = false;
        }

        [[nodiscard]] bool evaluate_lifted_combiner(const NodeView &view,
                                                     const ReduceNodeContext &context,
                                                     const ReduceNodeStorage &storage,
                                                     std::size_t position,
                                                     DateTime evaluation_time)
        {
            const LiftedKernel *kernel = context.spec.lifted_kernel;
            if (kernel == nullptr) { return false; }

            TSOutputView left = aggregate_output(
                view, context, storage, resolve_aggregate(storage, 2 * position + 1), evaluation_time);
            TSOutputView right = aggregate_output(
                view, context, storage, resolve_aggregate(storage, 2 * position + 2), evaluation_time);
            // Live dynamic collection entries acquire a value when created,
            // but retain the generic sampled behavior if a future collection
            // kind can expose an unset leaf: no output is published until
            // both sides have a current value.
            if (!left.valid() || !right.valid()) { return true; }

            std::array<ValueView, 2> args{left.value(), right.value()};
            TSOutputView destination = aggregate_output(
                view, context, storage, Aggregate{Aggregate::Kind::Node, position}, evaluation_time);
            if (!destination.bound())
            {
                throw std::logic_error("reduce lifted combiner output is unbound");
            }
            auto mutation = destination.begin_mutation(evaluation_time);
            kernel->eval_into(mutation,
                              std::span<const ValueView>{args.data(), args.size()});
            return true;
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
            storage.initialise(context.graph_layout);

            const bool resuming = storage.resume_candidate_plus_one != 0;
            if (!resuming)
            {
                storage.destroy_previous_generation_before(evaluation_time);
                const bool rebuilt = reduce_reconcile(view, context, storage, evaluation_time);
                prepare_reduce_evaluation_positions(view, context, storage, evaluation_time, rebuilt);
            }

            // Evaluate due combiners deepest-first (descending heap index): a leaf tick
            // schedules its combiner directly; that combiner's output tick schedules its
            // parent (lower index, processed later), so the cascade settles in one pass.
            // On resume, continue from the paused position downward (positions above it
            // already ran this cycle). Unevaluated children pull future schedules up to
            // this reduce node.
            const std::size_t start_candidate = resuming ? storage.resume_candidate_plus_one - 1 : 0;
            for (std::size_t candidate = start_candidate;
                 candidate < storage.evaluation_positions.size(); ++candidate)
            {
                const std::size_t position = storage.evaluation_positions[candidate];
                const auto &entry = storage.combiners[position];
                if (entry == nullptr || !entry->graph.has_value()) { continue; }
                if (!resuming && evaluate_lifted_combiner(
                                     view, context, storage, position, evaluation_time))
                {
                    continue;
                }
                auto       child       = entry->graph.view();
                const bool resume_this = resuming && candidate == start_candidate;
                if ((child.next_scheduled_time() <= evaluation_time || resume_this) &&
                    !child.evaluate(evaluation_time))
                {
                    storage.resume_candidate_plus_one = candidate + 1;
                    return false;
                }
                if (const DateTime next = child.next_scheduled_time(); next != MAX_DT && next > evaluation_time)
                {
                    storage.has_future_combiner_schedule = true;
                    view.graph().schedule_node(view.node_index(), next);
                }
            }
            storage.resume_candidate_plus_one = 0;
            storage.evaluation_positions.clear();
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
            for (const auto *entry : storage.combiners)
            {
                if (entry != nullptr && entry->graph.has_value()) { entry->graph.view().stop(); }
            }
            storage.evaluation_positions.clear();
            storage.modified_leaves.clear();
            storage.structural_leaves.clear();
            storage.structural_positions.clear();
            storage.resume_candidate_plus_one = 0;
            storage.has_future_combiner_schedule = false;
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
            if (fields[0].type == nullptr ||
                (fields[0].type->kind != TSTypeKind::TSD &&
                 (fields[0].type->kind != TSTypeKind::TSL || fields[0].type->fixed_size() != 0)))
            {
                throw std::invalid_argument("reduce_node first input must be a TSD or dynamic TSL");
            }
            if (meta.output_schema == nullptr)
            {
                throw std::invalid_argument("reduce_node requires an output schema");
            }

            const std::size_t child_node_count = spec.child.graph_builder.node_count();
            const auto       &output_binding   = *spec.child.output_binding;
            if (!output_binding.target_path.empty())
            {
                throw std::invalid_argument("reduce_node combiner output binding must target the output root");
            }
            if (output_binding.kind == NestedGraphOutputBinding::Kind::ParentInput)
            {
                if (output_binding.parent_source_path.empty() || output_binding.parent_source_path[0] > 1)
                {
                    throw std::invalid_argument(
                        "reduce_node parent-input combiner output must select lhs or rhs");
                }
            }
            else if (output_binding.source.node >= child_node_count)
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
        for (const auto *entry : combiners)
        {
            if (entry != nullptr) { ++count; }
        }
        return count;
    }

    bool ReduceNodeView::child_graphs_use_in_place_storage() const noexcept
    {
        const auto &combiners = MemoryUtils::cast<ReduceNodeStorage>(storage_)->combiners;
        for (const auto *entry : combiners)
        {
            if (entry != nullptr && entry->graph.has_value() && !entry->graph.uses_external_storage()) { return false; }
        }
        return true;
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
        meta.output_endpoint_schema = reduce_output_endpoint_schema(meta.output_schema);

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

        const MemoryUtils::StorageLayout graph_layout = spec.child.graph_builder.nested_storage_layout();
        const ReduceCollectionOps &collection_ops =
            reduce_collection_ops_for(*descriptor.schema.input_schema->fields()[0].type);

        descriptor.callbacks.stop            = &reduce_node_stop;
        descriptor.ops.evaluate_impl         = &reduce_evaluate_impl;
        descriptor.ops.extended_view_type_id = ReduceNodeView::node_view_type_id();
        descriptor.ops.extended_view_context = &register_reduce_node_context(
            std::move(spec), descriptor.storage_plan->component(reduce_storage_field_name).offset,
            graph_layout, collection_ops);

        return NodeBuilder::from_descriptor(std::move(descriptor));
    }
}  // namespace hgraph
