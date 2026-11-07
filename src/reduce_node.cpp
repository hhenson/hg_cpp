#include <hg/reduce/details/reduce_node.hpp>

#include <hg/allocator.hpp>
#include <hg/graph/details/compiled_graph_builder.hpp>
#include <hg/graph/details/graph_wiring_structure.hpp>
#include <hg/graph/details/nested_graph_context.hpp>
#include <hg/evaluation_profile.hpp>
#include <hg/graph/runtime_graph.hpp>
#include <hg/graph/runtime_graph_evaluator.hpp>
#include <hg/node/node_invocation.hpp>
#include <hg/node/runtime_signature_node_template.hpp>
#include <hg/reduce/details/reduce_node_definition.hpp>
#include <hg/time_series/ref/ref_input.hpp>
#include <hg/time_series/time_series_runtime_type.hpp>
#include <hg/time_series/ts/ts_output.hpp>
#include <hg/time_series/tsd/tsd_input.hpp>
#include <hg/type/time_series_dict_type.hpp>
#include <hg/type/time_series_reference_type.hpp>
#include <hg/type/time_series_value_type.hpp>
#include <hg/util/hash_dictionary.hpp>
#include <hg/util/occupancy_index.hpp>
#include <hg/util/sparse_bunch.hpp>

#include <algorithm>
#include <array>
#include <cassert>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory_resource>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

namespace hg::details {
namespace {

struct CompactReduceRuntime;
struct NestedReduceRuntime;

struct CompactReduceNodeState {
    CompactReduceRuntime *runtime{};
};

struct NestedReduceNodeState {
    NestedReduceRuntime *runtime{};
};

using CompactReduceInvocation = NodeInvocation<CompiledCompactReduceGraph, CompactReduceNodeState>;
using NestedReduceInvocation = NodeInvocation<CompiledNestedReduceGraph, NestedReduceNodeState>;

using ReduceNodeIndex = util::occupancy_index::size_type;
inline constexpr auto invalid_reduce_node = util::occupancy_index::npos;
inline constexpr auto compact_pair_storage_block_size = std::size_t{4096};

struct ReduceTreeNode {
    ReduceNodeIndex parent{invalid_reduce_node};
    ReduceNodeIndex left{invalid_reduce_node};
    ReduceNodeIndex right{invalid_reduce_node};
    union {
        item_id_t item_id{};
        ReduceNodeIndex pair_index;
    };
    std::uint32_t flags{leaf_flag};

    static constexpr std::uint32_t leaf_flag = std::uint32_t{1} << 31;
    static constexpr std::uint32_t left_changed_flag = std::uint32_t{1} << 30;
    static constexpr std::uint32_t right_changed_flag = std::uint32_t{1} << 29;
    static constexpr std::uint32_t left_replaced_flag = std::uint32_t{1} << 28;
    static constexpr std::uint32_t right_replaced_flag = std::uint32_t{1} << 27;
    static constexpr std::uint32_t evaluation_requested_flag = std::uint32_t{1} << 26;
    static constexpr std::uint32_t split_bit_mask = 0x1f;

    bool is_leaf() const noexcept { return (flags & leaf_flag) != 0; }
    std::uint8_t split_bit() const noexcept { return static_cast<std::uint8_t>(flags & split_bit_mask); }
    bool changed() const noexcept {
        return (flags & (left_changed_flag | right_changed_flag | evaluation_requested_flag)) != 0;
    }
    void mark_input_changed(std::size_t side) noexcept {
        flags |= side == 0 ? left_changed_flag : right_changed_flag;
    }
    bool input_replaced(std::size_t side) const noexcept {
        return (flags & (side == 0 ? left_replaced_flag : right_replaced_flag)) != 0;
    }
    void mark_input_replaced(std::size_t side) noexcept {
        flags |= side == 0 ? left_replaced_flag : right_replaced_flag;
        mark_input_changed(side);
    }
    void request_evaluation() noexcept { flags |= evaluation_requested_flag; }
    void clear_input_changes() noexcept {
        flags &= ~(left_changed_flag | right_changed_flag | left_replaced_flag | right_replaced_flag |
                   evaluation_requested_flag);
    }
};

// Stores the valid items of a TSD in a compressed binary Patricia tree keyed by physical item ID. Each branch holds
// the index of the reducer pair that combines its children. The owning runtime supplies pairs through
// acquire_pair(branch_index), which returns invalid_reduce_node when branches have no pair, and
// release_pair(pair_index).
class ReduceTree {
  public:
    explicit ReduceTree(allocator memory) : memory_{memory} {}

    ReduceTree(const ReduceTree &) = delete;
    ReduceTree &operator=(const ReduceTree &) = delete;

    ReduceTreeNode &operator[](ReduceNodeIndex index) noexcept { return nodes_[index]; }
    const ReduceTreeNode &operator[](ReduceNodeIndex index) const noexcept { return nodes_[index]; }
    ReduceNodeIndex root() const noexcept { return root_; }
    bool empty() const noexcept { return root_ == invalid_reduce_node; }
    bool root_changed() const noexcept { return root_changed_; }
    void clear_root_changed() noexcept { root_changed_ = false; }

    template <typename Values, typename Pairs> void insert_valid_items(const Values &values, Pairs &pairs) {
        values.for_each_id_value([&](const auto id, const auto &item) {
            if (item.valid()) {
                insert(id, pairs);
            }
        });
    }

    // Applies the current TSD changes and marks the path of each updated item.
    template <typename Values, typename Pairs> void update(const Values &values, Pairs &pairs) {
        if (!values.modified()) {
            return;
        }
        values.for_each_removed_physical_id([&](item_id_t id) { remove(id, pairs); });
        values.for_each_item_id_remapping([&](item_id_t old_id, item_id_t) { remove(old_id, pairs); });

        values.for_each_item_id_remapping([&](item_id_t, item_id_t new_id) {
            if (item_valid(values, new_id)) {
                insert(new_id, pairs);
            }
        });
        for (auto id = values.next_added_id(0); id != util::occupancy_index::npos; id = values.next_added_id(id + 1)) {
            if (item_valid(values, id)) {
                insert(id, pairs);
            }
        }
        values.for_each_updated([&](const auto &key) {
            const auto id = values.find_item_id(key);
            if (!item_valid(values, id)) {
                remove(id, pairs);
                return;
            }
            if (const auto *leaf = leaf_by_id_.find(id)) {
                mark_path_changed(*leaf);
            } else {
                insert(id, pairs);
            }
        });
    }

    void mark_path_changed(ReduceNodeIndex child) noexcept {
        auto current = child;
        auto parent = nodes_[current].parent;
        while (parent != invalid_reduce_node) {
            auto &branch = nodes_[parent];
            branch.mark_input_changed(branch.left == current ? 0 : 1);
            current = parent;
            parent = branch.parent;
        }
    }

    // Releases the tree nodes. The caller releases the pairs.
    void clear() noexcept {
        auto &memory = memory_.memory_resource();
        leaf_by_id_.release(memory);
        nodes_.release(positions_, memory);
        positions_.release(memory);
        root_ = invalid_reduce_node;
        root_changed_ = false;
    }

  private:
    template <typename Values> static bool item_valid(const Values &values, item_id_t id) {
        const auto &runtime_type = static_cast<const TimeSeriesDictionaryRuntimeType &>(values.runtime_type());
        return runtime_type.item(values.data(), id).last_updated.valid();
    }

    static bool bit(item_id_t id, std::uint8_t bit) noexcept { return ((id >> bit) & 1U) != 0; }

    ReduceNodeIndex allocate_node() {
        auto &memory = memory_.memory_resource();
        const auto index = positions_.acquire(memory);
        try {
            nodes_.emplace_at(index, memory);
        } catch (...) {
            positions_.reset(index);
            throw;
        }
        return index;
    }

    void release_node(ReduceNodeIndex index) noexcept {
        nodes_.erase_at(index, memory_.memory_resource());
        positions_.reset(index);
    }

    template <typename Pairs> ReduceNodeIndex allocate_branch(std::uint8_t split_bit, Pairs &pairs) {
        const auto index = allocate_node();
        auto release = util::scope_exit{[&]() noexcept { release_node(index); }};
        nodes_[index].flags = split_bit;
        const auto pair_index = pairs.acquire_pair(index);

        nodes_[index].pair_index = pair_index;
        release.release();
        return index;
    }

    template <typename Pairs> void release_branch(ReduceNodeIndex index, Pairs &pairs) noexcept {
        const auto pair_index = nodes_[index].pair_index;
        if (pair_index != invalid_reduce_node) {
            pairs.release_pair(pair_index);
        }
        release_node(index);
    }

    template <typename Pairs> void insert(item_id_t id, Pairs &pairs) {
        if (leaf_by_id_.contains(id)) {
            return;
        }

        const auto leaf_index = allocate_node();
        auto release_leaf = util::scope_exit{[&]() noexcept { release_node(leaf_index); }};
        auto &leaf = nodes_[leaf_index];
        leaf.item_id = id;
        leaf_by_id_.try_emplace(id, memory_.memory_resource(), leaf_index);
        auto remove_leaf_record = util::scope_exit{[&]() noexcept {
            leaf_by_id_.erase(id, memory_.memory_resource());
        }};

        if (root_ == invalid_reduce_node) {
            root_ = leaf_index;
            root_changed_ = true;
            release_leaf.release();
            remove_leaf_record.release();
            return;
        }

        auto existing_index = root_;
        while (!nodes_[existing_index].is_leaf()) {
            const auto &branch = nodes_[existing_index];
            existing_index = bit(id, branch.split_bit()) ? branch.right : branch.left;
        }
        const auto existing_id = nodes_[existing_index].item_id;
        const auto differing_bits = id ^ existing_id;
        const auto split_bit = static_cast<std::uint8_t>(std::bit_width(differing_bits) - 1);

        auto parent = invalid_reduce_node;
        auto subtree = root_;
        while (!nodes_[subtree].is_leaf() && nodes_[subtree].split_bit() > split_bit) {
            parent = subtree;
            const auto &branch = nodes_[subtree];
            subtree = bit(id, branch.split_bit()) ? branch.right : branch.left;
        }

        const auto branch_index = allocate_branch(split_bit, pairs);
        auto release_new_branch = util::scope_exit{[&]() noexcept { release_branch(branch_index, pairs); }};
        auto &branch = nodes_[branch_index];
        branch.parent = parent;
        branch.mark_input_changed(0);
        branch.mark_input_changed(1);
        if (bit(id, split_bit)) {
            branch.left = subtree;
            branch.right = leaf_index;
        } else {
            branch.left = leaf_index;
            branch.right = subtree;
        }
        nodes_[subtree].parent = branch_index;
        leaf.parent = branch_index;

        if (parent == invalid_reduce_node) {
            root_ = branch_index;
            root_changed_ = true;
        } else {
            auto &old_parent = nodes_[parent];
            if (old_parent.left == subtree) {
                old_parent.left = branch_index;
                old_parent.mark_input_replaced(0);
            } else {
                old_parent.right = branch_index;
                old_parent.mark_input_replaced(1);
            }
        }

        mark_path_changed(branch_index);
        release_new_branch.release();
        release_leaf.release();
        remove_leaf_record.release();
    }

    template <typename Pairs> void remove(item_id_t id, Pairs &pairs) noexcept {
        const auto *found = leaf_by_id_.find(id);
        if (found == nullptr) {
            return;
        }
        const auto leaf_index = *found;
        leaf_by_id_.erase(id, memory_.memory_resource());
        const auto parent = nodes_[leaf_index].parent;
        if (parent == invalid_reduce_node) {
            root_ = invalid_reduce_node;
            release_node(leaf_index);
            root_changed_ = true;
            return;
        }

        auto &branch = nodes_[parent];
        const auto sibling = branch.left == leaf_index ? branch.right : branch.left;
        const auto grandparent = branch.parent;
        nodes_[sibling].parent = grandparent;
        if (grandparent == invalid_reduce_node) {
            root_ = sibling;
            root_changed_ = true;
        } else {
            auto &ancestor = nodes_[grandparent];
            if (ancestor.left == parent) {
                ancestor.left = sibling;
                ancestor.mark_input_replaced(0);
            } else {
                ancestor.right = sibling;
                ancestor.mark_input_replaced(1);
            }
            mark_path_changed(sibling);
        }

        release_branch(parent, pairs);
        release_node(leaf_index);
    }

    allocator memory_;
    util::hash_dictionary<item_id_t, ReduceNodeIndex> leaf_by_id_;
    util::occupancy_index positions_;
    util::sparse_bunch<ReduceTreeNode> nodes_;
    ReduceNodeIndex root_{invalid_reduce_node};
    bool root_changed_{};
};

struct CompactReduceInput {
    item_id_t id{};
    TimeSeriesState last_updated;
    const void *value{};
    const TimeSeriesRuntimeType *runtime_type{};
};

struct CompactReducePairState {
    CompiledGraph::ScheduleWord input_validity{};
    std::array<TimeSeriesState, 2> input_states{};
    bool initialized{};
    bool profile_started{};
};

class CompactReduceRuntime {
  public:
    explicit CompactReduceRuntime(allocator memory)
        : memory_{memory}, tree_{memory}, compact_pair_blocks_{&memory_.memory_resource()} {}

    CompactReduceRuntime(const CompactReduceRuntime &) = delete;
    CompactReduceRuntime &operator=(const CompactReduceRuntime &) = delete;

    void evaluate(const CompiledCompactReduceGraph &compiled, CompactReduceInvocation invocation) {
        const auto values = invocation.template input<In<TSD<SCALAR, TS_T>>>(compiled.dictionary_parameter);
        if (!values.valid()) {
            clear_compact_graph();
            initialized_ = false;
            invocation.template output<TS_T>().invalidate();
            using_default_ = false;
            return;
        }

        auto force_publish = false;
        if (!initialized_) {
            build_compact_graph(compiled);
            tree_.insert_valid_items(values, *this);
            initialized_ = true;
            force_publish = true;
        } else {
            tree_.update(values, *this);
        }

        evaluate_compact_graph(values, invocation);
        publish_compact_result(compiled, values, invocation, force_publish);
    }

    void start(EvaluationContext &context) noexcept {
        profile_ = context.evaluation_profile();
        parent_graph_id_ = context.runtime_graph().instance_id();
        owner_node_index_ = static_cast<std::uint32_t>(context.node_index());
    }

    void stop() noexcept {
        if (profile_ != nullptr) {
            for (const auto pair_index : compact_pair_positions_) {
                stop_compact_pair(pair_index);
            }
        }
        profile_ = nullptr;
    }

    void destroy() noexcept {
        profile_ = nullptr;
        clear_compact_graph();
        memory_.delete_object(this);
    }

    ReduceNodeIndex acquire_pair(ReduceNodeIndex) {
        return compact_has_output_node_ ? allocate_compact_pair() : invalid_reduce_node;
    }

    void release_pair(ReduceNodeIndex index) noexcept { release_compact_pair(index); }

  private:
    template <typename Values> CompactReduceInput compact_item(const Values &values, item_id_t id) const {
        const auto &runtime_type =
            static_cast<const TimeSeriesDictionaryRuntimeType &>(values.runtime_type());
        const auto item = runtime_type.item(values.data(), id);
        return {id, item.last_updated, item.last_updated.valid() ? item.value : nullptr, &item.runtime_type};
    }

    ReduceNodeIndex allocate_compact_pair() {
        auto &memory = memory_.memory_resource();
        const auto index = compact_pair_positions_.acquire(memory);
        try {
            compact_pair_states_.emplace_at(index, memory);
            initialize_compact_pair_storage(index);
            start_compact_pair(index);
        } catch (...) {
            if (compact_pair_states_.get(index, compact_pair_positions_) != nullptr) {
                compact_pair_states_.erase_at(index, memory);
            }
            compact_pair_positions_.reset(index);
            throw;
        }
        return index;
    }

    void release_compact_pair(ReduceNodeIndex index) noexcept {
        stop_compact_pair(index);
        compact_pair_states_.erase_at(index, memory_.memory_resource());
        compact_pair_positions_.reset(index);
    }

    void start_compact_pair(ReduceNodeIndex index) noexcept {
        if (profile_ == nullptr) {
            return;
        }

        const auto graph_id = GraphInstanceId{compact_graph_id_base_.value + index};
        profile_->start_graph(graph_id, *compact_graph_->graph, parent_graph_id_, owner_node_index_, std::nullopt);
        compact_pair_states_[index].profile_started = true;
    }

    void stop_compact_pair(ReduceNodeIndex index) noexcept {
        auto &pair_state = compact_pair_states_[index];
        if (!pair_state.profile_started || profile_ == nullptr) {
            return;
        }

        const auto graph_id = GraphInstanceId{compact_graph_id_base_.value + index};
        profile_->stop_graph(graph_id);
        pair_state.profile_started = false;
    }

    std::byte *compact_pair_storage(ReduceNodeIndex index) const noexcept {
        const auto block_index = index / compact_storage_slots_per_block_;
        const auto block_offset = index % compact_storage_slots_per_block_;
        assert(block_index < compact_pair_blocks_.size());
        assert(compact_pair_blocks_[block_index] != nullptr);
        return compact_pair_blocks_[block_index] + block_offset * compact_storage_stride_;
    }

    void initialize_compact_pair_storage(ReduceNodeIndex index) {
        const auto block_index = index / compact_storage_slots_per_block_;
        while (compact_pair_blocks_.size() <= block_index) {
            compact_pair_blocks_.push_back(nullptr);
        }
        if (compact_pair_blocks_[block_index] == nullptr) {
            compact_pair_blocks_[block_index] = static_cast<std::byte *>(
                memory_.allocate_bytes(compact_storage_block_bytes_, compact_storage_alignment_));
        }

        const auto storage = std::span{compact_pair_storage(index), compact_storage_size_};
        RuntimeGraph::initialize_borrowed_storage(*compact_graph_->graph, storage);
    }

    void build_compact_graph(const CompiledCompactReduceGraph &compiled) {
        clear_compact_graph();
        compact_graph_ = &compiled;
        compact_has_output_node_ = compact_graph_->output_node.has_value();
        if (compact_has_output_node_) {
            compact_storage_size_ = RuntimeGraph::borrowed_storage_size(*compact_graph_->graph);
            compact_storage_alignment_ = RuntimeGraph::borrowed_storage_alignment(*compact_graph_->graph);
            compact_graph_id_base_ = GraphInstanceIdAllocator::reserve(std::uint64_t{1} << 32).at(0);
        }
        if (compact_has_output_node_) {
            if (compact_storage_size_ > std::numeric_limits<std::size_t>::max() - (compact_storage_alignment_ - 1)) {
                throw std::length_error{"reduce compact graph storage is too large"};
            }
            compact_storage_stride_ = (compact_storage_size_ + compact_storage_alignment_ - 1) &
                                      ~(compact_storage_alignment_ - 1);
            if (compact_storage_stride_ == 0) {
                throw std::length_error{"reduce compact graph storage is too large"};
            }
            compact_storage_slots_per_block_ =
                std::max<std::size_t>(1, compact_pair_storage_block_size / compact_storage_stride_);
            if (compact_storage_slots_per_block_ >
                std::numeric_limits<std::size_t>::max() / compact_storage_stride_) {
                throw std::length_error{"reduce compact graph storage is too large"};
            }
            compact_storage_block_bytes_ = compact_storage_slots_per_block_ * compact_storage_stride_;
        }
    }

    void clear_compact_graph() noexcept {
        auto &memory = memory_.memory_resource();
        for (const auto pair_index : compact_pair_positions_) {
            stop_compact_pair(pair_index);
        }
        tree_.clear();
        compact_pair_states_.release(compact_pair_positions_, memory);
        compact_pair_positions_.release(memory);
        for (auto *block : compact_pair_blocks_) {
            if (block != nullptr) {
                memory_.deallocate_bytes(block, compact_storage_block_bytes_, compact_storage_alignment_);
            }
        }
        compact_pair_blocks_.clear();
        compact_storage_size_ = 0;
        compact_storage_alignment_ = 0;
        compact_storage_stride_ = 0;
        compact_storage_slots_per_block_ = 0;
        compact_storage_block_bytes_ = 0;
        compact_graph_ = nullptr;
        initialized_ = false;
        compact_has_output_node_ = false;
    }

    template <typename Values>
    CompactReduceInput compact_tree_output(ReduceNodeIndex index, const Values &values,
                                           const CompiledCompactReduceGraph &compact) const {
        auto current = index;
        while (!tree_[current].is_leaf() && !compact.output_node) {
            const auto &branch = tree_[current];
            current = *compact.output_input == 0 ? branch.left : branch.right;
        }
        const auto &tree_node = tree_[current];
        if (tree_node.is_leaf()) {
            return compact_item(values, tree_node.item_id);
        }

        const auto &compiled_node = compact.graph->node(*compact.output_node);
        const auto runtime_node = Node{compiled_node, compact_pair_storage(tree_node.pair_index)};
        const auto &state = runtime_node.output_last_updated();
        return {invalid_reduce_node, state, state.valid() ? runtime_node.output_value() : nullptr,
                compact.output_runtime_type};
    }

    bool evaluate_compact_branch(ReduceNodeIndex index, const CompiledCompactReduceGraph &compiled,
                                 const auto &values, CompactReduceInvocation invocation) {
        auto &branch = tree_[index];
        const auto evaluation_time = invocation.context().evaluation_time();
        if (branch.is_leaf()) {
            return compact_item(values, branch.item_id).last_updated.time() == evaluation_time;
        }

        if (!branch.changed()) {
            const auto &compiled_node = compiled.graph->node(*compiled.output_node);
            const auto runtime_node = Node{compiled_node, compact_pair_storage(branch.pair_index)};
            return runtime_node.output_last_updated().time() == evaluation_time;
        }

        const auto left_changed = evaluate_compact_branch(branch.left, compiled, values, invocation);
        const auto right_changed = evaluate_compact_branch(branch.right, compiled, values, invocation);
        const auto &compact = compiled;
        auto &pair_state = compact_pair_states_[branch.pair_index];
        const auto left = compact_tree_output(branch.left, values, compact);
        const auto right = compact_tree_output(branch.right, values, compact);
        const auto storage = std::span{compact_pair_storage(branch.pair_index), compact_storage_size_};
        const auto graph_id = GraphInstanceId{compact_graph_id_base_.value + branch.pair_index};
        auto runtime = RuntimeGraph{*compact.graph, memory_, storage, invocation.context().global_state(), graph_id,
                                   pair_state.initialized ? std::optional{pair_state.input_validity} : std::nullopt};
        auto runtime_graph_inputs = std::array<RuntimeGraphInput, 2>{};
        const auto bind_input = [&](std::size_t side, std::size_t binding_index,
                                    const CompactReduceInput &input, bool child_replaced, bool child_changed) {
            auto &state = pair_state.input_states[side];
            auto change = RuntimeGraphInputChange::unchanged;
            if (child_replaced || !pair_state.initialized || child_changed) {
                if (input.last_updated.valid()) {
                    change = state.valid() ? RuntimeGraphInputChange::update : RuntimeGraphInputChange::revalidation;
                    state.publish(evaluation_time);
                } else if (state.valid()) {
                    change = RuntimeGraphInputChange::invalidation;
                    state.invalidate(evaluation_time);
                }
            }
            runtime_graph_inputs[binding_index] = RuntimeGraphInput{&state, input.value, input.runtime_type, change};
        };
        bind_input(0, compact.input_indices[0], left, branch.input_replaced(0), left_changed);
        bind_input(1, compact.input_indices[1], right, branch.input_replaced(1), right_changed);
        if (!runtime.evaluate(invocation.context(), runtime_graph_inputs.data())) {
            throw std::logic_error{"a borrowed reduce graph cannot pend evaluation"};
        }
        const auto &compiled_node = compact.graph->node(*compact.output_node);
        const auto runtime_node = Node{compiled_node, storage.data()};
        pair_state.input_validity = runtime.input_validity();
        pair_state.initialized = true;
        branch.clear_input_changes();
        return runtime_node.output_last_updated().time() == evaluation_time;
    }

    void evaluate_compact_graph(const auto &values, CompactReduceInvocation invocation) {
        if (tree_.empty() || !compact_has_output_node_) {
            return;
        }
        evaluate_compact_branch(tree_.root(), *compact_graph_, values, invocation);
    }

    void publish_compact_result(const CompiledCompactReduceGraph &compiled, const auto &values,
                                CompactReduceInvocation invocation, bool force) {
        auto output = invocation.template output<TS_T>();
        if (tree_.empty()) {
            tree_.clear_root_changed();
            publish_empty_result(compiled, invocation, output);
            return;
        }

        const auto sample = compact_tree_output(tree_.root(), values, compiled);
        if (!sample.last_updated.valid()) {
            output.invalidate();
            using_default_ = false;
            tree_.clear_root_changed();
            return;
        }
        if (!force && !tree_.root_changed() &&
            sample.last_updated.time() != invocation.context().evaluation_time()) {
            return;
        }

        const auto &value_type = static_cast<const TimeSeriesValueRuntimeType &>(*sample.runtime_type);
        const auto value = Scalar<>::from(value_type.scalar_runtime_type(), sample.value);
        output.visit([&](Out<TS<SCALAR>> *target) { target->set_value(value); }, ddv::noop);
        using_default_ = false;
        tree_.clear_root_changed();
    }

    template <typename Output>
    void publish_empty_result(const CompiledCompactReduceGraph &compiled, CompactReduceInvocation invocation,
                              Output &output) {
        if (compiled.default_parameter) {
            const auto fallback = invocation.scalar(*compiled.default_parameter);
            if (!fallback.has_value()) {
                output.invalidate();
                using_default_ = false;
                return;
            }
            if (!using_default_) {
                output.visit([&](Out<TS<SCALAR>> *value) { value->set_value(fallback.value()); }, ddv::noop);
            }
            using_default_ = true;
        } else {
            output.invalidate();
            using_default_ = false;
        }
    }

    allocator memory_;
    ReduceTree tree_;
    util::occupancy_index compact_pair_positions_;
    util::sparse_bunch<CompactReducePairState> compact_pair_states_;
    std::pmr::vector<std::byte *> compact_pair_blocks_;
    const CompiledCompactReduceGraph *compact_graph_{};
    EvaluationProfile *profile_{};
    GraphInstanceId parent_graph_id_{};
    std::uint32_t owner_node_index_{};
    GraphInstanceId compact_graph_id_base_{};
    std::size_t compact_storage_size_{};
    std::size_t compact_storage_alignment_{};
    std::size_t compact_storage_stride_{};
    std::size_t compact_storage_slots_per_block_{};
    std::size_t compact_storage_block_bytes_{};
    bool initialized_{};
    bool using_default_{};
    bool compact_has_output_node_{};
};

struct NestedReducePair {
    NestedGraph graph;
    ReduceNodeIndex branch{invalid_reduce_node};
    // A pair graph reads only the outputs of pair graphs with lower split bits, because a branch's split bit is
    // higher than the split bits below it and does not change.
    std::uint8_t split_bit{};
};

// Evaluates one reducer graph instance for each tree branch. A branch starts its graph when it is first evaluated and
// passes the item references or child graph outputs of its children as graph inputs.
class NestedReduceRuntime {
  public:
    NestedReduceRuntime(allocator memory, EvaluationContext &context)
        : memory_{memory}, tree_{memory}, retired_pairs_{&memory_.memory_resource()} {
        evaluator_.bind_owner(context.runtime_graph(), context.node_index(),
                              {.receiver = this, .inner_scheduled = pair_scheduled});
    }

    NestedReduceRuntime(const NestedReduceRuntime &) = delete;
    NestedReduceRuntime &operator=(const NestedReduceRuntime &) = delete;

    bool evaluate(const CompiledNestedReduceGraph &compiled, NestedReduceInvocation invocation) {
        auto &context = invocation.context();
        const auto values = invocation.template input<In<TSD<SCALAR, TS_T>>>(compiled.dictionary_parameter);
        if (!values.valid()) {
            clear(context);
            invocation.template output<TS_T>().invalidate();
            published_reference_ = {};
            using_default_ = false;
            return true;
        }

        // A pending evaluation resumes in the same cycle, after the tree has applied its changes.
        if (!pending_) {
            auto pairs = Pairs{*this, context};
            if (!initialized_) {
                tree_.insert_valid_items(values, pairs);
                initialized_ = true;
            } else {
                tree_.update(values, pairs);
            }
        }

        const auto items = invocation.template input<In<TSD<SCALAR, REF<TS_T>>>>(compiled.items_parameter);
        pending_ = !tree_.empty() && !evaluate_branch(tree_.root(), compiled, items, invocation);
        if (pending_) {
            return false;
        }
        return publish_result(compiled, invocation, items);
    }

    void stop(EvaluationContext &context) noexcept {
        clear(context);
        // NestedGraph::stop destroys the graph in a later cycle callback, and cycle callbacks run in reverse order.
        // Stopping lower branches first destroys each graph before the graphs whose outputs it reads.
        const auto split_bit = [this](ReduceNodeIndex index) { return pairs_[index].split_bit; };
        std::ranges::sort(retired_pairs_, std::ranges::less{}, split_bit);
        for (const auto index : retired_pairs_) {
            pairs_[index].graph.stop(context);
        }
    }

    void destroy(EvaluationContext &context) noexcept {
        auto &memory = memory_.memory_resource();
        clear(context);
        context.runtime_graph().remove_cycle_callback(this);
        for (const auto index : pair_positions_) {
            context.runtime_graph().remove_cycle_callback(&pairs_[index].graph);
        }
        release_retired_pairs(context);
        pairs_.release(pair_positions_, memory);
        pair_positions_.release(memory);
        retired_pairs_.clear();
        tree_.clear();
        memory_.delete_object(this);
    }

  private:
    struct Pairs {
        NestedReduceRuntime &runtime;
        EvaluationContext &context;

        ReduceNodeIndex acquire_pair(ReduceNodeIndex branch) { return runtime.acquire_pair(branch); }
        void release_pair(ReduceNodeIndex index) noexcept { runtime.retire_pair(index, context); }
    };

    ReduceNodeIndex acquire_pair(ReduceNodeIndex branch) {
        auto &memory = memory_.memory_resource();
        const auto index = pair_positions_.acquire(memory);
        try {
            pairs_.emplace_at(index, memory,
                              NestedReducePair{.branch = branch, .split_bit = tree_[branch].split_bit()});
        } catch (...) {
            pair_positions_.reset(index);
            throw;
        }
        return index;
    }

    // Detaches the pair from the tree. Its graph is destroyed at the end of the outer cycle so that references to the
    // graph output remain valid in the cycle in which the branch is removed.
    void retire_pair(ReduceNodeIndex index, EvaluationContext &context) noexcept {
        pairs_[index].branch = invalid_reduce_node;
        if (retired_pairs_.empty()) {
            context.add_cycle_callback(this, release_retired_pairs_callback);
        }
        retired_pairs_.push_back(index);
    }

    static void pair_scheduled(void *receiver, RuntimeGraphSchedulingHandler::token_type token) noexcept {
        static_cast<NestedReduceRuntime *>(receiver)->request_evaluation(static_cast<ReduceNodeIndex>(token));
    }

    void request_evaluation(ReduceNodeIndex pair_index) noexcept {
        const auto branch = pairs_[pair_index].branch;
        if (branch == invalid_reduce_node) {
            return;
        }
        tree_[branch].request_evaluation();
        tree_.mark_path_changed(branch);
    }

    static void release_retired_pairs_callback(void *receiver, EvaluationContext &context) noexcept {
        static_cast<NestedReduceRuntime *>(receiver)->release_retired_pairs(context);
    }

    // Destroys higher branches first, so each graph is destroyed before the graphs whose outputs it reads.
    void release_retired_pairs(EvaluationContext &context) noexcept {
        auto &memory = memory_.memory_resource();
        const auto split_bit = [this](ReduceNodeIndex index) { return pairs_[index].split_bit; };
        std::ranges::sort(retired_pairs_, std::ranges::greater{}, split_bit);
        for (const auto index : retired_pairs_) {
            pairs_[index].graph.stop_and_destroy(context);
            pairs_.erase_at(index, memory);
            pair_positions_.reset(index);
        }
        retired_pairs_.clear();
    }

    void clear(EvaluationContext &context) noexcept {
        for (const auto index : pair_positions_) {
            if (pairs_[index].branch != invalid_reduce_node) {
                retire_pair(index, context);
            }
        }
        tree_.clear();
        initialized_ = false;
        pending_ = false;
    }

    template <typename Items>
    Reference reference(ReduceNodeIndex index, const CompiledNestedReduceGraph &compiled, const Items &items) const {
        const auto &node = tree_[index];
        if (!node.is_leaf()) {
            return pairs_[node.pair_index].graph.output(compiled.nested);
        }

        const auto &runtime_type = static_cast<const TimeSeriesDictionaryRuntimeType &>(items.runtime_type());
        const auto item = runtime_type.item(items.data(), node.item_id);
        return item.last_updated.valid() ? item.runtime_type.reference(item.value) : Reference{};
    }

    // Evaluates the changed branches below index before the branch at index. Returns false when a reducer graph
    // pends.
    template <typename Items>
    bool evaluate_branch(ReduceNodeIndex index, const CompiledNestedReduceGraph &compiled, const Items &items,
                         NestedReduceInvocation invocation) {
        auto &branch = tree_[index];
        if (branch.is_leaf() || !branch.changed()) {
            return true;
        }
        if (!evaluate_branch(branch.left, compiled, items, invocation) ||
            !evaluate_branch(branch.right, compiled, items, invocation)) {
            return false;
        }

        auto &context = invocation.context();
        auto &graph = pairs_[branch.pair_index].graph;
        const auto inputs = std::array{NestedGraphInput{reference(branch.left, compiled, items)},
                                       NestedGraphInput{reference(branch.right, compiled, items)}};
        if (graph.running()) {
            graph.update_inputs(compiled.nested, inputs, context);
        } else {
            const auto inherited_count = compiled.nested.graph->inherited_time_series_count();
            auto startup = NestedGraphStartupContext{
                invocation, compiled.nested, invocation.parameter_count() - inherited_count, inherited_count};
            graph.start(compiled.nested, evaluator_, startup.context(), inputs, branch.pair_index);
        }
        if (!graph.evaluate(context)) {
            return false;
        }
        branch.clear_input_changes();
        return true;
    }

    template <typename Output>
    bool publish_empty_result(const CompiledNestedReduceGraph &compiled, NestedReduceInvocation invocation,
                              Output &output) {
        if (compiled.default_parameter) {
            if (compiled.reference_output) {
                const auto reference = invocation.template input<In<REF<TS_T>>>(*compiled.default_parameter).value();
                return publish_reference_result(reference, invocation, output, false);
            }

            const auto fallback = invocation.scalar(*compiled.default_parameter);
            if (!fallback.has_value()) {
                output.invalidate();
                using_default_ = false;
                return true;
            }
            if (!using_default_) {
                output.visit([&](Out<TS<SCALAR>> *value) { value->set_value(fallback.value()); }, ddv::noop);
            }
            using_default_ = true;
        } else {
            output.invalidate();
            using_default_ = false;
        }
        return true;
    }

    template <typename Output>
    bool publish_reference_result(Reference reference, NestedReduceInvocation invocation, Output &output, bool force) {
        if (!reference) {
            output.invalidate();
            using_default_ = false;
            return true;
        }

        const auto resolved = ResolvedTimeSeries::resolve(reference);
        const auto current_reference = output.valid() ? *static_cast<const Reference *>(output.data()) : Reference{};
        if (!force && current_reference == reference &&
            resolved.last_updated.time() != invocation.context().evaluation_time()) {
            return true;
        }

        output.visit([&](Out<REF<TS_T>> *target) { target->set_value(reference); }, ddv::noop);
        using_default_ = false;
        return true;
    }

    template <typename Items>
    bool publish_result(const CompiledNestedReduceGraph &compiled, NestedReduceInvocation invocation,
                        const Items &items) {
        auto output = invocation.template output<TS_T>();
        if (tree_.empty()) {
            published_reference_ = {};
            return publish_empty_result(compiled, invocation, output);
        }

        const auto reference = this->reference(tree_.root(), compiled, items);
        const auto force = reference != published_reference_;
        published_reference_ = reference;
        if (compiled.reference_output) {
            return publish_reference_result(reference, invocation, output, force);
        }

        const auto resolved = reference ? ResolvedTimeSeries::resolve(reference) : ResolvedTimeSeries{};
        if (!resolved.valid()) {
            output.invalidate();
            using_default_ = false;
            return true;
        }
        if (!force && resolved.last_updated.time() != invocation.context().evaluation_time()) {
            return true;
        }

        const auto &value_type = static_cast<const TimeSeriesValueRuntimeType &>(*resolved.runtime_type);
        const auto value = Scalar<>::from(value_type.scalar_runtime_type(), resolved.storage);
        output.visit([&](Out<TS<SCALAR>> *target) { target->set_value(value); }, ddv::noop);
        using_default_ = false;
        return true;
    }

    allocator memory_;
    RuntimeGraphEvaluator evaluator_;
    ReduceTree tree_;
    util::occupancy_index pair_positions_;
    util::sparse_bunch<NestedReducePair> pairs_;
    std::pmr::vector<ReduceNodeIndex> retired_pairs_;
    Reference published_reference_;
    bool initialized_{};
    bool pending_{};
    bool using_default_{};
};

class CompactReduceNodeTemplate : public details::CompactReduceNodeDefinition {
  public:
    using state_type = CompactReduceNodeState;

    using details::CompactReduceNodeDefinition::CompactReduceNodeDefinition;

    static void start(CompactReduceInvocation invocation) {
        auto &runtime = invocation.state().runtime;
        if (runtime == nullptr) {
            auto memory = invocation.context().runtime_graph().allocation();
            runtime = memory.new_object<CompactReduceRuntime>(memory);
        }
        runtime->start(invocation.context());
    }

    static void evaluate(CompactReduceInvocation invocation) {
        invocation.state().runtime->evaluate(invocation.compiled_data(), invocation);
    }

    static void stop(CompactReduceInvocation invocation) noexcept {
        invocation.state().runtime->stop();
    }

    static void destroy_state(CompactReduceInvocation invocation) noexcept {
        if (invocation.state().runtime != nullptr) {
            invocation.state().runtime->destroy();
            invocation.state().runtime = nullptr;
        }
    }
};

class NestedReduceNodeTemplate : public details::NestedReduceNodeDefinition {
  public:
    using state_type = NestedReduceNodeState;
    static constexpr bool uses_cycle_callbacks = true;
    static constexpr bool uses_timed_scheduling = true;

    using details::NestedReduceNodeDefinition::NestedReduceNodeDefinition;

    static void start(NestedReduceInvocation invocation) {
        auto &runtime = invocation.state().runtime;
        if (runtime == nullptr) {
            auto memory = invocation.context().runtime_graph().allocation();
            runtime = memory.new_object<NestedReduceRuntime>(memory, invocation.context());
        }
    }

    static bool evaluate(NestedReduceInvocation invocation) {
        return invocation.state().runtime->evaluate(invocation.compiled_data(), invocation);
    }

    static void stop(NestedReduceInvocation invocation) noexcept {
        if (invocation.state().runtime != nullptr) {
            invocation.state().runtime->stop(invocation.context());
        }
    }

    static void destroy_state(NestedReduceInvocation invocation) noexcept {
        if (invocation.state().runtime != nullptr) {
            invocation.state().runtime->destroy(invocation.context());
            invocation.state().runtime = nullptr;
        }
    }
};

} // namespace

std::unique_ptr<NodeTemplate> make_nested_reduce_node(ReduceNestedGraphWiring wiring,
                                                      const ResolvedSignature &signature,
                                                      std::uint32_t dictionary_parameter,
                                                      std::uint32_t items_parameter,
                                                      std::optional<std::uint32_t> default_parameter) {
    return std::make_unique<RuntimeSignatureNodeTemplate<NestedReduceNodeTemplate>>(NestedReduceNodeTemplate{
        std::move(wiring), signature, dictionary_parameter, items_parameter, default_parameter});
}

std::unique_ptr<NodeTemplate> make_compact_reduce_node(ReduceCompactGraphWiring wiring,
                                                       const ResolvedSignature &signature,
                                                       std::uint32_t dictionary_parameter,
                                                       std::optional<std::uint32_t> default_parameter) {
    return std::make_unique<RuntimeSignatureNodeTemplate<CompactReduceNodeTemplate>>(
        CompactReduceNodeTemplate{std::move(wiring), signature, dictionary_parameter, default_parameter});
}

} // namespace hg::details
