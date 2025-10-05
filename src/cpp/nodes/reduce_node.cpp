#include <hgraph/types/tss.h>

#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/nodes/reduce_node.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/traits.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/tsd.h>
#include <hgraph/util/lifecycle.h>
#include <hgraph/util/string_utils.h>

#include <algorithm>
#include <deque>

namespace hgraph
{
    ReduceNode::ReduceNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::ptr signature,
                           nb::dict scalars, graph_builder_ptr nested_graph_builder,
                           const std::tuple<int64_t, int64_t> &input_node_ids, int64_t output_node_id)
        : NestedNode(node_ndx, owning_graph_id, signature, scalars), nested_graph_builder_(nested_graph_builder),
          input_node_ids_(input_node_ids), output_node_id_(output_node_id) {
        nested_graph_ = new Graph(std::vector<int64_t>{node_ndx}, std::vector<node_ptr>{}, this, "", traits_ptr{});
    }

    std::unordered_map<int, graph_ptr> &ReduceNode::nested_graphs() {
        static std::unordered_map<int, graph_ptr> graphs;
        graphs[0] = nested_graph_;
        return graphs;
    }

    void ReduceNode::initialise() {
        nested_graph_->set_evaluation_engine(
            new NestedEvaluationEngine(&graph().evaluation_engine(),
                                       new NestedEngineEvaluationClock(&graph().evaluation_engine_clock(), this)));
    }

    void ReduceNode::do_start() {
        auto &tsd = dynamic_cast<TimeSeriesDictInput &>(*input()["ts"]);
        if (tsd.valid()) {
            // Get all keys via Python iterator
            std::vector<nb::object> all_keys;
            for (auto key_obj : tsd.py_keys()) {
                all_keys.push_back(nb::cast<nb::object>(key_obj));
            }

            // Get added keys via Python iterator
            std::vector<nb::object> added_keys_vec;
            for (auto key_obj : tsd.py_added_keys()) {
                added_keys_vec.push_back(nb::cast<nb::object>(key_obj));
            }

            // Find existing keys (not in added)
            std::vector<nb::object> existing_keys;
            for (const auto &key : all_keys) {
                bool is_added = false;
                for (const auto &added_key : added_keys_vec) {
                    if (nb::cast<std::string>(nb::str(key)) == nb::cast<std::string>(nb::str(added_key))) {
                        is_added = true;
                        break;
                    }
                }
                if (!is_added) {
                    existing_keys.push_back(key);
                }
            }

            if (!existing_keys.empty()) {
                add_nodes(existing_keys);
            } else {
                grow_tree();
            }
        } else {
            grow_tree();
        }
        start_component(*nested_graph_);
    }

    void ReduceNode::do_stop() {
        stop_component(*nested_graph_);
    }

    void ReduceNode::dispose() {}

    void ReduceNode::eval() {
        mark_evaluated();

        auto &tsd = dynamic_cast<TimeSeriesDictInput &>(*input()["ts"]);

        // Collect removed and added keys from iterators
        std::vector<nb::object> removed_keys_vec;
        for (auto key_obj : tsd.py_removed_keys()) {
            removed_keys_vec.push_back(nb::cast<nb::object>(key_obj));
        }

        std::vector<nb::object> added_keys_vec;
        for (auto key_obj : tsd.py_added_keys()) {
            added_keys_vec.push_back(nb::cast<nb::object>(key_obj));
        }

        // Process removals first, then additions
        remove_nodes(removed_keys_vec);
        add_nodes(added_keys_vec);

        // Re-balance the tree if required
        re_balance_nodes();

        // Evaluate the nested graph
        dynamic_cast<NestedEngineEvaluationClock &>(nested_graph_->evaluation_engine_clock())
            .reset_next_scheduled_evaluation_time();
        nested_graph_->evaluate_graph();
        dynamic_cast<NestedEngineEvaluationClock &>(nested_graph_->evaluation_engine_clock())
            .reset_next_scheduled_evaluation_time();

        // Propagate output if changed
        auto last_out = last_output();
        auto &ref_last_out = dynamic_cast<TimeSeriesReferenceOutput &>(*last_out);
        auto &ref_output = dynamic_cast<TimeSeriesReferenceOutput &>(output());

        if (!output().valid() || ref_output.value() != ref_last_out.value()) {
            ref_output.set_value(ref_last_out.value());
        }
    }

    TimeSeriesOutput::ptr ReduceNode::last_output() {
        auto sub_graph = get_node(node_count() - 1);
        auto out_node = sub_graph[output_node_id_];
        return out_node->output_ptr();
    }

    void ReduceNode::add_nodes(const std::vector<nb::object> &keys) {
        for (const auto &key : keys) {
            if (free_node_indexes_.empty()) {
                grow_tree();
            }
            auto ndx = free_node_indexes_.back();
            free_node_indexes_.pop_back();
            bind_key_to_node(key, ndx);
        }
    }

    void ReduceNode::remove_nodes(const std::vector<nb::object> &keys) {
        for (const auto &key : keys) {
            auto it = bound_node_indexes_.find(key);
            if (it == bound_node_indexes_.end()) {
                continue;
            }

            auto ndx = it->second;
            bound_node_indexes_.erase(it);

            if (!bound_node_indexes_.empty()) {
                // Find the largest bound index
                auto max_it = std::max_element(
                    bound_node_indexes_.begin(), bound_node_indexes_.end(),
                    [](const auto &a, const auto &b) { return std::get<0>(a.second) < std::get<0>(b.second); });

                if (std::get<0>(max_it->second) > std::get<0>(ndx)) {
                    swap_node(ndx, max_it->second);
                    bound_node_indexes_[max_it->first] = ndx;
                    ndx = max_it->second;
                }
            }
            free_node_indexes_.push_back(ndx);
            zero_node(ndx);
        }
    }

    void ReduceNode::swap_node(const std::tuple<int64_t, int64_t> &src_ndx, const std::tuple<int64_t, int64_t> &dst_ndx) {
        auto [src_node_id, src_side] = src_ndx;
        auto [dst_node_id, dst_side] = dst_ndx;

        auto src_nodes = get_node(src_node_id);
        auto dst_nodes = get_node(dst_node_id);

        auto src_node = src_nodes[src_side];
        auto dst_node = dst_nodes[dst_side];

        auto src_input = src_node->input()["ts"];
        auto dst_input = dst_node->input()["ts"];

        src_node->reset_input(src_node->input().copy_with(src_node.get(), {dst_input.get()}));
        dst_node->reset_input(dst_node->input().copy_with(dst_node.get(), {src_input.get()}));

        src_node->notify();
        dst_node->notify();
    }

    void ReduceNode::re_balance_nodes() {
        if (node_count() > 8 && (free_node_indexes_.size() * 0.75) > bound_node_indexes_.size()) {
            shrink_tree();
        }
    }

    void ReduceNode::grow_tree() {
        int64_t count = node_count();
        int64_t end = 2 * count + 1;
        int64_t top_layer_length = (end + 1) / 4;
        int64_t top_layer_end = std::max(count + top_layer_length, static_cast<int64_t>(1));
        int64_t last_node = end - 1;

        std::deque<int64_t> un_bound_outputs;

        for (int64_t i = count; i < end; ++i) {
            un_bound_outputs.push_back(i);
            nested_graph_->extend_graph(*nested_graph_builder_, true);

            if (i < top_layer_end) {
                auto ndx_lhs = std::make_tuple(i, std::get<0>(input_node_ids_));
                free_node_indexes_.push_back(ndx_lhs);
                zero_node(ndx_lhs);

                auto ndx_rhs = std::make_tuple(i, std::get<1>(input_node_ids_));
                free_node_indexes_.push_back(ndx_rhs);
                zero_node(ndx_rhs);
            } else {
                TimeSeriesOutput::ptr left_parent;
                TimeSeriesOutput::ptr right_parent;

                if (i < last_node) {
                    auto left_idx = un_bound_outputs.front();
                    un_bound_outputs.pop_front();
                    left_parent = get_node(left_idx)[output_node_id_]->output_ptr();

                    auto right_idx = un_bound_outputs.front();
                    un_bound_outputs.pop_front();
                    right_parent = get_node(right_idx)[output_node_id_]->output_ptr();
                } else {
                    auto old_root = get_node(count - 1)[output_node_id_];
                    left_parent = old_root->output_ptr();

                    auto new_root_idx = un_bound_outputs.front();
                    un_bound_outputs.pop_front();
                    auto new_root = get_node(new_root_idx)[output_node_id_];
                    right_parent = new_root->output_ptr();
                }

                auto sub_graph = get_node(i);
                auto lhs_input = sub_graph[std::get<0>(input_node_ids_)];
                auto rhs_input = sub_graph[std::get<1>(input_node_ids_)];

                dynamic_cast<TimeSeriesInput &>(*lhs_input->input()["ts"]).bind_output(left_parent.get());
                dynamic_cast<TimeSeriesInput &>(*rhs_input->input()["ts"]).bind_output(right_parent.get());

                lhs_input->notify();
                rhs_input->notify();
            }
        }

        if (nested_graph_->is_started() || nested_graph_->is_starting()) {
            // Start the newly added nodes
            int64_t start_idx = count * node_size();
            int64_t end_idx = nested_graph_->nodes().size();
            for (int64_t i = start_idx; i < end_idx; ++i) {
                auto node = nested_graph_->nodes()[i];
                start_component(*node.get());
            }
        }
    }

    void ReduceNode::shrink_tree() {
        int64_t capacity = bound_node_indexes_.size() + free_node_indexes_.size();
        if (capacity <= 8) {
            return;
        }

        int64_t halved_capacity = capacity / 2;
        int64_t active_count = bound_node_indexes_.size();
        if (halved_capacity < active_count) {
            return;
        }

        int64_t last_node = (node_count() - 1) / 2;
        int64_t start = last_node;
        nested_graph_->reduce_graph(start * node_size());

        // Keep only the first halved_capacity - active_count free nodes
        std::sort(free_node_indexes_.begin(), free_node_indexes_.end(),
                  [](const auto &a, const auto &b) { return std::get<0>(a) < std::get<0>(b); });

        int64_t to_keep = halved_capacity - active_count;
        if (static_cast<size_t>(to_keep) < free_node_indexes_.size()) {
            free_node_indexes_.resize(to_keep);
        }

        std::sort(free_node_indexes_.begin(), free_node_indexes_.end(),
                  [](const auto &a, const auto &b) { return std::get<0>(a) > std::get<0>(b); });
    }

    void ReduceNode::bind_key_to_node(const nb::object &key, const std::tuple<int64_t, int64_t> &ndx) {
        bound_node_indexes_[key] = ndx;
        auto [node_id, side] = ndx;
        auto nodes = get_node(node_id);
        auto node = nodes[side];

        auto &tsd = dynamic_cast<TimeSeriesDictInput &>(*input()["ts"]);
        auto ts = tsd.py_get_item(key);
        auto inner_input = dynamic_cast<TimeSeriesReferenceInput *>(node->input()["ts"].get());
        if (inner_input != nullptr) {
            auto &ts_input = dynamic_cast<TimeSeriesInput &>(*nb::cast<time_series_input_ptr>(ts).get());
            inner_input->clone_binding(dynamic_cast<TimeSeriesReferenceInput &>(ts_input));
        }
        node->notify();
    }

    void ReduceNode::zero_node(const std::tuple<int64_t, int64_t> &ndx) {
        auto [node_id, side] = ndx;
        auto nodes = get_node(node_id);
        auto node = nodes[side];

        auto zero_ts = dynamic_cast<TimeSeriesReferenceInput *>(input()["zero"].get());
        auto inner_input = dynamic_cast<TimeSeriesReferenceInput *>(node->input()["ts"].get());
        if (inner_input != nullptr && zero_ts != nullptr) {
            inner_input->clone_binding(*zero_ts);
        }
        node->notify();
    }

    int64_t ReduceNode::node_size() const { return nested_graph_builder_->node_builders.size(); }

    int64_t ReduceNode::node_count() const { return nested_graph_->nodes().size() / node_size(); }

    std::vector<node_ptr> ReduceNode::get_node(int64_t ndx) {
        auto &all_nodes = nested_graph_->nodes();
        int64_t ns = node_size();
        int64_t start = ndx * ns;
        int64_t end = start + ns;
        return std::vector<node_ptr>(all_nodes.begin() + start, all_nodes.begin() + end);
    }

    void register_reduce_node_with_nanobind(nb::module_ &m) {
        nb::class_<ReduceNode, NestedNode>(m, "ReduceNode")
            .def(nb::init<int64_t, std::vector<int64_t>, NodeSignature::ptr, nb::dict, graph_builder_ptr,
                          const std::tuple<int64_t, int64_t> &, int64_t>(),
                 "node_ndx"_a, "owning_graph_id"_a, "signature"_a, "scalars"_a, "nested_graph_builder"_a, "input_node_ids"_a,
                 "output_node_id"_a);
    }

}  // namespace hgraph
