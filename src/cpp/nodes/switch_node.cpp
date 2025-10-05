#include "hgraph/util/string_utils.h"

#include <fmt/format.h>
#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/nodes/switch_node.h>
#include <hgraph/runtime/record_replay.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/ts.h>
#include <hgraph/types/tsb.h>
#include <hgraph/util/lifecycle.h>

namespace hgraph
{

    // Helper to compare keys (special handling for nb::object)
    template <typename K> inline bool keys_equal(const K &a, const K &b) { return a == b; }
    template <> inline bool           keys_equal<nb::object>(const nb::object &a, const nb::object &b) { return a.equal(b); }

    // Helper to get DEFAULT object from Python
    static nb::object get_python_default() {
        static nb::object default_obj;
        if (!default_obj.is_valid()) {
            try {
                // Import DEFAULT from hgraph._types._scalar_types
                auto scalar_types = nb::module_::import_("hgraph._types._scalar_types");
                default_obj       = scalar_types.attr("DEFAULT");
            } catch (...) {
                // If import fails, return an invalid object
                default_obj = nb::object();
            }
        }
        return default_obj;
    }

    template <typename K>
    SwitchNode<K>::SwitchNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::ptr signature,
                              nb::dict scalars, const std::unordered_map<K, graph_builder_ptr> &nested_graph_builders,
                              const std::unordered_map<K, std::unordered_map<std::string, int>> &input_node_ids,
                              const std::unordered_map<K, int> &output_node_ids, bool reload_on_ticked)
        : NestedNode(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars)),
          nested_graph_builders_(nested_graph_builders), input_node_ids_(input_node_ids), output_node_ids_(output_node_ids),
          reload_on_ticked_(reload_on_ticked) {
        // TODO: I don't think this is actuall correct logic, need to work through the default behavior later.
        //  Extract DEFAULT graph builder if it exists
        //  DEFAULT is a special marker object from hgraph._types._scalar_types
        //  For nb::object template, we can directly look it up
        if constexpr (std::is_same_v<K, nb::object>) {
            auto default_marker = get_python_default();
            if (default_marker.is_valid()) {
                auto it = nested_graph_builders_.find(default_marker);
                if (it != nested_graph_builders_.end()) { default_graph_builder_ = it->second; }
            }
        } else {
            // For typed keys (bool, int, string, etc.), DEFAULT is not applicable
            // The Python code would have converted DEFAULT to a typed key or excluded it
            // So we just store nullptr - no default fallback for typed switches
            default_graph_builder_ = nullptr;
        }
    }

    template <typename K> void SwitchNode<K>::initialise() {
        // Switch node doesn't create graphs upfront
        // Graphs are created dynamically in do_eval when key changes
    }

    template <typename K> void SwitchNode<K>::do_start() {
        auto ts{input()["key"].get()};
        key_ts = dynamic_cast<TimeSeriesValueInput<K> *>(ts);
        if (!key_ts) { throw std::runtime_error("SwitchNode requires a TimeSeriesValueInput<K> for key input, but none found"); }
        // Check if graph has recordable ID trait
        if (has_recordable_id_trait(graph().traits())) {
            // NodeSignature::record_replay_id is std::optional<std::string>
            auto &record_replay_id = signature().record_replay_id;
            if (!record_replay_id.has_value() || record_replay_id.value().empty()) {
                recordable_id_ = get_fq_recordable_id(graph().traits(), "switch_");
            } else {
                recordable_id_ = get_fq_recordable_id(graph().traits(), record_replay_id.value());
            }
        }
        _initialise_inputs();
    }

    template <typename K> void SwitchNode<K>::do_stop() {
        if (active_graph_) { stop_component(*active_graph_); }
    }

    template <typename K> void SwitchNode<K>::dispose() {
        if (active_graph_) {
            unwire_graph(active_graph_);
            dispose_component(*active_graph_);
            active_graph_ = nullptr;
        }
    }

    template <typename K> void SwitchNode<K>::eval() {
        mark_evaluated();

        if (!key_ts->valid()) {
            return;  // No key input or invalid
        }

        // Check if key has been modified
        if (key_ts->modified()) {
            // Extract the key value from the input time series
            if (reload_on_ticked_ || !active_key_.has_value() || !keys_equal(key_ts->value(), active_key_.value())) {
                if (active_key_.has_value()) {
                    stop_component(*active_graph_);
                    unwire_graph(active_graph_);
                    dispose_component(*active_graph_);
                    active_graph_ = nullptr;
                }
                active_key_ = key_ts->value();

                // Look up graph builder for this key
                graph_builder_ptr builder = nullptr;
                auto              it      = nested_graph_builders_.find(active_key_.value());
                if (it != nested_graph_builders_.end()) {
                    builder = it->second;
                } else {
                    builder = default_graph_builder_;
                }

                if (!builder) { throw std::runtime_error("No graph defined for key and no default available"); }

                // Create new graph
                ++count_;
                std::vector<int64_t> new_node_id = node_id();
                new_node_id.push_back(-count_);
                active_graph_ = builder->make_instance(new_node_id, this, to_string(active_key_.value()));

                // Set up evaluation engine
                active_graph_->set_evaluation_engine(new NestedEvaluationEngine(
                    &graph().evaluation_engine(), new NestedEngineEvaluationClock(&graph().evaluation_engine_clock(), this)));

                // Initialize and wire the new graph
                initialise_component(*active_graph_);
                wire_graph(active_graph_);
                start_component(*active_graph_);
            }
        }

        // Evaluate the active graph if it exists
        if (active_graph_) {
            static_cast<NestedEngineEvaluationClock &>(
                active_graph_->evaluation_engine_clock())  // NOLINT(*-pro-type-static-cast-downcast)
                .reset_next_scheduled_evaluation_time();
            active_graph_->evaluate_graph();
            static_cast<NestedEngineEvaluationClock &>(
                active_graph_->evaluation_engine_clock())  // NOLINT(*-pro-type-static-cast-downcast)
                .reset_next_scheduled_evaluation_time();
        }
    }

    template <typename K> void SwitchNode<K>::wire_graph(graph_ptr &graph) {
        // Determine which graph key to use for lookups
        K graph_key = active_key_.value();

        // If active_key not found, try DEFAULT for nb::object
        if (input_node_ids_.find(graph_key) == input_node_ids_.end()) {
            if constexpr (std::is_same_v<K, nb::object>) {
                auto default_marker = get_python_default();
                if (default_marker.is_valid() && input_node_ids_.find(default_marker) != input_node_ids_.end()) {
                    graph_key = default_marker;
                }
            }
        }

        // Set recordable ID if needed
        if (!recordable_id_.empty()) {
            std::string key_str;
            if constexpr (std::is_same_v<K, std::string>) {
                key_str = graph_key;
            } else if constexpr (std::is_same_v<K, nb::object>) {
                key_str = nb::cast<std::string>(nb::str(graph_key));
            } else {
                key_str = to_string(graph_key);
            }
            std::string full_id = fmt::format("{}[{}]", recordable_id_, key_str);
            set_parent_recordable_id(*graph, full_id);
        }

        // Wire inputs
        auto input_ids_it = input_node_ids_.find(graph_key);
        if (input_ids_it != input_node_ids_.end()) {
            for (const auto &[arg, node_ndx] : input_ids_it->second) {
                auto node = graph->nodes()[node_ndx];
                node->notify();

                if (arg == "key") {
                    // The key node is a Python stub whose eval function exposes a 'key' attribute.
                    // Set this to the current active key so the nested graph sees the correct key value.
                    auto &key_node = dynamic_cast<PythonNode &>(*node);
                    nb::setattr(key_node.eval_fn(), "key", nb::cast(graph_key));
                } else {
                    // Regular input - clone binding of the reference input into the inner 'ts' input
                    auto ts          = input()[arg].get();
                    auto inner_input = dynamic_cast<TimeSeriesReferenceInput *>(node->input()["ts"].get());
                    if (ts != nullptr && inner_input != nullptr) {
                        auto other = dynamic_cast<TimeSeriesReferenceInput *>(ts);
                        inner_input->clone_binding(*other);
                    }
                }
            }
        }

        // Wire output
        auto output_id_it = output_node_ids_.find(graph_key);
        if (output_id_it != output_node_ids_.end()) {
            auto node   = graph->nodes()[output_id_it->second];
            old_output_ = node->output_ptr();
            node->set_output(output_ptr());
        }
    }

    template <typename K> void SwitchNode<K>::unwire_graph(graph_ptr &graph) {
        if (old_output_) {
            // Determine graph key
            K graph_key = active_key_.value();

            // If active_key not found, try DEFAULT for nb::object
            if (output_node_ids_.find(graph_key) == output_node_ids_.end()) {
                if constexpr (std::is_same_v<K, nb::object>) {
                    auto default_marker = get_python_default();
                    if (default_marker.is_valid() && output_node_ids_.find(default_marker) != output_node_ids_.end()) {
                        graph_key = default_marker;
                    }
                }
            }

            auto output_id_it = output_node_ids_.find(graph_key);
            if (output_id_it != output_node_ids_.end()) {
                auto node = graph->nodes()[output_id_it->second];
                node->set_output(old_output_);
                old_output_ = nullptr;
            }
        }
    }

    template <typename K> std::unordered_map<int, graph_ptr> SwitchNode<K>::nested_graphs() const {
        if (active_graph_) { return {{static_cast<int>(count_), active_graph_}}; }
        return {};
    }

    // Template instantiations
    template struct SwitchNode<bool>;
    template struct SwitchNode<int64_t>;
    template struct SwitchNode<double>;
    template struct SwitchNode<engine_date_t>;
    template struct SwitchNode<engine_time_t>;
    template struct SwitchNode<engine_time_delta_t>;
    template struct SwitchNode<nb::object>;

    void register_switch_node_with_nanobind(nb::module_ &m) {
        nb::class_<SwitchNode<bool>, NestedNode>(m, "SwitchNode_bool")
            .def_prop_ro("nested_graphs", &SwitchNode<bool>::nested_graphs);

        nb::class_<SwitchNode<int64_t>, NestedNode>(m, "SwitchNode_int")
            .def_prop_ro("nested_graphs", &SwitchNode<int64_t>::nested_graphs);

        nb::class_<SwitchNode<double>, NestedNode>(m, "SwitchNode_float")
            .def_prop_ro("nested_graphs", &SwitchNode<double>::nested_graphs);

        nb::class_<SwitchNode<engine_date_t>, NestedNode>(m, "SwitchNode_date")
            .def_prop_ro("nested_graphs", &SwitchNode<engine_date_t>::nested_graphs);

        nb::class_<SwitchNode<engine_time_t>, NestedNode>(m, "SwitchNode_date_time")
            .def_prop_ro("nested_graphs", &SwitchNode<engine_time_t>::nested_graphs);

        nb::class_<SwitchNode<engine_time_delta_t>, NestedNode>(m, "SwitchNode_time_delta")
            .def_prop_ro("nested_graphs", &SwitchNode<engine_time_delta_t>::nested_graphs);

        nb::class_<SwitchNode<nb::object>, NestedNode>(m, "SwitchNode_object")
            .def_prop_ro("nested_graphs", &SwitchNode<nb::object>::nested_graphs);
    }

}  // namespace hgraph
