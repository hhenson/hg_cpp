#include "hgraph/types/tss.h"

#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/nodes/tsd_map_node.h>
#include <hgraph/runtime/record_replay.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/traits.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/tsd.h>
#include <hgraph/util/lifecycle.h>
#include <hgraph/util/string_utils.h>

namespace hgraph
{
    template <typename K>
    MapNestedEngineEvaluationClock<K>::MapNestedEngineEvaluationClock(EngineEvaluationClock::ptr engine_evaluation_clock, K key,
                                                                      tsd_map_node_ptr<K> nested_node)
        : NestedEngineEvaluationClock(engine_evaluation_clock, reinterpret_cast<NestedNode *>(nested_node.get())), _key(key) {}

    template <typename K> void MapNestedEngineEvaluationClock<K>::update_next_scheduled_evaluation_time(engine_time_t next_time) {
        auto node_{reinterpret_cast<TsdMapNode<K> &>(*node())};
        auto let{node_.last_evaluation_time()};
        if ((let != MIN_DT && let >= next_time) || node_.is_stopping()) { return; }

        auto it{node_.scheduled_keys_.find(_key)};
        if (it == node_.scheduled_keys_.end() || it->second > next_time) { node_.scheduled_keys_[_key] = next_time; }

        NestedEngineEvaluationClock::update_next_scheduled_evaluation_time(next_time);
    }

    template <typename K>
    TsdMapNode<K>::TsdMapNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::ptr signature,
                              nb::dict scalars, graph_builder_ptr nested_graph_builder,
                              const std::unordered_map<std::string, int> &input_node_ids, int output_node_id,
                              const std::unordered_set<std::string> &multiplexed_args, const std::string &key_arg)
        : NestedNode(node_ndx, owning_graph_id, signature, scalars), nested_graph_builder_(nested_graph_builder),
          input_node_ids_(input_node_ids), output_node_id_(output_node_id), multiplexed_args_(multiplexed_args), key_arg_(key_arg) {
    }

    template <typename K> std::unordered_map<K, graph_ptr> &TsdMapNode<K>::nested_graphs() { return active_graphs_; }

    template <typename K> void TsdMapNode<K>::initialise() {}

    template <typename K> void TsdMapNode<K>::do_start() {
        auto trait{graph().traits().get_trait_or(RECORDABLE_ID_TRAIT, nb::none())};
        if (!trait.is_none()) {
            auto recordable_id{signature().record_replay_id};
            recordable_id_ = get_fq_recordable_id(graph().traits(), recordable_id.has_value() ? "map_" : recordable_id.value());
        }
    }

    template <typename K> void TsdMapNode<K>::do_stop() {
        for (const auto &[k, _] : active_graphs_) { remove_graph(k); }
        active_graphs_.clear();
        scheduled_keys_.clear();
        pending_keys_.clear();
    }

    template <typename K> void TsdMapNode<K>::dispose() {}

    template <typename K> void TsdMapNode<K>::eval() {
        mark_evaluated();

        auto &keys = dynamic_cast<TimeSeriesSetInput_T<K> &>(*input()[KEYS_ARG]);
        if (keys.modified()) {
            for (const auto &k : keys.added()) {
                if (active_graphs_.find(k) == active_graphs_.end()) {
                    create_new_graph(k);
                } else {
                    throw std::runtime_error(
                        fmt::format("[{}] Key {} already exists in active graphs", signature().wiring_path_name, to_string(k)));
                }
            }
            for (const auto &k : keys.removed()) {
                if (auto it = active_graphs_.find(k); it != active_graphs_.end()) {
                    remove_graph(k);
                    scheduled_keys_.erase(k);
                } else {
                    throw std::runtime_error(
                        fmt::format("[{}] Key {} does not exist in active graphs", signature().wiring_path_name, to_string(k)));
                }
            }
        }

        auto scheduled_keys{std::move(scheduled_keys_)};
        scheduled_keys_.clear();

        for (const auto &[k, dt] : scheduled_keys) {
            if (dt < last_evaluation_time()) {
                throw std::runtime_error(
                    fmt::format("Scheduled time is in the past; last evaluation time: {}, scheduled time: {}, evaluation time: {}",
                                last_evaluation_time(), dt, graph().evaluation_clock().evaluation_time()));
            }
            engine_time_t next_dt;
            if (dt == last_evaluation_time()) {
                next_dt = evaluate_graph(k);
            } else {
                next_dt = dt;
            }
            if (next_dt != MAX_DT && next_dt > last_evaluation_time()) {
                scheduled_keys_[k] = next_dt;
                graph().schedule_node(node_ndx(), next_dt);
            }
        }
    }

    template <typename K> TimeSeriesDictOutput_T<K> &TsdMapNode<K>::tsd_output() {
        return dynamic_cast<TimeSeriesDictOutput_T<K> &>(output());
    }

    template <typename K> void TsdMapNode<K>::create_new_graph(const K &key) {
        auto graph_{
            nested_graph_builder_->make_instance(std::vector<int64_t>{node_ndx(), -static_cast<int64_t>(count_++)}, this,
                                                 to_string(key))  // This will come back to haunt me :(
        };

        active_graphs_[key] = graph_;

        graph_->set_evaluation_engine(new NestedEvaluationEngine(
            &graph().evaluation_engine(),
            new MapNestedEngineEvaluationClock<K>(&graph().evaluation_engine().engine_evaluation_clock(), key, this)));

        initialise_component(*graph_);

        if (!recordable_id_.empty()) {
            auto nested_recordable_id = fmt::format("{}[{}]", recordable_id_, to_string(key));
            // TODO: implement
            // set_parent_recordable_id(*graph_, nested_recordable_id);
        }

        wire_graph(key, graph_);
        start_component(*graph_);
        scheduled_keys_[key] = last_evaluation_time();
    }

    template <typename K> void TsdMapNode<K>::remove_graph(const K &key) {
        if (signature().capture_exception) {
            // Remove the error output associated to the graph if there is one
            auto &error_output_ = dynamic_cast<TimeSeriesDictOutput_T<K> &>(error_output());
            error_output_.erase(key);
        }

        auto graph{active_graphs_[key]};
        active_graphs_.erase(key);

        un_wire_graph(key, graph);
        stop_component(*graph);
        dispose_component(*graph);
    }

    template <typename K> engine_time_t TsdMapNode<K>::evaluate_graph(const K &key) {
        auto &graph = active_graphs_[key];
        dynamic_cast<NestedEngineEvaluationClock &>(graph->evaluation_engine_clock()).reset_next_scheduled_evaluation_time();

        if (signature().capture_exception) {
            try {
                graph->evaluate_graph();
            } catch (const std::exception &e) {
                // TODO: Resolve processing error outputs
                // auto &error_output = dynamic_cast<TimeSeriesDictOutput_T<K> &>(error_output());
                // auto  node_error   = NodeError::capture_error(e, this, fmt::format("key: {}", key));
                // error_output.get_or_create(key).value(node_error);
            }
        } else {
            graph->evaluate_graph();
        }

        auto next = graph->evaluation_engine_clock().next_scheduled_evaluation_time();
        dynamic_cast<NestedEngineEvaluationClock &>(graph->evaluation_engine_clock()).reset_next_scheduled_evaluation_time();
        return next;
    }

    template <typename K> void TsdMapNode<K>::un_wire_graph(const K &key, Graph::ptr &graph) {
        for (const auto &[arg, node_ndx] : input_node_ids_) {
            auto node = graph->nodes()[node_ndx];
            if (arg != key_arg_) {
                if (multiplexed_args_.find(arg) != multiplexed_args_.end()) {
                    auto  ts{static_cast<TimeSeriesInput *>(input()[arg].get())};
                    auto &tsd =
                        dynamic_cast<TimeSeriesDictInput_T<K> &>(*ts);  // Since this is a multiplexed arg it must be of type K
                    node->input()["ts"]->re_parent(ts);
                    if (!tsd.key_set().valid() || !dynamic_cast<TimeSeriesSetInput_T<K> &>(tsd.key_set()).contains(key)) {
                        tsd.on_key_removed(key);
                    }
                }
            }
        }

        if (output_node_id_) { tsd_output().erase(key); }
    }

    template <typename K> void TsdMapNode<K>::wire_graph(const K &key, Graph::ptr &graph) {
        for (const auto &[arg, node_ndx] : input_node_ids_) {
            auto node{graph->nodes()[node_ndx]};
            node->notify();

            if (arg == key_arg_) {
                auto key_node{dynamic_cast<PythonNode &>(*node)};
                // This relies on the current stub binding mechanism with a stub python class to hold the key.
                nb::setattr(key_node.eval_fn(), "key", nb::cast(key));
            } else {
                if (multiplexed_args_.find(arg) != multiplexed_args_.end()) {
                    auto  ts       = static_cast<TimeSeriesInput *>(input()[arg].get());
                    auto &tsd      = dynamic_cast<TimeSeriesDictInput_T<K> &>(*ts);
                    auto &ts_value = tsd._get_or_create(key);

                    node->set_input(node->input().copy_with(node, {&ts_value}));
                    ts_value.re_parent(TimeSeriesType::ptr(&node->input()));
                } else {
                    auto ts          = dynamic_cast<TimeSeriesReferenceInput *>(input()[arg].get());
                    auto inner_input = dynamic_cast<TimeSeriesReferenceInput *>(node->input()["ts"].get());

                    if (ts != nullptr && inner_input != nullptr) { inner_input->clone_binding(*ts); }
                }
            }
        }

        if (output_node_id_) {
            auto  node       = graph->nodes()[output_node_id_];
            auto &output_tsd = tsd_output();
            node->set_output(&output_tsd._get_or_create(key));
        }
    }

    using TsdMapNode_bool = TsdMapNode<bool>;

    void register_tsd_map_with_nanobind(nb::module_ &m) {
        nb::class_<MapNestedEngineEvaluationClock<bool>, NestedEngineEvaluationClock>(m, "MapNestedEngineEvaluationClock_bool");
        nb::class_<MapNestedEngineEvaluationClock<int64_t>, NestedEngineEvaluationClock>(m, "MapNestedEngineEvaluationClock_int");
        nb::class_<MapNestedEngineEvaluationClock<double>, NestedEngineEvaluationClock>(m, "MapNestedEngineEvaluationClock_float");
        nb::class_<MapNestedEngineEvaluationClock<engine_date_t>, NestedEngineEvaluationClock>(
            m, "MapNestedEngineEvaluationClock_date");
        nb::class_<MapNestedEngineEvaluationClock<engine_time_t>, NestedEngineEvaluationClock>(
            m, "MapNestedEngineEvaluationClock_datetime");
        nb::class_<MapNestedEngineEvaluationClock<engine_time_delta_t>, NestedEngineEvaluationClock>(
            m, "MapNestedEngineEvaluationClock_timedelta");
        nb::class_<MapNestedEngineEvaluationClock<nb::object>, NestedEngineEvaluationClock>(
            m, "MapNestedEngineEvaluationClock_object");

        nb::class_<TsdMapNode<bool>, NestedNode>(m, "TsdMapNode_bool")
            .def(nb::init<int64_t, std::vector<int64_t>, NodeSignature::ptr, nb::dict, graph_builder_ptr,
                          const std::unordered_map<std::string, int> &, int, const std::unordered_set<std::string> &,
                          const std::string &>(),
                 "node_ndx"_a, "owning_graph_id"_a, "signature"_a, "scalars"_a, "nested_graph_builder"_a, "input_node_ids"_a,
                 "output_node_id"_a, "multiplexed_args"_a, "key_arg"_a);
        nb::class_<TsdMapNode<int64_t>, NestedNode>(m, "TsdMapNode_int")
            .def(nb::init<int64_t, std::vector<int64_t>, NodeSignature::ptr, nb::dict, graph_builder_ptr,
                          const std::unordered_map<std::string, int> &, int, const std::unordered_set<std::string> &,
                          const std::string &>(),
                 "node_ndx"_a, "owning_graph_id"_a, "signature"_a, "scalars"_a, "nested_graph_builder"_a, "input_node_ids"_a,
                 "output_node_id"_a, "multiplexed_args"_a, "key_arg"_a);
        nb::class_<TsdMapNode<double>, NestedNode>(m, "TsdMapNode_float")
            .def(nb::init<int64_t, std::vector<int64_t>, NodeSignature::ptr, nb::dict, graph_builder_ptr,
                          const std::unordered_map<std::string, int> &, int, const std::unordered_set<std::string> &,
                          const std::string &>(),
                 "node_ndx"_a, "owning_graph_id"_a, "signature"_a, "scalars"_a, "nested_graph_builder"_a, "input_node_ids"_a,
                 "output_node_id"_a, "multiplexed_args"_a, "key_arg"_a);
        nb::class_<TsdMapNode<engine_date_t>, NestedNode>(m, "TsdMapNode_date")
            .def(nb::init<int64_t, std::vector<int64_t>, NodeSignature::ptr, nb::dict, graph_builder_ptr,
                          const std::unordered_map<std::string, int> &, int, const std::unordered_set<std::string> &,
                          const std::string &>(),
                 "node_ndx"_a, "owning_graph_id"_a, "signature"_a, "scalars"_a, "nested_graph_builder"_a, "input_node_ids"_a,
                 "output_node_id"_a, "multiplexed_args"_a, "key_arg"_a);
        nb::class_<TsdMapNode<engine_time_t>, NestedNode>(m, "TsdMapNode_datetime")
            .def(nb::init<int64_t, std::vector<int64_t>, NodeSignature::ptr, nb::dict, graph_builder_ptr,
                          const std::unordered_map<std::string, int> &, int, const std::unordered_set<std::string> &,
                          const std::string &>(),
                 "node_ndx"_a, "owning_graph_id"_a, "signature"_a, "scalars"_a, "nested_graph_builder"_a, "input_node_ids"_a,
                 "output_node_id"_a, "multiplexed_args"_a, "key_arg"_a);
        nb::class_<TsdMapNode<engine_time_delta_t>, NestedNode>(m, "TsdMapNode_timedelta")
            .def(nb::init<int64_t, std::vector<int64_t>, NodeSignature::ptr, nb::dict, graph_builder_ptr,
                          const std::unordered_map<std::string, int> &, int, const std::unordered_set<std::string> &,
                          const std::string &>(),
                 "node_ndx"_a, "owning_graph_id"_a, "signature"_a, "scalars"_a, "nested_graph_builder"_a, "input_node_ids"_a,
                 "output_node_id"_a, "multiplexed_args"_a, "key_arg"_a);
        nb::class_<TsdMapNode<nb::object>, NestedNode>(m, "TsdMapNode_object")
            .def(nb::init<int64_t, std::vector<int64_t>, NodeSignature::ptr, nb::dict, graph_builder_ptr,
                          const std::unordered_map<std::string, int> &, int, const std::unordered_set<std::string> &,
                          const std::string &>(),
                 "node_ndx"_a, "owning_graph_id"_a, "signature"_a, "scalars"_a, "nested_graph_builder"_a, "input_node_ids"_a,
                 "output_node_id"_a, "multiplexed_args"_a, "key_arg"_a);
    }
}  // namespace hgraph