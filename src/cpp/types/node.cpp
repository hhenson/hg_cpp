#include <hgraph/python/pyb_wiring.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>

namespace hgraph
{
    void node_type_enum_py_register(nb::module_ &m) {
        nb::enum_<NodeTypeEnum>(m, "NodeTypeEnum")
            .value("SOURCE_NODE", NodeTypeEnum::SOURCE_NODE)
            .value("PUSH_SOURCE_NODE", NodeTypeEnum::PUSH_SOURCE_NODE)
            .value("PULL_SOURCE_NODE", NodeTypeEnum::PULL_SOURCE_NODE)
            .value("COMPUTE_NODE", NodeTypeEnum::COMPUTE_NODE)
            .value("SINK_NODE", NodeTypeEnum::SINK_NODE)
            .export_values();
    }

    void injectable_type_enum(nb::module_ &m) {
        nb::enum_<InjectableTypesEnum>(m, "InjectableTypes")
            .value("STATE", InjectableTypesEnum::STATE)
            .value("SCHEDULER", InjectableTypesEnum::SCHEDULER)
            .value("OUTPUT", InjectableTypesEnum::OUTPUT)
            .value("CLOCK", InjectableTypesEnum::CLOCK)
            .value("ENGINE_API", InjectableTypesEnum::ENGINE_API)
            .value("REPLAY_STATE", InjectableTypesEnum::REPLAY_STATE)
            .value("LOGGER", InjectableTypesEnum::LOGGER)
            .export_values();
    }

    [[nodiscard]] nb::object NodeSignature::get_arg_type(const std::string &arg) const {
        if (time_series_inputs && time_series_inputs->contains(arg)) { return time_series_inputs->at(arg); }
        if (scalars && scalars->contains(arg)) { return scalars->at(arg); }
        return nb::none();
    }

    [[nodiscard]] std::string NodeSignature::signature() const {
        std::ostringstream oss;
        bool               first = true;
        auto               none_str{std::string("None")};

        oss << name << "(";

        auto obj_to_type = [&](const nb::object &obj) { return obj.is_none() ? none_str : nb::cast<std::string>(obj); };

        for (const auto &arg : args) {
            if (!first) { oss << ", "; }
            oss << arg << ": " << obj_to_type(get_arg_type(arg));
            first = false;
        }

        oss << ")";

        if (bool(time_series_output)) {
            auto v = time_series_output.value();
            oss << " -> " << (v.is_none() ? none_str : nb::cast<std::string>(v));
        }

        return oss.str();
    }

    [[nodiscard]] bool NodeSignature::uses_scheduler() const {
        return (injectable_inputs & InjectableTypesEnum::SCHEDULER) == InjectableTypesEnum::SCHEDULER;
    }

    [[nodiscard]] bool NodeSignature::uses_clock() const {
        return (injectable_inputs & InjectableTypesEnum::CLOCK) == InjectableTypesEnum::CLOCK;
    }

    [[nodiscard]] bool NodeSignature::uses_engine() const {
        return (injectable_inputs & InjectableTypesEnum::ENGINE_API) == InjectableTypesEnum::ENGINE_API;
    }

    [[nodiscard]] bool NodeSignature::uses_state() const {
        return (injectable_inputs & InjectableTypesEnum::STATE) == InjectableTypesEnum::STATE;
    }

    [[nodiscard]] bool NodeSignature::uses_output_feedback() const {
        return (injectable_inputs & InjectableTypesEnum::OUTPUT) == InjectableTypesEnum::OUTPUT;
    }

    [[nodiscard]] bool NodeSignature::uses_replay_state() const {
        return (injectable_inputs & InjectableTypesEnum::REPLAY_STATE) == InjectableTypesEnum::REPLAY_STATE;
    }

    [[nodiscard]] bool NodeSignature::is_source_node() const {
        return (node_type & NodeTypeEnum::SOURCE_NODE) == NodeTypeEnum::SOURCE_NODE;
    }

    [[nodiscard]] bool NodeSignature::is_push_source_node() const {
        return (node_type & NodeTypeEnum::PUSH_SOURCE_NODE) == NodeTypeEnum::PUSH_SOURCE_NODE;
    }

    [[nodiscard]] bool NodeSignature::is_pull_source_node() const {
        return (node_type & NodeTypeEnum::PULL_SOURCE_NODE) == NodeTypeEnum::PULL_SOURCE_NODE;
    }

    [[nodiscard]] bool NodeSignature::is_compute_node() const {
        return (node_type & NodeTypeEnum::COMPUTE_NODE) == NodeTypeEnum::COMPUTE_NODE;
    }

    [[nodiscard]] bool NodeSignature::is_sink_node() const {
        return (node_type & NodeTypeEnum::SINK_NODE) == NodeTypeEnum::SINK_NODE;
    }

    [[nodiscard]] bool NodeSignature::is_recordable() const { return (bool)record_replay_id; }

    void NodeSignature::py_register(nb::module_ &m) {
        nb::class_<NodeSignature>(m, "NodeSignature")
            .def(nb::init<std::string, NodeTypeEnum, std::vector<std::string>,
                          std::optional<std::unordered_map<std::string, nb::object>>, std::optional<nb::object>,
                          std::optional<std::unordered_map<std::string, nb::object>>, nb::object,
                          std::optional<std::unordered_set<std::string>>, std::optional<std::unordered_set<std::string>>,
                          std::optional<std::unordered_set<std::string>>, std::optional<std::unordered_set<std::string>>,
                          InjectableTypesEnum, std::string, std::optional<std::string>, std::optional<std::string>, bool, bool,
                          char8_t>(),
                 "name"_a, "node_type"_a, "args"_a, "time_series_inputs"_a, "time_series_output"_a, "scalars"_a, "src_location"_a,
                 "active_inputs"_a, "valid_inputs"_a, "all_valid_inputs"_a, "context_inputs"_a, "injectable_inputs"_a,
                 "wiring_path_name"_a, "label"_a, "record_replay_id"_a, "capture_values"_a, "capture_exception"_a,
                 "trace_back_depth"_a)
            .def_prop_ro("signature", &NodeSignature::signature)
            .def_prop_ro("uses_scheduler", &NodeSignature::uses_scheduler)
            .def_prop_ro("uses_clock", &NodeSignature::uses_clock)
            .def_prop_ro("uses_engine", &NodeSignature::uses_engine)
            .def_prop_ro("uses_state", &NodeSignature::uses_state)
            .def_prop_ro("uses_output_feedback", &NodeSignature::uses_output_feedback)
            .def_prop_ro("uses_replay_state", &NodeSignature::uses_replay_state)
            .def_prop_ro("is_source_node", &NodeSignature::is_source_node)
            .def_prop_ro("is_push_source_node", &NodeSignature::is_push_source_node)
            .def_prop_ro("is_pull_source_node", &NodeSignature::is_pull_source_node)
            .def_prop_ro("is_compute_node", &NodeSignature::is_compute_node)
            .def_prop_ro("is_sink_node", &NodeSignature::is_sink_node)
            .def_prop_ro("is_recordable", &NodeSignature::is_recordable)
            .def("to_dict",
                 [](const NodeSignature &self) {
                     nb::dict d{};
                     d["name"] = self.name;
                     return d;
                 })
            // .def("copy_with", [](const NodeSignature& self, py::kwargs kwargs) {
            //     return std::shared_ptr<NodeSignature>(
            //         kwargs.contains("name") ? py::cast<std::string>(kwargs["name"]) : self->name
            //         ...
            //     );
            // })
            ;
    }

    int64_t Node::node_ndx() const { return _node_ndx; }

    const std::vector<int64_t> &Node::owning_graph_id() const { return _owning_graph_id; }

    const std::vector<int64_t> &Node::node_id() const { return _node_id; }

    const NodeSignature::ptr &Node::signature() const { return _signature; }

    const nb::dict &Node::scalars() const { return _scalars; }

    Graph &Node::graph() const { return *_graph; }

    void Node::set_graph(graph_ptr value) { _graph = value; }

    TimeSeriesBundleInput &Node::input() const { return *_input; }

    void Node::set_input(nb::ref<TimeSeriesBundleInput> value) { _input = value; }

    nb::ref<TimeSeriesOutput> Node::output() const { return _output; }

    void Node::set_output(nb::ref<TimeSeriesOutput> value) { _output = value; }

    nb::ref<TimeSeriesBundleOutput> Node::recordable_state() const { return _recordable_state; }

    void Node::set_recordable_state(nb::ref<TimeSeriesBundleOutput> value) { _recordable_state = value; }

    std::optional<NodeScheduler> Node::scheduler() const { return _scheduler; }

    nb::ref<TimeSeriesOutput> Node::error_output() const { return _error_output; }

}  // namespace hgraph
