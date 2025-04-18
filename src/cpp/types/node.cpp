#include <hgraph/types/ref.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/tsb.h>

#include <fmt/format.h>
#include <hgraph/python/pyb_wiring.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <nanobind/ndarray.h>
#include <ranges>
#include <sstream>

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
        nb::enum_<InjectableTypesEnum>(m, "InjectableTypesEnum")
            .value("NONE", InjectableTypesEnum::NONE)
            .value("STATE", InjectableTypesEnum::STATE)
            .value("RECORDABLE_STATE", InjectableTypesEnum::RECORDABLE_STATE)
            .value("SCHEDULER", InjectableTypesEnum::SCHEDULER)
            .value("OUTPUT", InjectableTypesEnum::OUTPUT)
            .value("CLOCK", InjectableTypesEnum::CLOCK)
            .value("ENGINE_API", InjectableTypesEnum::ENGINE_API)
            .value("LOGGER", InjectableTypesEnum::LOGGER)
            .value("NODE", InjectableTypesEnum::NODE)
            .export_values();
    }

    NodeSignature::NodeSignature(std::string name, NodeTypeEnum node_type, std::vector<std::string> args,
                                 std::optional<std::unordered_map<std::string, nb::object>> time_series_inputs,
                                 std::optional<nb::object> time_series_output, std::optional<nb::dict> scalars,
                                 nb::object src_location, std::optional<std::unordered_set<std::string>> active_inputs,
                                 std::optional<std::unordered_set<std::string>>                      valid_inputs,
                                 std::optional<std::unordered_set<std::string>>                      all_valid_inputs,
                                 std::optional<std::unordered_set<std::string>>                      context_inputs,
                                 std::optional<std::unordered_map<std::string, InjectableTypesEnum>> injectable_inputs,
                                 size_t injectables, bool capture_exception, int64_t trace_back_depth, std::string wiring_path_name,
                                 std::optional<std::string> label, bool capture_values, std::optional<std::string> record_replay_id)
        : name{std::move(name)}, node_type{node_type}, args{std::move(args)}, time_series_inputs{std::move(time_series_inputs)},
          time_series_output{std::move(time_series_output)}, scalars{std::move(scalars)}, src_location{std::move(src_location)},
          active_inputs{std::move(active_inputs)}, valid_inputs{std::move(valid_inputs)},
          all_valid_inputs{std::move(all_valid_inputs)}, context_inputs{std::move(context_inputs)},
          injectable_inputs{std::move(injectable_inputs)}, injectables{injectables}, capture_exception{capture_exception},
          trace_back_depth{trace_back_depth}, wiring_path_name{std::move(wiring_path_name)}, label{std::move(label)},
          capture_values{capture_values}, record_replay_id{std::move(record_replay_id)} {}

    void NodeSignature::register_with_nanobind(nb::module_ &m) {
        nb::class_<NodeSignature>(m, "NodeSignature")
            .def("__init__",
                 [](NodeSignature *self, nb::kwargs kwargs) {
                     new (self) NodeSignature(
                         nb::cast<std::string>(kwargs["name"]), nb::cast<NodeTypeEnum>(kwargs["node_type"]),
                         nb::cast<std::vector<std::string>>(kwargs["args"]),
                         kwargs.contains("time_series_inputs")
                             ? nb::cast<std::optional<std::unordered_map<std::string, nb::object>>>(kwargs["time_series_inputs"])
                             : std::nullopt,
                         kwargs.contains("time_series_output") ? nb::cast<std::optional<nb::object>>(kwargs["time_series_output"])
                                                               : std::nullopt,
                         kwargs.contains("scalars") ? nb::cast<std::optional<nb::dict>>(kwargs["scalars"]) : std::nullopt,
                         nb::cast<nb::object>(kwargs["src_location"]),
                         kwargs.contains("active_inputs")
                             ? nb::cast<std::optional<std::unordered_set<std::string>>>(kwargs["active_inputs"])
                             : std::nullopt,
                         kwargs.contains("valid_inputs")
                             ? nb::cast<std::optional<std::unordered_set<std::string>>>(kwargs["valid_inputs"])
                             : std::nullopt,
                         kwargs.contains("all_valid_inputs")
                             ? nb::cast<std::optional<std::unordered_set<std::string>>>(kwargs["all_valid_inputs"])
                             : std::nullopt,
                         kwargs.contains("context_inputs")
                             ? nb::cast<std::optional<std::unordered_set<std::string>>>(kwargs["context_inputs"])
                             : std::nullopt,
                         kwargs.contains("injectable_types")
                             ? nb::cast<std::optional<std::unordered_map<std::string, InjectableTypesEnum>>>(
                                   kwargs["injectable_types"])
                             : std::nullopt,
                         nb::cast<size_t>(kwargs["injectables"]), nb::cast<bool>(kwargs["capture_exception"]),
                         nb::cast<int64_t>(kwargs["trace_back_depth"]), nb::cast<std::string>(kwargs["wiring_path_name"]),
                         kwargs.contains("label") ? nb::cast<std::optional<std::string>>(kwargs["label"]) : std::nullopt,
                         nb::cast<bool>(kwargs["capture_values"]),
                         kwargs.contains("record_replay_id") ? nb::cast<std::optional<std::string>>(kwargs["record_replay_id"])
                                                             : std::nullopt);
                     ;
                 })
            // .def(nb::init<std::string, NodeTypeEnum, std::vector<std::string>,
            //               std::optional<std::unordered_map<std::string, nb::object>>, std::optional<nb::object>,
            //               std::optional<nb::kwargs>, nb::object, std::optional<std::unordered_set<std::string>>,
            //               std::optional<std::unordered_set<std::string>>, std::optional<std::unordered_set<std::string>>,
            //               std::optional<std::unordered_set<std::string>>,
            //               std::optional<std::unordered_map<std::string, InjectableTypesEnum>>, size_t, bool,
            //               int64_t, std::string, std::optional<std::string>, bool, std::optional<std::string>>(),
            //      "name"_a, "node_type"_a, "args"_a, "time_series_inputs"_a, "time_series_output"_a, "scalars"_a,
            //      "src_location"_a, "active_inputs"_a, "valid_inputs"_a, "all_valid_inputs"_a, "context_inputs"_a,
            //      "injectable_inputs"_a, "injectables"_a, "capture_exception"_a, "trace_back_depth"_a, "wiring_path_name"_a,
            //      "label"_a, "capture_values"_a, "record_replay_id"_a)

            .def_ro("name", &NodeSignature::name)
            .def_ro("node_type", &NodeSignature::node_type)
            .def_ro("args", &NodeSignature::args)
            .def_ro("time_series_inputs", &NodeSignature::time_series_inputs)
            .def_ro("time_series_output", &NodeSignature::time_series_output)
            .def_ro("scalars", &NodeSignature::scalars)
            .def_ro("injectable_inputs", &NodeSignature::injectable_inputs)
            .def_ro("src_location", &NodeSignature::src_location)
            .def_ro("active_inputs", &NodeSignature::active_inputs)
            .def_ro("valid_inputs", &NodeSignature::valid_inputs)
            .def_ro("all_valid_inputs", &NodeSignature::all_valid_inputs)
            .def_ro("context_inputs", &NodeSignature::context_inputs)
            .def_ro("injectables", &NodeSignature::injectables)
            .def_ro("wiring_path_name", &NodeSignature::wiring_path_name)
            .def_ro("label", &NodeSignature::label)
            .def_ro("record_replay_id", &NodeSignature::record_replay_id)
            .def_ro("capture_values", &NodeSignature::capture_values)
            .def_ro("capture_exception", &NodeSignature::capture_exception)
            .def_ro("trace_back_depth", &NodeSignature::trace_back_depth)

            .def_prop_ro("signature", &NodeSignature::signature)
            .def_prop_ro("uses_scheduler", &NodeSignature::uses_scheduler)
            .def_prop_ro("uses_clock", &NodeSignature::uses_clock)
            .def_prop_ro("uses_engine", &NodeSignature::uses_engine)
            .def_prop_ro("uses_state", &NodeSignature::uses_state)
            .def_prop_ro("uses_output_feedback", &NodeSignature::uses_output_feedback)
            .def_prop_ro("uses_recordable_state", &NodeSignature::uses_recordable_state)
            .def_prop_ro("is_source_node", &NodeSignature::is_source_node)
            .def_prop_ro("is_push_source_node", &NodeSignature::is_push_source_node)
            .def_prop_ro("is_pull_source_node", &NodeSignature::is_pull_source_node)
            .def_prop_ro("is_compute_node", &NodeSignature::is_compute_node)
            .def_prop_ro("is_sink_node", &NodeSignature::is_sink_node)
            .def_prop_ro("is_recordable", &NodeSignature::is_recordable)

            .def("to_dict", &NodeSignature::to_dict)
            .def("copy_with", &NodeSignature::copy_with);
    }

    [[nodiscard]] nb::object NodeSignature::get_arg_type(const std::string &arg) const {
        if (time_series_inputs && time_series_inputs->contains(arg)) { return time_series_inputs->at(arg); }
        if (scalars.has_value()) { return scalars->attr("get")(nb::cast(arg)); }
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
        return (injectables & InjectableTypesEnum::SCHEDULER) == InjectableTypesEnum::SCHEDULER;
    }

    [[nodiscard]] bool NodeSignature::uses_clock() const {
        return (injectables & InjectableTypesEnum::CLOCK) == InjectableTypesEnum::CLOCK;
    }

    [[nodiscard]] bool NodeSignature::uses_engine() const {
        return (injectables & InjectableTypesEnum::ENGINE_API) == InjectableTypesEnum::ENGINE_API;
    }

    [[nodiscard]] bool NodeSignature::uses_state() const {
        return (injectables & InjectableTypesEnum::STATE) == InjectableTypesEnum::STATE;
    }

    [[nodiscard]] bool NodeSignature::uses_output_feedback() const {
        return (injectables & InjectableTypesEnum::OUTPUT) == InjectableTypesEnum::OUTPUT;
    }

    [[nodiscard]] bool NodeSignature::uses_recordable_state() const {
        return (injectables & InjectableTypesEnum::RECORDABLE_STATE) == InjectableTypesEnum::RECORDABLE_STATE;
    }

    std::optional<std::string> NodeSignature::recordable_state_arg() const {
        // TODO: Implement
        return std::nullopt;
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

    nb::dict NodeSignature::to_dict() const {
        nb::dict d;
        d["name"]               = name;
        d["node_type"]          = node_type;
        d["args"]               = args;
        d["time_series_inputs"] = time_series_inputs;
        d["time_series_output"] = time_series_output;
        d["scalars"]            = scalars;
        d["src_location"]       = src_location;
        d["active_inputs"]      = active_inputs;
        d["valid_inputs"]       = valid_inputs;
        d["all_valid_inputs"]   = all_valid_inputs;
        d["injectable_inputs"]  = injectable_inputs;
        d["capture_exception"]  = capture_exception;
        d["trace_back_depth"]   = trace_back_depth;
        d["wiring_path_name"]   = wiring_path_name;
        d["label"]              = label;
        d["capture_values"]     = capture_values;
        d["record_replay_id"]   = record_replay_id;
        return d;
    }

    NodeSignature::ptr NodeSignature::copy_with(nb::kwargs kwargs) const {
        auto kwargs_ = to_dict();
        for (const auto &item : kwargs) { kwargs_[item.first] = item.second; }
        return new NodeSignature(
            nb::cast<std::string>(kwargs_["name"]), nb::cast<NodeTypeEnum>(kwargs_["node_type"]),
            nb::cast<std::vector<std::string>>(kwargs_["args"]),
            kwargs_.contains("time_series_inputs")
                ? nb::cast<std::optional<std::unordered_map<std::string, nb::object>>>(kwargs_["time_series_inputs"])
                : std::nullopt,
            kwargs_.contains("time_series_output") ? nb::cast<std::optional<nb::object>>(kwargs_["time_series_output"])
                                                   : std::nullopt,
            kwargs_.contains("scalars") ? nb::cast<std::optional<nb::kwargs>>(kwargs_["scalars"]) : std::nullopt,
            nb::cast<nb::object>(kwargs_["src_location"]),
            kwargs_.contains("active_inputs") ? nb::cast<std::optional<std::unordered_set<std::string>>>(kwargs_["active_inputs"])
                                              : std::nullopt,
            kwargs_.contains("valid_inputs") ? nb::cast<std::optional<std::unordered_set<std::string>>>(kwargs_["valid_inputs"])
                                             : std::nullopt,
            kwargs_.contains("all_valid_inputs")
                ? nb::cast<std::optional<std::unordered_set<std::string>>>(kwargs_["all_valid_inputs"])
                : std::nullopt,
            kwargs_.contains("context_inputs") ? nb::cast<std::optional<std::unordered_set<std::string>>>(kwargs_["context_inputs"])
                                               : std::nullopt,
            kwargs_.contains("injectables")
                ? nb::cast<std::optional<std::unordered_map<std::string, InjectableTypesEnum>>>(kwargs_["injectables"])
                : std::nullopt,
            nb::cast<InjectableTypesEnum>(kwargs_["wiring_path_name"]), nb::cast<bool>(kwargs_["label"]),
            nb::cast<int64_t>(kwargs_["capture_values"]), nb::cast<std::string>(kwargs_["capture_exception"]),
            kwargs_.contains("trace_back_depth") ? nb::cast<std::optional<std::string>>(kwargs_["trace_back_depth"]) : std::nullopt,
            nb::cast<bool>(kwargs_["trace_back_depth"]),
            kwargs_.contains("trace_back_depth") ? nb::cast<std::optional<std::string>>(kwargs_["trace_back_depth"])
                                                 : std::nullopt);
    }

    NodeScheduler::NodeScheduler(Node &node) : _node{node} {}

    engine_time_t NodeScheduler::next_scheduled_time() const {
        return !_scheduled_events.empty() ? (*_scheduled_events.begin()).first : MIN_DT;
    }

    bool NodeScheduler::is_scheduled() const { return !_scheduled_events.empty() || !_alarm_tags.empty(); }

    bool NodeScheduler::is_scheduled_node() const {
        return !_scheduled_events.empty() && _scheduled_events.begin()->first == _node.graph().evaluation_clock().evaluation_time();
    }

    bool NodeScheduler::has_tag(const std::string &tag) const { return _tags.contains(tag); }

    engine_time_t NodeScheduler::pop_tag(const std::string &tag) { return pop_tag(tag, MIN_DT); }

    engine_time_t NodeScheduler::pop_tag(const std::string &tag, engine_time_t default_time) {
        if (_tags.contains(tag)) {
            auto dt = _tags.at(tag);
            _tags.erase(tag);
            _scheduled_events.erase({dt, tag});
            return dt;
        } else {
            return default_time;
        }
    }

    void NodeScheduler::schedule(engine_time_t when, std::optional<std::string> tag, bool on_wall_clock) {
        std::optional<engine_time_t> original_time = std::nullopt;

        if (tag.has_value() && _tags.contains(tag.value())) {
            original_time = next_scheduled_time();
            _scheduled_events.erase({_tags.at(tag.value()), tag.value()});
        }

        if (on_wall_clock) {
            auto clock{dynamic_cast<RealTimeEvaluationClock *>(&_node.graph().evaluation_clock())};
            if (clock) {
                if (!tag.has_value()) { throw std::runtime_error("Can't schedule an alarm without a tag"); }
                auto        tag_{tag.value()};
                std::string alarm_tag = fmt::format("{}:{}", reinterpret_cast<std::uintptr_t>(this), tag_);
                clock->set_alarm(when, alarm_tag, [this, tag_](engine_time_t et) { _on_alarm(et, tag_); });
                _alarm_tags[alarm_tag] = when;
                return;
            }
        }

        auto is_started{_node.is_started()};
        auto now_{is_scheduled_node() ? _node.graph().evaluation_clock().evaluation_time() : MIN_DT};
        if (when > now_) {
            _tags[tag.value_or("")] = when;
            auto current_first      = !_scheduled_events.empty() ? _scheduled_events.begin()->first : MAX_DT;
            _scheduled_events.insert({when, tag.value_or("")});
            auto next_{next_scheduled_time()};
            if (is_started && current_first > next_) {
                bool force_set{original_time.has_value() && original_time.value() < when};
                _node.graph().schedule_node(_node.node_ndx(), next_, force_set);
            }
        }
    }

    void NodeScheduler::schedule(engine_time_delta_t when, std::optional<std::string> tag, bool on_wall_clock) {
        auto when_{_node.graph().evaluation_clock().evaluation_time() + when};
        schedule(when_, std::move(tag), on_wall_clock);
    }

    void NodeScheduler::un_schedule(std::optional<std::string> tag) {
        if (tag.has_value()) {
            auto it = _tags.find(tag.value());
            if (it != _tags.end()) {
                _scheduled_events.erase({it->second, tag.value()});
                _tags.erase(it);
            }
        } else if (!_scheduled_events.empty()) {
            _scheduled_events.erase(_scheduled_events.begin());
        }
    }

    void NodeScheduler::reset() {
        _scheduled_events.clear();
        _tags.clear();
        auto real_time_clock = dynamic_cast<RealTimeEvaluationClock *>(&_node.graph().evaluation_clock());
        if (real_time_clock) {
            for (const auto &alarm : _alarm_tags) { real_time_clock->cancel_alarm(alarm.first); }
            _alarm_tags.clear();
        }
    }

    void NodeScheduler::register_with_nanobind(nb::module_ &m) {
        nb::class_<NodeScheduler, intrusive_base>(m, "NodeScheduler")
            .def_prop_ro("next_scheduled_time", &NodeScheduler::next_scheduled_time)
            .def_prop_ro("is_scheduled", &NodeScheduler::is_scheduled)
            .def_prop_ro("is_scheduled_node", &NodeScheduler::is_scheduled_node)
            .def_prop_ro("has_tag", &NodeScheduler::has_tag)
            .def(
                "pop_tag", [](NodeScheduler &self, const std::string &tag) { return self.pop_tag(tag); }, "tag"_a)
            .def(
                "pop_tag",
                [](NodeScheduler &self, const std::string &tag, engine_time_t default_time) {
                    return self.pop_tag(tag, default_time);
                },
                "tag"_a, "default_time"_a)
            .def(
                "schedule",
                [](NodeScheduler &self, engine_time_t when, std::optional<std::string> tag, bool on_wall_clock) {
                    self.schedule(when, std::move(tag), on_wall_clock);
                },
                "when"_a, "tag"_a = nb::none(), "on_wall_clock"_a = false)
            .def(
                "schedule",
                [](NodeScheduler &self, engine_time_delta_t when, std::optional<std::string> tag, bool on_wall_clock) {
                    self.schedule(when, std::move(tag), on_wall_clock);
                },
                "when"_a, "tag"_a = nb::none(), "on_wall_clock"_a = false)
            .def("un_schedule", &NodeScheduler::un_schedule, "tag"_a = nb::none())
            .def("reset", &NodeScheduler::reset);
    }

    void NodeScheduler::_on_alarm(engine_time_t when, std::string tag) {
        _tags[tag]            = when;
        std::string alarm_tag = fmt::format("{}:{}", reinterpret_cast<std::uintptr_t>(this), tag);
        _alarm_tags.erase(alarm_tag);
        _scheduled_events.insert({when, tag});
        _node.graph().schedule_node(_node.node_ndx(), when);
    }

    Node::Node(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::ptr signature, nb::dict scalars)
        : _node_ndx{node_ndx}, _owning_graph_id{std::move(owning_graph_id)}, _signature{std::move(signature)},
          _scalars{std::move(scalars)} {}

    void Node::notify(engine_time_t modified_time) {
        if (is_started() || is_starting()) {
            graph().schedule_node(node_ndx(), modified_time);
        } else {
            scheduler()->schedule(MIN_ST, "start");
        }
    }

    void Node::notify() { notify(graph().evaluation_clock().evaluation_time()); }

    void Node::notify_next_cycle() {
        if (is_started() || is_starting()) {
            graph().schedule_node(node_ndx(), graph().evaluation_clock().next_cycle_evaluation_time());
        } else {
            notify();
        }
    }

    int64_t Node::node_ndx() const { return _node_ndx; }

    const std::vector<int64_t> &Node::owning_graph_id() const { return _owning_graph_id; }

    const std::vector<int64_t> &Node::node_id() const { return _node_id; }

    const NodeSignature &Node::signature() const { return *_signature; }

    const nb::dict &Node::scalars() const { return _scalars; }

    Graph &Node::graph() { return *_graph; }

    const Graph &Node::graph() const { return *_graph; }

    void Node::set_graph(graph_ptr value) { _graph = value; }

    TimeSeriesBundleInput &Node::input() { return *_input; }

    void Node::set_input(time_series_bundle_input_ptr value) { _input = value; }

    TimeSeriesOutput &Node::output() { return *_output; }

    void Node::set_output(time_series_output_ptr value) { _output = value; }

    TimeSeriesBundleOutput &Node::recordable_state() { return *_recordable_state; }

    void Node::set_recordable_state(nb::ref<TimeSeriesBundleOutput> value) { _recordable_state = value; }

    bool Node::has_recordable_state() const { return _recordable_state.get() != nullptr; }

    std::optional<NodeScheduler> Node::scheduler() const { return _scheduler; }

    TimeSeriesOutput &Node::error_output() { return *_error_output; }

    void Node::set_error_output(time_series_output_ptr value) { _error_output = std::move(value); }

    void Node::add_start_input(nb::ref<TimeSeriesReferenceInput> input) { _start_inputs.push_back(std::move(input)); }

    void Node::register_with_nanobind(nb::module_ &m) {
        nb::class_<Node, ComponentLifeCycle>(m, "Node")
            .def_prop_ro("node_ndx", &Node::node_ndx)
            .def_prop_ro("owning_graph_id", &Node::owning_graph_id)
            .def_prop_ro("node_id", &Node::node_id)
            .def_prop_ro("signature", &Node::signature)
            .def_prop_ro("scalars", &Node::scalars)
            .def_prop_ro("graph", static_cast<const Graph &(Node::*)() const>(&Node::graph))
            .def_prop_ro("input", &Node::input)
            .def_prop_ro("inputs",
                         [](Node &self) {
                             nb::dict d;
                             auto     inp_{self.input()};
                             for (const auto &key : self.input().schema().keys()) { d[key.c_str()] = inp_[key]; }
                             return d;
                         })
            .def_prop_ro("start_inputs",
                         [](Node &self) {
                             nb::list l;
                             for (const auto &input : self._start_inputs) { l.append(input); }
                             return l;
                         })
            .def_prop_ro("output", &Node::output)
            .def_prop_ro("recordable_state", &Node::recordable_state)
            .def_prop_ro("scheduler", &Node::scheduler)
            .def("eval", &Node::eval)
            .def("notify", [](Node &self) { self.notify(); })
            .def(
                "notify", [](Node &self, engine_time_t modified_time) { self.notify(modified_time); }, "modified_time"_a)
            .def("notify_next_cycle", &Node::notify_next_cycle)
            .def_prop_ro("error_output", &Node::error_output);

        nb::class_<BasePythonNode, Node>(m, "BasePythonNode");
        nb::class_<PythonNode, BasePythonNode>(m, "PythonNode");
        nb::class_<PythonGeneratorNode, BasePythonNode>(m, "PythonGeneratorNode");
    }

    BasePythonNode::BasePythonNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::ptr signature,
                                   nb::dict scalars, nb::callable eval_fn, nb::callable start_fn, nb::callable end_fn)
        : Node(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars)), _eval_fn{std::move(eval_fn)},
          _start_fn{std::move(start_fn)}, _end_fn{std::move(end_fn)} {}

    void BasePythonNode::_initialise_kwargs() {
        // Assuming Injector and related types are properly defined, and scalars is a map-like container
        auto &signature_args = signature().args;
        _kwargs              = {};

        bool has_injectables{signature().injectable_inputs.has_value()};
        for (const auto &[key_, value] : scalars()) {
            std::string key{nb::cast<std::string>(key_)};
            if (has_injectables && signature().injectable_inputs->contains(key)) {
                _kwargs[key_] = value(*(this));  // Assuming this call applies the Injector properly
            } else {
                _kwargs[key_] = value;
            }
        }

        for (size_t i = 0; i < input().schema().keys().size(); ++i) {
            // Apple does not yet support contains :(
            auto key{input().schema().keys()[i]};
            if (std::ranges::find(signature_args, key) != std::ranges::end(signature_args)) { _kwargs[key.c_str()] = input()[i]; }
        }
    }

    void Node::_initialise_inputs() {
        if (signature().time_series_inputs.has_value()) {
            for (auto &start_input : _start_inputs) {
                start_input->start();  // Assuming start_input is some time series type with a start method
            }
            for (size_t i = 0; i < signature().time_series_inputs->size(); ++i) {
                // Apple does not yet support contains :(
                if (!signature().active_inputs || (std::ranges::find(*signature().active_inputs, signature().args[i]) !=
                                                   std::ranges::end(*signature().active_inputs))) {
                    input()[i].make_active();  // Assuming `make_active` is a method of the `TimeSeriesInput` type
                }
            }
        }
    }

    void BasePythonNode::_initialise_state() {
        if (has_recordable_state()) {
            // TODO: Implement this

            // auto &record_context = RecordReplayContext::instance();
            // auto  mode           = record_context.mode();
            //
            // if (mode.contains(RecordReplayEnum::RECOVER)) {
            //     // TODO: make recordable_id unique by using parent node context information
            //     auto recordable_id   = get_fq_recordable_id(this->graph().traits(), this->signature().record_replay_id());
            //     auto clock           = this->graph().evaluation_clock();
            //     auto evaluation_time = clock.evaluation_time();
            //     auto as_of_time      = get_as_of(clock);
            //
            //     this->recordable_state().value() =
            //         replay_const("__state__", this->signature().recordable_state().tsb_type().py_type(), recordable_id,
            //                      evaluation_time - MIN_TD,  // We want the state just before now
            //                      as_of_time)
            //             .value();
            // }
        }
    }

    void BasePythonNode::initialise() {}

    void BasePythonNode::start() {}

    void BasePythonNode::stop() {}

    void BasePythonNode::dispose() {}

    void PythonNode::initialise() {}

    void PythonNode::start() {}

    void PythonNode::stop() {}

    void PythonNode::dispose() {}

    void PythonNode::eval() {}

    void PushQueueNode::eval() {}

    void PushQueueNode::enqueue_message(nb::object message) {
        ++_messages_queued;
        _receiver->enqueue({node_ndx(), std::move(message)});
    }

    bool PushQueueNode::apply_message(nb::object message) {
        if (_elide || output().can_apply_result(message)) {
            output().apply_result(std::move(message));
            return true;
        }
        return false;
    }

    int64_t PushQueueNode::messages_in_queue() const { return _messages_queued - _messages_dequeued; }

    void PushQueueNode::set_receiver(sender_receiver_state_ptr value) { _receiver = value; }

    void PushQueueNode::start() {
        _receiver = &graph().receiver();
        _elide    = scalars().contains("elide") ? nb::cast<bool>(scalars()["elide"]) : false;
    }

    void PythonGeneratorNode::eval() {

    }

    void PythonGeneratorNode::start() { BasePythonNode::start(); }

}  // namespace hgraph
