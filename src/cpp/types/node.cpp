#include <hgraph/types/time_series_type.h>

#include <hgraph/python/pyb_wiring.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <sstream>
#include <fmt/format.h>

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
    NodeSignature::NodeSignature(std::string name, NodeTypeEnum node_type, std::vector<std::string> args,
                                 std::optional<std::unordered_map<std::string, nb::object>> time_series_inputs,
                                 std::optional<nb::object>                                  time_series_output,
                                 std::optional<std::unordered_map<std::string, nb::object>> scalars, nb::object src_location,
                                 std::optional<std::unordered_set<std::string>> active_inputs,
                                 std::optional<std::unordered_set<std::string>> valid_inputs,
                                 std::optional<std::unordered_set<std::string>> all_valid_inputs,
                                 std::optional<std::unordered_set<std::string>> context_inputs,
                                 InjectableTypesEnum injectable_inputs, std::string wiring_path_name,
                                 std::optional<std::string> label, std::optional<std::string> record_replay_id, bool capture_values,
                                 bool capture_exception, int64_t trace_back_depth)
        : name{std::move(name)}, node_type{node_type}, args{std::move(args)}, time_series_inputs{std::move(time_series_inputs)},
          time_series_output{std::move(time_series_output)}, scalars{std::move(scalars)}, src_location{std::move(src_location)},
          active_inputs{std::move(active_inputs)}, valid_inputs{std::move(valid_inputs)},
          all_valid_inputs{std::move(all_valid_inputs)}, context_inputs{std::move(context_inputs)},
          injectable_inputs{injectable_inputs}, wiring_path_name{std::move(wiring_path_name)}, label{std::move(label)},
          record_replay_id{std::move(record_replay_id)}, capture_values{capture_values}, capture_exception{capture_exception},
          trace_back_depth{trace_back_depth} {}

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
                          int64_t>(),
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

    NodeScheduler::NodeScheduler(Node &node) : _node{node} {}

    engine_time_t NodeScheduler::next_scheduled_time() const {
        return !_scheduled_events.empty() ? (*_scheduled_events.begin()).first : MIN_DT;
    }

    bool NodeScheduler::is_scheduled() const { return !_scheduled_events.empty() || !_alarm_tags.empty(); }

    bool NodeScheduler::is_scheduled_node() const {
        return !_scheduled_events.empty() && _scheduled_events.begin()->first == _node.graph().evaluation_clock().evaluation_time();
    }

    bool NodeScheduler::has_tag(const std::string &tag) const { return _tags.contains(tag); }

    engine_time_t NodeScheduler::pop_tag(const std::string &tag, std::optional<engine_time_t> default_time) {
        if (_tags.contains(tag)) {
            auto dt = _tags.at(tag);
            _tags.erase(tag);
            _scheduled_events.erase({dt, tag});
            return dt;
        } else {
            return default_time.value_or(MIN_DT);
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

    std::vector<nb::ref<TimeSeriesInput>> Node::start_inputs() { return _start_inputs; }

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

    std::optional<NodeScheduler> Node::scheduler() const { return _scheduler; }

    TimeSeriesOutput &Node::error_output() { return *_error_output; }

    void Node::set_error_output(time_series_output_ptr value) { _error_output = std::move(value); }

    void PushQueueNode::eval() {}

    void PushQueueNode::enqueue_message(std::any message) {
        ++_messages_queued;
        _receiver->enqueue({node_ndx(), std::move(message)});
    }

    bool PushQueueNode::apply_message(std::any message) {
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

}  // namespace hgraph
