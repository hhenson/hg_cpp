#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/tsb.h>
#include <ranges>
#include <sstream>
#include <fmt/format.h>
#include <cstdlib>

namespace hgraph
{
    void node_type_enum_py_register(nb::module_ &m) {
        nb::enum_<NodeTypeEnum>(m, "NodeTypeEnum")
            .value("NONE", NodeTypeEnum::NONE)
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
            .value("TRAIT", InjectableTypesEnum::TRAIT)
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
        : intrusive_base(), name{std::move(name)}, node_type{node_type}, args{std::move(args)},
          time_series_inputs{std::move(time_series_inputs)}, time_series_output{std::move(time_series_output)},
          scalars{std::move(scalars)}, src_location{std::move(src_location)}, active_inputs{std::move(active_inputs)},
          valid_inputs{std::move(valid_inputs)}, all_valid_inputs{std::move(all_valid_inputs)},
          context_inputs{std::move(context_inputs)}, injectable_inputs{std::move(injectable_inputs)}, injectables{injectables},
          capture_exception{capture_exception}, trace_back_depth{trace_back_depth}, wiring_path_name{std::move(wiring_path_name)},
          label{std::move(label)}, capture_values{capture_values}, record_replay_id{std::move(record_replay_id)} {}

    void NodeSignature::register_with_nanobind(nb::module_ &m) {
        nb::class_<NodeSignature, intrusive_base>(m, "NodeSignature")
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
                         kwargs["src_location"],
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
                         kwargs.contains("injectable_inputs")
                             ? nb::cast<std::optional<std::unordered_map<std::string, InjectableTypesEnum>>>(
                                   kwargs["injectable_inputs"])
                             : std::nullopt,
                         nb::cast<size_t>(kwargs["injectables"]), nb::cast<bool>(kwargs["capture_exception"]),
                         nb::cast<int64_t>(kwargs["trace_back_depth"]), nb::cast<std::string>(kwargs["wiring_path_name"]),
                         kwargs.contains("label") ? nb::cast<std::optional<std::string>>(kwargs["label"]) : std::nullopt,
                         nb::cast<bool>(kwargs["capture_values"]),
                         kwargs.contains("record_replay_id") ? nb::cast<std::optional<std::string>>(kwargs["record_replay_id"])
                                                             : std::nullopt);
                 })
            // For some reason doing this does not work, and need to use the lambda form above, very annoying.
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
            .def("copy_with", &NodeSignature::copy_with)

            .def("__str__", [](const NodeSignature &self) {
                return self.signature();
            })
            .def("__repr__", [](const NodeSignature &self) {
                std::ostringstream oss;
                oss << "NodeSignature(name='" << self.name << "'";
                oss << ", node_type=" << static_cast<int>(self.node_type);

                // args
                oss << ", args=[";
                for (size_t i = 0; i < self.args.size(); ++i) {
                    if (i > 0) oss << ", ";
                    oss << "'" << self.args[i] << "'";
                }
                oss << "]";

                // time_series_inputs
                oss << ", time_series_inputs=";
                if (self.time_series_inputs.has_value()) {
                    oss << "{...}";  // dict with " << self.time_series_inputs->size() << " items
                } else {
                    oss << "None";
                }

                // time_series_output
                oss << ", time_series_output=";
                if (self.time_series_output.has_value()) {
                    oss << "<object>";
                } else {
                    oss << "None";
                }

                // scalars
                oss << ", scalars=";
                if (self.scalars.has_value()) {
                    oss << "{...}";
                } else {
                    oss << "None";
                }

                // src_location
                oss << ", src_location=<object>";

                // active_inputs
                oss << ", active_inputs=";
                if (self.active_inputs.has_value()) {
                    oss << "{";
                    bool first = true;
                    for (const auto& inp : *self.active_inputs) {
                        if (!first) oss << ", ";
                        oss << "'" << inp << "'";
                        first = false;
                    }
                    oss << "}";
                } else {
                    oss << "None";
                }

                // valid_inputs
                oss << ", valid_inputs=";
                if (self.valid_inputs.has_value()) {
                    oss << "{";
                    bool first = true;
                    for (const auto& inp : *self.valid_inputs) {
                        if (!first) oss << ", ";
                        oss << "'" << inp << "'";
                        first = false;
                    }
                    oss << "}";
                } else {
                    oss << "None";
                }

                // all_valid_inputs
                oss << ", all_valid_inputs=";
                if (self.all_valid_inputs.has_value()) {
                    oss << "{";
                    bool first = true;
                    for (const auto& inp : *self.all_valid_inputs) {
                        if (!first) oss << ", ";
                        oss << "'" << inp << "'";
                        first = false;
                    }
                    oss << "}";
                } else {
                    oss << "None";
                }

                // context_inputs
                oss << ", context_inputs=";
                if (self.context_inputs.has_value()) {
                    oss << "{";
                    bool first = true;
                    for (const auto& inp : *self.context_inputs) {
                        if (!first) oss << ", ";
                        oss << "'" << inp << "'";
                        first = false;
                    }
                    oss << "}";
                } else {
                    oss << "None";
                }

                // injectable_inputs
                oss << ", injectable_inputs=";
                if (self.injectable_inputs.has_value()) {
                    oss << "{...}";  // map with enum values
                } else {
                    oss << "None";
                }

                // injectables
                oss << ", injectables=" << self.injectables;

                // capture_exception
                oss << ", capture_exception=" << (self.capture_exception ? "True" : "False");

                // trace_back_depth
                oss << ", trace_back_depth=" << self.trace_back_depth;

                // wiring_path_name
                oss << ", wiring_path_name='" << self.wiring_path_name << "'";

                // label
                oss << ", label=";
                if (self.label.has_value()) {
                    oss << "'" << *self.label << "'";
                } else {
                    oss << "None";
                }

                // capture_values
                oss << ", capture_values=" << (self.capture_values ? "True" : "False");

                // record_replay_id
                oss << ", record_replay_id=";
                if (self.record_replay_id.has_value()) {
                    oss << "'" << *self.record_replay_id << "'";
                } else {
                    oss << "None";
                }

                oss << ")";
                return oss.str();
            });
    }

    [[nodiscard]] nb::object NodeSignature::get_arg_type(const std::string &arg) const {
        if (time_series_inputs && time_series_inputs->contains(arg)) { return time_series_inputs->at(arg); }
        if (scalars.has_value()) { return scalars->attr("get")(nb::cast(arg)); }
        return nb::none();
    }

    [[nodiscard]] std::string obj_to_str(const nb::object &obj) {
        if (obj.is_none()) { return "None"; }
        return nb::cast<std::string>(nb::str(obj));
    }

    [[nodiscard]] std::string NodeSignature::signature() const {
        std::ostringstream oss;
        bool               first = true;
        auto               none_str{std::string("None")};

        oss << name << "(";

        for (const auto &arg : args) {
            if (!first) { oss << ", "; }
            oss << arg << ": " << obj_to_str(get_arg_type(arg));
            first = false;
        }

        oss << ")";

        if (time_series_output.has_value()) {
            auto v = time_series_output.value();
            oss << " -> " << obj_to_str(v);
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
        d["context_inputs"]     = context_inputs;
        d["injectable_inputs"]  = injectable_inputs;
        d["injectables"]        = injectables;
        d["capture_exception"]  = capture_exception;
        d["trace_back_depth"]   = trace_back_depth;
        d["wiring_path_name"]   = wiring_path_name;
        d["label"]              = label;
        d["capture_values"]     = capture_values;
        d["record_replay_id"]   = record_replay_id;
        return d;
    }

    NodeSignature::ptr NodeSignature::copy_with(nb::kwargs kwargs) const {
        // Get override values from kwargs, otherwise use current values
        std::string name_val = kwargs.contains("name") ? nb::cast<std::string>(kwargs["name"]) : this->name;
        NodeTypeEnum node_type_val = kwargs.contains("node_type") ? nb::cast<NodeTypeEnum>(kwargs["node_type"]) : this->node_type;
        std::vector<std::string> args_val = kwargs.contains("args") ? nb::cast<std::vector<std::string>>(kwargs["args"]) : this->args;
        std::string wiring_path_name_val = kwargs.contains("wiring_path_name") ? nb::cast<std::string>(kwargs["wiring_path_name"]) : this->wiring_path_name;

        auto* raw = new NodeSignature(
            name_val,
            node_type_val,
            args_val,
            kwargs.contains("time_series_inputs") ? nb::cast<std::optional<std::unordered_map<std::string, nb::object>>>(kwargs["time_series_inputs"]) : this->time_series_inputs,
            kwargs.contains("time_series_output") ? nb::cast<std::optional<nb::object>>(kwargs["time_series_output"]) : this->time_series_output,
            kwargs.contains("scalars") ? nb::cast<std::optional<nb::dict>>(kwargs["scalars"]) : this->scalars,
            kwargs.contains("src_location") ? kwargs["src_location"] : this->src_location,
            kwargs.contains("active_inputs") ? nb::cast<std::optional<std::unordered_set<std::string>>>(kwargs["active_inputs"]) : this->active_inputs,
            kwargs.contains("valid_inputs") ? nb::cast<std::optional<std::unordered_set<std::string>>>(kwargs["valid_inputs"]) : this->valid_inputs,
            kwargs.contains("all_valid_inputs") ? nb::cast<std::optional<std::unordered_set<std::string>>>(kwargs["all_valid_inputs"]) : this->all_valid_inputs,
            kwargs.contains("context_inputs") ? nb::cast<std::optional<std::unordered_set<std::string>>>(kwargs["context_inputs"]) : this->context_inputs,
            kwargs.contains("injectable_inputs") ? nb::cast<std::optional<std::unordered_map<std::string, InjectableTypesEnum>>>(kwargs["injectable_inputs"]) : this->injectable_inputs,
            kwargs.contains("injectables") ? nb::cast<size_t>(kwargs["injectables"]) : this->injectables,
            kwargs.contains("capture_exception") ? nb::cast<bool>(kwargs["capture_exception"]) : this->capture_exception,
            kwargs.contains("trace_back_depth") ? nb::cast<int64_t>(kwargs["trace_back_depth"]) : this->trace_back_depth,
            wiring_path_name_val,
            kwargs.contains("label") ? nb::cast<std::optional<std::string>>(kwargs["label"]) : this->label,
            kwargs.contains("capture_values") ? nb::cast<bool>(kwargs["capture_values"]) : this->capture_values,
            kwargs.contains("record_replay_id") ? nb::cast<std::optional<std::string>>(kwargs["record_replay_id"]) : this->record_replay_id);
        // Wrap into a nanobind intrusive ref explicitly to ensure correct refcount semantics
        return nb::ref<NodeSignature>(raw);
    }

    NodeScheduler::NodeScheduler(Node &node) : _node{node} {}

    engine_time_t NodeScheduler::next_scheduled_time() const {
        return !_scheduled_events.empty() ? (*_scheduled_events.begin()).first : MIN_DT;
    }

    bool NodeScheduler::requires_scheduling() const { return !_scheduled_events.empty(); }

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

    void NodeScheduler::un_schedule(const std::string &tag) {
        auto it = _tags.find(tag);
        if (it != _tags.end()) {
            _scheduled_events.erase({it->second, tag});
            _tags.erase(it);
        }
    }

    void NodeScheduler::un_schedule() {
        if (!_scheduled_events.empty()) { _scheduled_events.erase(_scheduled_events.begin()); }
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

    static const std::string VERY_LARGE_STRING = "\xFF";

    void NodeScheduler::advance() {
        if (_scheduled_events.empty()) { return; }
        auto until = _node.graph().evaluation_clock().evaluation_time();
        // Note: empty string is considered smallest in std::string comparison,
        // so upper_bound will correctly find elements <= until regardless of tag value
        _scheduled_events.erase(_scheduled_events.begin(), _scheduled_events.upper_bound({until, VERY_LARGE_STRING}));

        if (!_scheduled_events.empty()) { _node.graph().schedule_node(_node.node_ndx(), _scheduled_events.begin()->first); }
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
            .def("un_schedule", static_cast<void (NodeScheduler::*)(const std::string &)>(&NodeScheduler::un_schedule), "tag"_a)
            .def("un_schedule", static_cast<void (NodeScheduler::*)()>(&NodeScheduler::un_schedule))
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
            scheduler().schedule(MIN_ST, "start");
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

    std::vector<int64_t> Node::node_id() const {
        // Check how often this is called, in the Python code this is a cached property, which means we could just
        // construct in the constructor, but if it is not used frequently this may be a better use of resources.
        std::vector<int64_t> node_id;
        node_id.reserve(_owning_graph_id.size() + 1);
        node_id.insert(node_id.end(), _owning_graph_id.begin(), _owning_graph_id.end());  // Copy graph_id into node_id
        node_id.push_back(_node_ndx);
        return node_id;
    }

    const NodeSignature &Node::signature() const { return *_signature; }

    const nb::dict &Node::scalars() const { return _scalars; }

    Graph &Node::graph() { return *_graph; }

    const Graph &Node::graph() const { return *_graph; }

    void Node::set_graph(graph_ptr value) { _graph = value; }

    TimeSeriesBundleInput       &Node::input() { return *_input; }
    const TimeSeriesBundleInput &Node::input() const { return *_input; }

    time_series_bundle_input_ptr Node::input_ptr() { return _input; }
    time_series_bundle_input_ptr Node::input_ptr() const { return _input; }

    void Node::set_input(time_series_bundle_input_ptr value) {
        if (has_input()) { throw std::runtime_error("Input already set on node: " + _signature->signature()); }
        reset_input(std::move(value));
    }
    void Node::reset_input(time_series_bundle_input_ptr value) {
        _input = std::move(value);
        _check_valid_inputs.reserve(signature().valid_inputs.has_value() ? signature().valid_inputs->size()
                                                                             : signature().time_series_inputs->size());
        if (signature().valid_inputs.has_value()) {
            for (const auto &key : std::views::all(*signature().valid_inputs)) { _check_valid_inputs.push_back(input()[key]); }
        } else {
            for (const auto &key : std::views::elements<0>(*signature().time_series_inputs)) {
                // Do not treat context inputs as required by default
                bool is_context = signature().context_inputs.has_value() && signature().context_inputs->contains(key);
                if (!is_context) { _check_valid_inputs.push_back(input()[key]); }
            }
        }
        if (signature().all_valid_inputs.has_value()) {
            _check_all_valid_inputs.reserve(signature().all_valid_inputs->size());
            for (const auto &key : *signature().all_valid_inputs) { _check_all_valid_inputs.push_back(input()[key]); }
        }
    }

    TimeSeriesOutput      &Node::output() { return *_output; }
    time_series_output_ptr Node::output_ptr() { return _output; }

    void Node::set_output(time_series_output_ptr value) { _output = value; }

    TimeSeriesBundleOutput       &Node::recordable_state() { return *_recordable_state; }
    time_series_bundle_output_ptr Node::recordable_state_ptr() { return _recordable_state; }

    void Node::set_recordable_state(nb::ref<TimeSeriesBundleOutput> value) { _recordable_state = value; }

    bool Node::has_recordable_state() const { return _recordable_state.get() != nullptr; }

    NodeScheduler &Node::scheduler() {
        if (_scheduler.get() == nullptr) { _scheduler = new NodeScheduler(*this); }
        return *_scheduler;
    }

    bool Node::has_scheduler() const { return _scheduler != nullptr; }

    void Node::unset_scheduler() { _scheduler.reset(); }

    TimeSeriesOutput      &Node::error_output() { return *_error_output; }
    time_series_output_ptr Node::error_output_ptr() { return _error_output; }

    void Node::set_error_output(time_series_output_ptr value) { _error_output = std::move(value); }

    void Node::add_start_input(nb::ref<TimeSeriesReferenceInput> input) { _start_inputs.push_back(std::move(input)); }

    void Node::register_with_nanobind(nb::module_ &m) {
        nb::class_<Node, ComponentLifeCycle>(m, "Node")
            .def_prop_ro("node_ndx", &Node::node_ndx)
            .def_prop_ro("owning_graph_id", [](const Node &n) {
                // Convert vector to tuple for Python compatibility
                // Python code expects owning_graph_id to be a tuple, not a list
                const auto &vec = n.owning_graph_id();
                nb::list py_list;
                for (const auto &id : vec) { py_list.append(id); }
                return nb::tuple(py_list);
            })
            .def_prop_ro("node_id", &Node::node_id)
            .def_prop_ro("signature", &Node::signature)
            .def_prop_ro("scalars", &Node::scalars)
            .def_prop_rw("graph", static_cast<const Graph &(Node::*)() const>(&Node::graph), &Node::set_graph)
            .def_prop_rw("input", static_cast<const TimeSeriesBundleInput &(Node::*)() const>(&Node::input), &Node::set_input)
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
            .def_prop_rw("output", &Node::output, &Node::set_output)
            .def_prop_rw("recordable_state", &Node::recordable_state, &Node::set_recordable_state)
            .def_prop_ro("scheduler", &Node::scheduler)
            .def("eval", &Node::eval)
            .def("notify", [](Node &self) { self.notify(); })
            .def(
                "notify", [](Node &self, engine_time_t modified_time) { self.notify(modified_time); }, "modified_time"_a)
            .def("notify_next_cycle", &Node::notify_next_cycle)
            .def_prop_rw("error_output", &Node::error_output, &Node::set_error_output)
            .def("__repr__", &Node::repr)
            .def("__str__", &Node::str);

        nb::class_<BasePythonNode, Node>(m, "BasePythonNode");
        nb::class_<PythonNode, BasePythonNode>(m, "PythonNode")
            .def(nb::init<int64_t, std::vector<int64_t>, NodeSignature::ptr, nb::dict, nb::callable, nb::callable, nb::callable>(),
                 "node_ndx"_a, "owning_graph_id"_a, "signature"_a, "scalars"_a, "eval_fn"_a = nb::none(), "start_fn"_a = nb::none(),
                 "stop_fn"_a = nb::none());
        nb::class_<PythonGeneratorNode, BasePythonNode>(m, "PythonGeneratorNode");
    }

    bool Node::has_input() const { return _input.get() != nullptr; }

    bool Node::has_output() const { return _output.get() != nullptr; }

    std::string Node::repr() const {
        std::ostringstream oss;
        bool               first = true;
        auto               none_str{std::string("None")};

        oss << "[";
        for (auto ndx : _owning_graph_id) {
            if (!first) { oss << ", "; }
            oss << ndx;
            first = false;
        }
        oss << ", " << _node_ndx << "]" << signature().name << "(";

        auto obj_to_type = [&](const nb::object &obj) { return obj.is_none() ? none_str : nb::cast<std::string>(obj); };

        first = true;
        for (const auto &arg : signature().args) {
            if (!first) { oss << ", "; }
            oss << arg << ": " << obj_to_type(signature().get_arg_type(arg));
            if (!signature().time_series_inputs->contains(arg)) {
                nb::handle key_handle{_scalars[arg.c_str()]};
                nb::str s{nb::str(key_handle)};
                size_t length{nb::len(s)};
                if (length > 8) { s = nb::str("{}...").format(s[nb::slice(0, 8)]); }
                oss << "=" << s.c_str();
            }
            first = false;
        }

        oss << ")";

        if (bool(signature().time_series_output)) {
            auto v = signature().time_series_output.value();
            oss << " -> " << (v.is_none() ? none_str : nb::cast<std::string>(v));
        }

        return oss.str();
    }

    std::string Node::str() const {
        if (signature().label.has_value()) { return fmt::format("{}.{}", signature().wiring_path_name, signature().label.value()); }
        return fmt::format("{}.{}", signature().wiring_path_name, signature().name);
    }
    void Node::start() {
        do_start();
        if (has_scheduler()) {
            auto pop_result = scheduler().pop_tag("start");
            if (pop_result != MIN_DT) {
                notify();
                if (!signature().uses_scheduler()) {
                    _scheduler.reset();
                }
            } else {
                scheduler().advance();
            }
        }
    }

    void Node::stop() {
        do_stop();
        if (has_input()) { input().un_bind_output(false); }
        if (has_scheduler()) { scheduler().reset(); }
    }

    BasePythonNode::BasePythonNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::ptr signature,
                                   nb::dict scalars, nb::callable eval_fn, nb::callable start_fn, nb::callable stop_fn)
        : Node(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars)), _eval_fn{std::move(eval_fn)},
          _start_fn{std::move(start_fn)}, _stop_fn{std::move(stop_fn)} {}

    void BasePythonNode::_initialise_kwargs() {
        // Assuming Injector and related types are properly defined, and scalars is a map-like container
        auto &signature_args = signature().args;
        _kwargs              = {};

        bool has_injectables{signature().injectables != 0};
        for (const auto &[key_, value] : scalars()) {
            std::string key{nb::cast<std::string>(key_)};
            if (has_injectables && signature().injectable_inputs->contains(key)) {
                // TODO: This may be better extracted directly, but for now use the python function calls.
                nb::object node{nb::cast(this)};
                nb::object key_handle{value(node)};
                _kwargs[key_] = key_handle;  // Assuming this call applies the Injector properly
            } else {
                _kwargs[key_] = value;
            }
        }
        for (size_t i = 0, l = signature().time_series_inputs.has_value() ? signature().time_series_inputs->size() : 0; i < l;
             ++i) {
            // Apple does not yet support ranges::contains :(
            auto key{input().schema().keys()[i]};
            if (std::ranges::find(signature_args, key) != std::ranges::end(signature_args)) {
                // Force exposure of inputs as base TimeSeriesInput to avoid double-wrapping as derived classes
                _kwargs[key.c_str()] = nb::cast<TimeSeriesInput&>(*input()[i]);
            }
        }
    }

    void Node::_initialise_inputs() {
        if (signature().time_series_inputs.has_value()) {
            for (auto &start_input : _start_inputs) {
                start_input->start();  // Assuming start_input is some time series type with a start method
            }
            for (size_t i = 0; i < signature().time_series_inputs->size(); ++i) {
                // Apple does not yet support ranges::contains :(
                if (!signature().active_inputs || (std::ranges::find(*signature().active_inputs, signature().args[i]) !=
                                                   std::ranges::end(*signature().active_inputs))) {
                    input()[i]->make_active();  // Assuming `make_active` is a method of the `TimeSeriesInput` type
                }
            }
        }
    }

    void BasePythonNode::_initialise_state() {
        if (has_recordable_state()) {
            // TODO: Implement this once a bit more infra is in place
            throw std::runtime_error("Recordable state not yet implemented");
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
    void BasePythonNode::do_eval() {
        try {
            auto out{_eval_fn(**_kwargs)};
            if (!out.is_none()) { output().apply_result(out); }
        } catch (nb::python_error &e) {
            // Convert Python error into enriched NodeException immediately to ensure readable propagation
            throw NodeException::capture_error(e, *this, "During Python node evaluation");
        }
    }

    void BasePythonNode::do_start() {
        if (_start_fn.is_valid() && !_start_fn.is_none()) {
            // Get the callable signature parameters using inspect.signature
            // This matches Python's approach: signature(self.start_fn).parameters.keys()
            // Using __code__.co_varnames includes local variables, not just parameters
            auto inspect = nb::module_::import_("inspect");
            auto sig = inspect.attr("signature")(_start_fn);
            auto params = sig.attr("parameters").attr("keys")();

            // Filter kwargs to only include parameters in start_fn signature
            nb::dict filtered_kwargs;
            for (auto k : params) {
                if (_kwargs.contains(k)) { filtered_kwargs[k] = _kwargs[k]; }
            }
            // Call start_fn with filtered kwargs
            _start_fn(**filtered_kwargs);
        }
    }

    void BasePythonNode::do_stop() {
        if (_stop_fn.is_valid() and !_stop_fn.is_none()) {
            // Get the callable signature parameters using inspect.signature
            // This matches Python's approach: signature(self.stop_fn).parameters.keys()
            // Using __code__.co_varnames includes local variables, not just parameters
            auto inspect = nb::module_::import_("inspect");
            auto sig = inspect.attr("signature")(_stop_fn);
            auto params = sig.attr("parameters").attr("keys")();

            // Filter kwargs to only include parameters in stop_fn signature
            nb::dict filtered_kwargs;
            for (auto k : params) {
                if (_kwargs.contains(k)) { filtered_kwargs[k] = _kwargs[k]; }
            }

            // Call stop_fn with filtered kwargs
            _stop_fn(**filtered_kwargs);
        }
    }

    void Node::eval() {
        bool scheduled{has_scheduler() ? _scheduler->is_scheduled_node() : false};
        bool should_eval{true};

        if (has_input()) {

            // Check validity of required inputs
            for (const auto &input_ : _check_valid_inputs) {
                if (!input_->valid()) {
                    should_eval = false;
                    break;
                }
            }

            if (should_eval) {
                // Check all_valid inputs
                if (signature().all_valid_inputs.has_value()) {
                    for (const auto &input_ : _check_all_valid_inputs) {
                        if (!input_->all_valid()) {
                            should_eval = false;
                            break;
                        }
                    }
                }
            }

            // Check scheduler state
            if (should_eval && _signature->uses_scheduler()) {
                // It is possible that this was scheduled and then unscheduled, in which case we should ensure
                // that at least on input was modified to make the call
                if (!scheduled) {
                    bool any_modified = false;
                    if (signature().time_series_inputs.has_value()) {
                        // This is a bit expensive, but hopefully fast enough
                        for (const auto &input_ : input().values()) {
                            if (input_->modified() && input_->active()) {
                                any_modified = true;
                                break;
                            }
                        }
                    }
                    if (!any_modified) { should_eval = false; }
                }
            }
        }

        if (should_eval) {
            // Handle context inputs - enter all valid context managers
            std::vector<nb::object> active_contexts;

            if (signature().context_inputs.has_value() && !signature().context_inputs->empty()) {
                // Enter all valid context inputs
                for (const auto& context_key : *signature().context_inputs) {
                    auto& context_input = input()[context_key];
                    if (context_input->valid()) {
                        nb::object context_value = context_input->py_value();
                        // Call __enter__() on the context manager
                        context_value.attr("__enter__")();
                        active_contexts.push_back(context_value);
                    }
                }
            }

            // Execute with error handling, ensuring contexts are exited via RAII-style cleanup
            try {
                // Execute the node evaluation
                // It may be worth reviewing how to remove some of these conditionals from this very performance sensitve method.
                if (_error_output) {
                    try {
                        do_eval();
                    } catch (const std::exception &e) {
                        // TODO: Implement proper error capture
                        // This needs a bit of machinery to be built.
                        throw std::runtime_error("Can't process error handling yet, error caught: " + std::string(e.what()));
                        //_error_output->apply_result(nb::cast(e.what()));
                    }
                } else {
                    try {
                        do_eval();
                    } catch (const NodeException &e) {
                        throw; // already enriched
                    } catch (const std::exception &e) {
                        throw NodeException::capture_error(e, *this, "During evaluation");
                    } catch (...) {
                        throw NodeException::capture_error(std::current_exception(), *this, "Unknown error during node evaluation");
                    }
                }

                // Exit contexts in reverse order (success case)
                for (auto it = active_contexts.rbegin(); it != active_contexts.rend(); ++it) {
                    it->attr("__exit__")(nb::none(), nb::none(), nb::none());
                }
            } catch (...) {
                // Exit contexts in reverse order (exception case)
                for (auto it = active_contexts.rbegin(); it != active_contexts.rend(); ++it) {
                    try {
                        it->attr("__exit__")(nb::none(), nb::none(), nb::none());
                    } catch (...) {
                        // Suppress exceptions during cleanup to preserve original exception
                    }
                }
                throw; // Re-throw the original exception
            }
        }

        // Handle scheduling
        if (scheduled) {
            // Must have a scheduler if it is scheduled
            _scheduler->advance();
        } else if (has_scheduler() && _scheduler->requires_scheduling()) {
            graph().schedule_node(node_ndx(), _scheduler->next_scheduled_time());
        }
    }

    void BasePythonNode::initialise() {}

    void BasePythonNode::start() {
        _initialise_kwargs();
        _initialise_inputs();
        _initialise_state();
        // Now call parent class
        Node::start();
    }

    void BasePythonNode::dispose() { _kwargs.clear(); }

    void PushQueueNode::do_eval() {}

    const nb::callable &PythonNode::eval_fn() {
        return _eval_fn;
    }

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
        _batch    = scalars().contains("batch") ? nb::cast<bool>(scalars()["batch"]) : false;
        
        // If an eval function was provided (from push_queue decorator), call it with a sender and scalar kwargs
        if (_eval_fn.is_valid() && !_eval_fn.is_none()) {
            // Create a Python-callable sender that enqueues messages into this node
            nb::object sender = nb::cpp_function([this](nb::object m) {
                this->enqueue_message(std::move(m));
            });
            // Call eval_fn(sender, **scalars)
            try {
                _eval_fn(sender, **scalars());
            } catch (nb::python_error &e) {
                throw NodeException::capture_error(e, *this, "During push-queue start");
            }
        }
    }

    void PythonGeneratorNode::do_eval() {
        auto       et = graph().evaluation_clock().evaluation_time();
        auto       next_time{MIN_DT};
        auto       sentinel{nb::iterator::sentinel()};
        nb::object out;
        for (nb::iterator v = ++generator; v != sentinel; ++v) {  // Returns NULL if there are no new values
            auto tpl = *v;
            auto time = nb::cast<nb::object>(tpl[0]);
            out       = nb::cast<nb::object>(tpl[1]);

            // Robustly handle either a timedelta (duration) or a datetime (time_point)
            bool handled = false;
            try {
                // Try as duration (e.g., datetime.timedelta)
                auto delta = nb::cast<engine_time_delta_t>(time);
                next_time = et + delta;
                handled = true;
            } catch (...) {
                // Not a duration, try as absolute time (e.g., datetime)
            }
            if (!handled) {
                try {
                    next_time = nb::cast<engine_time_t>(time);
                } catch (...) {
                    // As a last resort, treat unknown types as immediate (break)
                    next_time = et;
                }
            }
            if (next_time >= et) { break; }
        }

        if (next_time > MIN_DT && next_time <= et) {
            if (output().last_modified_time() == next_time) {
                throw std::runtime_error(
                    fmt::format("Duplicate time produced by generator: [{:%FT%T%z}] - {}", next_time, nb::str(out).c_str()));
            }
            output().apply_result(out);
            next_value = nb::none();
            do_eval();  // We are going to apply now! Prepare next step
            return;
        }

        if (next_value.is_valid() && !next_value.is_none()) {
            output().apply_result(next_value);
            next_value = nb::none();
        }

        if (next_time != MIN_DT) {
            next_value = out;
            graph().schedule_node(node_ndx(), next_time);
        }
    }

    void PythonGeneratorNode::start() {
        BasePythonNode::_initialise_kwargs();
        generator = nb::cast<nb::iterator>(_eval_fn(**_kwargs));
        graph().schedule_node(node_ndx(), graph().evaluation_clock().evaluation_time());
    }

}  // namespace hgraph