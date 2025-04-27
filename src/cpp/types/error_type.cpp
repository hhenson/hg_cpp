#include "hgraph/types/ref.h"
#include "hgraph/types/tsb.h"

#include <fmt/chrono.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <nanobind/nanobind.h>
#include <sstream>

namespace hgraph
{

    BacktraceSignature::BacktraceSignature(std::string name_, std::vector<std::string> args_, std::string wiring_path_name_,
                                           std::string runtime_path_name_, std::string node_id_)
        : name(std::move(name_)), args(std::move(args_)), wiring_path_name(std::move(wiring_path_name_)),
          runtime_path_name(std::move(runtime_path_name_)), node_id(std::move(node_id_)) {}

    std::string BackTrace::arg_str(const std::string &arg_name) const {
        if (active_inputs.contains(arg_name)) {
            std::string value = (input_short_values.contains(arg_name)) ? fmt::format("={}", input_short_values.at(arg_name)) : "";
            return fmt::format("*{}*{}", arg_name, value);
        }
        if (input_values.contains(arg_name)) { return fmt::format("{}={}", arg_name, input_short_values.at(arg_name)); }
        return arg_name;
    }

    std::string BackTrace::level_str(int level) const {
        if (!signature.has_value()) { return ""; }
        std::string              indent(2 * level, ' ');
        std::vector<std::string> filtered_args;
        for (const auto &arg : signature->args) {
            if (!arg.starts_with("_")) { filtered_args.push_back(arg_str(arg)); }
        }

        std::string s = fmt::format("{}{}<{}>: {}({})\n", indent, signature->runtime_path_name, fmt::join(signature->node_id, ", "),
                                    signature->name, fmt::join(filtered_args, ", "));

        std::vector<std::string> arg_strs;
        if (!input_values.empty()) {
            for (const auto &[arg, value] : input_values) {
                std::string arg_str = fmt::format("{}{}{}", indent, active_inputs.contains(arg) ? "*" + arg + "*" : arg, ":");

                if (input_delta_values.contains(arg)) {
                    arg_str += fmt::format("\n{}  delta_value={}", indent, input_delta_values.at(arg));
                }
                if (input_values.at(arg) !=
                    (input_delta_values.contains(arg) ? input_delta_values.at(arg) : input_values.at(arg))) {
                    arg_str += fmt::format("\n{}  value={}", indent, value);
                }

                arg_str += fmt::format("\n{}  last modified={}", indent,
                                       input_last_modified_time.contains(arg) ? input_last_modified_time.at(arg) : MIN_DT);

                if (active_inputs.contains(arg)) { arg_str += "\n" + active_inputs.at(arg).level_str(level + 1); }

                arg_strs.push_back(arg_str);
            }
        } else {
            for (const auto &[arg, value] : active_inputs) {
                arg_strs.push_back(fmt::format("{}{}:\n{}", indent, arg, value.level_str(level + 1)));
            }
        }

        return fmt::format("{}{}", s, fmt::join(arg_strs, "\n"));
    }

    std::string BackTrace::to_string() const { return level_str(); }

    static std::string remove_indices(const std::string &path) {
        std::string result;
        bool        in_brackets = false;
        for (char c : path) {
            if (c == '[') in_brackets = true;
            else if (c == ']')
                in_brackets = false;
            else if (!in_brackets)
                result += c;
        }
        return result;
    }

    std::string replace(std::string &str, const std::string &from, const std::string &to) {
        // Handle the edge case of an empty 'from' string to avoid infinite loops.
        if (from.empty()) { return str; }

        size_t start_pos = 0;  // Start searching from the beginning

        // std::string::npos is a static member constant representing a value greater
        // than the maximum possible string index. It's returned by find() when the
        // substring is not found.
        if ((start_pos = str.find(from, start_pos)) != std::string::npos) {
            auto str_{str};
            str_.replace(start_pos, from.length(), to);
            return str_;
        } else {
            return str;
        }
    }

    auto BackTrace::runtime_path_name(const Node &node, bool use_label) -> std::string {
        auto        sig    = node.signature();
        std::string suffix = use_label && sig.label.has_value() ? sig.label.value() : sig.name;

        if (auto parent_node = node.graph().parent_node()) {
            auto p_l = runtime_path_name(*parent_node);
            auto p_n = runtime_path_name(*parent_node, false);
            p_n      = remove_indices(p_n);

            std::string result =
                fmt::format("{}[{}].{}.{}", p_l, node.graph().label().value_or(""), replace(sig.wiring_path_name, p_n, ""), suffix);

            // Replace double dots with single dot
            size_t pos;
            while ((pos = result.find("..")) != std::string::npos) { result.replace(pos, 2, "."); }
            return result;
        }

        return fmt::format("{}.{}", sig.wiring_path_name, suffix);
    }

    auto BackTrace::capture_back_trace(const Node *node, bool capture_values, int64_t depth) -> BackTrace {
        std::optional<BacktraceSignature> signature;
        if (node) {
            auto node_sig = node->signature();
            signature     = BacktraceSignature(node_sig.name, node_sig.args, node_sig.wiring_path_name, runtime_path_name(*node),
                                               fmt::format("{}", fmt::join(node->node_id(), ",")));
        }

        if (depth > 0 && node) {
            std::unordered_map<std::string, BackTrace>     active_inputs;
            std::unordered_map<std::string, std::string>   input_values;
            std::unordered_map<std::string, std::string>   input_delta_values;
            std::unordered_map<std::string, std::string>   input_short_values;
            std::unordered_map<std::string, engine_time_t> input_last_modified_time;

            if (node->has_input()) {
                for (const auto &[input_name, input] : node->input().items()) {
                    capture_input(active_inputs, *input, input_name, capture_values, depth);

                    if (capture_values) {
                        auto   value_str{nb::cast<std::string>(input->py_value())};
                        size_t newline_pos = value_str.find('\n');
                        if (newline_pos != std::string::npos) { value_str = value_str.substr(0, newline_pos); }

                        input_short_values[input_name]       = value_str.substr(0, 32) + (value_str.length() > 32 ? "..." : "");
                        input_delta_values[input_name]       = nb::cast<std::string>(input->py_delta_value());
                        input_values[input_name]             = value_str;
                        input_last_modified_time[input_name] = input->last_modified_time();
                    }
                }
            }

            return BackTrace(signature, active_inputs, input_short_values, input_delta_values, input_values,
                             input_last_modified_time);
        } else {
            return BackTrace(signature, {},{}, {}, {}, {});
        }
    }

    auto BackTrace::capture_input(std::unordered_map<std::string, BackTrace> &active_inputs, const TimeSeriesInput &input,
                                  const std::string &input_name, bool capture_values, int64_t depth) -> void {
        if (input.modified()) {
            if (input.bound()) {
                if (input.has_peer()) {
                    active_inputs[input_name] =
                        BackTrace::capture_back_trace(&input.output()->owning_node(), capture_values, depth - 1);
                } else {
                    auto iterable_inputs{dynamic_cast<const TimeSeriesBundleInput *>(&input)};
                    if (iterable_inputs) {
                        for (const auto &[n, i] : iterable_inputs->items()) {
                            BackTrace::capture_input(active_inputs, *i, fmt::format("{}[{}]", input_name, n.get()), capture_values,
                                                     depth - 1);
                        }
                    }
                }
            } else if (auto ts_ref{dynamic_cast<const TimeSeriesReferenceInput *>(&input)}; ts_ref != nullptr) {
                auto value{nb::cast<TimeSeriesInput &>(input.py_value())};
                if (value.has_output()) {
                    active_inputs[input_name] =
                        BackTrace::capture_back_trace(&value.output()->owning_node(), capture_values, depth - 1);
                }
            }
        }
    }

    NodeError::NodeError(std::string signature_name_, std::string label_, std::string wiring_path_, std::string error_msg_,
                         std::string stack_trace_, std::string activation_back_trace_, std::string additional_context_)
        : signature_name(std::move(signature_name_)), label(std::move(label_)), wiring_path(std::move(wiring_path_)),
          error_msg(std::move(error_msg_)), stack_trace(std::move(stack_trace_)),
          activation_back_trace(std::move(activation_back_trace_)), additional_context(std::move(additional_context_)) {}

    std::string NodeError::to_string() const {
        std::stringstream ss;
        ss << *this;
        return ss.str();
    }

    void NodeError::register_with_nanobind(nb::module_ &m) {
        nb::class_<NodeError, intrusive_base>(m, "NodeError")
            .def_ro("signature_name", &NodeError::signature_name)
            .def_ro("label", &NodeError::label)
            .def_ro("wiring_path", &NodeError::wiring_path)
            .def_ro("error_msg", &NodeError::error_msg)
            .def_ro("stack_trace", &NodeError::stack_trace)
            .def_ro("activation_back_trace", &NodeError::activation_back_trace)
            .def_ro("additional_context", &NodeError::additional_context)
            .def("__str__", [](NodeError &self) { return self.to_string(); });
    }

    NodeException::NodeException(NodeError error) : std::runtime_error(error.to_string()), error{std::move(error)} {}

    std::string traceback_to_string(nb::python_error exception) {
        auto trace_back_mod{nb::module_::import_("traceback")};
        auto format_exception{trace_back_mod.attr("format_exception")};
        auto str_list{format_exception(exception.type(), exception.value(), exception.traceback())};
        auto result{nb::str("").attr("join")(str_list)};
        return nb::cast<std::string>(result);
    }

    NodeException NodeException::capture_error(const std::exception &e, const Node &node, const std::string &msg) {
        auto py_err{dynamic_cast<const nb::python_error *>(&e)};
        auto back_trace{BackTrace::capture_back_trace(&node, node.signature().capture_values, node.signature().trace_back_depth)};
        auto stack_trace{py_err == nullptr ? "" : traceback_to_string(*py_err)};
        return NodeException{
            NodeError(node.signature().signature(), node.signature().label.value_or(""), node.signature().wiring_path_name,
                      e.what(), stack_trace, back_trace.to_string(), msg)};
    }

    NodeException NodeException::capture_error(std::exception_ptr e, const Node &node, const std::string &msg) {
        try {
            rethrow_exception(std::move(e));
        } catch (exception &e_) { return NodeException::capture_error(e_, node, msg); }
    }

    std::ostream &operator<<(std::ostream &os, const NodeError &error) {
        os << error.signature_name << (error.label.empty() ? "" : "labelled " + error.label)
           << (error.wiring_path.empty() ? "" : " at " + error.wiring_path)
           << (error.additional_context.empty() ? "" : " :: " + error.additional_context) << "\nNodeError: " << error.error_msg
           << "\nStack trace:\n"
           << error.stack_trace << "\nActivation Back Trace:\n"
           << error.activation_back_trace;
        return os;
    }
}  // namespace hgraph