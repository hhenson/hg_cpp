//
// Created by Howard Henson on 14/12/2024.
//

#include <hgraph/types/time_series_type.h>

namespace hgraph
{
    void TimeSeriesType::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesType>(
            m, "TimeSeriesType",
            nb::intrusive_ptr<TimeSeriesType>([](TimeSeriesType *o, PyObject *po) noexcept { o->set_self_py(po); }))
            .def_prop_ro("owning_node", static_cast<const Node &(TimeSeriesType::*)() const>(&TimeSeriesType::owning_node))
            .def_prop_ro("owning_graph", static_cast<const Graph &(TimeSeriesType::*)() const>(&TimeSeriesType::owning_graph))
            .def_prop_ro("value", &TimeSeriesType::py_value)
            .def_prop_ro("delta_value", &TimeSeriesType::py_delta_value)
            .def_prop_ro("modified", &TimeSeriesType::modified)
            .def_prop_ro("valid", &TimeSeriesType::valid)
            .def_prop_ro("all_valid", &TimeSeriesType::all_valid)
            .def_prop_ro("last_modified_time", &TimeSeriesType::last_modified_time)
            .def("re_parent", static_cast<void (TimeSeriesType::*)(Node::ptr)>(&TimeSeriesType::re_parent));
    }

    Graph &TimeSeriesType::owning_graph() { return owning_node().graph(); }

    const Graph &TimeSeriesType::owning_graph() const { return owning_node().graph(); }

    void TimeSeriesOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesOutput>(
            m, "TimeSeriesType",
            nb::intrusive_ptr<TimeSeriesOutput>([](TimeSeriesOutput *o, PyObject *po) noexcept { o->set_self_py(po); }))
            .def_prop_ro("parent_output", &TimeSeriesOutput::parent_output)
            .def_prop_ro("has_parent_output", &TimeSeriesOutput::has_parent_output)
            .def("can_apply_result", &TimeSeriesOutput::can_apply_result)
            .def("apply_result", &TimeSeriesOutput::apply_result)
            .def("invalidate", &TimeSeriesOutput::invalidate)
            .def("mark_invalid", &TimeSeriesOutput::mark_invalid)
            .def("mark_modified", static_cast<void (TimeSeriesOutput::*)()>(&TimeSeriesOutput::mark_modified))
            .def("mark_modified", static_cast<void (TimeSeriesOutput::*)(engine_time_t)>(&TimeSeriesOutput::mark_modified))
            .def("subscribe", &TimeSeriesOutput::subscribe_node)
            .def("unsubscribe", &TimeSeriesOutput::un_subscribe_node)
            .def("copy_from_output", &TimeSeriesOutput::copy_from_output)
            .def("copy_from_input", &TimeSeriesOutput::copy_from_input)
            .def("re_parent", static_cast<void (TimeSeriesOutput::*)(TimeSeriesOutput::ptr)>(&TimeSeriesOutput::re_parent));
    }

    Node &TimeSeriesOutput::owning_node() { return const_cast<Node &>(_owning_node()); }

    const Node &TimeSeriesOutput::owning_node() const { return _owning_node(); }

    const Node &TimeSeriesOutput::_owning_node() const {
        if (_parent_output_or_node.has_value()) {
            return std::visit(
                [](auto &&value) -> const Node & {
                    using T = std::decay_t<decltype(value)>;  // Get the actual type
                    if constexpr (std::is_same_v<T, TimeSeriesOutput::ptr>) {
                        return (*value).owning_node();
                    } else if constexpr (std::is_same_v<T, Node::ptr>) {
                        return *value;
                    } else {
                        throw std::runtime_error("Unknown type");
                    }
                },
                _parent_output_or_node.value());
        } else {
            throw std::runtime_error("No node is accessible");
        }
    }

    TimeSeriesOutput::ptr TimeSeriesOutput::parent_output() const {
        if (_parent_output_or_node.has_value()) {
            return std::visit(
                [](auto &&value) -> TimeSeriesOutput::ptr {
                    using T = std::decay_t<decltype(value)>;  // Get the actual type
                    if constexpr (std::is_same_v<T, TimeSeriesOutput::ptr>) {
                        return value;
                    } else if constexpr (std::is_same_v<T, Node::ptr>) {
                        throw std::runtime_error("No parent output present");
                    } else {
                        throw std::runtime_error("Unknown type");
                    }
                },
                _parent_output_or_node.value());
        } else {
            throw std::runtime_error("No parent output present");
        }
    }

    bool TimeSeriesOutput::has_parent_output() const {
        if (_parent_output_or_node.has_value()) {
            return std::visit(
                [](auto &&value) -> bool {
                    using T = std::decay_t<decltype(value)>;  // Get the actual type
                    if constexpr (std::is_same_v<T, TimeSeriesOutput::ptr>) {
                        return true;
                    } else if constexpr (std::is_same_v<T, Node::ptr>) {
                        return false;
                    } else {
                        throw std::runtime_error("Unknown type");
                    }
                },
                _parent_output_or_node.value());
        } else {
            return false;
        }
    }

    void TimeSeriesOutput::re_parent(Node::ptr parent) { _parent_output_or_node.emplace(parent); }

    void TimeSeriesOutput::re_parent(TimeSeriesOutput::ptr parent) { _parent_output_or_node.emplace(parent); }

    bool TimeSeriesOutput::can_apply_result(std::any value) { return not modified(); }

    bool TimeSeriesOutput::modified() const { return owning_graph().evaluation_clock().evaluation_time() == _last_modified_time; }

    bool TimeSeriesOutput::valid() const { return _last_modified_time > MIN_DT; }

    bool TimeSeriesOutput::all_valid() const {
        return valid();  // By default, all valid is the same as valid
    }

    engine_time_t TimeSeriesOutput::last_modified_time() const { return _last_modified_time; }

    void TimeSeriesOutput::mark_invalid() {
        if (_last_modified_time > MIN_DT) {
            _last_modified_time = MIN_DT;
            _notify(owning_graph().evaluation_clock().evaluation_time());
        }
    }

    void TimeSeriesOutput::mark_modified() {
        if (_parent_output_or_node.has_value()) {
            mark_modified(owning_graph().evaluation_clock().evaluation_time());
        } else {
            mark_modified(MAX_ET);
        }
    }

    void TimeSeriesOutput::mark_modified(engine_time_t modified_time) {
        const auto &et{owning_graph().evaluation_clock().evaluation_time()};
        if (_last_modified_time < et) {
            _last_modified_time = et;
            std::visit([](auto &&value) {
                using T = std::decay_t<decltype(value)>;  // Get the actual type
                if constexpr (std::is_same_v<T, TimeSeriesOutput::ptr>) { value->mark_modified(); }
                else {}
            }, _parent_output_or_node.value());
            _notify(modified_time);
        }
    }

    void TimeSeriesOutput::subscribe_node(Node::ptr node) { _subscribers.subscribe(node.get()); }

    void TimeSeriesOutput::un_subscribe_node(Node::ptr node) { _subscribers.un_subscribe(node.get()); }

    void TimeSeriesOutput::_notify(engine_time_t modfied_time) {
        _subscribers.apply([modfied_time](Node::ptr node) { node->notify(modfied_time); });
    }
}  // namespace hgraph
