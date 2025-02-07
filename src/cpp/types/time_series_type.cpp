
#include <hgraph/python/pyb_wiring.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series_type.h>

namespace hgraph
{
    void TimeSeriesType::re_parent(Node::ptr parent) { _parent_ts_or_node = parent; }

    void TimeSeriesType::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesType, nb::intrusive_base>(m, "TimeSeriesType")
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

    const TimeSeriesType::ptr &TimeSeriesType::_time_series() const { return const_cast<TimeSeriesType *>(this)->_time_series(); }

    TimeSeriesType::ptr &TimeSeriesType::_time_series() {
        if (_parent_ts_or_node.has_value()) {
            return std::get<ptr>(_parent_ts_or_node.value());
        } else {
            throw std::runtime_error("No parent output present");
        }
    }

    bool TimeSeriesType::_has_time_series() const {
        if (_parent_ts_or_node.has_value()) {
            return std::holds_alternative<ptr>(_parent_ts_or_node.value());
        } else {
            return false;
        }
    }

    void TimeSeriesType::_set_time_series(TimeSeriesType *ts) { _parent_ts_or_node = ptr{ts}; }

    bool TimeSeriesType::has_parent_or_node() const { return _parent_ts_or_node.has_value(); }

    Graph &TimeSeriesType::owning_graph() { return owning_node().graph(); }

    const Graph &TimeSeriesType::owning_graph() const { return owning_node().graph(); }

    void TimeSeriesOutput::clear() {}

    void TimeSeriesOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesOutput, TimeSeriesType>(m, "TimeSeriesOutput")
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
            .def("re_parent", static_cast<void (TimeSeriesOutput::*)(ptr &)>(&TimeSeriesOutput::re_parent));
    }

    const Node &TimeSeriesType::_owning_node() const {
        if (_parent_ts_or_node.has_value()) {
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
                _parent_ts_or_node.value());
        } else {
            throw std::runtime_error("No node is accessible");
        }
    }

    TimeSeriesType::TimeSeriesType(node_ptr parent) : _parent_ts_or_node{parent} {}

    TimeSeriesType::TimeSeriesType(ptr parent) : _parent_ts_or_node{parent} {}

    Node &TimeSeriesType::owning_node() { return const_cast<Node &>(_owning_node()); }

    const Node &TimeSeriesType::owning_node() const { return _owning_node(); }

    TimeSeriesInput::ptr TimeSeriesInput::parent_input() const {
        return const_cast<TimeSeriesInput *>(static_cast<const TimeSeriesInput *>(_time_series().get()));
    }

    bool TimeSeriesInput::has_parent_input() const { return _has_time_series(); }

    bool TimeSeriesInput::bound() const { return _output.get() != nullptr; }

    bool TimeSeriesInput::has_peer() const {
        // By default, we assume that if there is an output then we are peered.
        // This is not always True, but is a good general assumption.
        return _output.get() != nullptr;
    }

    time_series_output_ptr TimeSeriesInput::output() const { return _output; }

    bool TimeSeriesInput::bind_output(time_series_output_ptr value) {
        bool peer;
        if (auto ref_output = dynamic_cast_ref<TimeSeriesReferenceOutput>(value); ref_output.get()) {
            if (ref_output->valid()) { ref_output->value()->bind_input(*this); }
            ref_output->observe_reference(this);
            _reference_output = ref_output;
            peer              = false;
        } else {
            if (value.get() == _output.get()) { return has_peer(); }

            peer = do_bind_output(value);
        }

        if ((owning_node().is_started() || owning_node().is_starting()) && _output && _output->valid()) {
            _sample_time = owning_graph().evaluation_clock().evaluation_time();
            if (active()) {
                notify(_sample_time);
                // TODO: This might belong to make_active, or not? There is a race with setting sample_time too.
            }
        }

        return peer;
    }

    bool TimeSeriesInput::active() const { return _active; }

    void TimeSeriesInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesInput, TimeSeriesType>(m, "TimeSeriesInput")
            .def_prop_ro("parent_input", &TimeSeriesInput::parent_input)
            .def_prop_ro("has_parent_input", &TimeSeriesInput::has_parent_input)
            .def_prop_ro("bound", &TimeSeriesInput::bound)
            .def_prop_ro("has_peer", &TimeSeriesInput::has_peer)
            .def_prop_ro("output", &TimeSeriesInput::output)
            .def_prop_ro("active", &TimeSeriesInput::active)
            .def("bind_output", &TimeSeriesInput::bind_output, "output"_a)
            .def("un_bind_output", &TimeSeriesInput::un_bind_output)
            .def("make_active", &TimeSeriesInput::make_active)
            .def("make_passive", &TimeSeriesInput::make_passive);
    }

    void TimeSeriesInput::notify(engine_time_t modified_time) {
        if (_notify_time != modified_time) {
            _notify_time = modified_time;
            if (has_parent_input()) {
                parent_input()->notify_parent(this, modified_time);
            } else {
                owning_node().notify(modified_time);
            }
        }
    }

    void TimeSeriesInput::notify_parent(TimeSeriesInput *child, engine_time_t modified_time) { notify(modified_time); }

    TimeSeriesOutput::ptr TimeSeriesOutput::parent_output() const {
        return const_cast<TimeSeriesOutput *>(static_cast<const TimeSeriesOutput *>(_time_series().get()));
    }

    bool TimeSeriesOutput::has_parent_output() const { return _has_time_series(); }

    void TimeSeriesOutput::re_parent(ptr &parent) { _set_time_series(parent.get()); }

    bool TimeSeriesOutput::can_apply_result(nb::object value) { return not modified(); }

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
        if (has_parent_output()) {
            mark_modified(owning_graph().evaluation_clock().evaluation_time());
        } else {
            mark_modified(MAX_ET);
        }
    }

    void TimeSeriesOutput::mark_modified(engine_time_t modified_time) {
        const auto &et{owning_graph().evaluation_clock().evaluation_time()};
        if (_last_modified_time < et) {
            _last_modified_time = et;
            if (_has_time_series()) { _time_series_output().mark_modified(); }
            _notify(modified_time);
        }
    }

    void TimeSeriesOutput::subscribe_node(Node::ptr node) { _subscribers.subscribe(node.get()); }

    void TimeSeriesOutput::un_subscribe_node(Node::ptr node) { _subscribers.un_subscribe(node.get()); }

    void TimeSeriesOutput::_notify(engine_time_t modfied_time) {
        _subscribers.apply([modfied_time](Node::ptr node) { node->notify(modfied_time); });
    }

    const TimeSeriesOutput &TimeSeriesOutput::_time_series_output() const {
        return *dynamic_cast<const TimeSeriesOutput *>(_time_series().get());
    }

    TimeSeriesOutput &TimeSeriesOutput::_time_series_output() { return *dynamic_cast<TimeSeriesOutput *>(_time_series().get()); }
}  // namespace hgraph
