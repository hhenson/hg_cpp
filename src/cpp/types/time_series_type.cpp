
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series_type.h>

#include <utility>

namespace hgraph
{
    void TimeSeriesType::re_parent(Node::ptr parent) { _parent_ts_or_node = std::move(parent); }

    void TimeSeriesType::re_parent(ptr parent) { _parent_ts_or_node = std::move(parent); }

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

    TimeSeriesType::ptr &TimeSeriesType::_parent_time_series() const {
        return const_cast<TimeSeriesType *>(this)->_parent_time_series();
    }

    TimeSeriesType::ptr &TimeSeriesType::_parent_time_series() {
        if (_parent_ts_or_node.has_value()) {
            return std::get<ptr>(_parent_ts_or_node.value());
        } else {
            throw std::runtime_error("No parent output present");
        }
    }

    bool TimeSeriesType::_has_parent_time_series() const {
        if (_parent_ts_or_node.has_value()) {
            return std::holds_alternative<ptr>(_parent_ts_or_node.value());
        } else {
            return false;
        }
    }

    void TimeSeriesType::_set_parent_time_series(TimeSeriesType *ts) { _parent_ts_or_node = ptr{ts}; }

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
            .def("subscribe", &TimeSeriesOutput::subscribe)
            .def("unsubscribe", &TimeSeriesOutput::un_subscribe)
            .def("copy_from_output", &TimeSeriesOutput::copy_from_output)
            .def("copy_from_input", &TimeSeriesOutput::copy_from_input)
            .def("re_parent", static_cast<void (TimeSeriesOutput::*)(ptr &)>(&TimeSeriesOutput::re_parent));
    }

    const Node &TimeSeriesType::_owning_node() const {
        if (_parent_ts_or_node.has_value()) {
            return std::visit(
                []<typename T_>(T_ &&value) -> const Node & {
                    using T = std::decay_t<T_>;  // Get the actual type
                    if constexpr (std::is_same_v<T, TimeSeriesType::ptr>) {
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

    TimeSeriesType::TimeSeriesType(const node_ptr &parent) : _parent_ts_or_node{parent} {}

    TimeSeriesType::TimeSeriesType(const ptr &parent) : _parent_ts_or_node{parent} {}

    Node &TimeSeriesType::owning_node() { return const_cast<Node &>(_owning_node()); }

    const Node &TimeSeriesType::owning_node() const { return _owning_node(); }

    TimeSeriesInput::ptr TimeSeriesInput::parent_input() const {
        return static_cast<TimeSeriesInput *>(_parent_time_series().get());  // NOLINT(*-pro-type-static-cast-downcast)
    }

    bool TimeSeriesInput::has_parent_input() const { return _has_parent_time_series(); }

    bool TimeSeriesInput::bound() const { return _output.get() != nullptr; }

    bool TimeSeriesInput::has_peer() const {
        // By default, we assume that if there is an output, then we are peered.
        // This is not always True but is a good general assumption.
        return _output.get() != nullptr;
    }

    time_series_output_ptr TimeSeriesInput::output() const { return _output; }

    bool TimeSeriesInput::has_output() const { return _output.get() != nullptr; }

    bool TimeSeriesInput::bind_output(time_series_output_ptr value) {
        bool peer;
        if (auto ref_output = dynamic_cast<TimeSeriesReferenceOutput *>(value.get())) {  // Is a TimeseriesReferenceOutput
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
    void TimeSeriesInput::un_bind_output() {
        if (not bound()) { return; }
        bool was_valid = valid();
        if (auto ref_output = dynamic_cast_ref<TimeSeriesReferenceOutput>(_output)) {
            ref_output->stop_observing_reference(this);
            _reference_output.reset();
        }
        do_un_bind_output();

        if (owning_node().is_started() && was_valid) {
            _sample_time = owning_graph().evaluation_clock().evaluation_time();
            if (active()) {
                // Notify as the state of the node has changed from bound to un_bound
                owning_node().notify(_sample_time);
            }
        }
    }

    bool TimeSeriesInput::active() const { return _active; }

    void TimeSeriesInput::make_active() {
        if (!_active) {
            _active = true;
            if (bound()) {
                output()->subscribe(subscribe_input() ? static_cast<Notifiable *>(this)
                                                      : static_cast<Notifiable *>(&owning_node()));
                if (output()->valid() && output()->modified()) {
                    notify(output()->last_modified_time());
                    return;  // If the output is modified, we do not need to check if sampled
                }
            }

            if (sampled()) { notify(_sample_time); }
        }
    }

    void TimeSeriesInput::make_passive() {
        if (_active) {
            _active = false;
            if (bound()) {
                output()->un_subscribe(subscribe_input() ? static_cast<Notifiable *>(this)
                                                         : static_cast<Notifiable *>(&owning_node()));
            }
        }
    }

    nb::object TimeSeriesInput::py_value() const {
        if (has_peer()) {
            return _output->py_value();
        } else {
            return nb::none();
        }
    }
    nb::object TimeSeriesInput::py_delta_value() const {
        if (has_peer()) {
            return _output->py_delta_value();
        } else {
            return nb::none();
        }
    }

    void TimeSeriesInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesInput, TimeSeriesType>(m, "TimeSeriesInput")
            .def_prop_ro("parent_input", &TimeSeriesInput::parent_input)
            .def_prop_ro("has_parent_input", &TimeSeriesInput::has_parent_input)
            .def_prop_ro("bound", &TimeSeriesInput::bound)
            .def_prop_ro("has_peer", &TimeSeriesInput::has_peer)
            .def_prop_ro("output", &TimeSeriesInput::output)
            .def_prop_ro("reference_output", &TimeSeriesInput::reference_output)
            .def_prop_ro("active", &TimeSeriesInput::active)
            .def("bind_output", &TimeSeriesInput::bind_output, "output"_a)
            .def("un_bind_output", &TimeSeriesInput::un_bind_output)
            .def("make_active", &TimeSeriesInput::make_active)
            .def("make_passive", &TimeSeriesInput::make_passive);
    }

    bool TimeSeriesInput::do_bind_output(time_series_output_ptr value) {
        bool active = this->active();
        this->make_passive();  // Ensure we are unsubscribed from the old output.
        _output = std::move(value);
        if (active) {
            this->make_active();  // If we were active now subscribe to the new output,
                                  // this is important even if we were not bound previously as this will ensure the new output gets
                                  // subscribed to
        }
        return true;
    }

    auto TimeSeriesInput::notify(engine_time_t modified_time) -> void {  // NOLINT(*-no-recursion)
        if (_notify_time != modified_time) {
            _notify_time = modified_time;
            if (has_parent_input()) {
                parent_input()->notify_parent(this, modified_time);
            } else {
                owning_node().notify(modified_time);
            }
        }
    }

    void TimeSeriesInput::do_un_bind_output() {
        if (_active) {
            output()->un_subscribe(subscribe_input() ? static_cast<Notifiable *>(this) : static_cast<Notifiable *>(&owning_node()));
        }
        _output = nullptr;
    }

    void TimeSeriesInput::notify_parent(TimeSeriesInput *child, engine_time_t modified_time) {
        notify(modified_time);
    }  // NOLINT(*-no-recursion)

    void TimeSeriesInput::set_sample_time(engine_time_t sample_time) { _sample_time = sample_time; }

    engine_time_t TimeSeriesInput::sample_time() const { return _sample_time; }

    void TimeSeriesInput::set_subscribe_method(bool subscribe_input) { _subscribe_input = subscribe_input; }

    bool TimeSeriesInput::subscribe_input() const { return _subscribe_input; }

    bool TimeSeriesInput::sampled() const {
        return _sample_time != MIN_DT && _sample_time == owning_graph().evaluation_clock().evaluation_time();
    }

    time_series_output_ptr TimeSeriesInput::reference_output() const { return _reference_output; }

    void TimeSeriesInput::reset_output() { _output = nullptr; }

    TimeSeriesOutput::ptr TimeSeriesOutput::parent_output() const {
        return static_cast<TimeSeriesOutput *>(_parent_time_series().get());  // NOLINT(*-pro-type-static-cast-downcast)
    }

    bool TimeSeriesOutput::has_parent_output() const { return _has_parent_time_series(); }

    void TimeSeriesOutput::re_parent(ptr &parent) { _set_parent_time_series(parent.get()); }

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
        if (has_parent_or_node()) {
            mark_modified(owning_graph().evaluation_clock().evaluation_time());
        } else {
            mark_modified(MAX_ET);
        }
    }

    void TimeSeriesOutput::mark_modified(engine_time_t modified_time) {  // NOLINT(*-no-recursion)
        if (_last_modified_time < modified_time) {
            _last_modified_time = modified_time;
            if (has_parent_output()) { parent_output()->mark_child_modified(*this, modified_time); }
            _notify(modified_time);
        }
    }

    void TimeSeriesOutput::mark_child_modified(TimeSeriesOutput &child, engine_time_t modified_time) {
        mark_modified(modified_time);
    }  // NOLINT(*-no-recursion)

    void TimeSeriesOutput::subscribe(Notifiable *notifiable) { _subscribers.subscribe(notifiable); }

    void TimeSeriesOutput::un_subscribe(Notifiable *notifiable) { _subscribers.un_subscribe(notifiable); }

    void TimeSeriesOutput::_notify(engine_time_t modified_time) {
        _subscribers.apply([modified_time](Notifiable *notifiable) { notifiable->notify(modified_time); });
    }

    const TimeSeriesOutput &TimeSeriesOutput::_time_series_output() const {
        return *dynamic_cast<const TimeSeriesOutput *>(_parent_time_series().get());
    }

    TimeSeriesOutput &TimeSeriesOutput::_time_series_output() {
        return *dynamic_cast<TimeSeriesOutput *>(_parent_time_series().get());
    }

    void TimeSeriesOutput::_reset_last_modified_time() { _last_modified_time = MIN_DT; }

    bool TimeSeriesInput::modified() const { return _output != nullptr && (_output->modified() || sampled()); }

    bool TimeSeriesInput::valid() const { return bound() && _output != nullptr && _output->valid(); }

    bool TimeSeriesInput::all_valid() const { return bound() && _output != nullptr && _output->all_valid(); }

    engine_time_t TimeSeriesInput::last_modified_time() const {
        return bound() ? std::max(_output->last_modified_time(), _sample_time) : MIN_DT;
    }

}  // namespace hgraph
