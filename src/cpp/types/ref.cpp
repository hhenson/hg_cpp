
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/ts_indexed.h>

#include <algorithm>

namespace hgraph
{

    TimeSeriesReference::ptr TimeSeriesReference::make() { return new EmptyTimeSeriesReference(); }

    TimeSeriesReference::ptr TimeSeriesReference::make(time_series_output_ptr output) {
        if (output.get() == nullptr) {
            return make();
        } else {
            return new BoundTimeSeriesReference(output);
        }
    }

    TimeSeriesReference::ptr TimeSeriesReference::make(std::vector<ptr> items) {
        if (items.empty()) { return make(); }
        return new UnBoundTimeSeriesReference(items);
    }

    TimeSeriesReference::ptr TimeSeriesReference::make(std::vector<nb::ref<TimeSeriesReferenceInput>> items) {
        if (items.empty()) { return make(); }
        std::vector<TimeSeriesReference::ptr> refs;
        refs.reserve(items.size());
        for (auto item : items) { refs.emplace_back(item->_value); }
        return new UnBoundTimeSeriesReference(refs);
    }

    void TimeSeriesReference::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesReference, nb::intrusive_base>(m, "TimeSeriesReference")
            .def("__str__", &TimeSeriesReference::to_string)
            .def("__ref__", &TimeSeriesReference::to_string)
            .def("bind_input", &TimeSeriesReference::bind_input)
            .def_prop_ro("has_output", &TimeSeriesReference::has_output)
            .def_prop_ro("is_empty", &TimeSeriesReference::is_empty)
            .def_prop_ro("is_valid", &TimeSeriesReference::is_valid)
            .def_static("make", static_cast<ptr (*)()>(&TimeSeriesReference::make))
            .def_static("make", static_cast<ptr (*)(TimeSeriesOutput::ptr)>(&TimeSeriesReference::make))
            .def_static("make", static_cast<ptr (*)(std::vector<ptr>)>(&TimeSeriesReference::make))
            .def_static(
                "make",
                [](nb::object ts, nb::object items) -> ptr {
                    if (not ts.is_none()) {
                        if (nb::isinstance<TimeSeriesOutput>(ts)) return make(nb::cast<TimeSeriesOutput::ptr>(ts));
                        if (nb::isinstance<TimeSeriesReferenceInput>(ts))
                            return nb::cast<TimeSeriesReferenceInput::ptr>(ts)->_value;
                        if (nb::isinstance<TimeSeriesInput>(ts)) {
                            auto ts_input = nb::cast<TimeSeriesInput::ptr>(ts);
                            if (ts_input->has_peer()) return make(ts_input->output());
                            // Deal with list of inputs
                            std::vector<ptr> items_list;
                            auto             ts_ndx{dynamic_cast<IndexedTimeSeriesInput *>(ts_input.get())};
                            items_list.reserve(ts_ndx->size());
                            for (auto &ts_ptr : ts_ndx->values()) {
                                items_list.emplace_back(dynamic_cast<TimeSeriesReferenceInput *>(ts_ptr.get())->_value);
                            }
                            return make(items_list);
                        }
                        // We may wish to raise an exception here?
                    } else if (not items.is_none()) {
                        auto items_list = nb::cast<std::vector<ptr>>(items);
                        return make(items_list);
                    }
                    return make();
                },
                "ts"_a = nb::none(), "from_items"_a = nb::none());

        nb::class_<EmptyTimeSeriesReference, TimeSeriesReference>(
            m, "EmptyTimeSeriesReference",
            nb::intrusive_ptr<EmptyTimeSeriesReference>(
                [](EmptyTimeSeriesReference *o, PyObject *po) noexcept { o->set_self_py(po); }));

        nb::class_<BoundTimeSeriesReference, TimeSeriesReference>(
            m, "BoundTimeSeriesReference",
            nb::intrusive_ptr<BoundTimeSeriesReference>(
                [](BoundTimeSeriesReference *o, PyObject *po) noexcept { o->set_self_py(po); }))
            .def_prop_ro("output", &BoundTimeSeriesReference::output);

        nb::class_<UnBoundTimeSeriesReference, TimeSeriesReference>(
            m, "UnBoundTimeSeriesReference",
            nb::intrusive_ptr<UnBoundTimeSeriesReference>(
                [](UnBoundTimeSeriesReference *o, PyObject *po) noexcept { o->set_self_py(po); }))
            .def_prop_ro("items", &UnBoundTimeSeriesReference::items);
    }

    void EmptyTimeSeriesReference::bind_input(TimeSeriesInput &ts_input) const { ts_input.un_bind_output(); }

    bool EmptyTimeSeriesReference::has_output() const { return false; }

    bool EmptyTimeSeriesReference::is_empty() const { return true; }

    bool EmptyTimeSeriesReference::is_valid() const { return false; }

    bool EmptyTimeSeriesReference::operator==(const TimeSeriesReferenceOutput &other) const {
        return dynamic_cast<const EmptyTimeSeriesReference *>(&other) != nullptr;
    }

    std::string EmptyTimeSeriesReference::to_string() const { return "REF[<UnSet>]"; }

    BoundTimeSeriesReference::BoundTimeSeriesReference(time_series_output_ptr output) : _output{std::move(output)} {}

    const TimeSeriesOutput::ptr &BoundTimeSeriesReference::output() const { return _output; }

    void BoundTimeSeriesReference::bind_input(TimeSeriesInput &ts_input) const {
        // TODO: This activate / reactivate logic should really be handled in the input
        auto reactivate{ts_input.active()};
        ts_input.bind_output(_output);
        if (reactivate) { ts_input.make_active(); }
    }

    bool BoundTimeSeriesReference::has_output() const { return true; }

    bool BoundTimeSeriesReference::is_empty() const { return false; }

    bool BoundTimeSeriesReference::is_valid() const { return true; }

    bool BoundTimeSeriesReference::operator==(const TimeSeriesReferenceOutput &other) const {
        auto bound_time_series_reference{dynamic_cast<const BoundTimeSeriesReference *>(&other)};
        return bound_time_series_reference != nullptr && bound_time_series_reference->output().get() == _output.get();
    }

    std::string BoundTimeSeriesReference::to_string() const {
        return fmt::format("REF[{}<{}>.out<{:p}>]", _output->owning_node().signature().name,
                           fmt::join(_output->owning_node().node_id(), ", "),
                           const_cast<void *>(static_cast<const void *>(_output.get())));
    }

    UnBoundTimeSeriesReference::UnBoundTimeSeriesReference(std::vector<ptr> items) : _items{std::move(items)} {}

    const std::vector<TimeSeriesReference::ptr> &UnBoundTimeSeriesReference::items() const { return _items; }

    void UnBoundTimeSeriesReference::bind_input(TimeSeriesInput &ts_input) const {
        IndexedTimeSeriesInput *indexed_input{dynamic_cast<IndexedTimeSeriesInput *>(&ts_input)};
        if (indexed_input == nullptr) {
            throw std::runtime_error("UnBoundTimeSeriesReference::bind_input: Expected an IndexedTimeSeriesInput");
        }
        for (size_t i = 0; i < _items.size(); ++i) { _items[i]->bind_input(*(*indexed_input)[i]); }
    }

    bool UnBoundTimeSeriesReference::has_output() const { return false; }

    bool UnBoundTimeSeriesReference::is_empty() const { return false; }

    bool UnBoundTimeSeriesReference::is_valid() const { return true; }

    bool UnBoundTimeSeriesReference::operator==(const TimeSeriesReferenceOutput &other) const {
        auto other_{dynamic_cast<const UnBoundTimeSeriesReference *>(&other)};
        return other_ != nullptr && other_->_items == _items;
    }

    std::string UnBoundTimeSeriesReference::to_string() const {
        std::vector<std::string> string_items;
        string_items.reserve(_items.size());
        for (const auto &item : _items) { string_items.push_back(item->to_string()); }
        return fmt::format("REF[{}]", fmt::join(string_items, ", "));
    }

    TimeSeriesReferenceOutput::~TimeSeriesReferenceOutput() {
        // Stop a potential memory leak if this goes away whilst
        // holding references to TimeSeriesInputs
        for (auto ref : _reference_observers) { ref->dec_ref(); }
    }

    const TimeSeriesReference::ptr &TimeSeriesReferenceOutput::value() const { return _value; }

    TimeSeriesReference::ptr &TimeSeriesReferenceOutput::value() { return _value; }

    void TimeSeriesReferenceOutput::set_value(TimeSeriesReference::ptr value) {
        _value = value;
        mark_modified();
        for (auto it{_reference_observers.begin()}; it != _reference_observers.end(); ++it) {
            TimeSeriesInput *input{*it};
            _value->bind_input(*input);
        }
    }

    void TimeSeriesReferenceOutput::apply_result(nb::object value) {
        if (value.is_none()) { return; }
        set_value(nb::cast<TimeSeriesReference::ptr>(value));
    }

    void TimeSeriesReferenceOutput::observe_reference(TimeSeriesInput::ptr input_) {
        auto result{_reference_observers.emplace(input_.get())};
        if (result.second) { (*result.first)->inc_ref(); }
    }

    void TimeSeriesReferenceOutput::stop_observing_reference(TimeSeriesInput::ptr input_) {
        auto result{_reference_observers.erase(input_.get())};
        if (result != 0) {
            // Since we have a ptr to the object, the input must have at least one additional
            // reference so we can ignore the dec_ref result here.
            // ReSharper disable once CppExpressionWithoutSideEffects
            input_->dec_ref();
        }
    }

    nb::object TimeSeriesReferenceOutput::py_value() const { return nb::cast(_value); }

    nb::object TimeSeriesReferenceOutput::py_delta_value() const { return py_value(); }

    void TimeSeriesReferenceOutput::invalidate() {
        set_value(TimeSeriesReference::make());
        mark_invalid();
    }

    void TimeSeriesReferenceOutput::copy_from_output(const TimeSeriesOutput &output) {
        auto output_t = dynamic_cast<const TimeSeriesReferenceOutput *>(&output);
        if (output_t) { set_value(output_t->_value); }
    }

    void TimeSeriesReferenceOutput::copy_from_input(const TimeSeriesInput &input) {
        auto input_t = dynamic_cast<const TimeSeriesReferenceInput *>(&input);
        if (input_t) { set_value(input_t->_value); }
    }

    void TimeSeriesReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesReferenceOutput, TimeSeriesOutput>(m, "TimeSeriesReferenceOutput")
            .def_prop_ro("reference_observers", [](TimeSeriesReferenceOutput &self) { return self._value.get(); });
    }

    void TimeSeriesReferenceInput::start() {
        set_sample_time(owning_graph().evaluation_clock().evaluation_time());
        notify(sample_time());
    }

    nb::object TimeSeriesReferenceInput::py_value() const {
        if (has_output()) { return TimeSeriesInput::py_value(); }
        if (_value.get() != nullptr) { return nb::cast(_value); }
        if (!_items.empty()) {
            _value = TimeSeriesReference::make(_items);
            return nb::cast(_value);
        }
        return nb::none();
    }

    nb::object TimeSeriesReferenceInput::py_delta_value() const { return py_value(); }

    void TimeSeriesReferenceInput::clone_binding(const TimeSeriesReferenceInput &other) {
        un_bind_output();
        if (other.has_output()) {
            bind_output(other.output());
        } else if (!other.items().empty()) {
            for (size_t i = 0, l{std::min(other.items().size(), _items.size())}; i < l; ++i) {
                _items[i]->clone_binding(*other.items()[i]);
            }
        } else if (other._value != nullptr){
            _value = other._value;
            if (owning_node().is_started()) {
                set_sample_time(owning_graph().evaluation_clock().evaluation_time());
                if (active()) { notify(sample_time()); }
            }
        }
    }

    bool TimeSeriesReferenceInput::modified() const {
        if (sampled()) { return true; }
        if (has_output()) { return output()->modified(); }
        if (!_items.empty()) {
            return std::any_of(_items.begin(), _items.end(), [](const auto &item) { return item->modified(); });
        }
        return false;
    }

    bool TimeSeriesReferenceInput::valid() const {
        return _value != nullptr ||
               (!_items.empty() && std::any_of(_items.begin(), _items.end(), [](const auto &item) { return item->valid(); })) ||
               TimeSeriesInput::valid();
    }

    bool TimeSeriesReferenceInput::all_valid() const {
        return (!_items.empty() && std::all_of(_items.begin(), _items.end(), [](const auto &item) { return item->all_valid(); })) ||
               _value != nullptr || TimeSeriesInput::all_valid();
    }

    engine_time_t TimeSeriesReferenceInput::last_modified_time() const {
        if (!_items.empty()) {
            return std::max_element(_items.begin(), _items.end(),
                                    [](const auto &a, const auto &b) { return a->last_modified_time() < b->last_modified_time(); })
                ->get()
                ->last_modified_time();
        } else if (has_output()) {
            return output()->last_modified_time();
        } else {
            return sample_time();
        }
    }

    bool TimeSeriesReferenceInput::bind_output(time_series_output_ptr value) {

        auto peer = do_bind_output(value);
        if (owning_node().is_started() && output() != nullptr && output()->valid()) {
            set_sample_time(owning_graph().evaluation_clock().evaluation_time());
            if (active()) { notify(sample_time()); }
        }

        return peer;
    }

    void TimeSeriesReferenceInput::un_bind_output() {
        bool was_valid = valid();
        do_un_bind_output();

        if (owning_node().is_started() && was_valid) {
            set_sample_time(owning_graph().evaluation_clock().evaluation_time());
            if (active()) {
                // Notify as the state of the node has changed from bound to unbound
                owning_node().notify(sample_time());
            }
        }
    }

    void TimeSeriesReferenceInput::make_active() {
        TimeSeriesInput::make_active();  // Call the base class's make_active
        if (!_items.empty()) {
            for (auto &item : _items) {
                if (item) {
                    item->make_active();  // Call make_active on each item
                }
            }
        }
    }

    void TimeSeriesReferenceInput::make_passive() {
        TimeSeriesInput::make_passive();  // Call the base class's make_passive
        if (!_items.empty()) {
            for (auto &item : _items) {
                if (item) {
                    item->make_passive();  // Call make_passive on each item
                }
            }
        }
    }

    TimeSeriesReferenceInput::ptr TimeSeriesReferenceInput::operator[](size_t ndx) {
        if (_items.empty()) { _items.reserve(ndx + 1); }
        while (ndx >= _items.size()) {
            auto new_item = new TimeSeriesReferenceInput(this);
            _items.push_back(new_item);
        }
        return _items.at(ndx);
    }

    bool TimeSeriesReferenceInput::do_bind_output(time_series_output_ptr value) {
        if (dynamic_cast<TimeSeriesReferenceOutput *>(value.get())) {
            _value = nullptr;
            return TimeSeriesInput::do_bind_output(value);
        } else {
            _value = TimeSeriesReference::make(std::move(value));
            reset_output();
            if (owning_node().is_started()) {
                set_sample_time(owning_graph().evaluation_clock().evaluation_time());
                notify(sample_time());
            } else {
                owning_node().add_start_input(this);
            }
            return false;
        }
    }

    void TimeSeriesReferenceInput::do_un_bind_output() {
        if (output() != nullptr) { TimeSeriesInput::do_un_bind_output(); }
        if (_value != nullptr) {
            _value = nullptr;
            // TODO: Do we need to notify here? Should we notify only if the input is active?
            set_sample_time(owning_node().is_started() ? owning_graph().evaluation_clock().evaluation_time() : MIN_ST);
        }
        if (!_items.empty()) {
            for (auto &item : _items) { item->un_bind_output(); }
            _items.clear();
        }
    }

    TimeSeriesReferenceOutput *TimeSeriesReferenceInput::as_reference_output() const {
        return const_cast<TimeSeriesReferenceInput *>(this)->as_reference_output();
    }

    TimeSeriesReferenceOutput *TimeSeriesReferenceInput::as_reference_output() {
        return dynamic_cast<TimeSeriesReferenceOutput *>(const_cast<TimeSeriesReferenceInput *>(this)->reference_output().get());
    }

    void TimeSeriesReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesReferenceInput, TimeSeriesInput>(m, "TimeSeriesReferenceInput");
    }

    void TimeSeriesReferenceInput::notify_parent(TimeSeriesInput *child, engine_time_t modified_time) {
        _value = nullptr;
        set_sample_time(modified_time);
        if (active()) { TimeSeriesInput::notify_parent(this, modified_time); }
    }

    std::vector<TimeSeriesReferenceInput::ptr> &TimeSeriesReferenceInput::items() { return _items; }

    const std::vector<TimeSeriesReferenceInput::ptr> &TimeSeriesReferenceInput::items() const { return _items; }

}  // namespace hgraph
