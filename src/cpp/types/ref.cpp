
#include <hgraph/builders/output_builder.h>
#include <hgraph/types/constants.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/ts.h>
#include <hgraph/types/ts_indexed.h>
#include <hgraph/types/ts_signal.h>
#include <hgraph/types/tsd.h>
#include <hgraph/types/tss.h>

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

        nb::class_<EmptyTimeSeriesReference, TimeSeriesReference>(m, "EmptyTimeSeriesReference");

        nb::class_<BoundTimeSeriesReference, TimeSeriesReference>(m, "BoundTimeSeriesReference")
            .def_prop_ro("output", &BoundTimeSeriesReference::output);

        nb::class_<UnBoundTimeSeriesReference, TimeSeriesReference>(m, "UnBoundTimeSeriesReference")
            .def_prop_ro("items", &UnBoundTimeSeriesReference::items)
            .def("__getitem__", [](UnBoundTimeSeriesReference &self, size_t index) -> TimeSeriesReference::ptr {
                const auto &items = self.items();
                if (index >= items.size()) { throw std::out_of_range("Index out of range"); }
                return items[index];
            });
    }

    void EmptyTimeSeriesReference::bind_input(TimeSeriesInput &ts_input) const {
        try {
            ts_input.un_bind_output();
        } catch (const std::exception &e) {
            throw std::runtime_error(std::string("Error in EmptyTimeSeriesReference::bind_input: ") + e.what());
        } catch (...) { throw std::runtime_error("Unknown error in EmptyTimeSeriesReference::bind_input"); }
    }

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
        try {
            bool reactivate = false;
            // Treat inputs previously bound via a reference as bound, so we unbind to generate correct deltas
            if ((ts_input.bound() || ts_input.reference_output().get() != nullptr) && !ts_input.has_peer()) {
                reactivate = ts_input.active();
                ts_input.un_bind_output();
            }
            ts_input.bind_output(_output);
            if (reactivate) { ts_input.make_active(); }
        } catch (const std::exception &e) {
            throw std::runtime_error(std::string("Error in BoundTimeSeriesReference::bind_input: ") + e.what());
        } catch (...) { throw std::runtime_error("Unknown error in BoundTimeSeriesReference::bind_input"); }
    }

    bool BoundTimeSeriesReference::has_output() const { return true; }

    bool BoundTimeSeriesReference::is_empty() const { return false; }

    bool BoundTimeSeriesReference::is_valid() const { return _output->valid(); }

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
        // Try to cast to supported input types
        auto *ref_input     = dynamic_cast<TimeSeriesReferenceInput *>(&ts_input);
        auto *indexed_input = dynamic_cast<IndexedTimeSeriesInput *>(&ts_input);
        auto *signal_input  = dynamic_cast<TimeSeriesSignalInput *>(&ts_input);

        if (ref_input == nullptr && indexed_input == nullptr && signal_input == nullptr) {
            throw std::runtime_error("UnBoundTimeSeriesReference::bind_input: Expected an IndexedTimeSeriesInput, "
                                     "TimeSeriesReferenceInput, or TimeSeriesSignalInput");
        }

        bool reactivate = false;
        if (ts_input.bound() && ts_input.has_peer()) {
            reactivate = ts_input.active();
            ts_input.un_bind_output();
        }

        for (size_t i = 0; i < _items.size(); ++i) {
            // Get the child input (from REF, Indexed, or Signal input)
            TimeSeriesInput::ptr item;
            if (ref_input) {
                item = (*ref_input)[i];
            } else if (indexed_input) {
                item = (*indexed_input)[i];
            } else if (signal_input) {
                item = (*signal_input)[i];
            }

            if (_items[i] != nullptr) {
                _items[i]->bind_input(*item);
            } else if (item->bound()) {
                item->un_bind_output();
            }
        }

        if (reactivate) { ts_input.make_active(); }
    }

    bool UnBoundTimeSeriesReference::has_output() const { return false; }

    bool UnBoundTimeSeriesReference::is_empty() const { return false; }

    bool UnBoundTimeSeriesReference::is_valid() const {
        return std::any_of(_items.begin(), _items.end(), [](const auto &item) { return item != nullptr && !item->is_empty(); });
    }

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
        try {
            for (TimeSeriesInput *input : _reference_observers) { _value->bind_input(*input); }
        } catch (const NodeException &e) {
            throw;  // already enriched upstream
        } catch (const std::exception &e) {
            throw std::runtime_error(std::string("During TimeSeriesReferenceOutput::set_value: ") + e.what());
        } catch (...) { throw std::runtime_error("Unknown error in TimeSeriesReferenceOutput::set_value"); }
    }

    void TimeSeriesReferenceOutput::apply_result(nb::object value) {
        if (value.is_none()) { return; }
        auto v{nb::cast<TimeSeriesReference::ptr>(value)};
        if (v == nullptr) { throw std::runtime_error("Expected TimeSeriesReference"); }
        set_value(v);
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
        _value = nullptr;
        mark_invalid();
    }

    void TimeSeriesReferenceOutput::copy_from_output(const TimeSeriesOutput &output) {
        auto output_t = dynamic_cast<const TimeSeriesReferenceOutput *>(&output);
        if (output_t) {
            set_value(output_t->_value);
        } else {
            throw std::runtime_error("TimeSeriesReferenceOutput::copy_from_output: Expected TimeSeriesReferenceOutput");
        }
    }

    void TimeSeriesReferenceOutput::copy_from_input(const TimeSeriesInput &input) {
        auto input_t = dynamic_cast<const TimeSeriesReferenceInput *>(&input);
        if (input_t) {
            set_value(input_t->_value);
        } else {
            throw std::runtime_error("TimeSeriesReferenceOutput::copy_from_input: Expected TimeSeriesReferenceInput");
        }
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
        try {
            nb::gil_scoped_acquire gil;
            auto                   v{value()};
            return v.get() == nullptr ? nb::none() : nb::cast(v);
        } catch (const std::exception &e) {
            throw std::runtime_error(std::string("Error in TimeSeriesReferenceInput::py_value: ") + e.what());
        } catch (...) { throw std::runtime_error("Unknown error in TimeSeriesReferenceInput::py_value"); }
    }

    nb::object TimeSeriesReferenceInput::py_delta_value() const { return py_value(); }

    TimeSeriesReference::ptr TimeSeriesReferenceInput::value() const {
        if (output() != nullptr) { return dynamic_cast<TimeSeriesReferenceOutput *>(output().get())->value(); }
        if (_value.get() != nullptr) { return _value; }
        if (!_items.empty()) {
            _value = TimeSeriesReference::make(_items);
            return _value;
        }
        return nullptr;
    }

    void TimeSeriesReferenceInput::clone_binding(const TimeSeriesReferenceInput &other) {
        un_bind_output();
        if (other.has_output()) {
            bind_output(other.output());
        } else if (!other.items().empty()) {
            for (size_t i = 0; i < other.items().size(); ++i) { _items[i]->clone_binding(*other.items()[i]); }
        } else if (other._value != nullptr) {
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

    bool TimeSeriesReferenceInput::bind_output(time_series_output_ptr output_) {
        auto peer = do_bind_output(output_);

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
            new_item->set_subscribe_method(true);
            _items.push_back(new_item);
        }
        return _items.at(ndx);
    }

    bool TimeSeriesReferenceInput::do_bind_output(time_series_output_ptr output_) {
        if (dynamic_cast<TimeSeriesReferenceOutput *>(output_.get()) != nullptr) {
            // Match Python behavior: bind to a TimeSeriesReferenceOutput as a normal peer
            _value = nullptr;
            return TimeSeriesInput::do_bind_output(output_);
        } else {
            // We are binding directly to a concrete output: wrap it as a reference value
            _value = TimeSeriesReference::make(std::move(output_));
            output().reset();
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
        nb::class_<TimeSeriesReferenceInput, TimeSeriesInput>(m, "TimeSeriesReferenceInput")
            .def("__getitem__", [](TimeSeriesReferenceInput &self, size_t index) -> TimeSeriesInput::ptr {
                return TimeSeriesInput::ptr{self[index].get()};
            });
    }

    bool TimeSeriesReferenceInput::is_reference() const { return true; }

    bool TimeSeriesReferenceInput::has_reference() const { return true; }

    void TimeSeriesReferenceInput::notify_parent(TimeSeriesInput *child, engine_time_t modified_time) {
        _value = nullptr;
        set_sample_time(modified_time);
        if (active()) { TimeSeriesInput::notify_parent(this, modified_time); }
    }

    std::vector<TimeSeriesReferenceInput::ptr> &TimeSeriesReferenceInput::items() { return _items; }

    const std::vector<TimeSeriesReferenceInput::ptr> &TimeSeriesReferenceInput::items() const { return _items; }

}  // namespace hgraph
