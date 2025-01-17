
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series_type.h>

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

    void EmptyTimeSeriesReference::bind_input(TimeSeriesInput &ts_input) const { ts_input.un_bind_output(); }

    bool EmptyTimeSeriesReference::has_peer() const { return false; }

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

    bool BoundTimeSeriesReference::has_peer() const { return true; }

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

    void UnBoundTimeSeriesReference::bind_input(TimeSeriesInput &ts_input) const {
        IndexedTimeSeriesInput *indexed_input{dynamic_cast<IndexedTimeSeriesInput *>(&ts_input)};
        if (indexed_input == nullptr) {
            throw std::runtime_error("UnBoundTimeSeriesReference::bind_input: Expected an IndexedTimeSeriesInput");
        }
        for (size_t i = 0; i < _items.size(); ++i) { _items[i]->bind_input(*(*indexed_input)[i]); }
    }

    bool UnBoundTimeSeriesReference::has_peer() const { return false; }

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

    const TimeSeriesReference::ptr &TimeSeriesReferenceOutput::value() const { return _value; }

    void TimeSeriesReferenceOutput::set_value(TimeSeriesReference::ptr value) {
        _value = value;
        mark_modified();
        auto it{_reference_observers.begin()};
        while (it != _reference_observers.end()) {
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

    void TimeSeriesReferenceOutput::copy_from_output(TimeSeriesOutput &output) {
        TimeSeriesReferenceOutput *output_t = dynamic_cast<TimeSeriesReferenceOutput *>(&output);
        if (output_t) { set_value(output_t->_value); }
    }

    void TimeSeriesReferenceOutput::copy_from_input(TimeSeriesInput &input) {
        TimeSeriesReferenceInput *input_t = dynamic_cast<TimeSeriesReferenceInput *>(&input);
        if (input_t) { set_value(input_t->value()); }
    }

    const TimeSeriesReferenceOutput &TimeSeriesReferenceInput::reference_output() const {
        return dynamic_cast<TimeSeriesReferenceOutput &>(*output());
    }

    const TimeSeriesReference::ptr &TimeSeriesReferenceInput::value() const { return reference_output().value(); }

    void TimeSeriesReferenceInput::start() {
        auto sample_time = owning_graph().evaluation_clock().evaluation_time();
        notify(sample_time);
    }

    void TimeSeriesReferenceInput::notify_parent(TimeSeriesInput *child, engine_time_t modified_time) {
        // TODO: Implement this
    }

}  // namespace hgraph
