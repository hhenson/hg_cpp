#include <hgraph/builders/output_builder.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/tsd.h>

namespace hgraph
{

    void TimeSeriesDictOutput::apply_result(nb::object value) {
        if (!valid()) {
            key_set().mark_modified();  // Even if we tick an empty set, we still need to mark this as modified
        }

        // For now only support
        if (nb::isinstance<nb::dict>(value)) {
            auto remove           = nb::module_::import_("hgraph").attr("REMOVE");
            auto remove_if_exists = nb::module_::import_("hgraph").attr("REMOVE_IF_EXISTS");
            for (const auto &[k, v] : nb::cast<nb::dict>(value)) {
                if (v.is_none()) { continue; }
                if (v.is(remove) || v.is(remove_if_exists)) {
                    if (v.is(remove_if_exists) && !py_contains(nb::borrow(k))) { continue; }
                    del_item(nb::borrow(k));
                } else {
                    get_or_create(nb::borrow(k)).apply_result(nb::borrow(v));
                }
            }
        } else {
            throw std::runtime_error("TimeSeriesDictOutput::apply_result: Only dictionary inputs are supported");
        }

        if (!has_added() || !has_removed()) {
            owning_graph().evaluation_engine_api().add_after_evaluation_notification(
                [this]() { clear_on_end_of_evaluation_cycle(); });
        }
    }

    bool TimeSeriesDictOutput::can_apply_result(nb::object value) {
        if (value.is_none()) { return true; }
        if (!value) { return true; }

        auto remove           = nb::module_::import_("hgraph").attr("REMOVE");
        auto remove_if_exists = nb::module_::import_("hgraph").attr("REMOVE_IF_EXISTS");

        if (nb::isinstance<nb::dict>(value)) {
            for (const auto &[k, v_] : nb::cast<nb::dict>(value)) {
                if (v_.is_none()) { continue; }
                if (v_.is(remove) || v_.is(remove_if_exists)) {
                    if (v_.is(remove_if_exists) && !py_contains(nb::borrow(k))) { continue; }
                    if (was_modified(nb::borrow(k))) { return false; }
                } else {
                    if (was_removed(nb::borrow(k))) { return false; }
                    if (py_contains(nb::borrow(k))) {
                        if (!get_item(nb::borrow(k)).can_apply_result(nb::borrow(v_))) { return false; }
                    }
                }
            }
        } else {
            throw std::runtime_error("TimeSeriesDictOutput::can_apply_result: Only dictionary inputs are supported");
        }
        return true;
    }

    void TimeSeriesDictOutput::mark_child_modified(TimeSeriesOutput &child, engine_time_t modified_time) {
        if (last_modified_time() < modified_time) {
            // _last_modified_time is set in mark_modified later
            // TODO: Is this actually required? and if it is should it also reset added?
            _modified_items.clear();
        }

        auto child_ptr{&child};
        if (child_ptr != &key_set()) { add_modified_value(child_ptr); }

        TimeSeriesOutput::mark_child_modified(child, modified_time);
    }

    void TimeSeriesDictOutput::mark_modified(engine_time_t modified_time) {
        if (last_modified_time() < modified_time) {
            TimeSeriesDict<TimeSeriesOutput>::mark_modified(modified_time);
            _value.clear();
        }
    }

    void TimeSeriesDictOutput::clear_on_end_of_evaluation_cycle() {
        _added_items.clear();
        _modified_items.clear();
    }

    void TimeSeriesDictOutput::add_added_item(void *key, TimeSeriesOutput *value) {
        _reverse_ts_values.emplace(value, key);
        _added_items.emplace(key, value);
        // TODO: Check if this second line is required as it may get marked modified anyhow
        _modified_items.emplace(key, value);
    }

    void TimeSeriesDictOutput::add_modified_value(TimeSeriesOutput *value) {
        _modified_items.emplace(_reverse_ts_values.at(value), value);
    }

    void TimeSeriesDictOutput::remove_value(TimeSeriesOutput *value) { _reverse_ts_values.erase(value); }

    template <typename T_Key>
    TimeSeriesDictOutput_T<T_Key>::TimeSeriesDictOutput_T(const node_ptr &parent) : TimeSeriesDictOutput(parent) {
        _initialise();
    }

    template <typename T_Key>
    TimeSeriesDictOutput_T<T_Key>::TimeSeriesDictOutput_T(const time_series_type_ptr &parent)
        : TimeSeriesDictOutput(static_cast<const TimeSeriesType::ptr &>(parent)) {
        _initialise();
    }

    template <typename T_Key> nb::object TimeSeriesDictOutput_T<T_Key>::py_value() const {
        if (_value.size() == 0) {
            for (const auto &[key, value] : _ts_values) {
                if (value->valid()) { _value[nb::cast(key)] = value->py_value(); }
            }
        }
        return _value;
    }

    template <typename T_Key> nb::object TimeSeriesDictOutput_T<T_Key>::py_delta_value() const {
        if (_delta_value.size() == 0) {
            for (const auto &[key, value] : _modified_items) { _delta_value[nb::cast(key)] = value->py_value(); }
            if (_removed_values.size() > 0) {
                auto removed{nb::module_::import_("hgraph").attr("REMOVED")};
                for (const auto &[key, _] : _removed_values) { _delta_value[nb::cast(key)] = removed; }
            }
        }
        return _delta_value;
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::clear() {
        key_set().clear();
        for (auto &[_, value] : _ts_values) { value->clear(); }
        _removed_values = _ts_values;
        _ts_values.clear();
        _reverse_ts_values.clear();
        if (_ref_ts_feature) {
            std::vector<T_Key> keys;
            for (const auto &[key, _] : _removed_values) { keys.push_back(key); }
            _ref_ts_feature.update_all(keys.begin(), keys.end());
        }
        _modified_items.clear();

        for (auto &observer : _key_observers) {
            for (const auto &[key, _] : _removed_values) { observer.on_key_removed(key); }
        }
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::invalidate() {
        for (auto &[_, value] : _ts_values) { value->invalidate(); }
        mark_invalid();
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::del_item(const nb::object &key) {
        auto k = nb::cast<T_Key>(key);
        auto it{_ts_values.find(k)};
        if (it == _ts_values.end()) { throw std::runtime_error("Key does not exist in TimeSeriesDictOutput"); }

        bool was_added = key_set().py_was_added(key);
        key_set().py_remove(key);

        for (auto &observer : _key_observers) { observer.on_key_removed(k); }

        auto item = _ts_values[k];
        item->clear();
        _ts_values.erase(k);

        if (!was_added) { _removed_values.emplace(k, item); }

        remove_value(item.get());
        if (_ref_ts_feature) { _ref_ts_feature.update(k); }

        _modified_items.erase(std::make_tuple(_reverse_ts_values.at(item.get()), item.get()));
    }

    template <typename T_Key>
    nb::object TimeSeriesDictOutput_T<T_Key>::pop(const nb::object &key, const nb::object &default_value) {
        nb::object value;
        auto       k = nb::cast<T_Key>(key);
        if (auto it = _ts_values.find(k); it != _ts_values.end()) {
            value = it->second->py_value();
            del_item(key);
        }
        if (!value.is_valid()) { value = nb::borrow(default_value); }
        return value;
    }

    template <typename T_Key>
    time_series_output_ptr TimeSeriesDictOutput_T<T_Key>::py_get_ref(const nb::object &key, const void *requester) {
        return _ref_ts_feature.create_or_increment(nb::cast<T_Key>(key), requester);
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::py_release_ref(const nb::object &key, const void *requester) {
        _ref_ts_feature.release(nb::cast<T_Key>(key), requester);
    }

    template <typename T_Key>
    time_series_output_ptr TimeSeriesDictOutput_T<T_Key>::get_ref(const key_type &key, const void *requester)
        requires(!std::is_same_v<T_Key, nanobind::object>)
    {
        return _ref_ts_feature.create_or_increment(key, requester);
    }

    template <typename T_Key>
    void TimeSeriesDictOutput_T<T_Key>::release_ref(const key_type &key, const void *requester)
        requires(!std::is_same_v<T_Key, nanobind::object>)
    {
        _ref_ts_feature.release(key, requester);
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::clear_on_end_of_evaluation_cycle() {
        TimeSeriesDictOutput::clear_on_end_of_evaluation_cycle();
        _removed_values.clear();
        _delta_value.clear();
    }

    template <typename T_Key>
    const typename TimeSeriesDictOutput_T<T_Key>::key_type &
    TimeSeriesDictOutput_T<T_Key>::key_from_value(TimeSeriesOutput *value) const {
        auto it = _reverse_ts_values.find(value);
        if (it != _reverse_ts_values.end()) { return *static_cast<const T_Key *>(it->second); }
        throw std::out_of_range("Value not found in TimeSeriesDictOutput");
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::_initialise() {
        _ref_ts_feature = FeatureOutputExtension<key_type>(
            this, _ts_ref_builder, [this](TimeSeriesOutput &ts, TimeSeriesOutput &ref, key_type key) {
                ref.apply_result(nb::cast(TimeSeriesReference::make(_ts_values.at(key))));
            });
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::_create(const key_type &key) {
        key_set().py_add(key);  // TODO: Fix this to point to correct key type
        auto item = _ts_builder->make_instance(this);
        _ts_values.emplace(key, item);
        _reverse_ts_values.emplace(item.get(), &_ts_values.find(key)->first);
        _ref_ts_feature.update(key);
        for (auto &observer : _key_observers) { observer.on_key_added(key); }
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::add_key_observer(TSDKeyObserver<key_type> *observer) {
        _key_observers.push_back(observer);
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::remove_key_observer(TSDKeyObserver<key_type> *observer) {
        auto it = std::find(_key_observers.begin(), _key_observers.end(), observer);
        if (it != _key_observers.end()) {
            *it = _key_observers.back();
            _key_observers.pop_back();
        }
    }

    void tsd_register_with_nanobind(nb::module_ &m) {

        nb::class_<TimeSeriesDictOutput, TimeSeriesOutput>(m, "TimeSeriesDictOutput")
            .def("__contains__", &TimeSeriesDictOutput::py_contains, "key"_a)
            .def("__getitem__", &TimeSeriesDictOutput::get_item, "key"_a)
            .def("__setitem__", &TimeSeriesDictOutput::set_item, "key"_a, "value"_a)
            .def("__delitem__", &TimeSeriesDictOutput::del_item, "key"_a)
            .def("__len__", &TimeSeriesDictOutput::size)
            .def("pop", &TimeSeriesDictOutput::pop, "key"_a, "default"_a = nb::none())
            .def("__iter__", &TimeSeriesDictOutput::py_keys)
            .def("keys", &TimeSeriesDictOutput::py_keys)
            .def("values", &TimeSeriesDictOutput::py_values)
            .def("items", &TimeSeriesDictOutput::py_items)
            .def("valid_keys", &TimeSeriesDictOutput::py_valid_keys)
            .def("valid_values", &TimeSeriesDictOutput::py_valid_values)
            .def("valid_items", &TimeSeriesDictOutput::py_valid_items)
            .def("modified_keys", &TimeSeriesDictOutput::py_modified_keys)
            .def("modified_values", &TimeSeriesDictOutput::py_modified_values)
            .def("modified_items", &TimeSeriesDictOutput::py_modified_items)
            .def("get_ref", &TimeSeriesDictOutput::py_get_ref, "key"_a, "requester"_a)
            .def("release_ref", &TimeSeriesDictOutput::py_get_ref, "key"_a, "requester"_a)
            .def("key_set", &TimeSeriesDictOutput::key_set)  // Not sure if this needs to be exposed to python?
            ;

        nb::class_<TimeSeriesDictInput, TimeSeriesInput>(m, "TimeSeriesDictInput")
            .def("__contains__", &TimeSeriesDictInput::py_contains, "key"_a)
            .def("__getitem__", &TimeSeriesDictInput::get_item, "key"_a)
            .def("__len__", &TimeSeriesDictInput::size)
            .def("__iter__", &TimeSeriesDictInput::py_keys)
            .def("keys", &TimeSeriesDictInput::py_keys)
            .def("values", &TimeSeriesDictInput::py_values)
            .def("items", &TimeSeriesDictInput::py_items)
            .def("valid_keys", &TimeSeriesDictInput::py_valid_keys)
            .def("valid_values", &TimeSeriesDictInput::py_valid_values)
            .def("valid_items", &TimeSeriesDictInput::py_valid_items)
            .def("modified_keys", &TimeSeriesDictInput::py_modified_keys)
            .def("modified_values", &TimeSeriesDictInput::py_modified_values)
            .def("modified_items", &TimeSeriesDictInput::py_modified_items)
            .def("key_set", &TimeSeriesDictInput::key_set)  // Not sure if this needs to be exposed to python?
            ;

        nb::class_<TimeSeriesDictOutput_T<nb::object>, TimeSeriesDictOutput>(m, "TimeSeriesDictOutput_object");
        nb::class_<TimeSeriesDictInput_T<nb::object>, TimeSeriesDictInput>(m, "TimeSeriesDictInput_object");
    }
}  // namespace hgraph
