#include <hgraph/builders/input_builder.h>
#include <hgraph/builders/output_builder.h>
#include <hgraph/types/constants.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/tsd.h>

namespace hgraph
{

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::apply_result(nb::object value) {
        if (!valid()) {
            key_set().mark_modified();  // Even if we tick an empty set, we still need to mark this as modified
        }

        // For now only support
        if (nb::isinstance<nb::dict>(value)) {
            auto remove{get_remove()};
            auto remove_if_exists{get_remove_if_exists()};
            for (const auto &[k, v] : nb::cast<nb::dict>(value)) {
                if (v.is_none()) { continue; }
                auto k_ = nb::cast<T_Key>(k);
                if (v.is(remove) || v.is(remove_if_exists)) {
                    if (v.is(remove_if_exists) && !contains(k_)) { continue; }
                    erase(k_);
                } else {
                    operator[](k_).apply_result(nb::borrow(v));
                }
            }
        } else {
            throw std::runtime_error("TimeSeriesDictOutput::apply_result: Only dictionary inputs are supported");
        }

        _post_modify();
    }

    template <typename T_Key> bool TimeSeriesDictOutput_T<T_Key>::can_apply_result(nb::object value) {
        if (value.is_none()) { return true; }
        if (!value) { return true; }

        auto remove           = get_remove();
        auto remove_if_exists = nb::module_::import_("hgraph").attr("REMOVE_IF_EXISTS");

        if (nb::isinstance<nb::dict>(value)) {
            for (const auto &[k, v_] : nb::cast<nb::dict>(value)) {
                if (v_.is_none()) { continue; }
                auto k_ = nb::cast<T_Key>(k);
                if (v_.is(remove) || v_.is(remove_if_exists)) {
                    if (v_.is(remove_if_exists) && !contains(k_)) { continue; }
                    if (was_modified(k_)) { return false; }
                } else {
                    if (was_removed(k_)) { return false; }
                    if (contains(k_)) {
                        if (!operator[](k_).can_apply_result(nb::borrow(v_))) { return false; }
                    }
                }
            }
        } else {
            throw std::runtime_error("TimeSeriesDictOutput::can_apply_result: Only dictionary inputs are supported");
        }
        return true;
    }

    template <typename T_Key>
    void TimeSeriesDictOutput_T<T_Key>::mark_child_modified(TimeSeriesOutput &child, engine_time_t modified_time) {
        if (last_modified_time() < modified_time) {
            // _last_modified_time is set in mark_modified later
            // TODO: Is this actually required? and if it is should it also reset added?
            _modified_items.clear();
        }

        auto child_ptr{&child};
        if (child_ptr != &key_set()) { add_modified_value(child_ptr); }

        TimeSeriesOutput::mark_child_modified(child, modified_time);
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::mark_modified(engine_time_t modified_time) {
        if (last_modified_time() < modified_time) {
            TimeSeriesDict<TimeSeriesOutput>::mark_modified(modified_time);
            _value = nb::none();
        }
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::add_added_item(key_type key, value_type value) {
        key_set_t().add(key);
        _ts_values.insert({key, value});
        _reverse_ts_values.insert({value, key});
        _added_items.insert({key, value});
        _ref_ts_feature.update(key);
        for (auto &observer : _key_observers) { observer->on_key_added(key); }
        add_modified_value(value);
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::add_modified_value(value_type value) {
        auto key{_reverse_ts_values.at(value)};
        _modified_items.insert({key, value});
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::remove_value(const key_type &key, bool raise_if_not_found) {
        auto it{_ts_values.find(key)};
        if (it == _ts_values.end()) {
            // TDOD: Fix the format latter, for now it compiles locally not not on CICD server :(
            if (raise_if_not_found) {
                throw std::runtime_error(/*std::format("Key '{}' not found in TSD", key)*/ "Key not found in TSD");
            }
            return;
        }
        bool was_added = key_set_t().was_added(key);
        key_set_t().remove(key);
        for (auto &observer : _key_observers) { observer->on_key_removed(key); }
        auto item{it->second};
        _ts_values.erase(it);
        _reverse_ts_values.erase(item);
        item->clear();
        _modified_items.erase(key);
        if (!was_added) { _removed_values.emplace(key, item); }

        _ref_ts_feature.update(key);
    }

    template <typename T_Key>
    TimeSeriesDictOutput_T<T_Key>::TimeSeriesDictOutput_T(const node_ptr &parent, output_builder_ptr ts_builder,
                                                          output_builder_ptr ts_ref_builder)
        : TimeSeriesDictOutput(parent), _key_set{this}, _ts_builder{std::move(ts_builder)},
          _ts_ref_builder{std::move(ts_ref_builder)},
          _ref_ts_feature{this,
                          _ts_ref_builder,
                          [](const TimeSeriesOutput &ts, TimeSeriesOutput &ref, const key_type &key) {
                              auto ts_t{dynamic_cast<const TimeSeriesDictOutput_T<T_Key> &>(ts)};
                              auto &value{
                                  ts_t[key]
                              };
                              auto r{TimeSeriesReference::make(&value)};
                              auto r_val{nb::cast(r)};
                              ref.apply_result(r_val);
                          },
                          {}} {}

    template <typename T_Key>
    TimeSeriesDictOutput_T<T_Key>::TimeSeriesDictOutput_T(const time_series_type_ptr &parent, output_builder_ptr ts_builder,
                                                          output_builder_ptr ts_ref_builder)
        : TimeSeriesDictOutput(static_cast<const TimeSeriesType::ptr &>(parent)), _key_set{this},
          _ts_builder{std::move(ts_builder)}, _ts_ref_builder{std::move(ts_ref_builder)},
          _ref_ts_feature{this,
                          _ts_ref_builder,
                          [this](const TimeSeriesOutput &ts, TimeSeriesOutput &ref, const key_type &key) {
                              ref.apply_result(nb::cast(TimeSeriesReference::make(&_get_or_create(key))));
                          },
                          {}} {}

    template <typename T_Key> nb::object TimeSeriesDictOutput_T<T_Key>::py_value() const {
        if (_value.is_none()) {
            auto v{nb::dict()};
            for (const auto &[key, value] : _ts_values) {
                if (value->valid()) { v[nb::cast(key)] = value->py_value(); }
            }
            _value = v;
        }
        return _value;
    }

    template <typename T_Key> nb::object TimeSeriesDictOutput_T<T_Key>::py_delta_value() const {
        auto delta_value{nb::dict()};
        if (delta_value.size() == 0) {
            for (const auto &[key, value] : _modified_items) { delta_value[nb::cast(key)] = value->py_delta_value(); }
            if (_removed_values.size() > 0) {
                auto removed{get_remove()};
                for (const auto &[key, _] : _removed_values) { delta_value[nb::cast(key)] = removed; }
            }
        }
        return delta_value;
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::clear() {
        key_set().clear();
        for (auto &[_, value] : _ts_values) { value->clear(); }

        _removed_values = _ts_values;
        _ts_values.clear();
        _reverse_ts_values.clear();

        _ref_ts_feature.update_all(std::views::keys(_removed_values).begin(), std::views::keys(_removed_values).end());

        _modified_items.clear();

        for (auto &observer : _key_observers) {
            for (const auto &[key, _] : _removed_values) { observer->on_key_removed(key); }
        }
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::invalidate() {
        for (auto &[_, value] : _ts_values) { value->invalidate(); }
        mark_invalid();
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::copy_from_output(const TimeSeriesOutput &output) {
        auto              &other = dynamic_cast<const TimeSeriesDictOutput_T<T_Key> &>(output);
        std::vector<T_Key> to_remove;
        for (const auto &[k, _] : _ts_values) {
            if (other._ts_values.find(k) == other._ts_values.end()) { to_remove.push_back(k); }
        }
        for (const auto &k : to_remove) { erase(k); }
        for (const auto &[k, v] : other._ts_values) { _get_or_create(k).copy_from_output(*v); }
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::copy_from_input(const TimeSeriesInput &input) {
        auto &dict_input = dynamic_cast<const TimeSeriesDictInput_T<T_Key> &>(input);

        std::vector<T_Key> to_remove;
        for (const auto &[k, _] : _ts_values) {
            if (!dict_input.contains(k)) { to_remove.push_back(k); }
        }
        for (const auto &k : to_remove) { erase(k); }
        for (const auto &[k, v] : dict_input) { _get_or_create(k).copy_from_input(*v); }
    }

    template <typename T_Key> bool TimeSeriesDictOutput_T<T_Key>::has_added() const { return !_added_items.empty(); }

    template <typename T_Key> bool TimeSeriesDictOutput_T<T_Key>::has_removed() const { return !_removed_values.empty(); }

    template <typename T_Key> size_t TimeSeriesDictOutput_T<T_Key>::size() const { return _ts_values.size(); }

    template <typename T_Key> bool TimeSeriesDictOutput_T<T_Key>::py_contains(const nb::object &item) const {
        return contains(nb::cast<T_Key>(item));
    }

    template <typename T_Key> bool TimeSeriesDictOutput_T<T_Key>::contains(const key_type &item) const {
        return _ts_values.find(item) != _ts_values.end();
    }

    template <typename T_Key> nb::object TimeSeriesDictOutput_T<T_Key>::py_get_item(const nb::object &item) const {
        auto KET_SET_ID = nb::module_::import_("hgraph").attr("KEY_SET_ID");
        if (KET_SET_ID.is(item)) { return nb::cast(_key_set); }
        auto k = nb::cast<T_Key>(item);
        return nb::cast(operator[](k));
    }

    template <typename T_Key>
    TimeSeriesDict<TimeSeriesOutput>::ts_type &TimeSeriesDictOutput_T<T_Key>::operator[](const key_type &item) {
        return _get_or_create(item);
    }

    template <typename T_Key>
    const TimeSeriesDict<TimeSeriesOutput>::ts_type &TimeSeriesDictOutput_T<T_Key>::operator[](const key_type &item) const {
        return *_ts_values.at(item);
    }

    template <typename T_Key>
    typename TimeSeriesDictOutput_T<T_Key>::const_item_iterator TimeSeriesDictOutput_T<T_Key>::begin() const {
        return _ts_values.begin();
    }

    template <typename T_Key> typename TimeSeriesDictOutput_T<T_Key>::item_iterator TimeSeriesDictOutput_T<T_Key>::begin() {
        return _ts_values.begin();
    }

    template <typename T_Key>
    typename TimeSeriesDictOutput_T<T_Key>::const_item_iterator TimeSeriesDictOutput_T<T_Key>::end() const {
        return _ts_values.end();
    }

    template <typename T_Key> typename TimeSeriesDictOutput_T<T_Key>::item_iterator TimeSeriesDictOutput_T<T_Key>::end() {
        return _ts_values.end();
    }

    template <typename T_Key> nb::iterator TimeSeriesDictOutput_T<T_Key>::py_keys() const {
        return nb::make_key_iterator(nb::type<map_type>(), "KeyIterator", begin(), end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictOutput_T<T_Key>::py_values() const {
        return nb::make_value_iterator(nb::type<map_type>(), "ValueIterator", begin(), end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictOutput_T<T_Key>::py_items() const {
        return nb::make_iterator(nb::type<map_type>(), "ItemIterator", begin(), end());
    }

    template <typename T_Key>
    const typename TimeSeriesDictOutput_T<T_Key>::map_type &TimeSeriesDictOutput_T<T_Key>::modified_items() const {
        return _modified_items;
    }

    template <typename T_Key> nb::iterator TimeSeriesDictOutput_T<T_Key>::py_modified_keys() const {
        return nb::make_key_iterator(nb::type<map_type>(), "ModifiedKeyIterator", modified_items().begin(), modified_items().end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictOutput_T<T_Key>::py_modified_values() const {
        return nb::make_value_iterator(nb::type<map_type>(), "ModifiedValueIterator", modified_items().begin(),
                                       modified_items().end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictOutput_T<T_Key>::py_modified_items() const {
        return nb::make_iterator(nb::type<map_type>(), "ModifiedItemIterator", modified_items().begin(), modified_items().end());
    }

    template <typename T_Key> bool TimeSeriesDictOutput_T<T_Key>::py_was_modified(const nb::object &key) const {
        return was_modified(nb::cast<T_Key>(key));
    }

    template <typename T_Key> bool TimeSeriesDictOutput_T<T_Key>::was_modified(const key_type &key) const {
        return _modified_items.find(key) != _modified_items.end();
    }

    template <typename T_Key> auto TimeSeriesDictOutput_T<T_Key>::valid_items() const {
        return _ts_values | std::views::filter([](const auto &item) { return item.second->valid(); });
    }

    template <typename T_Key> nb::iterator TimeSeriesDictOutput_T<T_Key>::py_valid_keys() const {
        auto valid_items_ = valid_items();
        return nb::make_key_iterator(nb::type<map_type>(), "ValidKeyIterator", valid_items_.begin(), valid_items_.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictOutput_T<T_Key>::py_valid_values() const {
        auto valid_items_ = valid_items();
        return nb::make_value_iterator(nb::type<map_type>(), "ValidValueIterator", valid_items_.begin(), valid_items_.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictOutput_T<T_Key>::py_valid_items() const {
        auto valid_items_ = valid_items();
        return nb::make_iterator(nb::type<map_type>(), "ValidItemIterator", valid_items_.begin(), valid_items_.end());
    }

    template <typename T_Key>
    const typename TimeSeriesDictOutput_T<T_Key>::map_type &TimeSeriesDictOutput_T<T_Key>::added_items() const {
        return _added_items;
    }

    template <typename T_Key> nb::iterator TimeSeriesDictOutput_T<T_Key>::py_added_keys() const {
        return nb::make_key_iterator(nb::type<map_type>(), "AddedKeyIterator", added_items().begin(), added_items().end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictOutput_T<T_Key>::py_added_values() const {
        return nb::make_value_iterator(nb::type<map_type>(), "AddedValueIterator", added_items().begin(), added_items().end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictOutput_T<T_Key>::py_added_items() const {
        return nb::make_iterator(nb::type<map_type>(), "AddedItemIterator", added_items().begin(), added_items().end());
    }

    template <typename T_Key> bool TimeSeriesDictOutput_T<T_Key>::py_was_added(const nb::object &key) const {
        return was_added(nb::cast<T_Key>(key));
    }

    template <typename T_Key> bool TimeSeriesDictOutput_T<T_Key>::was_added(const key_type &key) const {
        return _added_items.find(key) != _added_items.end();
    }

    template <typename T_Key>
    const typename TimeSeriesDictOutput_T<T_Key>::map_type &TimeSeriesDictOutput_T<T_Key>::removed_items() const {
        return _removed_values;
    }

    template <typename T_Key> nb::iterator TimeSeriesDictOutput_T<T_Key>::py_removed_keys() const {
        return nb::make_key_iterator(nb::type<map_type>(), "RemovedKeyIterator", removed_items().begin(), removed_items().end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictOutput_T<T_Key>::py_removed_values() const {
        return nb::make_value_iterator(nb::type<map_type>(), "RemovedValueIterator", removed_items().begin(),
                                       removed_items().end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictOutput_T<T_Key>::py_removed_items() const {
        return nb::make_iterator(nb::type<map_type>(), "RemovedItemIterator", removed_items().begin(), removed_items().end());
    }

    template <typename T_Key> bool TimeSeriesDictOutput_T<T_Key>::py_was_removed(const nb::object &key) const {
        return was_removed(nb::cast<T_Key>(key));
    }

    template <typename T_Key> bool TimeSeriesDictOutput_T<T_Key>::was_removed(const key_type &key) const {
        return _removed_values.find(key) != _removed_values.end();
    }

    template <typename T_Key> TimeSeriesSet<TimeSeriesDict<TimeSeriesOutput>::ts_type> &TimeSeriesDictOutput_T<T_Key>::key_set() {
        return key_set_t();
    }

    template <typename T_Key>
    const TimeSeriesSet<TimeSeriesDict<TimeSeriesOutput>::ts_type> &TimeSeriesDictOutput_T<T_Key>::key_set() const {
        return const_cast<TimeSeriesDictOutput_T *>(this)->key_set();
    }

    template <typename T_Key>
    TimeSeriesSetOutput_T<typename TimeSeriesDictOutput_T<T_Key>::key_type> &TimeSeriesDictOutput_T<T_Key>::key_set_t() {
        return _key_set;
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::py_set_item(const nb::object &key, const nb::object &value) {
        auto &ts{operator[](nb::cast<T_Key>(key))};
        ts.apply_result(value);
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::py_del_item(const nb::object &key) {
        erase(nb::cast<T_Key>(key));
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::erase(const key_type &key) { remove_value(key, true); }

    template <typename T_Key>
    nb::object TimeSeriesDictOutput_T<T_Key>::py_pop(const nb::object &key, const nb::object &default_value) {
        nb::object value;
        auto       k = nb::cast<T_Key>(key);
        if (auto it = _ts_values.find(k); it != _ts_values.end()) {
            value = it->second->py_value();
            remove_value(k, false);
        }
        if (!value.is_valid()) { value = default_value; }
        return value;
    }

    template <typename T_Key>
    time_series_output_ptr TimeSeriesDictOutput_T<T_Key>::py_get_ref(const nb::object &key, const void *requester) {
        return get_ref(nb::cast<key_type>(key), requester);
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::py_release_ref(const nb::object &key, const void *requester) {
        release_ref(nb::cast<T_Key>(key), requester);
    }

    template <typename T_Key>
    time_series_output_ptr TimeSeriesDictOutput_T<T_Key>::get_ref(const key_type &key, const void *requester) {
        return _ref_ts_feature.create_or_increment(key, requester);
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::release_ref(const key_type &key, const void *requester) {
        _ref_ts_feature.release(key, requester);
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::clear_on_end_of_evaluation_cycle() {
        _added_items.clear();
        _modified_items.clear();
        _removed_values.clear();
    }

    template <typename T_Key> TimeSeriesOutput &TimeSeriesDictOutput_T<T_Key>::_get_or_create(const key_type &key) {
        if (_ts_values.find(key) == _ts_values.end()) { _create(key); }
        return *_ts_values[key];
    }

    template <typename T_Key> bool TimeSeriesDictOutput_T<T_Key>::has_reference() const { return _ts_builder->has_reference(); }

    template <typename T_Key>
    const typename TimeSeriesDictOutput_T<T_Key>::key_type &
    TimeSeriesDictOutput_T<T_Key>::key_from_value(TimeSeriesOutput *value) const {
        auto it = _reverse_ts_values.find(value);
        if (it != _reverse_ts_values.end()) { return it->second; }
        throw std::out_of_range("Value not found in TimeSeriesDictOutput");
    }

    template <typename T_Key>
    TimeSeriesDictInput_T<T_Key>::TimeSeriesDictInput_T(const node_ptr &parent, input_builder_ptr ts_builder)
        : TimeSeriesDictInput(parent), _key_set{this}, _ts_builder{ts_builder} {}

    template <typename T_Key>
    TimeSeriesDictInput_T<T_Key>::TimeSeriesDictInput_T(const time_series_type_ptr &parent, input_builder_ptr ts_builder)
        : TimeSeriesDictInput(parent), _key_set{this}, _ts_builder{ts_builder} {}

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::has_peer() const { return _has_peer; }
    template <typename T_Key>
    typename TimeSeriesDictInput_T<T_Key>::const_item_iterator TimeSeriesDictInput_T<T_Key>::begin() const {
        return const_cast<TimeSeriesDictInput_T *>(this)->begin();
    }

    template <typename T_Key> typename TimeSeriesDictInput_T<T_Key>::item_iterator TimeSeriesDictInput_T<T_Key>::begin() {
        return _ts_values.begin();
    }

    template <typename T_Key> typename TimeSeriesDictInput_T<T_Key>::const_item_iterator TimeSeriesDictInput_T<T_Key>::end() const {
        return const_cast<TimeSeriesDictInput_T *>(this)->end();
    }

    template <typename T_Key> typename TimeSeriesDictInput_T<T_Key>::item_iterator TimeSeriesDictInput_T<T_Key>::end() {
        return _ts_values.end();
    }

    template <typename T_Key> size_t TimeSeriesDictInput_T<T_Key>::size() const { return _ts_values.size(); }

    template <typename T_Key> nb::object TimeSeriesDictInput_T<T_Key>::py_value() const {
        if (has_peer()) { return TimeSeriesInput::py_value(); }
        auto v{nb::dict()};
        for (const auto &[key, value] : _ts_values) {
            if (value->valid()) { v[nb::cast(key)] = value->py_value(); }
        }
        return v;
    }

    template <typename T_Key> nb::object TimeSeriesDictInput_T<T_Key>::py_delta_value() const {
        if (has_peer()) { return TimeSeriesInput::py_delta_value(); }
        auto delta{nb::dict()};
        // Build from currently modified and valid child inputs to avoid relying solely on observer-tracked state
        for (const auto &[key, value] : _ts_values) {
            if (value->modified() && value->valid()) { delta[nb::cast(key)] = value->py_delta_value(); }
        }
        if (!_removed_values.empty()) {
            auto removed{get_remove()};
            for (const auto &[key, _] : _removed_values) { delta[nb::cast(key)] = removed; }
        }
        return delta;
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::py_contains(const nb::object &item) const {
        return contains(nb::cast<T_Key>(item));
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::contains(const key_type &item) const {
        return _ts_values.find(item) != _ts_values.end();
    }

    template <typename T_Key> nb::object TimeSeriesDictInput_T<T_Key>::py_get_item(const nb::object &item) const {
        return nb::cast(operator[](nb::cast<T_Key>(item)));
    }

    template <typename T_Key>
    TimeSeriesDict<TimeSeriesInput>::ts_type &TimeSeriesDictInput_T<T_Key>::operator[](const key_type &item) const {
        return const_cast<TimeSeriesDictInput_T *>(this)->operator[](item);
    }

    template <typename T_Key>
    TimeSeriesDict<TimeSeriesInput>::ts_type &TimeSeriesDictInput_T<T_Key>::operator[](const key_type &item) {
        return _get_or_create(item);
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_keys() const {
        return nb::make_key_iterator(nb::type<map_type>(), "KeyIterator", _ts_values.begin(), _ts_values.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_values() const {
        return nb::make_value_iterator(nb::type<map_type>(), "ValueIterator", _ts_values.begin(), _ts_values.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_items() const {
        return nb::make_iterator(nb::type<map_type>(), "ItemIterator", _ts_values.begin(), _ts_values.end());
    }

    template <typename T_Key>
    const typename TimeSeriesDictInput_T<T_Key>::map_type &TimeSeriesDictInput_T<T_Key>::modified_items() const {
        return _modified_items;
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_modified_keys() const {
        return nb::make_key_iterator(nb::type<map_type>(), "ModifiedKeyIterator", _modified_items.begin(), _modified_items.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_modified_values() const {
        return nb::make_value_iterator(nb::type<map_type>(), "ModifiedValueIterator", _modified_items.begin(),
                                       _modified_items.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_modified_items() const {
        return nb::make_iterator(nb::type<map_type>(), "ModifiedItemIterator", _modified_items.begin(), _modified_items.end());
    }

    template <typename T_Key> TimeSeriesSet<TimeSeriesInput> &TimeSeriesDictInput_T<T_Key>::key_set() { return key_set_t(); }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::py_was_modified(const nb::object &key) const {
        return was_modified(nb::cast<T_Key>(key));
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::was_modified(const key_type &key) const {
        return _modified_items.find(key) != _modified_items.end();
    }

    template <typename T_Key> auto TimeSeriesDictInput_T<T_Key>::valid_items() const {
        return _ts_values | std::views::filter([](const auto &item) { return item.second->valid(); });
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_valid_keys() const {
        return nb::make_key_iterator(nb::type<map_type>(), "ValidKeyIterator", valid_items().begin(), valid_items().end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_valid_values() const {
        return nb::make_value_iterator(nb::type<map_type>(), "ValidValueIterator", valid_items().begin(), valid_items().end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_valid_items() const {
        return nb::make_iterator(nb::type<map_type>(), "ValidItemIterator", valid_items().begin(), valid_items().end());
    }

    template <typename T_Key>
    const typename TimeSeriesDictInput_T<T_Key>::map_type &TimeSeriesDictInput_T<T_Key>::added_items() const {
        return _added_items;
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_added_keys() const {
        return nb::make_key_iterator(nb::type<map_type>(), "AddedKeyIterator", _added_items.begin(), _added_items.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_added_values() const {
        return nb::make_value_iterator(nb::type<map_type>(), "AddedValueIterator", _added_items.begin(), _added_items.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_added_items() const {
        return nb::make_iterator(nb::type<map_type>(), "AddedItemIterator", _added_items.begin(), _added_items.end());
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::has_added() const { return !_added_items.empty(); }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::py_was_added(const nb::object &key) const {
        return was_added(nb::cast<T_Key>(key));
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::was_added(const key_type &key) const {
        return _added_items.find(key) != _added_items.end();
    }

    template <typename T_Key>
    const typename TimeSeriesDictInput_T<T_Key>::map_type &TimeSeriesDictInput_T<T_Key>::removed_items() const {
        return _removed_values;
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_removed_keys() const {
        return nb::make_key_iterator(nb::type<map_type>(), "RemovedKeyIterator", _removed_values.begin(), _removed_values.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_removed_values() const {
        return nb::make_value_iterator(nb::type<map_type>(), "RemovedValueIterator", _removed_values.begin(),
                                       _removed_values.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_removed_items() const {
        return nb::make_iterator(nb::type<map_type>(), "RemovedItemIterator", _removed_values.begin(), _removed_values.end());
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::has_removed() const { return !_removed_values.empty(); }

    template <typename T_Key> const TimeSeriesSet<TimeSeriesInput> &TimeSeriesDictInput_T<T_Key>::key_set() const {
        return key_set_t();
    }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::on_key_added(const key_type &key) {
        auto &value{_get_or_create(key)};
        if (!has_peer() && active()) { value.make_active(); }
        value.bind_output(&output_t()[key]);
        register_clear_key_changes();
    }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::on_key_removed(const key_type &key) {
        // NOTE: We were tracking the valid state on the removed item. Now we just track the value
        auto it = _ts_values.find(key);
        if (it == _ts_values.end()) { return; }

        auto value{it->second};

        if (value->parent_input().get() == this) {
            if (value->active()) { value->make_passive(); }
            _removed_values.insert({key, value});
            _modified_items.erase(key);
            _ts_values.erase(it);
        }
        register_clear_key_changes();
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::was_removed(const key_type &key) const {
        return _removed_values.find(key) != _removed_values.end();
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::py_was_removed(const nb::object &key) const {
        return was_removed(nb::cast<T_Key>(key));
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::do_bind_output(time_series_output_ptr value) {
        typename TimeSeriesDictOutput_T<T_Key>::ptr output_{dynamic_cast<TimeSeriesDictOutput_T<T_Key> *>(value.get())};

        bool peer;

        if (!is_same_type(*output_) &&
            (output_->has_reference() || this->has_reference())) {
            peer = false;
            key_set_t().set_subscribe_method(true);
        } else {
            peer = true;
            key_set_t().set_subscribe_method(this->subscribe_input());
        }

        _has_peer = peer;

        key_set_t().bind_output(&output_->key_set_t());

        if (owning_node().is_started() && has_output()) {
            output_t().remove_key_observer(this);
            _prev_output = {&output_t()};
            owning_graph().evaluation_engine_api().add_after_evaluation_notification([this]() { this->reset_prev(); });
        }

        TimeSeriesInput::do_bind_output(value);

        if (!_ts_values.empty()) { register_clear_key_changes(); }

        for (const auto &key : key_set_t().values()) { on_key_added(key); }

        for (const auto &key : key_set_t().removed()) { on_key_removed(key); }

        output_->add_key_observer(this);
        return peer;
    }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::do_un_bind_output() {
        key_set_t().un_bind_output();

        if (!_ts_values.empty()) {
            for (const auto &[key, value] : _ts_values) { _removed_values.insert({key, value}); }
            _ts_values.clear();
            _reverse_ts_values.clear();
            register_clear_key_changes();

            std::unordered_map<key_type, time_series_input_ptr> to_keep;
            for (auto &[key, value] : _removed_values) {
                if (value->parent_input().get() != this) {
                    // Check for transplanted items, these do not get removed, but can be un-bound
                    value->un_bind_output();
                    _ts_values.insert({key, value});
                    _reverse_ts_values.insert({value, key});
                } else {
                    to_keep.insert({key, value});
                }
            }
            _removed_values = std::move(to_keep);
        }

        if (has_output()) { output_t().remove_key_observer(this); }
        TimeSeriesInput::do_un_bind_output();
    }

    template <typename T_Key>
    TimeSeriesSetInput_T<typename TimeSeriesDictInput_T<T_Key>::key_type> &TimeSeriesDictInput_T<T_Key>::key_set_t() {
        return _key_set;
    }

    template <typename T_Key>
    const TimeSeriesSetInput_T<typename TimeSeriesDictInput_T<T_Key>::key_type> &TimeSeriesDictInput_T<T_Key>::key_set_t() const {
        return const_cast<TimeSeriesDictInput_T *>(this)->key_set_t();
    }

    template <typename T_Key>
    TimeSeriesDictOutput_T<typename TimeSeriesDictInput_T<T_Key>::key_type> &TimeSeriesDictInput_T<T_Key>::output_t() {
        return reinterpret_cast<TimeSeriesDictOutput_T<key_type> &>(*output());
    }

    template <typename T_Key>
    const TimeSeriesDictOutput_T<typename TimeSeriesDictInput_T<T_Key>::key_type> &TimeSeriesDictInput_T<T_Key>::output_t() const {
        return const_cast<TimeSeriesDictInput_T *>(this)->output_t();
    }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::reset_prev() { _prev_output = nullptr; }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::clear_key_changes() {
        _clear_key_changes_registered = false;
        _added_items.clear();
        _modified_items.clear();
        for (auto &[_, value] : _removed_values) {
            if (value->parent_input().get() != this || !has_peer()) { value->un_bind_output(); }
        }
        _removed_values.clear();
    }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::register_clear_key_changes() {
        if (!_clear_key_changes_registered) {
            _clear_key_changes_registered = true;
            owning_graph().evaluation_engine_api().add_after_evaluation_notification([this]() { clear_key_changes(); });
        }
    }

    template <typename T_Key> TimeSeriesInput &TimeSeriesDictInput_T<T_Key>::_get_or_create(const key_type &key) {
        if (_ts_values.find(key) == _ts_values.end()) { _create(key); }
        return *_ts_values[key];
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::is_same_type(TimeSeriesType &other) const {
        auto other_d = dynamic_cast<TimeSeriesDictInput_T<key_type> *>(&other);
        if (!other_d) { return false; }
        return _ts_builder->is_same_type(*other_d->_ts_builder);
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::has_reference() const { return _ts_builder->has_reference(); }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::make_active() {
        if (active()){return;}
        if (has_peer()) {
            TimeSeriesDictInput::make_active();
        } else {
            set_active(true);
            key_set().make_active();
            for (auto &[_, value] : _ts_values) { value->make_active(); }
        }
    }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::make_passive() {
        if (!active()) { return; }
        if (has_peer()) {
            TimeSeriesDictInput::make_passive();
        } else {
            set_active(false);
            key_set().make_passive();
            for (auto &[_, value] : _ts_values) { value->make_passive(); }
        }
    }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::notify_parent(TimeSeriesInput *child, engine_time_t modified_time) {
        if (_last_modified_time < modified_time) {
            _last_modified_time = modified_time;
            _modified_items.clear();  //TODO: Is this required?
        }

        if (child != &key_set()) {
            auto it{_reverse_ts_values.find(child)};
            if (it != _reverse_ts_values.end()) {
                _modified_items.insert({it->second, child});
            }
        }

        TimeSeriesInput::notify_parent(child, modified_time);
    }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::_create(const key_type &key) {
        auto value{_ts_builder->make_instance(this)};
        value->set_subscribe_method(!has_peer());
        _ts_values.insert({key, value});
        _reverse_ts_values.insert({value, key});
        _added_items.insert({key, value});
        _modified_items.insert({key, value});
        register_clear_key_changes();
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::_create(const key_type &key) {
        auto value = _ts_builder->make_instance(this);
        add_added_item(key, value);
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

    template struct TimeSeriesDictInput_T<bool>;
    template struct TimeSeriesDictInput_T<int64_t>;
    template struct TimeSeriesDictInput_T<double>;
    template struct TimeSeriesDictInput_T<engine_date_t>;
    template struct TimeSeriesDictInput_T<engine_time_t>;
    template struct TimeSeriesDictInput_T<engine_time_delta_t>;
    template struct TimeSeriesDictInput_T<nb::object>;

    using TSD_Bool      = TimeSeriesDictInput_T<bool>;
    using TSD_Int       = TimeSeriesDictInput_T<int64_t>;
    using TSD_Float     = TimeSeriesDictInput_T<double>;
    using TSD_Date      = TimeSeriesDictInput_T<engine_date_t>;
    using TSD_DateTime  = TimeSeriesDictInput_T<engine_time_t>;
    using TSD_TimeDelta = TimeSeriesDictInput_T<engine_time_delta_t>;
    using TSD_Object    = TimeSeriesDictInput_T<nb::object>;

    template struct TimeSeriesDictOutput_T<bool>;
    template struct TimeSeriesDictOutput_T<int64_t>;
    template struct TimeSeriesDictOutput_T<double>;
    template struct TimeSeriesDictOutput_T<engine_date_t>;
    template struct TimeSeriesDictOutput_T<engine_time_t>;
    template struct TimeSeriesDictOutput_T<engine_time_delta_t>;
    template struct TimeSeriesDictOutput_T<nb::object>;

    using TSD_OUT_Bool      = TimeSeriesDictOutput_T<bool>;
    using TSD_OUT_Int       = TimeSeriesDictOutput_T<int64_t>;
    using TSD_OUT_Float     = TimeSeriesDictOutput_T<double>;
    using TSD_OUT_Date      = TimeSeriesDictOutput_T<engine_date_t>;
    using TSD_OUT_DateTime  = TimeSeriesDictOutput_T<engine_time_t>;
    using TSD_OUT_TimeDelta = TimeSeriesDictOutput_T<engine_time_delta_t>;
    using TSD_OUT_Object    = TimeSeriesDictOutput_T<nb::object>;

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::post_modify() { _post_modify(); }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::_post_modify() {
        key_set().post_modify();
        if (has_added() || has_removed()) {
            owning_graph().evaluation_engine_api().add_after_evaluation_notification(
                [this]() { clear_on_end_of_evaluation_cycle(); });
        }
    }

    void tsd_register_with_nanobind(nb::module_ &m) {

        nb::class_<TimeSeriesDictOutput, TimeSeriesOutput>(m, "TimeSeriesDictOutput")
            .def("__contains__", &TimeSeriesDictOutput::py_contains, "key"_a)
            .def("__getitem__", &TimeSeriesDictOutput::py_get_item, "key"_a)
            .def("__setitem__", &TimeSeriesDictOutput::py_set_item, "key"_a, "value"_a)
            .def("__delitem__", &TimeSeriesDictOutput::py_del_item, "key"_a)
            .def("__len__", &TimeSeriesDictOutput::size)
            .def("pop", &TimeSeriesDictOutput::py_pop, "key"_a, "default"_a = nb::none())
            .def("__iter__", &TimeSeriesDictOutput::py_keys)
            .def("keys", &TimeSeriesDictOutput::py_keys)
            .def("values", &TimeSeriesDictOutput::py_values)
            .def("items", &TimeSeriesDictOutput::py_items)
            .def("valid_keys", &TimeSeriesDictOutput::py_valid_keys)
            .def("valid_values", &TimeSeriesDictOutput::py_valid_values)
            .def("valid_items", &TimeSeriesDictOutput::py_valid_items)
            .def("added_keys", &TimeSeriesDictOutput::py_added_keys)
            .def("added_values", &TimeSeriesDictOutput::py_added_values)
            .def("added_items", &TimeSeriesDictOutput::py_added_items)
            .def("was_added", &TimeSeriesDictOutput::py_was_added, "key"_a)
            .def_prop_ro("has_added", &TimeSeriesDictOutput::has_added)
            .def("modified_keys", &TimeSeriesDictOutput::py_modified_keys)
            .def("modified_values", &TimeSeriesDictOutput::py_modified_values)
            .def("modified_items", &TimeSeriesDictOutput::py_modified_items)
            .def("was_modified", &TimeSeriesDictOutput::py_was_modified, "key"_a)
            .def("removed_keys", &TimeSeriesDictOutput::py_removed_keys)
            .def("removed_values", &TimeSeriesDictOutput::py_removed_values)
            .def("removed_items", &TimeSeriesDictOutput::py_removed_items)
            .def("was_removed", &TimeSeriesDictOutput::py_was_removed, "key"_a)
            .def_prop_ro("has_removed", &TimeSeriesDictOutput::has_removed)
            .def("get_ref", [](TimeSeriesDictOutput &self, const nb::object &key, const nb::object &requester) {
                 return self.py_get_ref(key, requester.ptr());
             }, "key"_a, "requester"_a)
            .def("release_ref", [](TimeSeriesDictOutput &self, const nb::object &key, const nb::object &requester) {
                 self.py_release_ref(key, requester.ptr());
             }, "key"_a, "requester"_a)
            .def("key_set",
                 static_cast<const TimeSeriesSet<TimeSeriesDict<TimeSeriesOutput>::ts_type> &(TimeSeriesDictOutput::*)() const>(
                     &TimeSeriesDictOutput::key_set))  // Not sure if this needs to be exposed to python?
            ;

        nb::class_<TimeSeriesDictInput, TimeSeriesInput>(m, "TimeSeriesDictInput")
            .def("__contains__", &TimeSeriesDictInput::py_contains, "key"_a)
            .def("__getitem__", &TimeSeriesDictInput::py_get_item, "key"_a)
            .def("__len__", &TimeSeriesDictInput::size)
            .def("__iter__", &TimeSeriesDictInput::py_keys)
            .def("keys", &TimeSeriesDictInput::py_keys)
            .def("values", &TimeSeriesDictInput::py_values)
            .def("items", &TimeSeriesDictInput::py_items)
            .def("valid_keys", &TimeSeriesDictInput::py_valid_keys)
            .def("valid_values", &TimeSeriesDictInput::py_valid_values)
            .def("valid_items", &TimeSeriesDictInput::py_valid_items)
            .def("added_keys", &TimeSeriesDictInput::py_added_keys)
            .def("added_values", &TimeSeriesDictInput::py_added_values)
            .def("added_items", &TimeSeriesDictInput::py_added_items)
            .def("was_added", &TimeSeriesDictInput::py_was_added, "key"_a)
            .def_prop_ro("has_added", &TimeSeriesDictInput::has_added)
            .def("modified_keys", &TimeSeriesDictInput::py_modified_keys)
            .def("modified_values", &TimeSeriesDictInput::py_modified_values)
            .def("modified_items", &TimeSeriesDictInput::py_modified_items)
            .def("was_modified", &TimeSeriesDictInput::py_was_modified, "key"_a)
            .def("removed_keys", &TimeSeriesDictInput::py_removed_keys)
            .def("removed_values", &TimeSeriesDictInput::py_removed_values)
            .def("removed_items", &TimeSeriesDictInput::py_removed_items)
            .def("was_removed", &TimeSeriesDictInput::py_was_removed, "key"_a)
            .def_prop_ro("has_removed", &TimeSeriesDictInput::has_removed)
            .def("key_set",
                 static_cast<const TimeSeriesSet<TimeSeriesDict<TimeSeriesInput>::ts_type> &(TimeSeriesDictInput::*)() const>(
                     &TimeSeriesDictInput::key_set))  // Not sure if this needs to be exposed to python?
            ;

        nb::class_<TSD_OUT_Bool, TimeSeriesDictOutput>(m, "TimeSeriesDictOutput_Bool");
        nb::class_<TSD_OUT_Int, TimeSeriesDictOutput>(m, "TimeSeriesDictOutput_Int");
        nb::class_<TSD_OUT_Float, TimeSeriesDictOutput>(m, "TimeSeriesDictOutput_Float");
        nb::class_<TSD_OUT_Date, TimeSeriesDictOutput>(m, "TimeSeriesDictOutput_Date");
        nb::class_<TSD_OUT_DateTime, TimeSeriesDictOutput>(m, "TimeSeriesDictOutput_DateTime");
        nb::class_<TSD_OUT_TimeDelta, TimeSeriesDictOutput>(m, "TimeSeriesDictOutput_TimeDelta");
        nb::class_<TSD_OUT_Object, TimeSeriesDictOutput>(m, "TimeSeriesDictOutput_Object");

        nb::class_<TSD_Bool, TimeSeriesDictInput>(m, "TimeSeriesDictInput_Bool");
        nb::class_<TSD_Int, TimeSeriesDictInput>(m, "TimeSeriesDictInput_Int");
        nb::class_<TSD_Float, TimeSeriesDictInput>(m, "TimeSeriesDictInput_Float");
        nb::class_<TSD_Date, TimeSeriesDictInput>(m, "TimeSeriesDictInput_Date");
        nb::class_<TSD_DateTime, TimeSeriesDictInput>(m, "TimeSeriesDictInput_DateTime");
        nb::class_<TSD_TimeDelta, TimeSeriesDictInput>(m, "TimeSeriesDictInput_TimeDelta");
        nb::class_<TSD_Object, TimeSeriesDictInput>(m, "TimeSeriesDictInput_Object");
    }
}  // namespace hgraph
