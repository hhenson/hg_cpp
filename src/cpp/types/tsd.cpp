#include <hgraph/builders/input_builder.h>
#include <hgraph/builders/output_builder.h>
#include <hgraph/types/constants.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/tsd.h>
#include <hgraph/util/string_utils.h>

namespace hgraph
{

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::apply_result(nb::object value) {
        // Ensure any Python API interaction occurs under the GIL and protect against exceptions
        if (value.is_none()) { return; }
        py_set_value(value);
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
        auto parent_eval_time = owning_graph().evaluation_clock().evaluation_time();

        // Only clear modified_items when we're entering a new parent evaluation cycle
        // Use parent's evaluation time, not the nested graph's time
        if (last_modified_time() < parent_eval_time) {
            // _last_modified_time is set in mark_modified later
            // Clear modified items to start fresh for the new evaluation cycle
            _modified_items.clear();
        }

        if (&child != &key_set()) {
            // Use reverse map with raw pointer for efficient O(1) lookup
            auto it = _reverse_ts_values.find(&child);
            if (it != _reverse_ts_values.end()) { _modified_items.insert({it->second, _ts_values.at(it->second)}); }
        }

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
        _reverse_ts_values.insert({value.get(), key});  // Store raw pointer for efficient lookup
        _added_items.insert({key, value});
        _ref_ts_feature.update(key);
        for (auto &observer : _key_observers) { observer->on_key_added(key); }
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::remove_value(const key_type &key, bool raise_if_not_found) {
        auto it{_ts_values.find(key)};
        if (it == _ts_values.end()) {
            // TDOD: Fix the format latter, for now it compiles locally not not on CICD server :(
            if (raise_if_not_found) {
                std::string key_str{to_string(key)};
                throw std::runtime_error(std::string("Key '") + key_str + "' not found in TSD (in remove_value)");
            }
            return;
        }
        bool was_added = key_set_t().was_added(key);
        key_set_t().remove(key);
        for (auto &observer : _key_observers) { observer->on_key_removed(key); }
        auto item{it->second};
        _ts_values.erase(it);
        _reverse_ts_values.erase(item.get());  // Erase using raw pointer
        item->clear();
        _modified_items.erase(key);
        if (!was_added) { _removed_values.emplace(key, item); }

        _ref_ts_feature.update(key);

        // Schedule cleanup notification only once per evaluation cycle
        auto et = owning_graph().evaluation_clock().evaluation_time();
        if (_last_cleanup_time < et) {
            _last_cleanup_time = et;
            owning_graph().evaluation_engine_api().add_after_evaluation_notification(
                [this]() { clear_on_end_of_evaluation_cycle(); });
        }
    }

    template <typename T_Key>
    TimeSeriesDictOutput_T<T_Key>::TimeSeriesDictOutput_T(const node_ptr &parent, nb::ref<key_set_type> key_set,
                                                          output_builder_ptr ts_builder, output_builder_ptr ts_ref_builder)
        : TimeSeriesDictOutput(parent), _key_set{std::move(key_set)}, _ts_builder{std::move(ts_builder)},
          _ts_ref_builder{std::move(ts_ref_builder)},
          _ref_ts_feature{this,
                          _ts_ref_builder,
                          [](const TimeSeriesOutput &ts, TimeSeriesOutput &ref, const key_type &key) {
                              auto &ts_t{dynamic_cast<const TimeSeriesDictOutput_T<T_Key> &>(ts)};
                              auto  it = ts_t._ts_values.find(key);
                              if (it != ts_t._ts_values.end()) {
                                  auto r{TimeSeriesReference::make(it->second)};
                                  auto r_val{nb::cast(r)};
                                  ref.apply_result(r_val);
                              }
                          },
                          {}} {
        _key_set->TimeSeriesType::re_parent(TimeSeriesType::ptr{this});
    }

    template <typename T_Key>
    TimeSeriesDictOutput_T<T_Key>::TimeSeriesDictOutput_T(const time_series_type_ptr &parent, nb::ref<key_set_type> key_set,
                                                          output_builder_ptr ts_builder, output_builder_ptr ts_ref_builder)
        : TimeSeriesDictOutput(static_cast<const TimeSeriesType::ptr &>(parent)), _key_set{std::move(key_set)},
          _ts_builder{std::move(ts_builder)}, _ts_ref_builder{std::move(ts_ref_builder)},
          _ref_ts_feature{this,
                          _ts_ref_builder,
                          [this](const TimeSeriesOutput &ts, TimeSeriesOutput &ref, const key_type &key) {
                              auto it = _ts_values.find(key);
                              if (it != _ts_values.end()) { ref.apply_result(nb::cast(TimeSeriesReference::make(it->second))); }
                          },
                          {}} {
        _key_set->TimeSeriesType::re_parent(TimeSeriesType::ptr{this});
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::py_set_value(nb::object value) {
        if (value.is_none()) {
            invalidate();
            return;
        }
        try {
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
                        // Skip removal if key doesn't exist (both REMOVE and REMOVE_IF_EXISTS)
                        if (!contains(k_)) { continue; }
                        erase(k_);
                    } else {
                        // Apply to child, but guard to enrich any errors
                        try {
                            operator[](k_).apply_result(nb::borrow(v));
                        } catch (const NodeException &e) {
                            throw;  // already enriched upstream
                        } catch (const std::exception &e) {
                            throw std::runtime_error(std::string("Error applying TSD value for key: ") +
                                                     nb::cast<std::string>(nb::str(k)) + ": " + e.what());
                        } catch (...) { throw std::runtime_error("Unknown error applying TSD value"); }
                    }
                }
            } else {
                throw std::runtime_error("TimeSeriesDictOutput::apply_result: Only dictionary inputs are supported");
            }

            _post_modify();
        } catch (const NodeException &e) {
            throw;  // already enriched
        } catch (const std::exception &e) {
            throw std::runtime_error(std::string("During TimeSeriesDictOutput_T::apply_result: ") + e.what());
        } catch (...) { throw std::runtime_error("Unknown error in TimeSeriesDictOutput_T::apply_result"); }
    }

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
        for (const auto &[key, value] : _modified_items) {
            if (value->valid()) { delta_value[nb::cast(key)] = value->py_delta_value(); }
        }
        if (!_removed_values.empty()) {
            auto removed{get_remove()};
            for (const auto &[key, _] : _removed_values) { delta_value[nb::cast(key)] = removed; }
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

        // Remove keys that are no longer in the input
        std::vector<T_Key> to_remove;
        for (const auto &[k, _] : _ts_values) {
            if (!dict_input.contains(k)) { to_remove.push_back(k); }
        }
        for (const auto &k : to_remove) { erase(k); }

        // Copy values from input
        // Iterate over dict_input but skip removed items (which may still be in _ts_values)
        for (const auto &[k, v_input] : dict_input) {
            // Skip if this key has been removed (removed items may still be in _ts_values)
            if (!v_input || dict_input.was_removed(k)) { continue; }
            _get_or_create(k).copy_from_input(*v_input);
        }
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
        auto  k  = nb::cast<T_Key>(item);
        auto &ts = operator[](k);
        auto *py = ts.self_py();
        if (py) return nb::borrow(py);
        // Prefer wrapping as base type to avoid double-wrapping under different derived bindings
        return nb::cast(const_cast<TimeSeriesOutput *>(&ts));
    }

    template <typename T_Key> nb::object TimeSeriesDictOutput_T<T_Key>::py_get_or_create(const nb::object &key) {
        auto &ts = _get_or_create(nb::cast<T_Key>(key));
        return nb::cast(&ts);
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
        // For Output: delegate to key_set.added() like Input does
        // (Python Output's _added_keys appears to be broken/never populated)
        _added_items.clear();
        for (const auto &k : key_set_t().added()) {
            auto it = _ts_values.find(k);
            if (it != _ts_values.end()) { _added_items.emplace(k, it->second); }
        }
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

    template <typename T_Key> nb::object TimeSeriesDictOutput_T<T_Key>::py_key_set() const { return nb::cast(_key_set); }

    template <typename T_Key> TimeSeriesSet<TimeSeriesDict<TimeSeriesOutput>::ts_type> &TimeSeriesDictOutput_T<T_Key>::key_set() {
        return key_set_t();
    }

    template <typename T_Key>
    const TimeSeriesSet<TimeSeriesDict<TimeSeriesOutput>::ts_type> &TimeSeriesDictOutput_T<T_Key>::key_set() const {
        return const_cast<TimeSeriesDictOutput_T *>(this)->key_set();
    }

    template <typename T_Key>
    TimeSeriesSetOutput_T<typename TimeSeriesDictOutput_T<T_Key>::key_type> &TimeSeriesDictOutput_T<T_Key>::key_set_t() {
        return *_key_set;
    }

    template <typename T_Key>
    const TimeSeriesSetOutput_T<typename TimeSeriesDictOutput_T<T_Key>::key_type> &
    TimeSeriesDictOutput_T<T_Key>::key_set_t() const {
        return *_key_set;
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

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::_dispose() {
        // Release all removed items first
        for (auto &[_, value] : _removed_values) { _ts_builder->release_instance(value); }
        _removed_values.clear();

        // Release all current values
        for (auto &[_, value] : _ts_values) { _ts_builder->release_instance(value); }
        _ts_values.clear();
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::clear_on_end_of_evaluation_cycle() {
        // Release removed instances before clearing
        for (auto &[_, value] : _removed_values) { _ts_builder->release_instance(value); }
        _removed_values.clear();
        _added_items.clear();
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
        : TimeSeriesDictInput(parent), _key_set{new typename TimeSeriesDictInput_T<T_Key>::key_set_type{this}},
          _ts_builder{ts_builder} {}

    template <typename T_Key>
    TimeSeriesDictInput_T<T_Key>::TimeSeriesDictInput_T(const time_series_type_ptr &parent, input_builder_ptr ts_builder)
        : TimeSeriesDictInput(parent), _key_set{new typename TimeSeriesDictInput_T<T_Key>::key_set_type{this}},
          _ts_builder{ts_builder} {}

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
        auto v{nb::dict()};
        for (const auto &[key, value] : _ts_values) {
            if (value->valid()) { v[nb::cast(key)] = value->py_value(); }
        }
        return get_frozendict()(v);
    }

    template <typename T_Key> nb::object TimeSeriesDictInput_T<T_Key>::py_delta_value() const {
        auto delta{nb::dict()};
        // Build from currently modified and valid child inputs to avoid relying solely on observer-tracked state
        const auto &modified = modified_items();
        for (const auto &[key, value] : modified) {
            if (value->valid()) { delta[nb::cast(key)] = value->py_delta_value(); }
        }
        // Use key_set.removed() like Python does - this excludes keys added in the same cycle
        // Only include REMOVE sentinel for keys that were valid before removal
        const auto &removed_keys = key_set_t().removed();
        if (!removed_keys.empty()) {
            auto removed{get_remove()};
            for (const auto &key : removed_keys) {
                auto it = _removed_values.find(key);
                if (it != _removed_values.end() && it->second.second) {  // Check was_valid flag
                    delta[nb::cast(key)] = removed;
                }
            }
        }
        return get_frozendict()(delta);
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::py_contains(const nb::object &item) const {
        return contains(nb::cast<T_Key>(item));
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::contains(const key_type &item) const {
        return _ts_values.contains(item);
    }

    template <typename T_Key> nb::object TimeSeriesDictInput_T<T_Key>::py_get_item(const nb::object &item) const {
        if (get_key_set_id().is(item)) { return nb::cast(const_cast<TimeSeriesDictInput_T *>(this)->key_set()); }
        return nb::cast(_ts_values.at(nb::cast<T_Key>(item)));
    }

    template <typename T_Key>
    const TimeSeriesDictInput_T<T_Key>::value_type &TimeSeriesDictInput_T<T_Key>::operator[](const key_type &item) const {
        return _ts_values.at(item);
    }

    template <typename T_Key>
    TimeSeriesDictInput_T<T_Key>::value_type TimeSeriesDictInput_T<T_Key>::operator[](const key_type &item) {
        return get_or_create(item);
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
        // This will compute a cached value or use an already computed cached value.
        // TODO: Would like to review this logic to see if there is an improvement that can be made to the cases where
        // we currently clean the cache without checking if it is valid.
        if (sampled()) {
            // Return all valid items when sampled
            _modified_items.clear();
            for (const auto &[key, value] : valid_items()) { _modified_items.emplace(key, value); }
        } else if (has_peer()) {
            // When peered, only return items that are modified in the output
            _modified_items.clear();
            for (const auto &[key, _] : output_t().modified_items()) {
                auto it = _ts_values.find(key);
                if (it != _ts_values.end()) { _modified_items.emplace(key, it->second); }
            }
        } else if (active()) {
            // When active but not sampled or peered, only return cached modified items
            // during the current evaluation cycle
            if (last_modified_time() != owning_graph().evaluation_clock().evaluation_time()) {
                return empty_;  // Return empty set if not in current cycle
            }
        } else {
            // When not active, return all modified items
            for (const auto &[key, value] : _ts_values) {
                if (value->modified()) { _modified_items.emplace(key, value); }
            }
        }
        return _modified_items;
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_modified_keys() const {
        const auto &items = modified_items();  // Ensure modified_items is populated first
        return nb::make_key_iterator(nb::type<map_type>(), "ModifiedKeyIterator", items.begin(), items.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_modified_values() const {
        const auto &items = modified_items();
        return nb::make_value_iterator(nb::type<map_type>(), "ModifiedValueIterator", items.begin(), items.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_modified_items() const {
        const auto &items = modified_items();
        return nb::make_iterator(nb::type<map_type>(), "ModifiedItemIterator", items.begin(), items.end());
    }

    template <typename T_Key> TimeSeriesSet<TimeSeriesInput> &TimeSeriesDictInput_T<T_Key>::key_set() { return key_set_t(); }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::py_was_modified(const nb::object &key) const {
        return was_modified(nb::cast<T_Key>(key));
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::was_modified(const key_type &key) const {
        const auto &it{_ts_values.find(key)};
        return it != _ts_values.end() && it->second->modified();
    }

    template <typename T_Key> auto TimeSeriesDictInput_T<T_Key>::valid_items() const {
        // TODO: look into maintaining this cached.
        _valid_items.clear();
        for (const auto &item : _ts_values | std::views::filter([](const auto &item) { return item.second->valid(); })) {
            _valid_items.insert(item);
        }
        return _valid_items;
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_valid_keys() const {
        const auto &items{valid_items()};
        return nb::make_key_iterator(nb::type<map_type>(), "ValidKeyIterator", items.begin(), items.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_valid_values() const {
        const auto &items{valid_items()};
        return nb::make_value_iterator(nb::type<map_type>(), "ValidValueIterator", items.begin(), items.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_valid_items() const {
        const auto &items{valid_items()};
        return nb::make_iterator(nb::type<map_type>(), "ValidItemIterator", items.begin(), items.end());
    }

    template <typename T_Key>
    const typename TimeSeriesDictInput_T<T_Key>::map_type &TimeSeriesDictInput_T<T_Key>::added_items() const {
        // TODO: Try and ensure that we cache the result where possible
        _added_items.clear();
        const auto &key_set{key_set_t()};
        for (const auto &k : key_set.added()) { _added_items.emplace(k, _ts_values.at(k)); }
        return _added_items;
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_added_keys() const {
        const auto &items = added_items();  // Ensure cache is populated
        return nb::make_key_iterator(nb::type<map_type>(), "AddedKeyIterator", items.begin(), items.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_added_values() const {
        const auto &items = added_items();  // Ensure cache is populated
        return nb::make_value_iterator(nb::type<map_type>(), "AddedValueIterator", items.begin(), items.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_added_items() const {
        const auto &items = added_items();  // Ensure cache is populated
        return nb::make_iterator(nb::type<map_type>(), "AddedItemIterator", items.begin(), items.end());
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::has_added() const { return !key_set_t().added().empty(); }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::py_was_added(const nb::object &key) const {
        return was_added(nb::cast<T_Key>(key));
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::was_added(const key_type &key) const {
        const auto &added{key_set_t().added()};
        return added.find(key) != added.end();
    }

    template <typename T_Key>
    const typename TimeSeriesDictInput_T<T_Key>::map_type &TimeSeriesDictInput_T<T_Key>::removed_items() const {
        _removed_items.clear();
        for (const auto &key : key_set_t().removed()) {
            auto it{_removed_values.find(key)};
            if (it == _removed_values.end()) { continue; }
            _removed_items.emplace(key, it->second.first);
        }
        return _removed_items;
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_removed_keys() const {
        auto const &removed_{removed_items()};
        return nb::make_key_iterator(nb::type<map_type>(), "RemovedKeyIterator", removed_.begin(), removed_.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_removed_values() const {
        auto const &removed_{removed_items()};
        return nb::make_value_iterator(nb::type<map_type>(), "RemovedValueIterator", removed_.begin(), removed_.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_removed_items() const {
        auto const &removed_{removed_items()};
        return nb::make_iterator(nb::type<map_type>(), "RemovedItemIterator", removed_.begin(), removed_.end());
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::has_removed() const { return !_removed_values.empty(); }

    template <typename T_Key> const TimeSeriesSet<TimeSeriesInput> &TimeSeriesDictInput_T<T_Key>::key_set() const {
        return key_set_t();
    }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::on_key_added(const key_type &key) {
        auto value{get_or_create(key)};
        // Activate if: (not peered AND this is active) OR value is already active (transplanted input case)
        // v.active can be true if this was a transplanted input
        if ((!has_peer() && active()) || value->active()) { value->make_active(); }
        value->bind_output(&output_t()[key]);
    }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::on_key_removed(const key_type &key) {
        // Pop the value from _ts_values first (matching Python: self._ts_values.pop(key, None))
        auto it = _ts_values.find(key);
        if (it == _ts_values.end()) { return; }

        auto value{it->second};
        _ts_values.erase(it);                  // Remove from _ts_values first
        _ts_values_to_key.erase(value.get());  // Remove from reverse map

        register_clear_key_changes();
        auto was_valid = value->valid();

        if (value->parent_input().get() == this) {
            // This is our own input - deactivate and track for cleanup
            if (value->active()) { value->make_passive(); }
            _removed_values.insert({key, {value, was_valid}});
            auto it_{_modified_items.find(key)};
            if (it_ != _modified_items.end()) { _modified_items.erase(it_); }
            if (!has_peer()) { value->un_bind_output(false); }
        } else {
            // This is a transplanted input - put it back and unbind it
            _ts_values.insert({key, value});
            _ts_values_to_key.insert({value.get(), key});
            value->un_bind_output(true);  // unbind_refs=True
        }
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::was_removed(const key_type &key) const {
        return _removed_values.find(key) != _removed_values.end();
    }
    template <typename T_Key> nb::object TimeSeriesDictInput_T<T_Key>::py_key_set() const { return nb::cast(key_set()); }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::py_was_removed(const nb::object &key) const {
        return was_removed(nb::cast<T_Key>(key));
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::do_bind_output(time_series_output_ptr &value) {
        TimeSeriesDictOutput_T<T_Key> *output_{dynamic_cast<TimeSeriesDictOutput_T<T_Key> *>(value.get())};

        // Peer when types match AND neither has references (matching Python logic)
        bool peer = is_same_type(output_) && !output_->has_reference() && !this->has_reference();

        auto *_key_set = const_cast<TimeSeriesOutput *>(static_cast<const TimeSeriesOutput *>(&output_->key_set()));
        key_set_t().bind_output({_key_set});

        if (owning_node().is_started() && has_output()) {
            output_t().remove_key_observer(this);
            _prev_output = {&output_t()};
            owning_graph().evaluation_engine_api().add_after_evaluation_notification([this]() { this->reset_prev(); });
        }

        // This is a copy of the base implementation, however caters for peerage changes
        // Critical: make_passive() BEFORE changing _has_peer, because make_passive behavior depends on has_peer
        make_passive();  // Ensure we are unsubscribed from the old output while has_peer has the old value

        // Now update has_peer BEFORE calling base bind, so make_active uses the correct mode
        _has_peer = peer;

        // Call base implementation which will set _output and call make_active if needed
        // Note: Base calls make_passive first, but we already did that above with the OLD has_peer
        // Base then sets _output and calls make_active with the NEW has_peer (which we just set)
        TimeSeriesInput::do_bind_output(value);

        if (!_ts_values.empty()) { register_clear_key_changes(); }

        for (const auto &key : key_set_t().values()) { on_key_added(key); }

        for (const auto &key : key_set_t().removed()) { on_key_removed(key); }

        output_->add_key_observer(this);
        return peer;
    }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::do_un_bind_output(bool unbind_refs) {
        key_set_t().un_bind_output(unbind_refs);

        if (!_ts_values.empty()) {
            _removed_values.clear();
            for (const auto &[key, value] : _ts_values) { _removed_values.insert({key, {value, value->valid()}}); }
            _ts_values.clear();
            _ts_values_to_key.clear();
            register_clear_key_changes();

            removed_map_type to_keep;
            for (auto &[key, v] : _removed_values) {
                auto &[value, was_valid] = v;
                if (value->parent_input().get() != this) {
                    // Check for transplanted items, these do not get removed, but can be un-bound
                    value->un_bind_output(false);
                    _ts_values.insert({key, value});
                    _ts_values_to_key.insert({value.get(), key});
                } else {
                    to_keep.insert({key, {value, was_valid}});
                }
            }
            _removed_values = std::move(to_keep);
        }
        // If we are un-binding then the output must exist by definition.
        output_t().remove_key_observer(this);
        TimeSeriesInput::do_un_bind_output(false);
    }

    template <typename T_Key>
    TimeSeriesSetInput_T<typename TimeSeriesDictInput_T<T_Key>::key_type> &TimeSeriesDictInput_T<T_Key>::key_set_t() {
        return *_key_set;
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

    template <typename T_Key>
    const typename TimeSeriesDictInput_T<T_Key>::key_type &
    TimeSeriesDictInput_T<T_Key>::key_from_value(TimeSeriesInput *value) const {
        return _ts_values_to_key.at(value);
    }

    template <typename T_Key>
    const typename TimeSeriesDictInput_T<T_Key>::key_type &TimeSeriesDictInput_T<T_Key>::key_from_value(value_type value) const {
        return key_from_value(value.get());
    }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::reset_prev() { _prev_output = nullptr; }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::clear_key_changes() {
        _clear_key_changes_registered = false;

        // Guard against cleared node (matches Python: if self.owning_node is None)
        if (!has_parent_or_node()) { return; }

        // Release instances with deferred callback to ensure cleanup happens after all processing
        // This matches Python: add_after_evaluation_notification(lambda b=self._ts_builder, i=v[0]: b.release_instance(i))
        for (const auto &key : key_set_t().removed()) {
            auto it = _removed_values.find(key);
            if (it != _removed_values.end()) {
                auto &[value, was_valid] = it->second;
                // Capture by value to ensure the lambda has valid references
                auto builder  = _ts_builder;
                auto instance = value;
                owning_graph().evaluation_engine_api().add_after_evaluation_notification(
                    [builder, instance]() { builder->release_instance(instance); });
                value->un_bind_output(true);  // unbind_refs=True
            }
        }

        _removed_values.clear();
    }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::register_clear_key_changes() const {
        // This has side effects, but they are not directly impacting the behaviour of the class
        const_cast<TimeSeriesDictInput_T *>(this)->register_clear_key_changes();
    }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::register_clear_key_changes() {
        if (!_clear_key_changes_registered) {
            _clear_key_changes_registered = true;
            owning_graph().evaluation_engine_api().add_after_evaluation_notification([this]() { clear_key_changes(); });
        }
    }

    template <typename T_Key>
    TimeSeriesDictInput_T<T_Key>::value_type TimeSeriesDictInput_T<T_Key>::get_or_create(const key_type &key) {
        if (!_ts_values.contains(key)) { _create(key); }
        return _ts_values[key];
    }

    template <typename T_Key> nb::object TimeSeriesDictInput_T<T_Key>::py_get_or_create(const nb::object &key) {
        auto ts = get_or_create(nb::cast<T_Key>(key));
        return nb::cast(ts.get());
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::is_same_type(const TimeSeriesType *other) const {
        auto other_d = dynamic_cast<const TimeSeriesDictInput_T<key_type> *>(other);
        if (!other_d) { return false; }
        return _ts_builder->is_same_type(*other_d->_ts_builder);
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::has_reference() const { return _ts_builder->has_reference(); }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::make_active() {
        if (has_peer()) {
            TimeSeriesDictInput::make_active();
            // Reactivate transplanted inputs that might have been deactivated in make_passive()
            // This is an approximate solution but at this point the information about active state is lost
            for (auto &[_, value] : _ts_values) {
                // Check if this input was transplanted from another parent
                if (value->parent_input().get() != this) { value->make_active(); }
            }
        } else {
            set_active(true);
            key_set().make_active();
            for (auto &[_, value] : _ts_values) { value->make_active(); }
        }
    }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::make_passive() {
        if (has_peer()) {
            TimeSeriesDictInput::make_passive();
        } else {
            set_active(false);
            key_set().make_passive();
            for (auto &[_, value] : _ts_values) { value->make_passive(); }
        }
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::modified() const {
        if (has_peer()) { return TimeSeriesDictInput::modified(); }
        if (active()) {
            auto et{owning_graph().evaluation_clock().evaluation_time()};
            return _last_modified_time == et || key_set_t().modified() || sample_time() == et;
        }
        return key_set_t().modified() ||
               std::any_of(_ts_values.begin(), _ts_values.end(), [](const auto &pair) { return pair.second->modified(); });
    }

    template <typename T_Key> engine_time_t TimeSeriesDictInput_T<T_Key>::last_modified_time() const {
        if (has_peer()) { return TimeSeriesDictInput::last_modified_time(); }
        if (active()) { return std::max(std::max(_last_modified_time, key_set_t().last_modified_time()), sample_time()); }
        auto max_e{std::max_element(_ts_values.begin(), _ts_values.end(), [](const auto &pair1, const auto &pair2) {
            return pair1.second->last_modified_time() < pair2.second->last_modified_time();
        })};
        return std::max(key_set_t().last_modified_time(), max_e == end() ? MIN_DT : max_e->second->last_modified_time());
    }

    template <typename T_Key>
    void TimeSeriesDictInput_T<T_Key>::notify_parent(TimeSeriesInput *child, engine_time_t modified_time) {
        if (_last_modified_time < modified_time) {
            _last_modified_time = modified_time;
            _modified_items.clear();
        }

        if (child != &key_set_t()) {
            auto it{_ts_values_to_key.find(child)};
            if (it != _ts_values_to_key.end()) {
                _modified_items[it->second] = child;  // Use operator[] instead of insert to ensure update
            }
        }

        TimeSeriesInput::notify_parent(this, modified_time);
    }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::_create(const key_type &key) {
        auto item{_ts_builder->make_instance(this)};
        _ts_values.insert({key, item});
        _ts_values_to_key.insert({item.get(), key});
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::_create(const key_type &key) {
        auto value = _ts_builder->make_instance(this);
        add_added_item(key, value);

        // Schedule cleanup notification only once per evaluation cycle
        auto et = owning_graph().evaluation_clock().evaluation_time();
        if (_last_cleanup_time < et) {
            _last_cleanup_time = et;
            owning_graph().evaluation_engine_api().add_after_evaluation_notification(
                [this]() { clear_on_end_of_evaluation_cycle(); });
        }
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

    // template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::post_modify() { _post_modify(); }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::_post_modify() {
        // key_set_t()._post_modify();
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
            .def("get_or_create", &TimeSeriesDictOutput::py_get_or_create, "key"_a)
            .def("clear", &TimeSeriesDictOutput::clear)
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
            .def(
                "get_ref",
                [](TimeSeriesDictOutput &self, const nb::object &key, const nb::object &requester) {
                    return self.py_get_ref(key, requester.ptr());
                },
                "key"_a, "requester"_a)
            .def(
                "release_ref",
                [](TimeSeriesDictOutput &self, const nb::object &key, const nb::object &requester) {
                    self.py_release_ref(key, requester.ptr());
                },
                "key"_a, "requester"_a)
            .def_prop_ro("key_set", &TimeSeriesDictOutput::py_key_set);

        nb::class_<TimeSeriesDictInput, TimeSeriesInput>(m, "TimeSeriesDictInput")
            .def("__contains__", &TimeSeriesDictInput::py_contains, "key"_a)
            .def("__getitem__", &TimeSeriesDictInput::py_get_item, "key"_a)
            .def(
                "get",
                [](TimeSeriesDictInput &self, const nb::object &key, const nb::object &default_value) {
                    return self.py_contains(key) ? self.py_get_item(key) : default_value;
                },
                "key"_a, "default"_a = nb::none())
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
            .def_prop_ro(
                "key_set",
                static_cast<const TimeSeriesSet<TimeSeriesDict<TimeSeriesInput>::ts_type> &(TimeSeriesDictInput::*)() const>(
                    &TimeSeriesDictInput::key_set));

        nb::class_<TSD_OUT_Bool, TimeSeriesDictOutput>(m, "TimeSeriesDictOutput_Bool");
        nb::class_<TSD_OUT_Int, TimeSeriesDictOutput>(m, "TimeSeriesDictOutput_Int");
        nb::class_<TSD_OUT_Float, TimeSeriesDictOutput>(m, "TimeSeriesDictOutput_Float");
        nb::class_<TSD_OUT_Date, TimeSeriesDictOutput>(m, "TimeSeriesDictOutput_Date");
        nb::class_<TSD_OUT_DateTime, TimeSeriesDictOutput>(m, "TimeSeriesDictOutput_DateTime");
        nb::class_<TSD_OUT_TimeDelta, TimeSeriesDictOutput>(m, "TimeSeriesDictOutput_TimeDelta");
        nb::class_<TSD_OUT_Object, TimeSeriesDictOutput>(m, "TimeSeriesDictOutput_Object");

        nb::class_<TSD_Bool, TimeSeriesDictInput>(m, "TimeSeriesDictInput_Bool")
            .def("_create", &TSD_Bool::_create)
            .def("on_key_removed", &TSD_Bool::on_key_removed);
        nb::class_<TSD_Int, TimeSeriesDictInput>(m, "TimeSeriesDictInput_Int")
            .def("_create", &TSD_Int::_create)
            .def("on_key_removed", &TSD_Int::on_key_removed);
        nb::class_<TSD_Float, TimeSeriesDictInput>(m, "TimeSeriesDictInput_Float")
            .def("_create", &TSD_Float::_create)
            .def("on_key_removed", &TSD_Float::on_key_removed);
        nb::class_<TSD_Date, TimeSeriesDictInput>(m, "TimeSeriesDictInput_Date")
            .def("_create", &TSD_Date::_create)
            .def("on_key_removed", &TSD_Date::on_key_removed);
        nb::class_<TSD_DateTime, TimeSeriesDictInput>(m, "TimeSeriesDictInput_DateTime")
            .def("_create", &TSD_DateTime::_create)
            .def("on_key_removed", &TSD_DateTime::on_key_removed);
        nb::class_<TSD_TimeDelta, TimeSeriesDictInput>(m, "TimeSeriesDictInput_TimeDelta")
            .def("_create", &TSD_TimeDelta::_create)
            .def("on_key_removed", &TSD_TimeDelta::on_key_removed);
        nb::class_<TSD_Object, TimeSeriesDictInput>(m, "TimeSeriesDictInput_Object")
            .def("_create", &TSD_Object::_create)
            .def("on_key_removed", &TSD_Object::on_key_removed);
    }
}  // namespace hgraph
