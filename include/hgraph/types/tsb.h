//
// Created by Howard Henson on 27/12/2024.
//

#ifndef TSB_H
#define TSB_H
#include <hgraph/types/time_series_type.h>

namespace hgraph
{

    // struct TimeSeriesBundle
    // {
    //
    //     // Returns the size of the time-series bundle
    //     size_t size() const { return _ts_values.size(); }
    //
    //     // Accesses time-series value for the given key
    //     TimeSeriesType &operator[](const std::string &key) {
    //         auto it = _ts_values.find(key);
    //         if (it != _ts_values.end()) { return it->second; }
    //         throw std::out_of_range("Key not found in time-series values.");
    //     }
    //
    //     // Accesses time-series value for a sequence index
    //     TimeSeriesType &operator[](size_t index) {
    //         if (index < _schema_meta_data.size()) {
    //             auto schema_key = std::next(_schema_meta_data.cbegin(), index)->first;
    //             return _ts_values.at(schema_key);
    //         }
    //         throw std::out_of_range("Index out of range of the schema.");
    //     }
    //
    //     // Finds the first key for a matching value
    //     std::string key_from_value(const TimeSeriesType &value) const {
    //         auto it = std::find_if(_ts_values.cbegin(), _ts_values.cend(), [&](const auto &pair) { return pair.second == value; });
    //         if (it != _ts_values.cend()) { return it->first; }
    //         return "";
    //     }
    //
    //     // Retrieves keys of the schema
    //     std::vector<std::string> keys() const {
    //         std::vector<std::string> result;
    //         for (const auto &[key, _] : _ts_values) { result.push_back(key); }
    //         return result;
    //     }
    //
    //     // Retrieves items of the bundle as key/value pairs
    //     std::vector<std::pair<std::string, TimeSeriesType>> items() const { return {_ts_values.begin(), _ts_values.end()}; }
    //
    //     // Retrieves values of the bundle
    //     std::vector<TimeSeriesType> values() const {
    //         std::vector<TimeSeriesType> result;
    //         for (const auto &[_, value] : _ts_values) { result.push_back(value); }
    //         return result;
    //     }
    //
    //     // Retrieves modified keys
    //     std::vector<std::string> modified_keys() const {
    //         std::vector<std::string> result;
    //         for (const auto &[key, value] : _ts_values) {
    //             if (value.modified) { result.push_back(key); }
    //         }
    //         return result;
    //     }
    //
    //     // Retrieves modified values
    //     std::vector<TimeSeriesType> modified_values() const {
    //         std::vector<TimeSeriesType> result;
    //         for (const auto &[_, value] : _ts_values) {
    //             if (value.modified) { result.push_back(value); }
    //         }
    //         return result;
    //     }
    //
    //     // Retrieves modified items
    //     std::vector<std::pair<std::string, TimeSeriesType>> modified_items() const {
    //         std::vector<std::pair<std::string, TimeSeriesType>> result;
    //         for (const auto &pair : _ts_values) {
    //             if (pair.second.modified) { result.push_back(pair); }
    //         }
    //         return result;
    //     }
    //
    //     // Retrieves valid keys
    //     std::vector<std::string> valid_keys() const {
    //         std::vector<std::string> result;
    //         for (const auto &[key, value] : _ts_values) {
    //             if (value.valid) { result.push_back(key); }
    //         }
    //         return result;
    //     }
    //
    //     // Retrieves valid values
    //     std::vector<TimeSeriesType> valid_values() const {
    //         std::vector<TimeSeriesType> result;
    //         for (const auto &[_, value] : _ts_values) {
    //             if (value.valid) { result.push_back(value); }
    //         }
    //         return result;
    //     }
    //
    //     // Retrieves valid items
    //     std::vector<std::pair<std::string, TimeSeries>> valid_items() const {
    //         std::vector<std::pair<std::string, TimeSeries>> result;
    //         for (const auto &pair : _ts_values) {
    //             if (pair.second.valid) { result.push_back(pair); }
    //         }
    //         return result;
    //     }
    //
    //   private:
    //     std::unordered_map<std::string, TimeSeries> _ts_values;
    //     std::unordered_map<std::string, MetaData>   _schema_meta_data;
    // };

    struct TimeSeriesSchema
    {
        using ptr = nb::ref<TimeSeriesSchema>;

        const std::vector<std::string> &keys() const;
    };

    struct TimeSeriesBundleOutput : TimeSeriesOutput
    {

        // Retrieves valid keys
        std::vector<std::string> valid_keys() const {
            std::vector<std::string> result;
            for (size_t i=0, l=_ts_values.size(); i<l; i++) {
                auto &ts{_ts_values[i]};
                if (ts->valid()) { result.push_back(_schema->keys()[i]); }
            }
            return result;
        }

        // Retrieves valid values
        std::vector<time_series_output_ptr> valid_values() const {
            std::vector<time_series_output_ptr> result;
            for (size_t i=0, l=_ts_values.size(); i<l; i++) {
                auto &ts{_ts_values[i]};
                if (ts->valid()) { result.push_back(ts); }
            }
            return result;
        }

        // Retrieves valid items
        std::vector<std::pair<std::string, time_series_output_ptr>> valid_items() const {
            std::vector<std::pair<std::string, time_series_output_ptr>> result;
            for (size_t i=0, l=_ts_values.size(); i<l; i++) {
                auto &ts{_ts_values[i]};
                if (ts->valid()) { result.push_back({_schema->keys()[i], ts}); }
            }
            return result;
        }

    private:
        TimeSeriesSchema::ptr _schema;
        std::vector<time_series_output_ptr> _ts_values;
    };

    struct TimeSeriesBundleInput : TimeSeriesInput
    {

    };
}

#endif //TSB_H
