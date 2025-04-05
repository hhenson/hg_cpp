//
// Created by Howard Henson on 27/12/2024.
//

#ifndef TSB_H
#define TSB_H
#include <hgraph/types/time_series_type.h>

namespace hgraph
{

    struct TimeSeriesSchema
    {
        using ptr = nb::ref<TimeSeriesSchema>;

        const std::vector<std::string> &keys() const;

        static void register_with_nanobind(nb::module_ &m);
    };

    struct TimeSeriesBundleOutput : TimeSeriesOutput
    {

        using TimeSeriesOutput::TimeSeriesOutput;

        // Retrieves valid keys
        std::vector<std::string> valid_keys() const {
            std::vector<std::string> result;
            for (size_t i = 0, l = _ts_values.size(); i < l; i++) {
                auto &ts{_ts_values[i]};
                if (ts->valid()) { result.push_back(_schema->keys()[i]); }
            }
            return result;
        }

        // Retrieves valid values
        std::vector<time_series_output_ptr> valid_values() const {
            std::vector<time_series_output_ptr> result;
            for (size_t i = 0, l = _ts_values.size(); i < l; i++) {
                auto &ts{_ts_values[i]};
                if (ts->valid()) { result.push_back(ts); }
            }
            return result;
        }

        // Retrieves valid items
        std::vector<std::pair<std::string, time_series_output_ptr>> valid_items() const {
            std::vector<std::pair<std::string, time_series_output_ptr>> result;
            for (size_t i = 0, l = _ts_values.size(); i < l; i++) {
                auto &ts{_ts_values[i]};
                if (ts->valid()) { result.push_back({_schema->keys()[i], ts}); }
            }
            return result;
        }

        static void register_with_nanobind(nb::module_ &m);

      private:
        TimeSeriesSchema::ptr               _schema;
        std::vector<time_series_output_ptr> _ts_values;
    };

    struct TimeSeriesBundleInput : TimeSeriesInput
    {
        using TimeSeriesInput::TimeSeriesInput;

        // Define an iterator type for the unordered_map
        using iterator       = std::unordered_map<std::string, TimeSeriesInput::ptr>::iterator;
        using const_iterator = std::unordered_map<std::string, TimeSeriesInput::ptr>::const_iterator;

        // Begin iterator
        iterator       begin();
        const_iterator begin() const;

        // End iterator
        iterator       end();
        const_iterator end() const;

        // Access elements by key
        TimeSeriesInput       &operator[](const std::string &key);
        const TimeSeriesInput &operator[](const std::string &key) const;

        // Check if a key exists
        bool contains(const std::string &key) const;

        // Static method for nanobind registration
        static void register_with_nanobind(nb::module_ &m);

      private:
        // Stores the time-series data
        std::unordered_map<std::string, TimeSeriesInput::ptr> _ts_values;
    };
}  // namespace hgraph

#endif  // TSB_H
