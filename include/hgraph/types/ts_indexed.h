//
// Created by Howard Henson on 03/05/2025.
//

#ifndef TS_INDEXED_H
#define TS_INDEXED_H

#include <hgraph/types/time_series_type.h>

namespace hgraph
{
 template <typename T_TS>
    concept TimeSeries = std::is_same_v<T_TS, TimeSeriesInput> || std::is_same_v<T_TS, TimeSeriesOutput>;

    template <typename T_TS>
        requires TimeSeries<T_TS>
    struct IndexedTimeSeries : T_TS
    {
        using ptr                        = nb::ref<IndexedTimeSeries<T_TS>>;
        using collection_type            = std::vector<typename T_TS::ptr>;
        using enumerated_collection_type = std::vector<std::pair<size_t, typename T_TS::ptr>>;
        using index_collection_type      = std::vector<size_t>;
        using value_iterator             = typename collection_type::iterator;
        using value_const_iterator       = typename collection_type::const_iterator;

        using T_TS::T_TS;
        using T_TS::valid;

        [[nodiscard]] bool all_valid() const override {
            return valid() && std::ranges::all_of(_ts_values, [](const auto &ts) { return ts->valid(); });
        }

        [[nodiscard]] typename T_TS::ptr       &operator[](size_t ndx) { return _ts_values.at(ndx); }
        [[nodiscard]] const typename T_TS::ptr &operator[](size_t ndx) const {
            return const_cast<IndexedTimeSeries<T_TS> *>(this)->operator[](ndx);
        }

        [[nodiscard]] collection_type values() { return _ts_values; }
        [[nodiscard]] collection_type values() const { return const_cast<IndexedTimeSeries<T_TS> *>(this)->values(); }

        [[nodiscard]] collection_type valid_values() {
            return values_with_constraint([](const T_TS &ts) { return ts.valid(); });
        }
        [[nodiscard]] collection_type valid_values() const { return const_cast<IndexedTimeSeries<T_TS> *>(this)->valid_values(); }

        [[nodiscard]] collection_type modified_values() {
            return values_with_constraint([](const T_TS &ts) { return ts.modified(); });
        }
        [[nodiscard]] collection_type modified_values() const {
            return const_cast<IndexedTimeSeries<T_TS> *>(this)->modified_values();
        }

        [[nodiscard]] size_t size() const { return _ts_values.size(); }

      protected:
        [[nodiscard]] collection_type       &ts_values() { return _ts_values; }
        [[nodiscard]] const collection_type &ts_values() const { return _ts_values; }

        void set_ts_values(collection_type ts_values) { _ts_values = std::move(ts_values); }

        [[nodiscard]] index_collection_type index_with_constraint(const std::function<bool(const T_TS &)> &constraint) const {
            index_collection_type result;
            result.reserve(_ts_values.size());
            for (size_t i = 0; i < _ts_values.size(); ++i) {
                if (constraint(*_ts_values[i])) { result.push_back(i); }
            }
            return result;
        }

        [[nodiscard]] collection_type values_with_constraint(const std::function<bool(const T_TS &)> &constraint) const {
            collection_type result;
            result.reserve(_ts_values.size());
            for (const auto &value : _ts_values) {
                if (constraint(*value)) { result.push_back(value); }
            }
            return result;
        }

        [[nodiscard]] enumerated_collection_type items_with_constraint(const std::function<bool(const T_TS &)> &constraint) const {
            enumerated_collection_type result;
            result.reserve(_ts_values.size());
            for (size_t i = 0; i < _ts_values.size(); ++i) {
                if (constraint(*_ts_values[i])) { result.emplace_back(i, _ts_values[i]); }
            }
            return result;
        }

      private:
        collection_type _ts_values;
    };

    struct IndexedTimeSeriesOutput : IndexedTimeSeries<TimeSeriesOutput>
    {
        using IndexedTimeSeries<TimeSeriesOutput>::IndexedTimeSeries;

        void invalidate() override;

        void copy_from_output(TimeSeriesOutput &output) override;

        void copy_from_input(TimeSeriesInput &input) override;

        void clear() override;

        static void register_with_nanobind(nb::module_ &m);
    };

    struct IndexedTimeSeriesInput : IndexedTimeSeries<TimeSeriesInput>
    {
        using IndexedTimeSeries<TimeSeriesInput>::IndexedTimeSeries;

        static void register_with_nanobind(nb::module_ &m);
    };
}

#endif //TS_INDEXED_H
