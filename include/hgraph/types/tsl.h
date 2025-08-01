//
// Created by Howard Henson on 02/05/2025.
//

#ifndef TSL_H
#define TSL_H


#include <hgraph/types/ts_indexed.h>

namespace hgraph
{

    template <typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    struct TimeSeriesList : T_TS
    {
        using list_type = TimeSeriesList<T_TS>;
        using ptr       = nb::ref<list_type>;
        using typename T_TS::index_ts_type;
        using typename T_TS::ts_type;

        using value_iterator             = typename T_TS::value_iterator;
        using value_const_iterator       = typename T_TS::value_const_iterator;
        using collection_type            = typename T_TS::collection_type;
        using enumerated_collection_type = typename T_TS::enumerated_collection_type;
        using index_collection_type      = typename T_TS::index_collection_type;

        using index_ts_type::size;
        using T_TS::T_TS;

        virtual nb::object py_value() const override {
            nb::list result;
            for (const auto &ts : this->ts_values()) {
                if (ts->valid()) {
                    result.append(ts->py_value());
                } else {
                    result.append(nb::none());
                }
            }
            return nb::tuple(result);
        }
        virtual nb::object py_delta_value() const override {
            nb::dict result;
            for (auto &[ndx, ts] : modified_items()) { result[nb::cast(ndx)] = ts->py_delta_value(); }
            return result;
        }

        value_iterator       begin() { return ts_values().begin(); }
        value_const_iterator begin() const { return const_cast<list_type *>(this)->begin(); }
        value_iterator       end() { return ts_values().end(); }
        value_const_iterator end() const { return const_cast<list_type *>(this)->end(); }

        // Retrieves valid keys
        [[nodiscard]] index_collection_type keys() const {
            index_collection_type result;
            result.reserve(size());
            for (size_t i = 0; i < size(); ++i) { result.push_back(i); }
            return result;
        }
        [[nodiscard]] index_collection_type valid_keys() const {
            return index_with_constraint([](const ts_type &ts) { return ts.valid(); });
        }
        [[nodiscard]] index_collection_type modified_keys() const {
            return index_with_constraint([](const ts_type &ts) { return ts.modified(); });
        }

        // Retrieves valid items
        [[nodiscard]] enumerated_collection_type items() {
            enumerated_collection_type result;
            result.reserve(size());
            for (size_t i = 0; i < size(); ++i) { result.push_back({i, ts_values()[i]}); }
            return result;
        }
        [[nodiscard]] enumerated_collection_type items() const {
            return const_cast<list_type *>(this)->items();
        }
        [[nodiscard]] enumerated_collection_type valid_items() {
            return this->items_with_constraint([](const ts_type &ts) { return ts.valid(); });
        }
        [[nodiscard]] enumerated_collection_type valid_items() const {
            return const_cast<list_type *>(this)->valid_items();
        }
        [[nodiscard]] enumerated_collection_type modified_items() {
            return this->items_with_constraint([](const ts_type &ts) { return ts.modified(); });
        }
        [[nodiscard]] enumerated_collection_type modified_items() const {
            return const_cast<list_type *>(this)->modified_items();
        }

      protected:
        using T_TS::index_with_constraint;
        using T_TS::ts_values;
    };

    struct TimeSeriesListOutputBuilder;
    struct TimeSeriesListOutput : TimeSeriesList<IndexedTimeSeriesOutput>
    {
        using list_type::TimeSeriesList;

        void apply_result(nb::object value) override;

        static void register_with_nanobind(nb::module_ &m);
    protected:
        friend TimeSeriesListOutputBuilder;
    };

    struct TimeSeriesListInputBuilder;
    struct TimeSeriesListInput : TimeSeriesList<IndexedTimeSeriesInput>
    {
        using list_type::TimeSeriesList;

        static void register_with_nanobind(nb::module_ &m);
    protected:
        friend TimeSeriesListInputBuilder;
    };
}  // namespace hgraph
#endif  // TSL_H
