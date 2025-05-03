//
// Created by Howard Henson on 02/05/2025.
//

#ifndef TSL_H
#define TSL_H
#include "time_series_type.h"

namespace hgraph
{
    struct TimeSeriesListOutput : IndexedTimeSeriesOutput
    {
        using IndexedTimeSeriesOutput::IndexedTimeSeriesOutput;
        using key_collection_type     = std::vector<size_t>;
        using key_iterator            = key_collection_type::iterator;
        using key_const_iterator      = key_collection_type::const_iterator;
        using key_value_collection_type = std::vector<std::pair<size_t, time_series_output_ptr>>;

        virtual nb::object py_value() const override;
        virtual nb::object py_delta_value() const override;

        virtual void apply_result(nb::handle value) override;

        virtual iterator       begin();
        virtual const_iterator begin() const;
        virtual iterator       end();
        virtual const_iterator end() const;

        // Retrieves valid keys
        [[nodiscard]] key_collection_type keys() const;
        [[nodiscard]] key_collection_type valid_keys() const;
        [[nodiscard]] key_collection_type modified_keys() const;

        // Retrieves valid items
        [[nodiscard]] key_value_collection_type items();
        [[nodiscard]] key_value_collection_type items() const;
        [[nodiscard]] key_value_collection_type valid_items();
        [[nodiscard]] key_value_collection_type valid_items() const;
        [[nodiscard]] key_value_collection_type modified_items();
        [[nodiscard]] key_value_collection_type modified_items() const;

        static void register_with_nanobind(nb::module_ &m);
    };
}
#endif //TSL_H
