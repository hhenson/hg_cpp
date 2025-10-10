//
// Created by Howard Henson on 06/06/2025.
//

#ifndef TS_SIGNAL_H
#define TS_SIGNAL_H

#include <hgraph/types/time_series_type.h>

namespace hgraph
{


    struct TimeSeriesSignalInput : TimeSeriesInput
    {
        using ptr = nb::ref<TimeSeriesSignalInput>;
        using TimeSeriesInput::TimeSeriesInput;

        [[nodiscard]] nb::object py_value() const override;

        [[nodiscard]] nb::object py_delta_value() const override;

        [[nodiscard]] bool is_same_type(TimeSeriesType &other) const override { return true; }

        // Support for indexing - creates child signals on demand
        // This allows SIGNAL inputs to be bound to indexed outputs like TSB
        TimeSeriesInput::ptr operator[](size_t index);

        static void register_with_nanobind(nb::module_ &m);

    private:
        mutable std::vector<ptr> _ts_values;  // Lazily created child signals
    };

}

#endif //TS_SIGNAL_H
