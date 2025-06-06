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
        using TimeSeriesInput::TimeSeriesInput;

        [[nodiscard]] nb::object py_value() const override;

        [[nodiscard]] nb::object py_delta_value() const override;

        static void register_with_nanobind(nb::module_ &m);
    };

}

#endif //TS_SIGNAL_H
