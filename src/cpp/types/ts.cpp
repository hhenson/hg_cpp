
#include <hgraph/types/ts.h>

namespace hgraph
{

    // TODO: How to better track the types we have registered as there is a corresponding item to deal with in the output and input builders.

    void register_ts_with_nanobind(nb::module_ &m) {

        using TS_Bool     = TimeSeriesValueInput<bool>;
        using TS_Out_Bool = TimeSeriesValueOutput<bool>;

        nb::class_<TS_Out_Bool, TimeSeriesOutput>(m, "TS_Out_Bool");
        nb::class_<TS_Bool, TimeSeriesInput>(m, "TS_Bool");

        using TS_Int     = TimeSeriesValueInput<int>;
        using TS_Out_Int  = TimeSeriesValueOutput<int>;

        nb::class_<TS_Out_Int , TimeSeriesOutput>(m, "TS_Out_Int ");
        nb::class_<TS_Int , TimeSeriesInput>(m, "TS_Int ");

        using TS_Float     = TimeSeriesValueInput<float>;
        using TS_Out_Float  = TimeSeriesValueOutput<float>;

        nb::class_<TS_Out_Float , TimeSeriesOutput>(m, "TS_Out_Float ");
        nb::class_<TS_Float , TimeSeriesInput>(m, "TS_Float ");

        using TS_Date     = TimeSeriesValueInput<engine_date_t>;
        using TS_Out_Date  = TimeSeriesValueOutput<engine_date_t>;

        nb::class_<TS_Out_Date , TimeSeriesOutput>(m, "TS_Out_Date ");
        nb::class_<TS_Date , TimeSeriesInput>(m, "TS_Date ");

        using TS_DateTime     = TimeSeriesValueInput<engine_time_t>;
        using TS_Out_DateTime  = TimeSeriesValueOutput<engine_time_t>;

        nb::class_<TS_Out_DateTime , TimeSeriesOutput>(m, "TS_Out_DateTime ");
        nb::class_<TS_DateTime , TimeSeriesInput>(m, "TS_DateTime ");

        using TS_TimeDelta     = TimeSeriesValueInput<engine_time_delta_t>;
        using TS_Out_TimeDelta  = TimeSeriesValueOutput<engine_time_delta_t>;

        nb::class_<TS_Out_TimeDelta , TimeSeriesOutput>(m, "TS_Out_TimeDelta ");
        nb::class_<TS_TimeDelta , TimeSeriesInput>(m, "TS_TimeDelta ");

        // using TS_Time     = TimeSeriesValueInput<engine_time_t>;
        // using TS_Out_Time  = TimeSeriesValueOutput<engine_time_t>;
        //
        // nb::class_<TS_Out_Time , TimeSeriesOutput>(m, "TS_Out_Time ");
        // nb::class_<TS_Time , TimeSeriesInput>(m, "TS_Time ");

        using TS_Object     = TimeSeriesValueInput<nb::object>;
        using TS_Out_Object  = TimeSeriesValueOutput<nb::object>;

        nb::class_<TS_Out_Object , TimeSeriesOutput>(m, "TS_Out_Object ");
        nb::class_<TS_Object , TimeSeriesInput>(m, "TS_Object ");
    }
}  // namespace hgraph