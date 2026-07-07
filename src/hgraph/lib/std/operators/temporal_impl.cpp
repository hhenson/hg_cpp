#include <hgraph/lib/std/operators/impl/temporal_impl.h>

#include <hgraph/lib/std/operators/arithmetic.h>   // sub_ (date - timedelta)

namespace hgraph::stdlib
{
    void register_temporal_operators()
    {
        register_overload<day_of_month, day_of_month_impl>();
        register_overload<day, day_of_month_impl>();
        register_overload<sub_, sub_date_timedelta_impl>();
        register_overload<isoformat, isoformat_impl>();
        register_overload<evaluation_time_in_range, evaluation_time_in_range_datetime_impl>();
        register_overload<evaluation_time_in_range, evaluation_time_in_range_date_impl>();
        register_overload<evaluation_time_in_range, evaluation_time_in_range_time_impl>();
        register_overload<month, month_of_year_impl>();
        register_overload<weekday, weekday_impl>();
        register_overload<isoweekday, isoweekday_impl>();
        register_overload<month_of_year, month_of_year_impl>();
        register_overload<year, year_impl>();
        register_overload<explode, explode_date_impl>();
        register_overload<valid, valid_impl>();
        register_overload<modified, modified_impl>();
        register_overload<last_modified_time, last_modified_time_impl>();
        register_overload<last_modified_wall_clock_time, last_modified_wall_clock_time_impl>();
        register_overload<last_modified_date, last_modified_date_impl>();
    }
}  // namespace hgraph::stdlib
