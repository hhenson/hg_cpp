//
// Created by Howard Henson on 03/04/2021.
//

#ifndef HGRAPH_DATE_TIME_H
#define HGRAPH_DATE_TIME_H

#include <chrono>

namespace hgraph
{

    using engine_clock        = std::chrono::system_clock;
    using engine_time_t       = engine_clock::time_point;
    using engine_time_delta_t = std::chrono::microseconds;

    constexpr engine_time_t min_time() noexcept { return engine_time_t{}; }
    constexpr engine_time_t max_time() noexcept {
        auto specific_date = std::chrono::year(2300) / std::chrono::January / std::chrono::day(1);
        return std::chrono::sys_days(specific_date);
    }
    constexpr engine_time_delta_t smallest_time_increment() noexcept { return engine_time_delta_t(1); }
    constexpr engine_time_t       min_start_time() noexcept { return min_time() + smallest_time_increment(); }
    constexpr engine_time_t       max_end_time() noexcept { return max_time() - smallest_time_increment(); }

    inline auto static MIN_DT = min_time();
    inline auto static MAX_DT = max_time();
    inline auto static MIN_ST = min_start_time();
    inline auto static MAX_ET = max_end_time();

    inline auto static MIN_TD = smallest_time_increment();

    using engine_date_t = std::chrono::year_month_day;

}  // namespace hgraph
#endif  // HGRAPH_DATE_TIME_H
