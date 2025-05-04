//
// Created by Howard Henson on 03/04/2021.
//

#ifndef HGRAPH_DATE_TIME_H
#define HGRAPH_DATE_TIME_H

#include <chrono>

namespace std
{

    // Specialization for std::chrono::time_point
    template <class Clock, class Duration> struct hash<std::chrono::time_point<Clock, Duration>>
    {
        size_t operator()(const std::chrono::time_point<Clock, Duration> &tp) const noexcept {
            return std::hash<typename Duration::rep>()(tp.time_since_epoch().count());
        }
    };

    // Specialization for std::chrono::duration
    template <class Rep, class Period> struct hash<std::chrono::duration<Rep, Period>>
    {
        size_t operator()(const std::chrono::duration<Rep, Period> &d) const noexcept { return std::hash<Rep>()(d.count()); }
    };


    template <> struct hash<std::chrono::year_month_day>
    {
        size_t operator()(const std::chrono::year_month_day &ymd) const noexcept {
            size_t h1 = std::hash<int>{}(static_cast<int>(ymd.year()));
            size_t h2 = std::hash<unsigned>{}(static_cast<unsigned>(ymd.month()));
            size_t h3 = std::hash<unsigned>{}(static_cast<unsigned>(ymd.day()));
            // Combine hashes (boost::hash_combine-like)
            return h1 ^ (h2 << 1) ^ (h3 << 2);
        }
    };

}  // namespace std

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
