//
// Created by Howard Henson on 03/04/2021.
//

#ifndef HGRAPH_DATE_TIME_H
#define HGRAPH_DATE_TIME_H

#include <chrono>

namespace std {
    /** ``std::hash`` specialisation for ``std::chrono::time_point``. */
    template<class Clock, class Duration>
    struct hash<std::chrono::time_point<Clock, Duration> > {
        size_t operator()(const std::chrono::time_point<Clock, Duration> &tp) const noexcept {
            return std::hash<typename Duration::rep>()(tp.time_since_epoch().count());
        }
    };

    /** ``std::hash`` specialisation for ``std::chrono::duration``. */
    template<class Rep, class Period>
    struct hash<std::chrono::duration<Rep, Period> > {
        size_t operator()(const std::chrono::duration<Rep, Period> &d) const noexcept {
            return std::hash<Rep>()(d.count());
        }
    };


    /** ``std::hash`` specialisation for ``std::chrono::year_month_day``. */
    template<>
    struct hash<std::chrono::year_month_day> {
        size_t operator()(const std::chrono::year_month_day &ymd) const noexcept {
            size_t h1 = std::hash<int>{}(static_cast<int>(ymd.year()));
            size_t h2 = std::hash<unsigned>{}(static_cast<unsigned>(ymd.month()));
            size_t h3 = std::hash<unsigned>{}(static_cast<unsigned>(ymd.day()));
            // Combine hashes (boost::hash_combine-like)
            return h1 ^ (h2 << 1) ^ (h3 << 2);
        }
    };
} // namespace std

namespace hgraph {
    /** Reference clock used by the runtime; aliases ``std::chrono::system_clock``. */
    using engine_clock = std::chrono::system_clock;

    /**
     * Engine time point with microsecond resolution.
     *
     * Microseconds are used (rather than the platform default for
     * ``system_clock``) so the representable range exceeds 2262 — the limit
     * that nanosecond precision would impose with a 64-bit count.
     */
    using engine_time_t = std::chrono::time_point<engine_clock, std::chrono::microseconds>;

    /** Engine time-delta with the same microsecond resolution as ``engine_time_t``. */
    using engine_time_delta_t = std::chrono::microseconds;

    /** The earliest representable engine time. Used as the never-modified sentinel. */
    constexpr engine_time_t min_time() noexcept { return engine_time_t{}; }

    /**
     * The latest representable engine time, clamped to the smaller of:
     *
     * - the desired logical cap (2300-01-01 00:00:00), and
     * - the largest whole-day time point representable by ``engine_clock``.
     *
     * Some platforms (notably libstdc++ on Linux) define
     * ``system_clock::duration`` at nanosecond resolution, which limits 64-bit
     * timestamps to roughly 2262-04-11. The clamp keeps behaviour consistent
     * across platforms and avoids overflow into a negative timestamp.
     */
    inline engine_time_t max_time() noexcept {
        using namespace std::chrono;
        // Desired logical cap
        const sys_days desired_cap = year(2300) / January / day(1);
        // Compute the largest whole-day that can be represented without overflow
        const auto max_whole_day = floor<days>(engine_time_t::max());
        // Pick the earlier of the two
        const sys_days chosen_day = (desired_cap <= max_whole_day) ? desired_cap : max_whole_day;
        // Convert back to the engine_time_t representation at midnight of the chosen day
        return engine_time_t{chosen_day.time_since_epoch()};
    }

    /** Smallest representable time-delta in engine time (1 microsecond). */
    constexpr engine_time_delta_t smallest_time_increment() noexcept { return engine_time_delta_t(1); }

    /** Earliest start time accepted by the runtime (``min_time()`` plus one tick). */
    constexpr engine_time_t min_start_time() noexcept { return min_time() + smallest_time_increment(); }

    /** Latest end time accepted by the runtime (``max_time()`` minus one tick). */
    constexpr engine_time_t max_end_time() noexcept { return max_time() - smallest_time_increment(); }

    /** Sentinel for the never-modified engine time; equal to ``min_time()``. */
    inline auto static MIN_DT = min_time();
    /** Latest representable engine time; equal to ``max_time()``. */
    inline auto static MAX_DT = max_time();
    /** Earliest valid start time; equal to ``min_start_time()``. */
    inline auto static MIN_ST = min_start_time();
    /** Latest valid end time; equal to ``max_end_time()``. */
    inline auto static MAX_ET = max_end_time();

    /** Smallest engine-time delta; equal to ``smallest_time_increment()``. */
    inline auto static MIN_TD = smallest_time_increment();

    /** Calendar date alias used by the runtime. */
    using engine_date_t = std::chrono::year_month_day;
} // namespace hgraph
#endif  // HGRAPH_DATE_TIME_H
