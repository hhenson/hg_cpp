#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_TEMPORAL_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_TEMPORAL_IMPL_H

#include <hgraph/lib/std/operators/temporal.h>

#include <fmt/format.h>
#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/util/date_time.h>

#include <chrono>
#include <cstddef>
#include <string>

namespace hgraph::stdlib
{
    struct day_of_month_impl
    {
        static void eval(In<"ts", TS<Date>> ts, Out<TS<Int>> out)
        {
            out.set(static_cast<Int>(static_cast<unsigned>(ts.value().day())));
        }
    };

    struct weekday_impl
    {
        static void eval(In<"ts", TS<Date>> ts, Out<TS<Int>> out)
        {
            // python date.weekday(): Monday == 0.
            const std::chrono::weekday wd{std::chrono::sys_days{ts.value()}};
            out.set(static_cast<Int>(wd.iso_encoding() - 1));
        }
    };

    struct isoweekday_impl
    {
        static void eval(In<"ts", TS<Date>> ts, Out<TS<Int>> out)
        {
            // python date.isoweekday(): Monday == 1.
            const std::chrono::weekday wd{std::chrono::sys_days{ts.value()}};
            out.set(static_cast<Int>(wd.iso_encoding()));
        }
    };

    struct sub_date_timedelta_impl
    {
        static constexpr auto name = "sub_date_timedelta";

        static void eval(In<"lhs", TS<Date>> lhs, In<"rhs", TS<TimeDelta>> rhs, Out<TS<Date>> out)
        {
            const auto shifted = std::chrono::sys_days{lhs.value()} -
                                 std::chrono::floor<std::chrono::days>(rhs.value());
            out.set(Date{std::chrono::year_month_day{std::chrono::floor<std::chrono::days>(shifted)}});
        }
    };

    struct isoformat_impl
    {
        static constexpr auto name = "isoformat";

        static void eval(In<"ts", TS<Date>> ts, Out<TS<Str>> out)
        {
            const auto d = ts.value();
            out.set(fmt::format("{:04}-{:02}-{:02}", static_cast<int>(d.year()),
                                static_cast<unsigned>(d.month()), static_cast<unsigned>(d.day())));
        }
    };

    namespace time_range_detail
    {
        /** Publish + schedule for the [start, end] window at ``et``. */
        inline CmpResult classify_and_schedule(DateTime start, DateTime end, DateTime et,
                                               const NodeScheduler &scheduler)
        {
            if (start > end)
            {
                throw std::invalid_argument("evaluation_time_in_range: start must be before end");
            }
            if (et < start)
            {
                scheduler.schedule(start, "_next");
                return CmpResult::LT;
            }
            if (et <= end)
            {
                scheduler.schedule(end + MIN_TD, "_next");
                return CmpResult::EQ;
            }
            // Past the window: clear any pending boundary wake-up (the end
            // may have moved backwards).
            scheduler.un_schedule("_next");
            return CmpResult::GT;
        }

        [[nodiscard]] inline DateTime date_at(Date date, Time at) noexcept
        {
            return DateTime{std::chrono::sys_days{date}.time_since_epoch() +
                            std::chrono::microseconds{at.microseconds}};
        }
    }  // namespace time_range_detail

    struct evaluation_time_in_range_datetime_impl
    {
        static constexpr auto name = "evaluation_time_in_range_datetime";

        static void eval(In<"start_time", TS<DateTime>> start_time, In<"end_time", TS<DateTime>> end_time,
                         NodeScheduler scheduler, DateTime now, Out<TS<CmpResult>> out)
        {
            out.set(time_range_detail::classify_and_schedule(start_time.value(), end_time.value(), now, scheduler));
        }
    };

    struct evaluation_time_in_range_date_impl
    {
        static constexpr auto name = "evaluation_time_in_range_date";

        static void eval(In<"start_time", TS<Date>> start_time, In<"end_time", TS<Date>> end_time,
                         NodeScheduler scheduler, DateTime now, Out<TS<CmpResult>> out)
        {
            // hgraph: the date window spans [start 00:00:00.000000,
            // end 23:59:59.999999].
            const DateTime start = time_range_detail::date_at(start_time.value(), Time{});
            const DateTime end   = time_range_detail::date_at(end_time.value(), time_of_day(23, 59, 59, 999999));
            out.set(time_range_detail::classify_and_schedule(start, end, now, scheduler));
        }
    };

    struct evaluation_time_in_range_time_impl
    {
        static constexpr auto name = "evaluation_time_in_range_time";

        static void eval(In<"start_time", TS<Time>> start_time, In<"end_time", TS<Time>> end_time,
                         NodeScheduler scheduler, DateTime now, Out<TS<CmpResult>> out)
        {
            // A DAILY-recurring window, possibly wrapping midnight (hgraph's
            // time overload; after the window it reports LT for the NEXT
            // day's window and schedules its start).
            const Date today = Date{std::chrono::floor<std::chrono::days>(now)};
            DateTime   start = time_range_detail::date_at(today, start_time.value());
            DateTime   end   = time_range_detail::date_at(today, end_time.value());
            constexpr auto day = std::chrono::days{1};
            if (start > end)
            {
                if (now < end) { start -= day; }
                else { end += day; }
            }
            if (now < start)
            {
                scheduler.schedule(start, "_next");
                out.set(CmpResult::LT);
            }
            else if (now <= end)
            {
                scheduler.schedule(end + MIN_TD, "_next");
                out.set(CmpResult::EQ);
            }
            else
            {
                scheduler.schedule(start + day, "_next");
                out.set(CmpResult::LT);
            }
        }
    };

    struct month_of_year_impl
    {
        static void eval(In<"ts", TS<Date>> ts, Out<TS<Int>> out)
        {
            out.set(static_cast<Int>(static_cast<unsigned>(ts.value().month())));
        }
    };

    struct year_impl
    {
        static void eval(In<"ts", TS<Date>> ts, Out<TS<Int>> out)
        {
            out.set(static_cast<Int>(static_cast<int>(ts.value().year())));
        }
    };

    struct explode_date_impl
    {
        static void eval(In<"ts", TS<Date>> ts, Out<TSL<TS<Int>, 3>> out)
        {
            const Date value = ts.value();
            set_if_changed(out, 0, static_cast<Int>(static_cast<int>(value.year())));
            set_if_changed(out, 1, static_cast<Int>(static_cast<unsigned>(value.month())));
            set_if_changed(out, 2, static_cast<Int>(static_cast<unsigned>(value.day())));
        }

      private:
        static void set_if_changed(const Out<TSL<TS<Int>, 3>> &out, std::size_t index, Int value)
        {
            auto child = out[index];
            if (!child.valid() || child.value().template checked_as<Int>() != value) { child.set(value); }
        }
    };

    struct valid_impl
    {
        static constexpr bool schedule_on_start = true;

        static void eval(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts, Out<TS<Bool>> out)
        {
            // hgraph parity: ticks only when the answer CHANGES.
            const Bool value = ts.valid();
            const auto &erased = static_cast<const TSOutputView &>(out);
            if (erased.valid() && erased.data_view().value().checked_as<Bool>() == value) { return; }
            out.set(value);
        }
    };

    struct modified_impl
    {
        static constexpr bool schedule_on_start = true;

        static void eval(In<"ts", SIGNAL, InputValidity::Unchecked> ts, NodeScheduler scheduler, Out<TS<Bool>> out)
        {
            const Bool is_modified = ts.modified();
            out.set(is_modified);
            if (is_modified) { scheduler.schedule(MIN_TD, std::string{"reset"}); }
        }
    };

    struct last_modified_time_impl
    {
        static void eval(In<"ts", SIGNAL, InputValidity::Unchecked> ts, Out<TS<DateTime>> out)
        {
            out.set(ts.last_modified_time());
        }
    };

    struct last_modified_wall_clock_time_impl
    {
        static void eval(In<"ts", SIGNAL, InputValidity::Unchecked> ts, EvaluationClockView clock,
                         Out<TS<DateTime>> out)
        {
            static_cast<void>(ts);
            out.set(clock.now());
        }
    };

    struct last_modified_date_impl
    {
        static void eval(In<"ts", SIGNAL, InputValidity::Unchecked> ts, Out<TS<Date>> out)
        {
            out.set(Date{std::chrono::floor<std::chrono::days>(ts.last_modified_time())});
        }
    };

    void register_temporal_operators();
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_TEMPORAL_IMPL_H
