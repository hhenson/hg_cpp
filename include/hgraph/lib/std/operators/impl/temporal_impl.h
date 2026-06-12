#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_TEMPORAL_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_TEMPORAL_IMPL_H

#include <hgraph/lib/std/operators/temporal.h>
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
            out.set(ts.valid());
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

    inline void register_temporal_operators()
    {
        register_overload<day_of_month, day_of_month_impl>();
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

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_TEMPORAL_IMPL_H
