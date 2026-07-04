#include <hgraph/lib/std/component.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/record_replay.h>

#include <catch2/catch_test_macros.hpp>

// Step 5 of the record/replay/table design record: component<G> - Python's
// @component as a wiring function over the mode scope + the record/replay
// operators (whatever backend the active model selects).

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;
    using record_replay::Mode;

    struct SumGraph
    {
        [[maybe_unused]] static constexpr auto name = "component_sum_graph";

        static Port<TS<Int>> compose(Wiring &w, NamedPort<"lhs", TS<Int>> lhs, NamedPort<"rhs", TS<Int>> rhs)
        {
            return wire<stdlib::add_>(w, lhs, rhs).as<TS<Int>>();
        }
    };

    struct RecordingHarness
    {
        [[maybe_unused]] static constexpr auto name = "component_record_harness";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            record_replay::scope mode{Mode::Record};
            return stdlib::component<SumGraph>(w, "calc", lhs, rhs);
        }
    };

    struct ReplayHarness
    {
        [[maybe_unused]] static constexpr auto name = "component_replay_harness";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            // Live inputs are wired but REPLACED by the recordings.
            record_replay::scope mode{Mode::Replay};
            return stdlib::component<SumGraph>(w, "calc", lhs, rhs);
        }
    };

    struct ReplayOutputHarness
    {
        [[maybe_unused]] static constexpr auto name = "component_replay_output_harness";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            record_replay::scope mode{Mode::ReplayOutput};
            return stdlib::component<SumGraph>(w, "calc", lhs, rhs);
        }
    };

    struct InnerDouble
    {
        [[maybe_unused]] static constexpr auto name = "component_inner_double";

        static Port<TS<Int>> compose(Wiring &w, NamedPort<"ts", TS<Int>> ts)
        {
            return wire<stdlib::add_>(w, ts, ts).as<TS<Int>>();
        }
    };

    struct OuterGraph
    {
        [[maybe_unused]] static constexpr auto name = "component_outer_graph";

        static Port<TS<Int>> compose(Wiring &w, NamedPort<"ts", TS<Int>> ts)
        {
            return stdlib::component<InnerDouble>(w, "inner", Port<TS<Int>>{ts});
        }
    };

    struct NestedHarness
    {
        [[maybe_unused]] static constexpr auto name = "component_nested_harness";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            record_replay::scope mode{Mode::Record};
            return stdlib::component<OuterGraph>(w, "outer", ts);
        }
    };

    struct RecoverHarness
    {
        [[maybe_unused]] static constexpr auto name = "component_recover_harness";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            record_replay::scope mode{Mode::Recover};
            return stdlib::component<SumGraph>(w, "calc", lhs, rhs);
        }
    };

    struct NoIdHarness
    {
        [[maybe_unused]] static constexpr auto name = "component_no_id_harness";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            record_replay::scope mode{Mode::Record};   // no id anywhere
            return stdlib::component<SumGraph>(w, "", lhs, rhs);
        }
    };

    struct PlainHarness
    {
        [[maybe_unused]] static constexpr auto name = "component_plain_harness";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            // No ambient mode: the component is a plain wire<G>.
            return stdlib::component<SumGraph>(w, "calc", lhs, rhs);
        }
    };
}  // namespace

TEST_CASE("component: Record mode records the named inputs and __out__ while passing values through")
{
    stdlib::register_standard_operators();
    record_replay::set_config(record_replay::Config{.model = std::string{record_replay::DATA_FRAME}});

    CHECK_OUTPUT(eval_node<RecordingHarness>(values<Int>(1, none, 3), values<Int>(10, 20, none)),
                 values<Int>(11, 21, 23));

    CHECK(record_replay::store_contains("calc.lhs"));
    CHECK(record_replay::store_contains("calc.rhs"));
    CHECK(record_replay::store_contains("calc.__out__"));
    CHECK(frame_rows(record_replay::store_read("calc.lhs")) == 2);
    CHECK(frame_rows(record_replay::store_read("calc.__out__")) == 3);
}

TEST_CASE("component: Replay mode replaces live inputs with the recordings")
{
    stdlib::register_standard_operators();
    record_replay::set_config(record_replay::Config{.model = std::string{record_replay::DATA_FRAME}});

    (void)eval_node<RecordingHarness>(values<Int>(1, none, 3), values<Int>(10, 20, none));

    // Different live inputs - the recorded ones win.
    CHECK_OUTPUT(eval_node<ReplayHarness>(values<Int>(100, 100, 100), values<Int>(100, 100, 100)),
                 values<Int>(11, 21, 23));
}

TEST_CASE("component: ReplayOutput mode replays the recorded output directly")
{
    stdlib::register_standard_operators();
    record_replay::set_config(record_replay::Config{.model = std::string{record_replay::DATA_FRAME}});

    (void)eval_node<RecordingHarness>(values<Int>(1, none, 3), values<Int>(10, 20, none));

    CHECK_OUTPUT(eval_node<ReplayOutputHarness>(values<Int>(100), values<Int>(100)),
                 values<Int>(11, 21, 23));
}

TEST_CASE("component: nested components chain fully-qualified ids through the scope")
{
    stdlib::register_standard_operators();
    record_replay::set_config(record_replay::Config{.model = std::string{record_replay::DATA_FRAME}});

    CHECK_OUTPUT(eval_node<NestedHarness>(values<Int>(5)), values<Int>(10));
    CHECK(record_replay::store_contains("outer.ts"));
    CHECK(record_replay::store_contains("outer.__out__"));
    CHECK(record_replay::store_contains("outer.inner.ts"));
    CHECK(record_replay::store_contains("outer.inner.__out__"));
}

TEST_CASE("component: no ambient mode wires plainly; active mode without an id throws")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<PlainHarness>(values<Int>(2), values<Int>(3)), values<Int>(5));
    CHECK_THROWS_AS((void)eval_node<NoIdHarness>(values<Int>(1), values<Int>(1)), std::invalid_argument);
}

TEST_CASE("component: Recover seeds inputs at start from the recordings; live ticks override")
{
    stdlib::register_standard_operators();
    record_replay::set_config(record_replay::Config{.model = std::string{record_replay::DATA_FRAME}});

    // Record: lhs ticks 1 @t0 and 3 @t2; rhs ticks 10 @t0 and 20 @t1.
    (void)eval_node<RecordingHarness>(values<Int>(1, none, 3), values<Int>(10, 20, none));

    // Recover (same start time): the seeds are the values recorded AT OR
    // BEFORE the start time - lhs=1, rhs=10 - so cycle 0 computes 11 with
    // SILENT live inputs; the live lhs=100 @t1 then overrides (100+10).
    CHECK_OUTPUT(eval_node<RecoverHarness>(values<Int>(none, 100), values<Int>(none, none)),
                 values<Int>(11, 110));
}
