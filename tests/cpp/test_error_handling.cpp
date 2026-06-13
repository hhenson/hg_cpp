#include <catch2/catch_test_macros.hpp>

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/runtime/node_error.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/subgraph_wiring.h>

#include <stdexcept>
#include <string>

using namespace std::string_literals;

namespace
{
    using namespace hgraph;

    // A compute node that throws when its input is negative; otherwise doubles.
    struct ThrowOnNegative
    {
        static constexpr auto name = "throw_on_negative";
        static void           eval(In<"x", TS<Int>> x, Out<TS<Int>> out)
        {
            if (x.value() < 0) { throw std::runtime_error("negative input"); }
            out.set(x.value() * 2);
        }
    };

    // Sub-graph form of the above (for try_except over a graph).
    struct DoublerOrThrowG
    {
        static constexpr auto name = "doubler_or_throw_g";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> x) { return wire<ThrowOnNegative>(w, x); }
    };

    struct ThrowUnknownOnNegative
    {
        static constexpr auto name = "throw_unknown_on_negative";
        static void           eval(In<"x", TS<Int>> x, Out<TS<Int>> out)
        {
            if (x.value() < 0) { throw 7; }
            out.set(x.value() * 3);
        }
    };

    struct TriplerOrUnknownG
    {
        static constexpr auto name = "tripler_or_unknown_g";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> x) { return wire<ThrowUnknownOnNegative>(w, x); }
    };

    // A sink node that throws when its input is negative (no output).
    struct SinkOrThrow
    {
        static constexpr auto name = "sink_or_throw";
        static void           eval(In<"x", TS<Int>> x)
        {
            if (x.value() < 0) { throw std::runtime_error("sink negative"); }
        }
    };

    struct SinkOrThrowG
    {
        static constexpr auto name = "sink_or_throw_g";
        static void           compose(Wiring &w, Port<TS<Int>> x) { wire<SinkOrThrow>(w, x); }
    };

    // Extracts the ``error_msg`` field out of a TS<NodeError> as a string.
    struct ErrorMsgOf
    {
        static constexpr auto name = "error_msg_of";
        static void           eval(In<"e", TS<NodeError>> e, Out<TS<Str>> out)
        {
            out.set(e.base().value().as_bundle().at("error_msg").checked_as<Str>());
        }
    };

    using TryIntResult = UnNamedTSB<Field<"exception", TS<NodeError>>, Field<"out", TS<Int>>>;

    // Splits the try_except TSB result: the ``out`` value when it ticks.
    struct TryOutValue
    {
        static constexpr auto name = "try_out_value";
        static void           eval(In<"r", TryIntResult, InputValidity::Unchecked> r, Out<TS<Int>> out)
        {
            auto field = r.template field<"out">();
            if (field.valid() && field.modified()) { out.set(field.value()); }
        }
    };

    // Splits the try_except TSB result: the exception's error_msg when it ticks.
    struct TryExcMsg
    {
        static constexpr auto name = "try_exc_msg";
        static void           eval(In<"r", TryIntResult, InputValidity::Unchecked> r, Out<TS<Str>> out)
        {
            auto field = r.template field<"exception">();
            if (field.valid() && field.modified())
            {
                out.set(field.base().value().as_bundle().at("error_msg").checked_as<Str>());
            }
        }
    };

    // --- whole-graph wrappers (the agreed lightweight-graph testing pattern) ---

    struct PerNodeCaptureGraph
    {
        static constexpr auto name = "per_node_capture_graph";
        static void           compose(Wiring &w)
        {
            auto x       = wire<testing::replay, TS<Int>>(w, Str{"x"});
            auto doubled = wire<ThrowOnNegative>(w, x);
            auto err     = exception_time_series(doubled);
            wire<testing::record>(w, doubled, Str{"out"});
            wire<testing::record>(w, wire<ErrorMsgOf>(w, err), Str{"err"});
        }
    };

    struct TryExceptValueGraph
    {
        static constexpr auto name = "try_except_value_graph";
        static void           compose(Wiring &w)
        {
            auto x      = wire<testing::replay, TS<Int>>(w, Str{"x"});
            auto result = try_except_<DoublerOrThrowG>(w, x).as<TryIntResult>();
            wire<testing::record>(w, wire<TryOutValue>(w, result), Str{"out"});
            wire<testing::record>(w, wire<TryExcMsg>(w, result), Str{"err"});
        }
    };

    struct PerNodeUnknownCaptureGraph
    {
        static constexpr auto name = "per_node_unknown_capture_graph";
        static void           compose(Wiring &w)
        {
            auto x       = wire<testing::replay, TS<Int>>(w, Str{"x"});
            auto tripled = wire<ThrowUnknownOnNegative>(w, x);
            auto err     = exception_time_series(tripled);
            wire<testing::record>(w, tripled, Str{"out"});
            wire<testing::record>(w, wire<ErrorMsgOf>(w, err), Str{"err"});
        }
    };

    struct TryExceptUnknownGraph
    {
        static constexpr auto name = "try_except_unknown_graph";
        static void           compose(Wiring &w)
        {
            auto x      = wire<testing::replay, TS<Int>>(w, Str{"x"});
            auto result = try_except_<TriplerOrUnknownG>(w, x).as<TryIntResult>();
            wire<testing::record>(w, wire<TryOutValue>(w, result), Str{"out"});
            wire<testing::record>(w, wire<TryExcMsg>(w, result), Str{"err"});
        }
    };

    struct TryExceptSinkGraph
    {
        static constexpr auto name = "try_except_sink_graph";
        static void           compose(Wiring &w)
        {
            auto x   = wire<testing::replay, TS<Int>>(w, Str{"x"});
            auto err = try_except_<SinkOrThrowG>(w, x).as<TS<NodeError>>();
            wire<testing::record>(w, wire<ErrorMsgOf>(w, err), Str{"err"});
        }
    };
}  // namespace

TEST_CASE("NodeError: a value-layer bundle with the reference fields")
{
    using namespace hgraph;

    REQUIRE(node_error_ts_meta() != nullptr);

    NodeErrorFields fields;
    fields.signature_name = "my_node";
    fields.error_msg      = "boom";
    Value error = make_node_error_value(fields);

    auto bundle = error.as_bundle();
    CHECK(bundle.at("signature_name").checked_as<Str>() == "my_node");
    CHECK(bundle.at("error_msg").checked_as<Str>() == "boom");
    CHECK(bundle.at("label").checked_as<Str>().empty());
}

TEST_CASE("error handling: exception_time_series captures a node throw")
{
    using namespace hgraph;
    using namespace hgraph::testing;

    GraphBuilder gb = build_graph<PerNodeCaptureGraph>();
    set_replay_values<Int>(gb.global_state(), "x", values<Int>(5, -3, 7));

    GraphExecutorValue ex = run_graph(std::move(gb), MIN_ST, MAX_ET);
    const auto        &gs = ex.view().graph().global_state();

    // The throw cycle (x=-3) ticks the error. This node throws before writing
    // output; write-then-throw output is deliberately unspecified.
    CHECK_OUTPUT(get_recorded_values<Int>(gs, "out"), values<Int>(10, none, 14));
    CHECK_OUTPUT(get_recorded_values<Str>(gs, "err"), values<Str>(none, "negative input"s));
}

TEST_CASE("error handling: exception_time_series catches non-standard exceptions as unknown errors")
{
    using namespace hgraph;
    using namespace hgraph::testing;

    GraphBuilder gb = build_graph<PerNodeUnknownCaptureGraph>();
    set_replay_values<Int>(gb.global_state(), "x", values<Int>(5, -3, 7));

    GraphExecutorValue ex = run_graph(std::move(gb), MIN_ST, MAX_ET);
    const auto        &gs = ex.view().graph().global_state();

    CHECK_OUTPUT(get_recorded_values<Int>(gs, "out"), values<Int>(15, none, 21));
    CHECK_OUTPUT(get_recorded_values<Str>(gs, "err"), values<Str>(none, "unknown error"s));
}

TEST_CASE("error handling: a clean run never ticks the error output")
{
    using namespace hgraph;
    using namespace hgraph::testing;

    GraphBuilder gb = build_graph<PerNodeCaptureGraph>();
    set_replay_values<Int>(gb.global_state(), "x", values<Int>(1, 2, 3));

    GraphExecutorValue ex = run_graph(std::move(gb), MIN_ST, MAX_ET);
    const auto        &gs = ex.view().graph().global_state();

    CHECK_OUTPUT(get_recorded_values<Int>(gs, "out"), values<Int>(2, 4, 6));
    CHECK_OUTPUT(get_recorded_values<Str>(gs, "err"), values<Str>());
}

TEST_CASE("error handling: try_except over a sub-graph routes value and exception")
{
    using namespace hgraph;
    using namespace hgraph::testing;

    GraphBuilder gb = build_graph<TryExceptValueGraph>();
    set_replay_values<Int>(gb.global_state(), "x", values<Int>(5, -3, 7));

    GraphExecutorValue ex = run_graph(std::move(gb), MIN_ST, MAX_ET);
    const auto        &gs = ex.view().graph().global_state();

    CHECK_OUTPUT(get_recorded_values<Int>(gs, "out"), values<Int>(10, none, 14));
    CHECK_OUTPUT(get_recorded_values<Str>(gs, "err"), values<Str>(none, "negative input"s));
}

TEST_CASE("error handling: try_except catches non-standard exceptions as unknown errors")
{
    using namespace hgraph;
    using namespace hgraph::testing;

    GraphBuilder gb = build_graph<TryExceptUnknownGraph>();
    set_replay_values<Int>(gb.global_state(), "x", values<Int>(5, -3, 7));

    GraphExecutorValue ex = run_graph(std::move(gb), MIN_ST, MAX_ET);
    const auto        &gs = ex.view().graph().global_state();

    CHECK_OUTPUT(get_recorded_values<Int>(gs, "out"), values<Int>(15, none, 21));
    CHECK_OUTPUT(get_recorded_values<Str>(gs, "err"), values<Str>(none, "unknown error"s));
}

TEST_CASE("error handling: try_except over a sink sub-graph yields just the error series")
{
    using namespace hgraph;
    using namespace hgraph::testing;

    GraphBuilder gb = build_graph<TryExceptSinkGraph>();
    set_replay_values<Int>(gb.global_state(), "x", values<Int>(5, -3, 7));

    GraphExecutorValue ex = run_graph(std::move(gb), MIN_ST, MAX_ET);
    const auto        &gs = ex.view().graph().global_state();

    CHECK_OUTPUT(get_recorded_values<Str>(gs, "err"), values<Str>(none, "sink negative"s));
}
