// The runtime, type-erased delta machinery `capture_delta(const TSInputView&) ->
// Value` / `apply_delta(const TSOutputView&, const ValueView&)` — schema-as-data,
// dispatched through the live endpoint's TSDataOps table. This suite drives them
// directly through hand-authored probe source/sink nodes and shows the cycle-aligned buffers
// round-trip identically (against the canonical delta builders) for TS / SIGNAL /
// TSS / TSD / TSL / TSB / TSW.

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <optional>
#include <string>
#include <type_traits>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;  // none, make_buffer/make_any/empty_any/cycle_offset, set_replay_*, get_recorded_*
    using Quote = TSB<"DeltaQuote", Field<"bid", TS<std::int32_t>>, Field<"ask", TS<std::int32_t>>>;
    using QuoteWithSet = TSB<"DeltaQuoteWithSet", Field<"levels", TSS<std::int32_t>>, Field<"last", TS<std::int32_t>>>;

    // The erased free functions take the base TSInputView / TSOutputView. A typed
    // `In<Name,S>` reaches it uniformly via `base()`; a typed `Out<S>` is the base
    // itself only for the scalar case (`Out<TS<T>> : TSOutputView`), while the
    // container outputs compose one (reached via `base()`). (Phase 2's `Out<TsVar>`
    // *is* a TSOutputView, so this asymmetry disappears for the real erased nodes.)
    template <typename S>
    const TSOutputView &out_view(const Out<S> &out)
    {
        if constexpr (std::is_base_of_v<TSOutputView, Out<S>>) { return out; }
        else { return out.base(); }
    }

    // A `record` clone whose eval captures the per-cycle delta with the runtime
    // `capture_delta` instead of `ts_delta<S>::capture`. `ts` (an In<"ts", S>)
    // slices to the erased `TSInputView` the function takes.
    template <typename S>
    struct ProbeRecord
    {
        static constexpr auto name = "ts_delta_probe_record";
        static void           start(Scalar<"key", std::string> key, GlobalStateView gs) { gs.set(key.value(), make_buffer()); }
        static void           eval(In<"ts", S> ts, Scalar<"key", std::string> key, GlobalStateView gs, engine_time_t now)
        {
            const std::size_t offset   = cycle_offset(now);
            const ValueView   buffer   = gs.get(key.value());
            auto              list     = buffer.as_list();
            std::size_t       size     = list.size();
            auto              mutation = list.begin_mutation();
            while (size < offset)
            {
                mutation.push_back(empty_any().view());
                ++size;
            }
            mutation.push_back(make_any(capture_delta(ts.base())).view());
        }
    };

    // A `replay` clone whose eval re-creates ticks with the runtime `apply_delta`
    // instead of `ts_delta<S>::apply`. `out` (an Out<S>) slices to the erased
    // `TSOutputView` the function takes.
    template <typename S>
    struct ProbeReplay
    {
        static constexpr auto name             = "ts_delta_probe_replay";
        static constexpr bool schedule_on_start = true;
        static void           eval(Scalar<"key", std::string> key, GlobalStateView gs, NodeScheduler sched,
                                   State<std::int32_t> index, Out<S> out)
        {
            const ValueView buffer = gs.get(key.value());
            if (!buffer.valid()) { return; }
            const auto list = buffer.as_list();
            const int  i    = index.get();
            const int  size = static_cast<int>(list.size());
            if (i < size)
            {
                const auto element = list.at(static_cast<std::size_t>(i)).as_any();
                if (element.has_value()) { apply_delta(out_view(out), element.get()); }
            }
            index.set(i + 1);
            if (i + 1 < size) { sched.schedule(MIN_TD); }
        }
    };

    // capture parity: replay (ts_delta::apply) -> ProbeRecord (capture_delta).
    template <typename S>
    struct CaptureGraph
    {
        static constexpr auto name = "ts_delta_capture_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<testing::replay, S>(w, std::string{"in"});
            wire<ProbeRecord<S>>(w, src, std::string{"out"});
        }
    };

    // apply parity: ProbeReplay (apply_delta) -> record (ts_delta::capture).
    template <typename S>
    struct ApplyGraph
    {
        static constexpr auto name = "ts_delta_apply_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<ProbeReplay<S>>(w, std::string{"in"});
            wire<testing::record>(w, src, std::string{"out"});
        }
    };

    // both new functions: ProbeReplay (apply_delta) -> ProbeRecord (capture_delta).
    template <typename S>
    struct RoundTripGraph
    {
        static constexpr auto name = "ts_delta_roundtrip_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<ProbeReplay<S>>(w, std::string{"in"});
            wire<ProbeRecord<S>>(w, src, std::string{"out"});
        }
    };

    template <typename Graph, typename Seed>
    auto run_graph(Seed seed)
    {
        GraphBuilder gb = build_graph<Graph>();
        seed(gb.global_state());
        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + engine_time_delta_t{10});
        GraphExecutorValue ex = eb.make_executor();
        ex.view().run();
        return ex;
    }
}  // namespace

TEST_CASE("ts_delta: capture_delta matches ts_delta<S>::capture for a scalar TS")
{
    (void)TypeRegistry::instance().register_scalar<std::int32_t>("int32");
    auto ex = run_graph<CaptureGraph<TS<std::int32_t>>>(
        [](const GlobalStateView &gs) { set_replay_values<std::int32_t>(gs, "in", {1, none, 3}); });
    CHECK_OUTPUT(get_recorded_values<std::int32_t>(ex.view().graph().global_state(), "out"), {1, none, 3});
}

TEST_CASE("ts_delta: apply_delta matches ts_delta<S>::apply for a scalar TS")
{
    (void)TypeRegistry::instance().register_scalar<std::int32_t>("int32");
    auto ex = run_graph<ApplyGraph<TS<std::int32_t>>>(
        [](const GlobalStateView &gs) { set_replay_values<std::int32_t>(gs, "in", {4, none, 6}); });
    CHECK_OUTPUT(get_recorded_values<std::int32_t>(ex.view().graph().global_state(), "out"), {4, none, 6});
}

TEST_CASE("ts_delta: capture/apply round-trip a TSS set delta")
{
    (void)TypeRegistry::instance().register_scalar<std::int32_t>("int32");
    const std::vector<std::optional<Value>> deltas{
        set_delta<std::int32_t>({1, 2}, {}), set_delta<std::int32_t>({3}, {1}), set_delta<std::int32_t>({}, {2, 3})};

    // capture parity
    auto cap = run_graph<CaptureGraph<TSS<std::int32_t>>>(
        [&](const GlobalStateView &gs) { set_replay_deltas(gs, "in", deltas); });
    CHECK_OUTPUT(get_recorded_deltas(cap.view().graph().global_state(), "out"),
                 {set_delta<std::int32_t>({1, 2}, {}), set_delta<std::int32_t>({3}, {1}), set_delta<std::int32_t>({}, {2, 3})});

    // apply parity
    auto app = run_graph<ApplyGraph<TSS<std::int32_t>>>(
        [&](const GlobalStateView &gs) { set_replay_deltas(gs, "in", deltas); });
    CHECK_OUTPUT(get_recorded_deltas(app.view().graph().global_state(), "out"),
                 {set_delta<std::int32_t>({1, 2}, {}), set_delta<std::int32_t>({3}, {1}), set_delta<std::int32_t>({}, {2, 3})});
}

TEST_CASE("ts_delta: capture/apply round-trip a fixed-TSL-of-scalar list delta")
{
    (void)TypeRegistry::instance().register_scalar<std::int32_t>("int32");
    const std::vector<std::optional<Value>> deltas{
        list_delta<TS<std::int32_t>>({{0, 1}, {1, 2}}), list_delta<TS<std::int32_t>>({{0, 5}}), list_delta<TS<std::int32_t>>({{1, 9}})};

    // both new functions together: ProbeReplay -> ProbeRecord must reproduce the input.
    auto rt = run_graph<RoundTripGraph<TSL<TS<std::int32_t>, 2>>>(
        [&](const GlobalStateView &gs) { set_replay_deltas(gs, "in", deltas); });
    CHECK_OUTPUT(get_recorded_deltas(rt.view().graph().global_state(), "out"),
                 {list_delta<TS<std::int32_t>>({{0, 1}, {1, 2}}), list_delta<TS<std::int32_t>>({{0, 5}}), list_delta<TS<std::int32_t>>({{1, 9}})});
}

TEST_CASE("ts_delta: capture/apply round-trip a dynamic TSL-of-scalar list delta")
{
    (void)TypeRegistry::instance().register_scalar<std::int32_t>("int32");
    const std::vector<std::optional<Value>> deltas{
        list_delta<TS<std::int32_t>>({{0, 1}}),
        list_delta<TS<std::int32_t>>({{0, 5}, {1, 9}}),
        list_delta<TS<std::int32_t>>({{1, 11}}),
    };

    auto rt = run_graph<RoundTripGraph<TSL<TS<std::int32_t>>>>(
        [&](const GlobalStateView &gs) { set_replay_deltas(gs, "in", deltas); });
    CHECK_OUTPUT(get_recorded_deltas(rt.view().graph().global_state(), "out"),
                 {list_delta<TS<std::int32_t>>({{0, 1}}), list_delta<TS<std::int32_t>>({{0, 5}, {1, 9}}),
                  list_delta<TS<std::int32_t>>({{1, 11}})});
}

TEST_CASE("ts_delta: capture/apply round-trip a TSD-of-scalar dict delta")
{
    using namespace std::string_literals;

    (void)TypeRegistry::instance().register_scalar<std::int32_t>("int32");
    (void)TypeRegistry::instance().register_scalar<std::string>("string");
    const std::vector<std::optional<Value>> deltas{
        dict_delta<std::string, TS<std::int32_t>>({{"a"s, 1}, {"b"s, 2}}),
        dict_delta<std::string, TS<std::int32_t>>({{"a"s, 5}}, {"b"s}),
        dict_delta<std::string, TS<std::int32_t>>({{"b"s, 9}}),
    };

    auto rt = run_graph<RoundTripGraph<TSD<std::string, TS<std::int32_t>>>>(
        [&](const GlobalStateView &gs) { set_replay_deltas(gs, "in", deltas); });
    CHECK_OUTPUT(get_recorded_deltas(rt.view().graph().global_state(), "out"),
                 {dict_delta<std::string, TS<std::int32_t>>({{"a"s, 1}, {"b"s, 2}}),
                  dict_delta<std::string, TS<std::int32_t>>({{"a"s, 5}}, {"b"s}),
                  dict_delta<std::string, TS<std::int32_t>>({{"b"s, 9}})});
}

TEST_CASE("ts_delta: capture/apply round-trip a TSB sparse field delta")
{
    (void)TypeRegistry::instance().register_scalar<std::int32_t>("int32");
    const std::vector<std::optional<Value>> deltas{
        tsb_delta<Quote>(1, 10),
        tsb_delta<Quote>(std::nullopt, 20),
        tsb_delta<Quote>(3, std::nullopt),
    };

    auto rt = run_graph<RoundTripGraph<Quote>>(
        [&](const GlobalStateView &gs) { set_replay_deltas(gs, "in", deltas); });
    CHECK_OUTPUT(get_recorded_deltas(rt.view().graph().global_state(), "out"),
                 {tsb_delta<Quote>(1, 10), tsb_delta<Quote>(std::nullopt, 20), tsb_delta<Quote>(3, std::nullopt)});
}

TEST_CASE("ts_delta: capture/apply round-trip a TSB with a container field delta")
{
    (void)TypeRegistry::instance().register_scalar<std::int32_t>("int32");
    const std::vector<std::optional<Value>> deltas{
        tsb_delta<QuoteWithSet>(set_delta<std::int32_t>({1, 2}, {}), std::nullopt),
        tsb_delta<QuoteWithSet>(std::nullopt, 5),
        tsb_delta<QuoteWithSet>(set_delta<std::int32_t>({3}, {1}), 6),
    };

    auto rt = run_graph<RoundTripGraph<QuoteWithSet>>(
        [&](const GlobalStateView &gs) { set_replay_deltas(gs, "in", deltas); });
    CHECK_OUTPUT(get_recorded_deltas(rt.view().graph().global_state(), "out"),
                 {tsb_delta<QuoteWithSet>(set_delta<std::int32_t>({1, 2}, {}), std::nullopt),
                  tsb_delta<QuoteWithSet>(std::nullopt, 5),
                  tsb_delta<QuoteWithSet>(set_delta<std::int32_t>({3}, {1}), 6)});
}

TEST_CASE("ts_delta: capture/apply round-trip a TSW scalar push delta")
{
    (void)TypeRegistry::instance().register_scalar<std::int32_t>("int32");
    const std::vector<std::optional<Value>> deltas{Value{std::int32_t{1}}, Value{std::int32_t{2}}, Value{std::int32_t{3}}};

    auto rt = run_graph<RoundTripGraph<TSW<std::int32_t, 3, 1>>>(
        [&](const GlobalStateView &gs) { set_replay_deltas(gs, "in", deltas); });
    CHECK_OUTPUT(get_recorded_deltas(rt.view().graph().global_state(), "out"), {Value{std::int32_t{1}}, Value{std::int32_t{2}}, Value{std::int32_t{3}}});
}

TEST_CASE("ts_delta: capture/apply round-trip a SIGNAL tick delta")
{
    (void)TypeRegistry::instance().register_scalar<bool>("bool");
    const std::vector<std::optional<Value>> deltas{Value{true}, Value{true}, Value{true}};

    auto rt = run_graph<RoundTripGraph<SIGNAL>>(
        [&](const GlobalStateView &gs) { set_replay_deltas(gs, "in", deltas); });
    CHECK_OUTPUT(get_recorded_deltas(rt.view().graph().global_state(), "out"),
                 {Value{true}, Value{true}, Value{true}});
}
