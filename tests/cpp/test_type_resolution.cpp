// Phase 3 of the type-var node work: a node authored ONCE over deferred schemas
// (TsVar / ScalarVar) is resolved to a concrete schema at WIRING time — from a
// connected input port, from an inferred scalar value, or from an explicit type.
// These hand-authored generic nodes exercise that resolution end-to-end alongside
// the real utility nodes (replay/record/const_).

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>

#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <string>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    // Generic compute node: re-emit the input's per-cycle delta. Authored once over
    // a deferred TS type ``S`` (input and output share it), driven entirely by the
    // erased runtime delta machinery — works for any resolved kind.
    struct Passthrough
    {
        static constexpr auto name = "rt_passthrough";
        static void           eval(In<"in", TsVar<"S">> in, Out<TsVar<"S">> out)
        {
            const Value delta = capture_delta(in.base());
            apply_delta(out, delta.view());
        }
    };

    // Generic source: emit a configured constant. The scalar variable ``T`` is
    // inferred from the supplied value; the default resolver binds output ``S`` to
    // ``TS<T>`` when no explicit output schema is supplied.
    struct GenConst
    {
        static constexpr auto name             = "rt_gen_const";
        static constexpr bool schedule_on_start = true;
        static void resolve_default_types(ResolutionMap &resolution)
        {
            resolution.bind_ts("S", TypeRegistry::instance().ts(resolution.scalar("T")));
        }
        static void eval(Scalar<"value", ScalarVar<"T">> value, Out<TsVar<"S">> out)
        {
            out.apply(value.value());
        }
    };

    // replay<TS<int>> -> Passthrough (resolved from the port) -> record<TS<int>>.
    struct PassthroughGraph
    {
        static constexpr auto name = "rt_passthrough_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<testing::replay, TS<int>>(w, std::string{"in"});
            auto pt  = wire<Passthrough>(w, src);   // generic; returns an erased Port
            wire<testing::record>(w, pt, std::string{"out"});
        }
    };

    // Same Passthrough definition, now resolved to TSS<int> from its input port.
    struct PassthroughSetGraph
    {
        static constexpr auto name = "rt_passthrough_set_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<testing::replay, TSS<int>>(w, std::string{"in"});
            auto pt  = wire<Passthrough>(w, src);
            wire<testing::record>(w, pt, std::string{"out"});
        }
    };

    // GenConst resolved to TS<int> from the value 7 -> record<TS<int>>.
    struct GenConstGraph
    {
        static constexpr auto name = "rt_gen_const_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<GenConst>(w, 7);   // T inferred = int; output TS<int> (erased Port)
            wire<testing::record>(w, src, std::string{"out"});
        }
    };

    // The SAME GenConst definition resolved to TS<double> from the value 2.5 — proves
    // two resolutions of one generic source do not collide when interned.
    struct GenConstDoubleGraph
    {
        static constexpr auto name = "rt_gen_const_double_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<GenConst>(w, 2.5);
            wire<testing::record>(w, src, std::string{"out"});
        }
    };

    template <typename Graph, typename Seed>
    GraphExecutorValue run_graph(Seed seed)
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

TEST_CASE("type_resolution: a generic node resolves its TS type from the connected input port")
{
    (void)TypeRegistry::instance().register_scalar<int>("int");
    auto ex = run_graph<PassthroughGraph>(
        [](const GlobalStateView &gs) { set_replay_values<int>(gs, "in", {1, none, 3}); });
    CHECK_OUTPUT(get_recorded_values<int>(ex.view().graph().global_state(), "out"), {1, none, 3});
}

TEST_CASE("type_resolution: the same generic node resolves to TSS from a set-valued port")
{
    (void)TypeRegistry::instance().register_scalar<int>("int");
    const std::vector<std::optional<Value>> deltas{set_delta<int>({1, 2}, {}), set_delta<int>({3}, {1})};
    auto ex = run_graph<PassthroughSetGraph>([&](const GlobalStateView &gs) { set_replay_deltas(gs, "in", deltas); });
    CHECK_OUTPUT(get_recorded_deltas(ex.view().graph().global_state(), "out"),
                 {set_delta<int>({1, 2}, {}), set_delta<int>({3}, {1})});
}

TEST_CASE("type_resolution: a generic source infers its type from the configured scalar value")
{
    (void)TypeRegistry::instance().register_scalar<int>("int");
    auto ex = run_graph<GenConstGraph>([](const GlobalStateView &) {});
    CHECK_OUTPUT(get_recorded_values<int>(ex.view().graph().global_state(), "out"), {7});
}

TEST_CASE("type_resolution: a second resolution of the same generic source does not collide")
{
    (void)TypeRegistry::instance().register_scalar<double>("double");
    auto ex = run_graph<GenConstDoubleGraph>([](const GlobalStateView &) {});
    CHECK_OUTPUT(get_recorded_values<double>(ex.view().graph().global_state(), "out"), {2.5});
}
