// Slice 1: In<TSS<T>> / Out<TSS<T>> authoring selectors, exercised end-to-end —
// a TS<int> source feeds an accumulator that adds into a TSS<int> output, whose
// size is read back through a TSS<int> input.

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>

#include <catch2/catch_test_macros.hpp>

#include <string>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    // Add each ticked input value into a TSS<int> output.
    struct Accumulate
    {
        static constexpr auto name = "accumulate";
        static void           eval(In<"in", TS<int>> in, Out<TSS<int>> out) { out.add(in.value()); }
    };

    // Emit the size of a TSS<int> input whenever it ticks.
    struct SetSize
    {
        static constexpr auto name = "set_size";
        static void           eval(In<"s", TSS<int>> s, Out<TS<int>> out) { out.set(static_cast<int>(s.size())); }
    };

    // Emit how many elements were added this cycle, via the typed In<TSS> accessor.
    struct AddedCount
    {
        static constexpr auto name = "added_count";
        static void           eval(In<"s", TSS<int>> s, Out<TS<int>> out)
        {
            out.set(static_cast<int>(s.added().size()));
        }
    };

    struct TssGraph
    {
        static constexpr auto name = "tss_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<testing::replay<TS<int>>>(w, std::string{"in"});
            auto acc = wire<Accumulate>(w, src);   // -> Port<TSS<int>>
            auto sz  = wire<SetSize>(w, acc);       // In<TSS<int>> -> Out<TS<int>>
            wire<testing::record<TS<int>>>(w, sz, std::string{"out"});
        }
    };

    // replay_set feeds set deltas straight into record_set (delta round-trip).
    struct TssDeltaGraph
    {
        static constexpr auto name = "tss_delta_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<testing::replay<TSS<int>>>(w, std::string{"in"});
            wire<testing::record<TSS<int>>>(w, src, std::string{"out"});
        }
    };

    // replay_set -> added_count -> record: reads the delta via In<TSS>::delta().
    struct TssAddedCountGraph
    {
        static constexpr auto name = "tss_added_count_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<testing::replay<TSS<int>>>(w, std::string{"in"});
            auto cnt = wire<AddedCount>(w, src);
            wire<testing::record<TS<int>>>(w, cnt, std::string{"out"});
        }
    };
}  // namespace

TEST_CASE("tss: Out<TSS> accumulates and In<TSS> reads the growing set")
{
    (void)TypeRegistry::instance().register_scalar<int>("int");

    GraphBuilder gb = build_graph<TssGraph>();
    testing::set_replay_values<int>(gb.global_state(), "in", {1, 2, 3});

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + engine_time_delta_t{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    // The set grows {1} -> {1,2} -> {1,2,3}; sizes 1, 2, 3.
    CHECK_OUTPUT(testing::get_recorded_values<int>(ex.view().graph().global_state(), "out"), {1, 2, 3});
}

TEST_CASE("tss: replay<TSS> -> record<TSS> round-trips set deltas (added/removed)")
{
    (void)TypeRegistry::instance().register_scalar<int>("int");

    const std::vector<std::optional<Value>> deltas{
        set_delta<int>({1, 2}, {}),   // add 1,2          -> {1,2}
        set_delta<int>({3}, {1}),      // add 3, remove 1  -> {2,3}
        set_delta<int>({}, {2, 3}),    // remove 2,3       -> {}
    };

    GraphBuilder gb = build_graph<TssDeltaGraph>();
    testing::set_replay_deltas(gb.global_state(), "in", deltas);

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + engine_time_delta_t{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_deltas(ex.view().graph().global_state(), "out"),
                 {set_delta<int>({1, 2}, {}), set_delta<int>({3}, {1}), set_delta<int>({}, {2, 3})});
}

TEST_CASE("tss: In<TSS> typed added() exposes this cycle's added elements")
{
    (void)TypeRegistry::instance().register_scalar<int>("int");

    const std::vector<std::optional<Value>> deltas{
        set_delta<int>({1, 2}, {}),   // 2 added
        set_delta<int>({3}, {1}),      // 1 added
        set_delta<int>({}, {2, 3}),    // 0 added
    };

    GraphBuilder gb = build_graph<TssAddedCountGraph>();
    testing::set_replay_deltas(gb.global_state(), "in", deltas);

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + engine_time_delta_t{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_values<int>(ex.view().graph().global_state(), "out"), {2, 1, 0});
}
