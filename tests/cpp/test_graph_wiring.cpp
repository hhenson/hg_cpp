// Graph wiring (slice 1): author a graph as a struct with a static wire(Wiring&)
// body, compose nodes with wire<T>(w, ports...), and build it with
// build_graph<G>() — no node indices or edges by hand. See docs: Graph Wiring.

#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>

#include <catch2/catch_test_macros.hpp>

namespace
{
    using namespace hgraph;

    struct ConstantSource
    {
        static constexpr auto name = "source";
        static void           eval(Out<TS<int>> out) { out.set(41); }
    };

    struct AddOne
    {
        static constexpr auto name = "add_one";
        static void           eval(In<"in", TS<int>> in, Out<TS<int>> out) { out.set(in.value() + 1); }
    };

    // source -> add_one, wired declaratively.
    struct AddOneGraph
    {
        static constexpr auto name = "add_one_graph";
        static void           wire(Wiring &w)
        {
            auto source = hgraph::wire<ConstantSource>(w);
            hgraph::wire<AddOne>(w, source);
        }
    };
}  // namespace

TEST_CASE("graph wiring: build_graph wires source -> add_one and runs in simulation")
{
    using namespace hgraph;

    GraphBuilder graph_builder = build_graph<AddOneGraph>();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + engine_time_delta_t{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    REQUIRE(graph.node_count() == 2);
    // The rank pass orders source (no inputs) before add_one, so node 1 is add_one.
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<int>() == 42);
}

TEST_CASE("graph wiring: identical nodes are interned to one")
{
    using namespace hgraph;

    Wiring w;
    auto   a = wire<ConstantSource>(w);
    auto   b = wire<ConstantSource>(w);

    CHECK(a.node() != nullptr);
    CHECK(a.node() == b.node());   // same interned wiring instance

    GraphBuilder graph_builder = std::move(w).finish();
    GraphValue   graph         = graph_builder.make_graph();
    CHECK(graph.view().node_count() == 1);   // deduped to a single runtime node
}
