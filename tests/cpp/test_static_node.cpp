// Static node authoring: declare compute / source / sink nodes as stateless
// structs with a typed eval(In<>, Out<>, State<>) signature, and have
// NodeBuilder::implementation<T>() build the runtime node. This is the C++
// static wiring port from ext/2603, adjusted to the current type-erased
// runtime (see docs: Wiring, Schemas > Static Schema).

#include <hgraph/runtime/runtime.h>
#include <hgraph/types/static_node.h>

#include <catch2/catch_test_macros.hpp>

namespace
{
    using namespace hgraph;

    // Source: Out only, explicit PullSource kind. Writes a constant.
    struct ConstantSource
    {
        static constexpr auto     name      = "constant_source";
        static constexpr NodeKind node_kind = NodeKind::PullSource;

        static void eval(Out<TS<int>> out) { out.set(41); }
    };

    // Compute: In + Out -> kind inferred as Compute.
    struct AddOne
    {
        static constexpr auto name = "add_one";

        static void eval(In<"in", TS<int>> in, Out<TS<int>> out) { out.set(in.value() + 1); }
    };

    // Compute with two named inputs.
    struct Sum
    {
        static constexpr auto name = "sum";

        static void eval(In<"lhs", TS<int>> lhs, In<"rhs", TS<int>> rhs, Out<TS<int>> out)
        {
            out.set(lhs.value() + rhs.value());
        }
    };

    // Stateful source exercising State<int> across start/eval.
    struct Counter
    {
        static constexpr auto     name      = "counter";
        static constexpr NodeKind node_kind = NodeKind::PullSource;

        static void start(State<int> state) { state.set(0); }

        static void eval(State<int> state, Out<TS<int>> out)
        {
            const int next = state.get() + 1;
            state.set(next);
            out.set(next);
        }
    };
}  // namespace

TEST_CASE("static node: node kind is inferred from In/Out selectors")
{
    using namespace hgraph;

    auto source = NodeBuilder{}.label("src").implementation<ConstantSource>().make_node();
    CHECK(source.view().node_kind() == NodeKind::PullSource);
    CHECK(source.view().has_output());
    CHECK_FALSE(source.view().has_input());

    auto compute = NodeBuilder{}.label("inc").implementation<AddOne>().make_node();
    CHECK(compute.view().node_kind() == NodeKind::Compute);
    CHECK(compute.view().has_input());
    CHECK(compute.view().has_output());
}

TEST_CASE("static node: source -> compute graph runs in simulation mode")
{
    using namespace hgraph;

    GraphBuilder builder;
    builder.add_node(NodeBuilder{}.label("src").implementation<ConstantSource>())
        .add_node(NodeBuilder{}.label("inc").implementation<AddOne>())
        .add_edge(GraphEdge{.source_node = 0, .source_path = {}, .target_node = 1, .target_path = {0}});

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + engine_time_delta_t{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<int>() == 42);
}

TEST_CASE("static node: two sources feed a two-input compute node")
{
    using namespace hgraph;

    GraphBuilder builder;
    builder.add_node(NodeBuilder{}.label("a").implementation<ConstantSource>())
        .add_node(NodeBuilder{}.label("b").implementation<ConstantSource>())
        .add_node(NodeBuilder{}.label("sum").implementation<Sum>())
        .add_edge(GraphEdge{.source_node = 0, .source_path = {}, .target_node = 2, .target_path = {0}})
        .add_edge(GraphEdge{.source_node = 1, .source_path = {}, .target_node = 2, .target_path = {1}});

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + engine_time_delta_t{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    CHECK(graph.node_at(2).output(MIN_ST).value().checked_as<int>() == 82);
}

TEST_CASE("static node: State<int> is constructed and mutated across evaluations")
{
    using namespace hgraph;

    auto       node = NodeBuilder{}.label("counter").implementation<Counter>().make_node();
    const auto t1   = MIN_ST;
    const auto t2   = t1 + engine_time_delta_t{1};

    auto view = node.view();
    REQUIRE(view.has_state());

    view.start(t1);
    view.evaluate(t1, true);
    CHECK(node.view().state().checked_as<int>() == 1);
    CHECK(node.view().output(t1).value().checked_as<int>() == 1);

    node.view().evaluate(t2, true);
    CHECK(node.view().state().checked_as<int>() == 2);
    CHECK(node.view().output(t2).value().checked_as<int>() == 2);
}
