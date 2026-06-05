// Static node authoring: declare compute / source / sink nodes as stateless
// structs with a typed eval(In<>, Out<>, State<>) signature, and have
// NodeBuilder::implementation<T>() build the runtime node. This is the C++
// static wiring port from ext/2603, adjusted to the current type-erased
// runtime (see docs: Wiring, Schemas > Static Schema).

#include <hgraph/runtime/runtime.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_node.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

#include <string>
#include <string_view>
#include <vector>

namespace
{
    using namespace hgraph;

    // Source: Out only, no In -> kind inferred as PullSource. Writes a constant.
    struct ConstantSource
    {
        static constexpr auto name = "constant_source";

        static constexpr bool schedule_on_start = true;
        static void           eval(Out<TS<std::int32_t>> out) { out.set(41); }
    };

    // Compute: In + Out -> kind inferred as Compute.
    struct AddOne
    {
        static constexpr auto name = "add_one";

        static void eval(In<"in", TS<std::int32_t>> in, Out<TS<std::int32_t>> out) { out.set(in.value() + 1); }
    };

    // Compute with two named inputs.
    struct Sum
    {
        static constexpr auto name = "sum";

        static void eval(In<"lhs", TS<std::int32_t>> lhs, In<"rhs", TS<std::int32_t>> rhs, Out<TS<std::int32_t>> out)
        {
            out.set(lhs.value() + rhs.value());
        }
    };

    // Stateful source (Out only, no In -> PullSource) exercising State<std::int32_t>.
    struct Counter
    {
        static constexpr auto name = "counter";

        static void start(State<std::int32_t> state) { state.set(0); }

        static void eval(State<std::int32_t> state, Out<TS<std::int32_t>> out)
        {
            const std::int32_t next = state.get() + 1;
            state.set(next);
            out.set(next);
        }
    };

    // Source configured by a scalar input (no time-series inputs -> PullSource).
    // Emits its configured value; the scalar is read-only wiring-time config.
    struct ScaledSource
    {
        static constexpr auto name = "scaled_source";

        static void eval(Scalar<"value", std::int32_t> value, Out<TS<std::int32_t>> out) { out.set(value.value()); }
    };

    // Compute node mixing a time-series input with a scalar input. The scalar
    // is configuration (not part of the input TSB); Out is last by convention.
    struct Shift
    {
        static constexpr auto name = "shift";

        static void eval(In<"in", TS<std::int32_t>> in, Scalar<"delta", std::int32_t> delta, Out<TS<std::int32_t>> out)
        {
            out.set(in.value() + delta.value());
        }
    };

    struct DuplicateInputNames
    {
        static void eval(In<"x", TS<std::int32_t>>, In<"x", TS<std::int32_t>>, Out<TS<std::int32_t>>);
    };

    struct DuplicateScalarNames
    {
        static void eval(Scalar<"x", std::int32_t>, Scalar<"x", std::int32_t>, Out<TS<std::int32_t>>);
    };

    struct MultipleStateSlots
    {
        static void eval(State<std::int32_t>, State<std::int32_t>, Out<TS<std::int32_t>>);
    };

    // Build a single-field compound scalar configuration {field: value}.
    Value int_scalar_config(std::string_view field, std::int32_t value)
    {
        auto       &registry    = TypeRegistry::instance();
        const auto *int_meta    = registry.register_scalar<std::int32_t>("int32");
        const auto *bundle_meta = registry.un_named_bundle({{std::string{field}, int_meta}});
        const auto *binding     = ValuePlanFactory::instance().binding_for(bundle_meta);

        Value scalars{*binding};
        {
            auto mutation = scalars.as_bundle().begin_mutation();
            mutation[field].checked_mutable_as<std::int32_t>() = value;
        }
        return scalars;
    }
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

TEST_CASE("static node: signature detects ambiguous selector contracts")
{
    using namespace hgraph;

    STATIC_REQUIRE(StaticNodeSignature<Sum>::input_names_unique());
    STATIC_REQUIRE(StaticNodeSignature<Shift>::scalar_names_unique());
    STATIC_REQUIRE(StaticNodeSignature<Counter>::state_count() == 1);

    STATIC_REQUIRE_FALSE(StaticNodeSignature<DuplicateInputNames>::input_names_unique());
    STATIC_REQUIRE_FALSE(StaticNodeSignature<DuplicateScalarNames>::scalar_names_unique());
    STATIC_REQUIRE(StaticNodeSignature<MultipleStateSlots>::state_count() == 2);
}

TEST_CASE("static node: set_delta construction survives value registry resets")
{
    using namespace hgraph;

    {
        (void)TypeRegistry::instance().register_scalar<std::int32_t>("int32");
        const Value first = set_delta<std::int32_t>({1}, {});  // canonical Bundle{added:{1}, removed:{}}
        CHECK(first.equals(set_delta<std::int32_t>({1}, {})));
    }

    ValuePlanFactory::instance().reset();
    ValueTypeBinding::clear();
    TypeRegistry::instance().reset();

    (void)TypeRegistry::instance().register_scalar<std::int32_t>("int32");
    const Value second = set_delta<std::int32_t>({2}, {1});
    CHECK(second.equals(set_delta<std::int32_t>({2}, {1})));       // order-independent, type-erased
    CHECK_FALSE(second.equals(set_delta<std::int32_t>({2}, {})));  // different removed -> not equal
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
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<std::int32_t>() == 42);
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
    CHECK(graph.node_at(2).output(MIN_ST).value().checked_as<std::int32_t>() == 82);
}

TEST_CASE("static node: Scalar<> configures a source from per-instance values")
{
    using namespace hgraph;

    auto node = NodeBuilder{}
                    .label("scaled")
                    .implementation<ScaledSource>()
                    .scalars(int_scalar_config("value", 7))
                    .make_node();

    auto view = node.view();
    REQUIRE(view.node_kind() == NodeKind::PullSource);   // Scalar is config, not a TS input
    REQUIRE(view.schema()->has_scalars());
    REQUIRE(view.has_scalars());
    REQUIRE_FALSE(view.has_input());
    REQUIRE(view.scalars().as_bundle().field("value").checked_as<std::int32_t>() == 7);

    const auto t1 = MIN_ST;
    view.start(t1);
    view.evaluate(t1, true);
    CHECK(node.view().output(t1).value().checked_as<std::int32_t>() == 7);
}

TEST_CASE("static node: Scalar<> coexists with a time-series input")
{
    using namespace hgraph;

    GraphBuilder builder;
    builder.add_node(NodeBuilder{}.label("src").implementation<ConstantSource>())
        .add_node(NodeBuilder{}.label("shift").implementation<Shift>().scalars(int_scalar_config("delta", 5)))
        .add_edge(GraphEdge{.source_node = 0, .source_path = {}, .target_node = 1, .target_path = {0}});

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + engine_time_delta_t{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    // Compute node: Compute kind (In present), one TS input field, scalar excluded.
    REQUIRE(graph.node_at(1).node_kind() == NodeKind::Compute);
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<std::int32_t>() == 46);   // 41 + 5
}

TEST_CASE("static node: State<std::int32_t> is constructed and mutated across evaluations")
{
    using namespace hgraph;

    auto       node = NodeBuilder{}.label("counter").implementation<Counter>().make_node();
    const auto t1   = MIN_ST;
    const auto t2   = t1 + engine_time_delta_t{1};

    auto view = node.view();
    REQUIRE(view.has_state());

    view.start(t1);
    view.evaluate(t1, true);
    CHECK(node.view().state().checked_as<std::int32_t>() == 1);
    CHECK(node.view().output(t1).value().checked_as<std::int32_t>() == 1);

    node.view().evaluate(t2, true);
    CHECK(node.view().state().checked_as<std::int32_t>() == 2);
    CHECK(node.view().output(t2).value().checked_as<std::int32_t>() == 2);
}
