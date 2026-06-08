// Graph wiring (slice 1): author a graph as a struct with a static wire(Wiring&)
// body, compose nodes with wire<T>(w, ports...), and build it with
// build_graph<G>() — no node indices or edges by hand. See docs: Graph Wiring.

#include <hgraph/lib/std/std_nodes.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/time_series/ts_delta.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <type_traits>
#include <utility>

namespace
{
    using namespace hgraph;

    template <typename TSchema>
    [[nodiscard]] const TSOutputView &erased_output(const Out<TSchema> &out)
    {
        if constexpr (std::is_base_of_v<TSOutputView, Out<TSchema>>)
        {
            return out;
        }
        else
        {
            return out.base();
        }
    }

    struct ConstantSource
    {
        static constexpr auto name              = "source";
        static constexpr bool schedule_on_start = true;
        static void           eval(Out<TS<Int>> out) { out.set(Int{41}); }
    };

    struct AddOne
    {
        static constexpr auto name = "add_one";
        static void           eval(In<"in", TS<Int>> in, Out<TS<Int>> out) { out.set(in.value() + 1); }
    };

    // source -> add_one, wired declaratively.
    struct AddOneGraph
    {
        static constexpr auto name = "add_one_graph";
        static void           compose(Wiring &w)
        {
            auto source = wire<ConstantSource>(w);
            wire<AddOne>(w, source);
        }
    };

    // A sub-graph: TS<Int> -> TS<Int>, adding two via two add_one nodes.
    struct PlusTwo
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> x)
        {
            return wire<AddOne>(w, wire<AddOne>(w, x));
        }
    };

    // Top-level: source(41) -> PlusTwo -> 43. The sub-graph flattens into the parent.
    struct PlusTwoGraph
    {
        static constexpr auto name = "plus_two_graph";
        static void           compose(Wiring &w)
        {
            auto source = wire<ConstantSource>(w);
            wire<PlusTwo>(w, source);
        }
    };

    // Two-input compute node (exercises compile-time per-port schema matching).
    struct Sum
    {
        static constexpr auto name = "sum";
        static void           eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs, Out<TS<Int>> out)
        {
            out.set(lhs.value() + rhs.value());
        }
    };

    struct SumGraph
    {
        static constexpr auto name = "sum_graph";
        static void           compose(Wiring &w)
        {
            auto source = wire<ConstantSource>(w);   // interns to one node
            wire<Sum>(w, source, source);            // 41 + 41
        }
    };

    // Source configured by a scalar argument (no TS inputs -> PullSource).
    struct ScaledSource
    {
        static constexpr auto name              = "scaled_source";
        static constexpr bool schedule_on_start = true;
        static void           eval(Scalar<"value", Int> value, Out<TS<Int>> out) { out.set(value.value()); }
    };

    struct ScaledSourceGraph
    {
        static constexpr auto name = "scaled_source_graph";
        static void           compose(Wiring &w) { wire<ScaledSource>(w, Int{7}); }
    };

    // Compute node mixing a TS input port with a scalar argument; wire args are
    // given in eval-parameter order: the port, then the scalar.
    struct Shift
    {
        static constexpr auto name = "shift";
        static void           eval(In<"in", TS<Int>> in, Scalar<"delta", Int> delta, Out<TS<Int>> out)
        {
            out.set(in.value() + delta.value());
        }
    };

    struct ShiftGraph
    {
        static constexpr auto name = "shift_graph";
        static void           compose(Wiring &w)
        {
            auto source = wire<ConstantSource>(w);   // 41
            wire<Shift>(w, source, Int{5});          // 41 + 5 = 46
        }
    };

    // Top-level graph that takes a Scalar parameter, threaded into a node scalar.
    struct ConfiguredSourceGraph
    {
        static constexpr auto name = "configured_source_graph";
        static void           compose(Wiring &w, Scalar<"value", Int> value)
        {
            wire<ScaledSource>(w, value.value());
        }
    };

    // Top-level graph whose scalar parameter offsets a constant source.
    struct OffsetGraph
    {
        static constexpr auto name = "offset_graph";
        static void           compose(Wiring &w, Scalar<"offset", Int> offset)
        {
            auto source = wire<ConstantSource>(w);    // 41
            wire<Shift>(w, source, offset.value());   // 41 + offset
        }
    };

    // Sub-graph with a port input AND a scalar parameter: (TS<Int>, by) -> TS<Int>.
    // The received Scalar<"by", Int> is forwarded straight to wire<Shift> (whose
    // scalar parameter is Scalar<"delta", Int>) — no .value() needed; the wiring
    // layer unpacks the Scalar and re-applies it (names need not match).
    struct ShiftBy
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> x, Scalar<"by", Int> by)
        {
            return wire<Shift>(w, x, by);
        }
    };

    // Top-level: source(41) -> ShiftBy(by=5) -> 46. The literal 5 is auto-wrapped
    // into the sub-graph's Scalar<"by", Int> parameter, and ShiftBy flattens.
    struct ShiftBySubGraph
    {
        static constexpr auto name = "shift_by_sub_graph";
        static void           compose(Wiring &w)
        {
            auto source = wire<ConstantSource>(w);
            wire<ShiftBy>(w, source, Int{5});
        }
    };

    struct AddOneSubGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> x)
        {
            return wire<AddOne>(w, x);
        }
    };

    struct GenericSourceIntoTypedSubGraph
    {
        static constexpr auto name = "generic_source_into_typed_sub_graph";
        static void           compose(Wiring &w)
        {
            auto source = wire<stdlib::const_>(w, Int{41});  // erased Port<void>, resolved to TS<Int>
            wire<AddOneSubGraph>(w, source);
        }
    };

    struct CountSignal
    {
        static constexpr auto name = "count_signal";
        static void           eval(In<"pulse", SIGNAL> pulse, State<Int> count, Out<TS<Int>> out)
        {
            if (pulse.ticked())
            {
                const Int next = count.get() + 1;
                count.set(next);
                out.set(next);
            }
        }
    };

    struct CountSignalSubGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<SIGNAL> pulse)
        {
            return wire<CountSignal>(w, pulse);
        }
    };

    struct SignalSubGraphFromTsPort
    {
        static constexpr auto name = "signal_sub_graph_from_ts_port";
        static void           compose(Wiring &w)
        {
            auto source = wire<ConstantSource>(w);
            wire<CountSignalSubGraph>(w, source);
        }
    };

    struct RefProbe
    {
        static constexpr auto name              = "ref_probe";
        static constexpr bool schedule_on_start = true;

        static void eval(In<"ref", REF<TS<Int>>> ref, Out<TS<Bool>> out)
        {
            const TimeSeriesReference value = ref.value();
            out.set(value.is_peered() &&
                    time_series_schema_equivalent(value.target_schema(), ts_type<TS<Int>>()));
        }
    };

    struct StructuralListRefProbe
    {
        static constexpr auto name              = "structural_list_ref_probe";
        static constexpr bool schedule_on_start = true;

        static void eval(In<"ref", REF<TSL<TS<Int>, 2>>> ref, Out<TS<Bool>> out)
        {
            const TimeSeriesReference value = ref.value();
            out.set(value.is_non_peered() &&
                    time_series_schema_equivalent(value.target_schema(), ts_type<TSL<TS<Int>, 2>>()) &&
                    value.items().size() == 2 &&
                    value[0].is_peered() &&
                    value[1].is_peered());
        }
    };

    struct RefCopy
    {
        static constexpr auto name              = "ref_copy";
        static constexpr bool schedule_on_start = true;

        static void eval(In<"ref", REF<TS<Int>>> ref, Out<REF<TS<Int>>> out)
        {
            out.set(ref.value());
        }
    };

    struct RefDeref
    {
        static constexpr auto name              = "ref_deref";
        static constexpr bool schedule_on_start = true;

        static void eval(In<"value", TS<Int>> value, Out<TS<Int>> out)
        {
            out.set(value.value());
        }
    };

    struct RefProbeGraph
    {
        static constexpr auto name = "ref_probe_graph";

        static void compose(Wiring &w)
        {
            auto source = wire<ConstantSource>(w);
            wire<RefProbe>(w, source);
        }
    };

    struct StructuralListRefProbeGraph
    {
        static constexpr auto name = "structural_list_ref_probe_graph";

        static void compose(Wiring &w)
        {
            auto source = wire<ConstantSource>(w);
            auto list   = stdlib::to_tsl<TSL<TS<Int>, 2>>(w, source, source);
            wire<StructuralListRefProbe>(w, list);
        }
    };

    struct RefRoundTripGraph
    {
        static constexpr auto name = "ref_round_trip_graph";

        static void compose(Wiring &w)
        {
            auto source = wire<ConstantSource>(w);
            auto ref    = wire<RefCopy>(w, source);
            wire<RefDeref>(w, ref);
        }
    };

    template <typename TSchema>
    struct RefCopyFor
    {
        static constexpr auto name              = "ref_copy_for";
        static constexpr bool schedule_on_start = true;

        static void eval(In<"ref", REF<TSchema>> ref, Out<REF<TSchema>> out)
        {
            out.set(ref.value());
        }
    };

    template <typename TSchema>
    struct RefDerefPass
    {
        static constexpr auto name              = "ref_deref_pass";
        static constexpr bool schedule_on_start = true;

        static void eval(In<"value", TSchema> value, Out<TSchema> out)
        {
            Value delta = capture_delta(value.base());
            apply_delta(erased_output(out), delta.view());
        }
    };

    template <typename TSource, typename TSchema>
    struct RefRoundTripFor
    {
        static constexpr auto name = "ref_round_trip_for";

        static void compose(Wiring &w)
        {
            auto source = wire<TSource>(w);
            auto ref    = wire<RefCopyFor<TSchema>>(w, source);
            wire<RefDerefPass<TSchema>>(w, ref);
        }
    };

    struct SetSource
    {
        static constexpr auto name              = "set_source";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<TSS<Int>> out)
        {
            out.add(Int{1});
            out.add(Int{2});
        }
    };

    struct DictSource
    {
        static constexpr auto name              = "dict_source";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<TSD<Int, TS<Int>>> out)
        {
            out.set(Int{1}, Int{10});
            out.set(Int{2}, Int{20});
        }
    };

    struct ListSource
    {
        static constexpr auto name              = "list_source";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<TSL<TS<Int>, 2>> out)
        {
            out.set(0, Int{7});
            out.set(1, Int{8});
        }
    };

    using RefRoundTripBundle = TSB<"RefRoundTripBundle",
                                   Field<"a", TS<Int>>,
                                   Field<"b", TS<Int>>>;

    struct BraceListInputSum
    {
        static constexpr auto name = "brace_list_input_sum";

        static void eval(In<"tsl", TSL<TS<Int>, 2>> tsl, Out<TS<Int>> out)
        {
            if (tsl[0].valid() && tsl[1].valid()) { out.set(tsl[0].value() + tsl[1].value()); }
        }
    };

    struct BraceListInputGraph
    {
        static constexpr auto name = "brace_list_input_graph";

        static void compose(Wiring &w)
        {
            auto source = wire<ConstantSource>(w);
            wire<BraceListInputSum>(w, {source, source});
        }
    };

    struct BraceBundleInputSum
    {
        static constexpr auto name = "brace_bundle_input_sum";

        static void eval(In<"tsb", RefRoundTripBundle> tsb, Out<TS<Int>> out)
        {
            auto a = tsb.field<"a">();
            auto b = tsb.field<"b">();
            if (a.valid() && b.valid()) { out.set(a.value() + b.value()); }
        }
    };

    struct BraceBundleInputGraph
    {
        static constexpr auto name = "brace_bundle_input_graph";

        static void compose(Wiring &w)
        {
            auto source = wire<ConstantSource>(w);
            wire<BraceBundleInputSum>(w, {source, source});
        }
    };

    struct BraceBundleNamedInputGraph
    {
        static constexpr auto name = "brace_bundle_named_input_graph";

        static void compose(Wiring &w)
        {
            auto a = wire<ScaledSource>(w, Int{3});
            auto b = wire<ScaledSource>(w, Int{40});
            wire<BraceBundleInputSum>(w, {{"b", b}, {"a", a}});
        }
    };

    struct BraceBundlePartialInputProbe
    {
        static constexpr auto name = "brace_bundle_partial_input_probe";

        static void eval(In<"tsb", RefRoundTripBundle> tsb, Out<TS<Int>> out)
        {
            auto a = tsb.field<"a">();
            auto b = tsb.field<"b">();
            if (a.valid() && !b.valid()) { out.set(a.value()); }
        }
    };

    struct BraceBundlePartialInputGraph
    {
        static constexpr auto name = "brace_bundle_partial_input_graph";

        static void compose(Wiring &w)
        {
            auto source = wire<ConstantSource>(w);
            wire<BraceBundlePartialInputProbe>(w, {{"a", source}});
        }
    };

    struct StructuralBundleRefProbe
    {
        static constexpr auto name              = "structural_bundle_ref_probe";
        static constexpr bool schedule_on_start = true;

        static void eval(In<"ref", REF<RefRoundTripBundle>> ref, Out<TS<Bool>> out)
        {
            const TimeSeriesReference value = ref.value();
            out.set(value.is_non_peered() &&
                    time_series_schema_equivalent(value.target_schema(), ts_type<RefRoundTripBundle>()) &&
                    value.items().size() == 2 &&
                    value[0].is_peered() &&
                    value[1].is_peered());
        }
    };

    struct BundleSource
    {
        static constexpr auto name              = "bundle_source";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<RefRoundTripBundle> out)
        {
            out.field<"a">().set(Int{11});
            out.field<"b">().set(Int{12});
        }
    };

    struct StructuralBundleRefProbeGraph
    {
        static constexpr auto name = "structural_bundle_ref_probe_graph";

        static void compose(Wiring &w)
        {
            auto source = wire<ConstantSource>(w);
            auto bundle = stdlib::to_tsb<RefRoundTripBundle>(w, source, source);
            wire<StructuralBundleRefProbe>(w, bundle);
        }
    };

    struct NestedListSetSource
    {
        static constexpr auto name              = "nested_list_set_source";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<TSL<TSS<Int>, 2>> out)
        {
            out[0].add(Int{3});
            out[0].add(Int{4});
            out[1].add(Int{5});
        }
    };

    GraphExecutorValue run_start(GraphBuilder graph_builder)
    {
        GraphExecutorBuilder executor_builder;
        executor_builder.graph_builder(std::move(graph_builder))
            .start_time(MIN_ST)
            .end_time(MIN_ST + engine_time_delta_t{2});

        GraphExecutorValue executor = executor_builder.make_executor();
        executor.view().run();
        return executor;
    }

    NodeBuilder static_node_builder_for_add_one()
    {
        NodeBuilder builder;
        builder.implementation<AddOne>();
        return builder;
    }

    NodeBuilder static_node_builder_for_constant_source()
    {
        NodeBuilder builder;
        builder.implementation<ConstantSource>();
        return builder;
    }

    NodeBuilder nested_add_one_builder()
    {
        const auto *ts_int = ts_type<TS<Int>>();

        GraphBuilder child;
        child.label("nested_add_one_child").add_node(static_node_builder_for_add_one());

        NodeTypeMetaData meta;
        meta.display_name = "nested_add_one";
        meta.input_schema = TypeRegistry::instance().un_named_tsb({{"in", ts_int}});
        meta.output_schema = ts_int;

        SingleNestedGraphNodeSpec spec;
        spec.graph_builder = std::move(child);
        spec.input_bindings.push_back(NestedGraphInputBinding{
            .source_path = {0},
            .target = NestedGraphEndpoint{.node = 0, .path = {0}},
        });
        spec.output_binding = NestedGraphOutputBinding{.source = NestedGraphEndpoint{.node = 0}};
        return single_nested_graph_node(std::move(meta), std::move(spec));
    }

    NodeBuilder nested_constant_builder()
    {
        const auto *ts_int = ts_type<TS<Int>>();

        GraphBuilder child;
        child.label("nested_constant_child").add_node(static_node_builder_for_constant_source());

        NodeTypeMetaData meta;
        meta.display_name  = "nested_constant";
        meta.output_schema = ts_int;

        SingleNestedGraphNodeSpec spec;
        spec.graph_builder = std::move(child);
        spec.output_binding = NestedGraphOutputBinding{.source = NestedGraphEndpoint{.node = 0}};
        return single_nested_graph_node(std::move(meta), std::move(spec));
    }
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
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<Int>() == Int{42});
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

TEST_CASE("graph wiring: sub-graph composition inlines (flattens) into the parent")
{
    using namespace hgraph;

    GraphBuilder graph_builder = build_graph<PlusTwoGraph>();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + engine_time_delta_t{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    REQUIRE(graph.node_count() == 3);   // source + two add_one (the PlusTwo sub-graph flattened)
    CHECK(graph.node_at(2).output(MIN_ST).value().checked_as<Int>() == Int{43});
}

TEST_CASE("graph wiring: sub-graph typed input accepts an erased generic source port")
{
    using namespace hgraph;
    stdlib::register_standard_operators();   // const_ is an operator

    GraphBuilder graph_builder = build_graph<GenericSourceIntoTypedSubGraph>();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + engine_time_delta_t{2});

    GraphExecutorValue executor = executor_builder.make_executor();
    auto               view     = executor.view();
    view.run();

    auto graph = view.graph();
    REQUIRE(graph.node_count() == 2);
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<Int>() == Int{42});
}

TEST_CASE("graph wiring: sub-graph SIGNAL input accepts any time-series port")
{
    using namespace hgraph;

    GraphBuilder graph_builder = build_graph<SignalSubGraphFromTsPort>();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + engine_time_delta_t{2});

    GraphExecutorValue executor = executor_builder.make_executor();
    auto               view     = executor.view();
    view.run();

    auto graph = view.graph();
    REQUIRE(graph.node_count() == 2);
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<Int>() == Int{1});
}

TEST_CASE("graph wiring: TS output can bind to a REF input")
{
    using namespace hgraph;

    GraphBuilder graph_builder = build_graph<RefProbeGraph>();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + engine_time_delta_t{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    REQUIRE(graph.node_count() == 2);
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<Bool>());
}

TEST_CASE("graph wiring: structural TSL source can bind to a REF input as non-peered")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    GraphBuilder graph_builder = build_graph<StructuralListRefProbeGraph>();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + engine_time_delta_t{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    REQUIRE(graph.node_count() == 3);
    REQUIRE(graph.node_at(2).output(MIN_ST).valid());
    CHECK(graph.node_at(2).output(MIN_ST).value().checked_as<Bool>());
}

TEST_CASE("graph wiring: structural TSB source can bind to a REF input as non-peered")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    GraphBuilder graph_builder = build_graph<StructuralBundleRefProbeGraph>();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + engine_time_delta_t{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    REQUIRE(graph.node_count() == 3);
    REQUIRE(graph.node_at(2).output(MIN_ST).valid());
    CHECK(graph.node_at(2).output(MIN_ST).value().checked_as<Bool>());
}

TEST_CASE("graph wiring: brace initializer wires fixed TSL input as non-peered structure")
{
    using namespace hgraph;

    GraphExecutorValue executor = run_start(build_graph<BraceListInputGraph>());

    auto graph = executor.view().graph();
    REQUIRE(graph.node_count() == 2);
    REQUIRE(graph.node_at(1).output(MIN_ST).valid());
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<Int>() == Int{82});
}

TEST_CASE("graph wiring: brace initializer wires TSB input as non-peered structure")
{
    using namespace hgraph;

    GraphExecutorValue executor = run_start(build_graph<BraceBundleInputGraph>());

    auto graph = executor.view().graph();
    REQUIRE(graph.node_count() == 2);
    REQUIRE(graph.node_at(1).output(MIN_ST).valid());
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<Int>() == Int{82});
}

TEST_CASE("graph wiring: named brace initializer wires TSB fields by name")
{
    using namespace hgraph;

    GraphExecutorValue executor = run_start(build_graph<BraceBundleNamedInputGraph>());

    auto graph = executor.view().graph();
    REQUIRE(graph.node_count() == 3);
    REQUIRE(graph.node_at(2).output(MIN_ST).valid());
    CHECK(graph.node_at(2).output(MIN_ST).value().checked_as<Int>() == Int{43});
}

TEST_CASE("graph wiring: partial named TSB initializer fills missing fields with null sources")
{
    using namespace hgraph;

    GraphExecutorValue executor = run_start(build_graph<BraceBundlePartialInputGraph>());

    auto graph = executor.view().graph();
    REQUIRE(graph.node_count() == 2);
    REQUIRE(graph.node_at(1).output(MIN_ST).valid());
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<Int>() == Int{41});
}

TEST_CASE("graph wiring: nested node binds outer input into child graph input")
{
    using namespace hgraph;

    GraphBuilder graph_builder;
    graph_builder.label("outer_nested_add_one")
        .add_node(static_node_builder_for_constant_source())
        .add_node(nested_add_one_builder())
        .add_edge(GraphEdge{.source_node = 0, .target_node = 1, .target_path = {0}});

    GraphExecutorValue executor = run_start(std::move(graph_builder));

    auto graph = executor.view().graph();
    REQUIRE(graph.node_count() == 2);
    REQUIRE(graph.node_at(1).output(MIN_ST).valid());
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<Int>() == Int{42});
}

TEST_CASE("graph wiring: nested node forwards child output to downstream input")
{
    using namespace hgraph;

    GraphBuilder graph_builder;
    graph_builder.label("outer_nested_add_two")
        .add_node(static_node_builder_for_constant_source())
        .add_node(nested_add_one_builder())
        .add_node(static_node_builder_for_add_one())
        .add_edge(GraphEdge{.source_node = 0, .target_node = 1, .target_path = {0}})
        .add_edge(GraphEdge{.source_node = 1, .target_node = 2, .target_path = {0}});

    GraphExecutorValue executor = run_start(std::move(graph_builder));

    auto graph = executor.view().graph();
    REQUIRE(graph.node_count() == 3);
    REQUIRE(graph.node_at(2).output(MIN_ST).valid());
    CHECK(graph.node_at(2).output(MIN_ST).value().checked_as<Int>() == Int{43});
}

TEST_CASE("graph wiring: nested node propagates child graph schedule")
{
    using namespace hgraph;

    GraphBuilder graph_builder;
    graph_builder.label("outer_nested_constant").add_node(nested_constant_builder());

    GraphExecutorValue executor = run_start(std::move(graph_builder));

    auto graph = executor.view().graph();
    REQUIRE(graph.node_count() == 1);
    REQUIRE(graph.node_at(0).output(MIN_ST).valid());
    CHECK(graph.node_at(0).output(MIN_ST).value().checked_as<Int>() == Int{41});
}

TEST_CASE("graph wiring: REF output can bind back to a dereferenced TS input")
{
    using namespace hgraph;

    GraphBuilder graph_builder = build_graph<RefRoundTripGraph>();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + engine_time_delta_t{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    REQUIRE(graph.node_count() == 3);
    CHECK(graph.node_at(2).output(MIN_ST).value().checked_as<Int>() == Int{41});
}

TEST_CASE("graph wiring: REF round trip supports TSS")
{
    using namespace hgraph;

    GraphExecutorValue executor = run_start(build_graph<RefRoundTripFor<SetSource, TSS<Int>>>());

    auto graph  = executor.view().graph();
    auto output = graph.node_at(2).output(MIN_ST);
    auto set    = output.as_set();
    Value one{Int{1}};
    Value two{Int{2}};

    REQUIRE(graph.node_count() == 3);
    CHECK(set.size() == 2);
    CHECK(set.contains(one.view()));
    CHECK(set.contains(two.view()));
}

TEST_CASE("graph wiring: REF round trip supports TSD")
{
    using namespace hgraph;

    GraphExecutorValue executor = run_start(build_graph<RefRoundTripFor<DictSource, TSD<Int, TS<Int>>>>());

    auto graph  = executor.view().graph();
    auto output = graph.node_at(2).output(MIN_ST);
    auto dict   = output.as_dict();
    Value one{Int{1}};
    Value two{Int{2}};

    REQUIRE(graph.node_count() == 3);
    REQUIRE(dict.contains(one.view()));
    REQUIRE(dict.contains(two.view()));
    CHECK(dict.at(one.view()).value().checked_as<Int>() == Int{10});
    CHECK(dict.at(two.view()).value().checked_as<Int>() == Int{20});
}

TEST_CASE("graph wiring: REF round trip supports fixed TSL")
{
    using namespace hgraph;

    GraphExecutorValue executor = run_start(build_graph<RefRoundTripFor<ListSource, TSL<TS<Int>, 2>>>());

    auto graph  = executor.view().graph();
    auto output = graph.node_at(2).output(MIN_ST);
    auto list   = output.as_list();

    REQUIRE(graph.node_count() == 3);
    REQUIRE(list.size() == 2);
    CHECK(list.at(0).value().checked_as<Int>() == Int{7});
    CHECK(list.at(1).value().checked_as<Int>() == Int{8});
}

TEST_CASE("graph wiring: REF round trip supports TSB")
{
    using namespace hgraph;

    GraphExecutorValue executor = run_start(build_graph<RefRoundTripFor<BundleSource, RefRoundTripBundle>>());

    auto graph  = executor.view().graph();
    auto output = graph.node_at(2).output(MIN_ST);
    auto bundle = output.as_bundle();

    REQUIRE(graph.node_count() == 3);
    CHECK(bundle.field("a").value().checked_as<Int>() == Int{11});
    CHECK(bundle.field("b").value().checked_as<Int>() == Int{12});
}

TEST_CASE("graph wiring: REF round trip supports nested collection children")
{
    using namespace hgraph;

    GraphExecutorValue executor =
        run_start(build_graph<RefRoundTripFor<NestedListSetSource, TSL<TSS<Int>, 2>>>());

    auto graph         = executor.view().graph();
    auto output        = graph.node_at(2).output(MIN_ST);
    auto list          = output.as_list();
    auto first_child   = list.at(0);
    auto second_child  = list.at(1);
    auto first_values  = first_child.as_set();
    auto second_values = second_child.as_set();
    Value three{Int{3}};
    Value four{Int{4}};
    Value five{Int{5}};

    REQUIRE(graph.node_count() == 3);
    CHECK(first_values.size() == 2);
    CHECK(first_values.contains(three.view()));
    CHECK(first_values.contains(four.view()));
    CHECK(second_values.size() == 1);
    CHECK(second_values.contains(five.view()));
}

TEST_CASE("graph wiring: multi-input node wires and type-checks its ports")
{
    using namespace hgraph;

    GraphBuilder graph_builder = build_graph<SumGraph>();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + engine_time_delta_t{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    REQUIRE(graph.node_count() == 2);   // one interned source + sum
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<Int>() == Int{82});
}

TEST_CASE("graph wiring: a scalar argument configures a wired node")
{
    using namespace hgraph;

    GraphBuilder graph_builder = build_graph<ScaledSourceGraph>();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + engine_time_delta_t{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    REQUIRE(graph.node_count() == 1);
    CHECK(graph.node_at(0).output(MIN_ST).value().checked_as<Int>() == Int{7});
}

TEST_CASE("graph wiring: a scalar argument coexists with a time-series input port")
{
    using namespace hgraph;

    GraphBuilder graph_builder = build_graph<ShiftGraph>();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + engine_time_delta_t{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    REQUIRE(graph.node_count() == 2);   // source + shift
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<Int>() == Int{46});
}

TEST_CASE("graph wiring: scalar values participate in node interning")
{
    using namespace hgraph;

    Wiring w;
    auto   a = wire<ScaledSource>(w, Int{7});
    auto   b = wire<ScaledSource>(w, Int{7});   // equal scalar -> same interned instance
    auto   c = wire<ScaledSource>(w, Int{8});   // different scalar -> distinct instance

    CHECK(a.node() == b.node());
    CHECK(a.node() != c.node());

    GraphBuilder graph_builder = std::move(w).finish();
    GraphValue   graph         = graph_builder.make_graph();
    CHECK(graph.view().node_count() == 2);   // {7} deduped, {8} distinct
}

TEST_CASE("graph wiring: StaticGraphSignature reflects a graph's compose parameters")
{
    using namespace hgraph;

    using bare = StaticGraphSignature<AddOneGraph>;   // compose(Wiring &)
    STATIC_REQUIRE(bare::param_count() == 0);
    STATIC_REQUIRE(bare::input_count() == 0);
    STATIC_REQUIRE(bare::scalar_count() == 0);

    using configured = StaticGraphSignature<ConfiguredSourceGraph>;   // compose(Wiring &, Scalar<...>)
    STATIC_REQUIRE(configured::param_count() == 1);
    STATIC_REQUIRE(configured::input_count() == 0);
    STATIC_REQUIRE(configured::scalar_count() == 1);
}

TEST_CASE("graph wiring: a top-level graph takes a scalar parameter via build_graph")
{
    using namespace hgraph;

    GraphBuilder graph_builder = build_graph<ConfiguredSourceGraph>(Int{9});

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + engine_time_delta_t{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    REQUIRE(graph.node_count() == 1);
    CHECK(graph.node_at(0).output(MIN_ST).value().checked_as<Int>() == Int{9});
}

TEST_CASE("graph wiring: a graph scalar parameter threads into a node's scalar")
{
    using namespace hgraph;

    GraphBuilder graph_builder = build_graph<OffsetGraph>(Int{5});   // 41 + 5

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + engine_time_delta_t{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    REQUIRE(graph.node_count() == 2);
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<Int>() == Int{46});
}

TEST_CASE("graph wiring: wire<G> auto-wraps a scalar literal for a sub-graph parameter")
{
    using namespace hgraph;

    GraphBuilder graph_builder = build_graph<ShiftBySubGraph>();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + engine_time_delta_t{2});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    auto graph = executor_view.graph();
    REQUIRE(graph.node_count() == 2);   // source + shift (ShiftBy flattened away)
    CHECK(graph.node_at(1).output(MIN_ST).value().checked_as<Int>() == Int{46});
}
