// Operator overload dispatch (Phase 1): one logical name (`add_`) collecting several
// implementations, with the most specific selected at the `wire<>` call. Proves the
// runtime registry + TypePattern matcher end to end — specific beats generic, generic
// is the fallback, and no-match / ambiguity raise.

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/lib/std/std_nodes.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/type_pattern.h>
#include <hgraph/types/type_resolution.h>

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    // The operator: a name + general signature. Not executable — it only anchors the
    // overload set under the name "add".
    struct add_ : Operator<"add", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };

    // Specific implementation: integer add (rank 0 — fully concrete).
    struct add_ints
    {
        static constexpr auto name = "add_ints";
        static void           eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs, Out<TS<Int>> out)
        {
            out.set(lhs.value() + rhs.value());
        }
    };

    // A second integer add with the identical signature — used only to provoke an
    // ambiguity (two candidates tied at the minimum rank).
    struct add_ints_dup
    {
        static constexpr auto name = "add_ints_dup";
        static void           eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs, Out<TS<Int>> out)
        {
            out.set(lhs.value() + rhs.value());
        }
    };

    // Generic fallback (a stand-in for a real generic add): authored over a deferred
    // ``S`` and emits its left input — enough to prove it was the one selected. Rank is
    // high (two top-level variables), so a concrete impl always beats it.
    struct add_generic
    {
        static constexpr auto name = "add_generic";
        static void eval(In<"lhs", TsVar<"S">> lhs, In<"rhs", TsVar<"S">> rhs, Out<TsVar<"S">> out)
        {
            static_cast<void>(rhs);
            const Value delta = capture_delta(lhs.base());
            apply_delta(out, delta.view());
        }
    };

    // --- a scalar-argument operator: in * factor (exercises scalar matching + bundle) ---
    struct scale_ : Operator<"scale", In<"in", TsVar<"S">>, Scalar<"factor", ScalarVar<"T">>, Out<TsVar<"S">>>
    {
    };
    struct scale_int
    {
        static constexpr auto name = "scale_int";
        static void           eval(In<"in", TS<Int>> in, Scalar<"factor", Int> factor, Out<TS<Int>> out)
        {
            out.set(in.value() * factor.value());
        }
    };

    // --- a requires_-gated operator: the gate vetoes the specific overload ---
    struct gated_ : Operator<"gated", In<"in", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };
    struct gated_passthrough
    {
        static void eval(In<"in", TsVar<"S">> in, Out<TsVar<"S">> out)
        {
            const Value delta = capture_delta(in.base());
            apply_delta(out, delta.view());
        }
    };
    struct gated_int_on  // specific, accepted
    {
        static bool requires_(const ResolutionMap &) { return true; }
        static void eval(In<"in", TS<Int>> in, Out<TS<Int>> out) { out.set(in.value()); }
    };
    struct gated_int_off  // specific, vetoed
    {
        static bool requires_(const ResolutionMap &) { return false; }
        static void eval(In<"in", TS<Int>> in, Out<TS<Int>> out) { out.set(in.value()); }
    };

    struct sink_ : Operator<"sink", In<"ts", TsVar<"S">>>
    {
    };
    struct sink_any
    {
        static void eval(In<"ts", TsVar<"S">> ts) { static_cast<void>(ts); }
    };

    struct double_ : Operator<"double", In<"in", TS<Int>>, Out<TS<Int>>>
    {
    };
    struct double_graph
    {
        static constexpr auto name = "double_graph";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> in)
        {
            return wire<add_ints>(w, in, in);
        }
    };

    struct choose_ : Operator<"choose", In<"lhs", TS<Int>>, In<"rhs", TS<Int>>,
                                      Scalar<"side", Str>, Out<TS<Int>>>
    {
    };
    struct choose_lhs
    {
        static constexpr auto name = "choose_lhs";
        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            const Str *side = context.scalar_as<Str>("side");
            return side != nullptr && *side == "lhs";
        }
        static void eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs,
                         Scalar<"side", Str> side, Out<TS<Int>> out)
        {
            static_cast<void>(rhs);
            static_cast<void>(side);
            out.set(lhs.value());
        }
    };
    struct choose_rhs
    {
        static constexpr auto name = "choose_rhs";
        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            const Str *side = context.scalar_as<Str>("side");
            return side != nullptr && *side == "rhs";
        }
        static void eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs,
                         Scalar<"side", Str> side, Out<TS<Int>> out)
        {
            static_cast<void>(lhs);
            static_cast<void>(side);
            out.set(rhs.value());
        }
    };

    struct rank_ : Operator<"rank", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };
    struct rank_aligned
    {
        static constexpr auto name = "rank_aligned";
        static void eval(In<"lhs", TS<ScalarVar<"T">>> lhs, In<"rhs", TS<ScalarVar<"T">>> rhs,
                         Out<TS<ScalarVar<"T">>> out)
        {
            static_cast<void>(rhs);
            out.apply(lhs.base().value());
        }
    };
    struct rank_independent
    {
        static constexpr auto name = "rank_independent";
        static void eval(In<"lhs", TS<ScalarVar<"A">>> lhs, In<"rhs", TS<ScalarVar<"B">>> rhs,
                         Out<TS<ScalarVar<"A">>> out)
        {
            static_cast<void>(rhs);
            out.apply(lhs.base().value());
        }
    };

    struct constrained_ : Operator<"constrained", In<"in", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };
    struct constrained_int
    {
        static void eval(In<"in", TS<ScalarVar<"T", Int>>> in,
                         Out<TS<ScalarVar<"T", Int>>> out)
        {
            out.apply(in.base().value());
        }
    };

    struct echo_ : Operator<"echo", In<"in", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };
    [[nodiscard]] inline WiringArg ts_arg(const TSValueTypeMetaData *schema)
    {
        WiringArg arg;
        arg.kind        = WiringArg::Kind::TimeSeries;
        arg.port.schema = schema;
        return arg;
    }

    struct SinkGraph
    {
        static constexpr auto name = "sink_graph";
        static void           compose(Wiring &w)
        {
            auto a = wire<testing::replay, TS<Int>>(w, Str{"a"});
            wire<sink_>(w, a);
        }
    };

    struct ZeroGraph
    {
        static constexpr auto name = "zero_graph";
        static void           compose(Wiring &w)
        {
            auto z = wire<stdlib::zero_, TS<Int>>(w);
            wire<testing::record>(w, z, Str{"out"});
        }
    };

    struct EchoSetGraph
    {
        static constexpr auto name = "echo_set_graph";
        static void           compose(Wiring &w)
        {
            auto out = wire<echo_, TSS<Int>>(w, stdlib::make_set<Int>({Int{1}, Int{2}}));
            wire<testing::record>(w, out, Str{"out"});
        }
    };

    template <typename Graph, typename Seed>
    GraphExecutorValue run_graph(Seed seed)
    {
        GraphBuilder gb = build_graph<Graph>();
        seed(gb.global_state());
        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
        GraphExecutorValue ex = eb.make_executor();
        ex.view().run();
        return ex;
    }
}  // namespace

TEST_CASE("operators: the most specific overload is selected (TS<Int> beats the generic)")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    register_overload<add_, add_ints>();
    register_overload<add_, add_generic>();

    // add_ints ran (sums); had the generic won, the output would equal "a".
    CHECK_OUTPUT(eval_node<add_>(values<Int>(1, 2, 3), values<Int>(10, 20, 30)), values<Int>(11, 22, 33));
}

TEST_CASE("operators: the generic overload is the fallback when no specific one matches")
{
    (void)TypeRegistry::instance().register_scalar<Str>("str");
    register_overload<add_, add_ints>();     // rejects TS<Str>
    register_overload<add_, add_generic>();  // matches anything

    // add_generic ran: it emits its left input.
    CHECK_OUTPUT(eval_node<add_>(values<Str>(Str{"x"}, Str{"y"}), values<Str>(Str{"p"}, Str{"q"})),
                 values<Str>(Str{"x"}, Str{"y"}));
}

TEST_CASE("operators: no matching overload raises")
{
    (void)TypeRegistry::instance().register_scalar<Str>("str");
    register_overload<add_, add_ints>();  // only the int overload — nothing matches TS<Str>

    REQUIRE_THROWS_AS(eval_node<add_>(values<Str>(Str{"x"}), values<Str>(Str{"y"})), OperatorResolutionError);
}

TEST_CASE("operators: an ambiguous overload (tied at the minimum rank) raises")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    register_overload<add_, add_ints>();
    register_overload<add_, add_ints_dup>();  // identical signature -> same rank

    REQUIRE_THROWS_AS(eval_node<add_>(values<Int>(1), values<Int>(2)), OperatorResolutionError);
}

TEST_CASE("operators: an unregistered operator name raises")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    // No overloads registered this case (the registry is reset between cases).
    REQUIRE_THROWS_AS(eval_node<add_>(values<Int>(1), values<Int>(2)), OperatorResolutionError);
}

TEST_CASE("operators: a scalar argument is coerced and forwarded to the resolved node")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    register_overload<scale_, scale_int>();

    // The int32 factor is a wiring-time scalar argument; the resolved node coerces it to Int.
    CHECK_OUTPUT(eval_node<scale_>(values<Int>(1, 2, 3), std::int32_t{3}), values<Int>(3, 6, 9));
}

TEST_CASE("operators: a requires_ predicate that passes keeps the specific overload")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    register_overload<gated_, gated_int_on>();      // specific (rank 0), accepted
    register_overload<gated_, gated_passthrough>();  // generic fallback (high rank)

    std::array<WiringArg, 1> args{ts_arg(ts_type<TS<Int>>())};
    auto [impl, map] = OperatorRegistry::instance().resolve("gated", std::span<const WiringArg>{args});
    CHECK(impl->rank == 0);  // the specific overload won
}

TEST_CASE("operators: a requires_ predicate that fails vetoes the specific overload")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    register_overload<gated_, gated_int_off>();      // specific (rank 0), vetoed
    register_overload<gated_, gated_passthrough>();  // generic fallback (high rank)

    std::array<WiringArg, 1> args{ts_arg(ts_type<TS<Int>>())};
    auto [impl, map] = OperatorRegistry::instance().resolve("gated", std::span<const WiringArg>{args});
    CHECK(impl->rank > 0);  // the gate rejected the specific overload; the generic was selected
}

TEST_CASE("operators: the TypePattern interpreter matches and ranks a nested TSL")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    const TSValueTypeMetaData *concrete = ts_type<TSL<TS<Int>, 2>>();

    // TSL[TS[~T], 2] — a partially-generic nested pattern.
    const TypePattern pattern = to_pattern<TSL<TS<ScalarVar<"T">>, 2>>();
    ResolutionMap     map;
    REQUIRE(ts_pattern_match(pattern, concrete, map));
    CHECK(map.find_scalar("T") == scalar_type<Int>());                 // the inner variable bound to int
    CHECK(ts_pattern_rank(pattern) < ts_pattern_rank(TypePattern::var("S")));  // nested var beats a bare var

    // A different fixed size does not match.
    ResolutionMap other;
    CHECK_FALSE(ts_pattern_match(to_pattern<TSL<TS<ScalarVar<"T">>, 3>>(), concrete, other));
}

TEST_CASE("operators: a repeated type-variable name forces the operands to be aligned")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    (void)TypeRegistry::instance().register_scalar<Float>("float");
    // add_generic is In<~S>, In<~S>, Out<~S> — the repeated ``S`` aligns both operands.
    register_overload<add_, add_generic>();

    // Aligned operands (both TS<Int>): ``S`` binds consistently -> matches.
    {
        std::array<WiringArg, 2> args{ts_arg(ts_type<TS<Int>>()), ts_arg(ts_type<TS<Int>>())};
        auto [impl, map] = OperatorRegistry::instance().resolve("add", std::span<const WiringArg>{args});
        CHECK(impl != nullptr);
        CHECK(map.find_ts("S") == ts_type<TS<Int>>());
    }

    // Misaligned operands (TS<Int>, TS<Float>): ``S`` cannot be both -> no candidate.
    {
        std::array<WiringArg, 2> args{ts_arg(ts_type<TS<Int>>()), ts_arg(ts_type<TS<Float>>())};
        REQUIRE_THROWS_AS(OperatorRegistry::instance().resolve("add", std::span<const WiringArg>{args}),
                          OperatorResolutionError);
    }
}

TEST_CASE("operators: scalar values auto-wire as const inputs for TS parameters")
{
    register_overload<add_, add_ints>();

    // The Int{3} second argument is a scalar where add_ expects a TS input; it auto-wires as
    // a const TS<Int> and is added per cycle.
    CHECK_OUTPUT(eval_node<add_>(values<Int>(1, 2, 3), Int{3}), values<Int>(4, 5, 6));
}

TEST_CASE("operators: sink operators return void and do not expose a null output port")
{
    register_overload<sink_, sink_any>();

    auto ex = run_graph<SinkGraph>(
        [](const GlobalStateView &gs) { set_replay_values<Int>(gs, "a", {1, 2, 3}); });
    static_cast<void>(ex);
}

TEST_CASE("operators: graph implementations can be registered as overloads")
{
    register_graph_overload<double_, double_graph>();

    CHECK_OUTPUT(eval_node<double_>(values<Int>(1, 2, 3)), values<Int>(2, 4, 6));
}

TEST_CASE("operators: requires_ can inspect named scalar arguments")
{
    register_overload<choose_, choose_lhs>();
    register_overload<choose_, choose_rhs>();

    // The "side" scalar selects the rhs implementation via its requires_ predicate.
    CHECK_OUTPUT(eval_node<choose_>(values<Int>(1, 2, 3), values<Int>(10, 20, 30), Str{"rhs"}),
                 values<Int>(10, 20, 30));
}

TEST_CASE("operators: repeated generic variables rank ahead of independent variables")
{
    register_overload<rank_, rank_independent>();
    register_overload<rank_, rank_aligned>();

    std::array<WiringArg, 2> args{ts_arg(ts_type<TS<Int>>()), ts_arg(ts_type<TS<Int>>())};
    auto [impl, map] = OperatorRegistry::instance().resolve("rank", std::span<const WiringArg>{args});
    REQUIRE(impl != nullptr);
    CHECK(impl->label.find("rank_aligned") != std::string::npos);
}

TEST_CASE("operators: scalar variable constraints reject unsupported scalar types")
{
    register_overload<constrained_, constrained_int>();

    std::array<WiringArg, 1> int_args{ts_arg(ts_type<TS<Int>>())};
    auto [impl, map] = OperatorRegistry::instance().resolve("constrained", std::span<const WiringArg>{int_args});
    CHECK(impl != nullptr);
    CHECK(map.find_scalar("T") == scalar_type<Int>());

    std::array<WiringArg, 1> float_args{ts_arg(ts_type<TS<Float>>())};
    REQUIRE_THROWS_AS(OperatorRegistry::instance().resolve("constrained", std::span<const WiringArg>{float_args}),
                      OperatorResolutionError);

    ResolutionMap late_constraint;
    REQUIRE(ts_pattern_match(to_pattern<TS<ScalarVar<"T">>>(), ts_type<TS<Float>>(), late_constraint));
    CHECK_FALSE(ts_pattern_match(to_pattern<TS<ScalarVar<"T", Int>>>(), ts_type<TS<Float>>(), late_constraint));
}

TEST_CASE("operators: TypePattern matches generic TSW and TSB structures")
{
    {
        const TypePattern pattern = to_pattern<TSW<ScalarVar<"T">, 3, 1>>();
        ResolutionMap map;
        REQUIRE(ts_pattern_match(pattern, ts_type<TSW<Int, 3, 1>>(), map));
        CHECK(map.find_scalar("T") == scalar_type<Int>());
        ResolutionMap other;
        CHECK_FALSE(ts_pattern_match(pattern, ts_type<TSW<Int, 4, 1>>(), other));
    }

    {
        using GenericBundle = UnNamedTSB<Field<"x", TS<ScalarVar<"T">>>,
                                         Field<"w", TSW<ScalarVar<"T">, 3, 1>>>;
        using ConcreteBundle = UnNamedTSB<Field<"x", TS<Int>>, Field<"w", TSW<Int, 3, 1>>>;
        const TypePattern pattern = to_pattern<GenericBundle>();
        ResolutionMap map;
        REQUIRE(ts_pattern_match(pattern, ts_type<ConcreteBundle>(), map));
        CHECK(map.find_scalar("T") == scalar_type<Int>());
    }
}

TEST_CASE("operators: explicit output schemas participate in operator resolution")
{
    register_overload<stdlib::zero_, stdlib::zero_int>();

    auto ex = run_graph<ZeroGraph>([](const GlobalStateView &) {});
    CHECK_OUTPUT(get_recorded_values<Int>(ex.view().graph().global_state(), "out"), {0});
}

TEST_CASE("operators: explicit collection output schema drives scalar auto-const matching")
{
    register_overload<echo_, stdlib::pass_through_node>();

    auto ex = run_graph<EchoSetGraph>([](const GlobalStateView &) {});
    const auto out = get_recorded_deltas(ex.view().graph().global_state(), "out");
    REQUIRE(out.size() == 1);
    REQUIRE(out[0].has_value());
    CHECK(out[0]->equals(set_delta<Int>({1, 2}, {})));
}
