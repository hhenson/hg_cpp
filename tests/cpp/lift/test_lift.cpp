#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/lift.h>
#include <hgraph/types/wired_fn.h>

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    [[nodiscard]] Int value_as_int(Value value)
    {
        return value.view().checked_as<Int>();
    }

    struct LiftedFormat
    {
        static constexpr const char *name = "lifted_format";
        static constexpr std::array<std::string_view, 2> parameter_names{"a", "b"};

        [[nodiscard]] static Str apply(Int a, const Str &b) { return std::to_string(a) + ":" + b; }
    };

    struct LiftedFormatGraph
    {
        static constexpr auto name = "lifted_format_graph";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Int>> a, Port<TS<Str>> b)
        {
            const WiredFn f = lift<LiftedFormat>();
            const std::array<WiringPortRef, 2> args{a.erased(), b.erased()};
            return Port<TS<Str>>{w, f.wire(w, std::span<const WiringPortRef>{args.data(), args.size()})};
        }
    };

    struct LiftedAddNoIdentity
    {
        static constexpr const char *name = "lifted_add_no_identity";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};
        static constexpr bool associative = true;
        static constexpr bool commutative = true;

        [[nodiscard]] static Int apply(Int lhs, Int rhs) { return lhs + rhs; }
    };
}  // namespace

TEST_CASE("lift: a scalar function wires as a time-series compute node")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<LiftedFormatGraph>(values<Int>(1, 2), values<Str>(Str{"x"}, Str{"y"})),
                 values<Str>(Str{"1:x"}, Str{"2:y"}));
}

TEST_CASE("lift: explicit identity is kernel metadata and part of function identity")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    const WiredFn no_identity = lift<LiftedAddNoIdentity>();
    REQUIRE(no_identity.lifted != nullptr);
    CHECK_FALSE(no_identity.lifted->has_identity());
    CHECK_THROWS_AS(no_identity.lifted->identity_value(), std::logic_error);

    const WiredFn explicit_zero = lift<LiftedAddNoIdentity, Int{0}>();
    REQUIRE(explicit_zero.lifted != nullptr);
    CHECK(explicit_zero.lifted->has_identity());
    CHECK(value_as_int(explicit_zero.lifted->identity_value()) == Int{0});

    const WiredFn explicit_five = lift<LiftedAddNoIdentity, Int{5}>();
    REQUIRE(explicit_five.lifted != nullptr);
    CHECK(explicit_five.lifted->has_identity());
    CHECK(value_as_int(explicit_five.lifted->identity_value()) == Int{5});

    CHECK(explicit_zero == lift<LiftedAddNoIdentity, Int{0}>());
    CHECK_FALSE(explicit_zero == no_identity);
    CHECK_FALSE(explicit_zero == explicit_five);

    Value lhs{Int{2}};
    Value rhs{Int{3}};
    std::array<ValueView, 2> args{lhs.view(), rhs.view()};
    CHECK(value_as_int(explicit_zero.lifted->eval(std::span<const ValueView>{args.data(), args.size()})) ==
          Int{5});
}

TEST_CASE("lift: explicit identity overrides a function-provided identity")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    const WiredFn function_identity = lift<stdlib::scalar_add<Int>>();
    REQUIRE(function_identity.lifted != nullptr);
    CHECK(function_identity.lifted->has_identity());
    CHECK(value_as_int(function_identity.lifted->identity_value()) == Int{0});

    const WiredFn explicit_identity = lift<stdlib::scalar_add<Int>, Int{7}>();
    REQUIRE(explicit_identity.lifted != nullptr);
    CHECK(explicit_identity.lifted->has_identity());
    CHECK(value_as_int(explicit_identity.lifted->identity_value()) == Int{7});
    CHECK_FALSE(explicit_identity == function_identity);
}
