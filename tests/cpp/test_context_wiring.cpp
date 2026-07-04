#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/context_wiring.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

// The user-facing context wiring surface (design record: services.rst,
// *Contexts*): context::scope<"name"> publishes a port for a wiring scope;
// Context<"name", S> signature inputs and context::get resolve the nearest
// publication. Approved 2026-07-04.

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    struct DoubledConsumer
    {
        static constexpr auto name = "doubled_consumer";

        static void eval(Context<"price", TS<Int>> price, Out<TS<Int>> out)
        {
            out.set(price.value() * 2);
        }
    };

    // A consumer mixing a caller input with a context input: the caller passes
    // ONLY ``scale`` — ``price`` binds from the ambient context.
    struct ScaledConsumer
    {
        static constexpr auto name = "scaled_consumer";

        static void eval(In<"scale", TS<Int>> scale, Context<"price", TS<Int>> price, Out<TS<Int>> out)
        {
            out.set(scale.value() * price.value());
        }
    };

    // A generic consumer: the context schema resolves from whatever is published.
    struct EchoConsumer
    {
        static constexpr auto name = "echo_consumer";

        static void eval(Context<"price", TS<ScalarVar<"T">>> price, Out<TS<ScalarVar<"T">>> out)
        {
            const Value delta = capture_delta(price.base());
            apply_delta(out, delta.view());
        }
    };

    struct ContextGraph
    {
        [[maybe_unused]] static constexpr auto name = "context_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> price)
        {
            context::scope<"price"> ctx{w, price};
            return wire<DoubledConsumer>(w);   // no args: price binds from the context
        }
    };

    struct MixedArgsGraph
    {
        [[maybe_unused]] static constexpr auto name = "mixed_args_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> scale, Port<TS<Int>> price)
        {
            context::scope<"price"> ctx{w, price};
            return wire<ScaledConsumer>(w, scale);   // positional arg maps to ``scale``, not ``price``
        }
    };

    struct ShadowingGraph
    {
        [[maybe_unused]] static constexpr auto name = "shadowing_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> outer, Port<TS<Int>> inner)
        {
            context::scope<"price"> outer_scope{w, outer};
            Port<TS<Int>>           result{};
            {
                context::scope<"price"> inner_scope{w, inner};
                result = wire<DoubledConsumer>(w);   // nearest wins: doubles ``inner``
            }
            // After the inner scope pops, the outer publication is visible again.
            auto again = context::get<TS<Int>>(w, "price");
            static_cast<void>(again);
            return result;
        }
    };

    struct OverrideGraph
    {
        [[maybe_unused]] static constexpr auto name = "override_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ambient, Port<TS<Int>> explicit_price)
        {
            context::scope<"price"> ctx{w, ambient};
            // An explicit keyword argument overrides the ambient context.
            return wire<DoubledConsumer>(w, arg<"price">(explicit_price));
        }
    };

    struct GetFormGraph
    {
        [[maybe_unused]] static constexpr auto name = "get_form_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> price)
        {
            context::scope<"price"> ctx{w, price};
            REQUIRE(context::has(w, "price"));
            REQUIRE_FALSE(context::has(w, "volatility"));
            auto looked_up = context::get<TS<Int>>(w, "price");
            return wire<DoubledConsumer>(w, arg<"price">(looked_up));
        }
    };

    struct GenericContextGraph
    {
        [[maybe_unused]] static constexpr auto name = "generic_context_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> price)
        {
            context::scope<"price"> ctx{w, price};
            return wire<EchoConsumer>(w).as<TS<Int>>();
        }
    };

    struct MissingContextGraph
    {
        [[maybe_unused]] static constexpr auto name = "missing_context_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> price)
        {
            static_cast<void>(price);
            return wire<DoubledConsumer>(w);   // no scope published: wiring error
        }
    };
}  // namespace

TEST_CASE("context wiring: a Context input binds from the enclosing scope")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<ContextGraph>(values<Int>(3, none, 5)), values<Int>(6, none, 10));
}

TEST_CASE("context wiring: positional caller args skip Context params")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<MixedArgsGraph>(values<Int>(2, 3), values<Int>(10, none)),
                 values<Int>(20, 30));
}

TEST_CASE("context wiring: nested scopes shadow, nearest publication wins")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<ShadowingGraph>(values<Int>(1), values<Int>(100)), values<Int>(200));
}

TEST_CASE("context wiring: an explicit keyword argument overrides the ambient context")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<OverrideGraph>(values<Int>(1), values<Int>(7)), values<Int>(14));
}

TEST_CASE("context wiring: context::get and context::has function forms")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<GetFormGraph>(values<Int>(4)), values<Int>(8));
}

TEST_CASE("context wiring: a generic Context input resolves from the published schema")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<GenericContextGraph>(values<Int>(9)), values<Int>(9));
}

TEST_CASE("context wiring: a missing context is a wiring error naming the context")
{
    stdlib::register_standard_operators();
    CHECK_THROWS_WITH((void)eval_node<MissingContextGraph>(values<Int>(1)),
                      Catch::Matchers::ContainsSubstring("price"));
}
