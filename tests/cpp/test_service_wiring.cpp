#include <catch2/catch_test_macros.hpp>

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/service_wiring.h>
#include <hgraph/types/static_node.h>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    struct PricesService
    {
        static constexpr std::string_view name{"prices"};
        using key_type     = Int;
        using value_schema = TS<Int>;
    };

    struct PricesImplNode
    {
        static constexpr auto name = "prices_impl_node";

        static void eval(In<"keys", TSS<Int>, InputValidity::Unchecked> keys,
                         Out<TSD<Int, TS<Int>>> out)
        {
            if (!keys.valid()) { return; }

            auto mutation = out.begin_mutation(out.evaluation_time());
            for (Int removed : keys.removed()) { static_cast<void>(mutation.erase(Value{removed}.view())); }
            for (Int key : keys.values())
            {
                Value key_value{key};
                Value price{key * Int{10}};
                mutation.set(key_value.view(), price.view());
            }
        }
    };

    struct PricesImpl
    {
        [[maybe_unused]] static constexpr auto name = "prices_impl";

        static Port<TSD<Int, TS<Int>>> compose(Wiring &w, Port<TSS<Int>> keys)
        {
            return wire<PricesImplNode>(w, keys).as<TSD<Int, TS<Int>>>();
        }
    };

    struct PriceClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "price_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> instrument)
        {
            auto prices = service::subscription_service_impl<PricesService, PricesImpl>(w);
            return prices(instrument);
        }
    };

    struct RegisteredPriceClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "registered_price_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> instrument)
        {
            service::register_subscription_service<PricesService, PricesImpl>(w);
            auto prices = service::subscription_service<PricesService>(w);
            return prices(instrument);
        }
    };
}  // namespace

TEST_CASE("service wiring: subscription client reads implementation output by reference")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<PriceClientGraph>(values<Int>(7, none, 8)),
                 values<Int>(none, 70, none, 80));
}

TEST_CASE("service wiring: implementation registration is separate from client use")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<RegisteredPriceClientGraph>(values<Int>(7, none, 8)),
                 values<Int>(none, 70, none, 80));
}
