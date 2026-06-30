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

    struct ReferencePricesService
    {
        static constexpr std::string_view name{"reference_prices"};
        using output_schema = TSD<Int, TS<Int>>;
    };

    struct ReferencePricesImplNode
    {
        static constexpr auto name              = "reference_prices_impl_node";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<TSD<Int, TS<Int>>> out)
        {
            auto mutation = out.begin_mutation(out.evaluation_time());
            Value key_7{Int{7}};
            Value price_7{Int{70}};
            Value key_8{Int{8}};
            Value price_8{Int{80}};
            mutation.set(key_7.view(), price_7.view());
            mutation.set(key_8.view(), price_8.view());
        }
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

    struct ReferencePricesImpl
    {
        [[maybe_unused]] static constexpr auto name = "reference_prices_impl";

        static Port<TSD<Int, TS<Int>>> compose(Wiring &w)
        {
            return wire<ReferencePricesImplNode>(w).as<TSD<Int, TS<Int>>>();
        }
    };

    struct ReferencePriceClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "reference_price_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> instrument)
        {
            service::register_reference_service<ReferencePricesService, ReferencePricesImpl>(w);
            auto prices = service::reference_service<ReferencePricesService>(w);
            return wire<stdlib::getitem_>(w, prices, instrument).as<TS<Int>>();
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

TEST_CASE("service wiring: reference service client reads implementation output by reference")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<ReferencePriceClientGraph>(values<Int>(7, none, 8)),
                 values<Int>(70, none, 80));
}

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
