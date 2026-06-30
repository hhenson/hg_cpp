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

    struct ReferencePricesAltImplNode
    {
        static constexpr auto name              = "reference_prices_alt_impl_node";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<TSD<Int, TS<Int>>> out)
        {
            auto mutation = out.begin_mutation(out.evaluation_time());
            Value key_7{Int{7}};
            Value price_7{Int{700}};
            Value key_8{Int{8}};
            Value price_8{Int{800}};
            mutation.set(key_7.view(), price_7.view());
            mutation.set(key_8.view(), price_8.view());
        }
    };

    struct ReferencePricesPathImplNode
    {
        static constexpr auto name              = "reference_prices_path_impl_node";
        static constexpr bool schedule_on_start = true;

        static void eval(Scalar<"path", Str> path, Out<TSD<Int, TS<Int>>> out)
        {
            auto mutation = out.begin_mutation(out.evaluation_time());
            Value key{Int{7}};
            Value price{path.value() == "premium" ? Int{777} : Int{70}};
            mutation.set(key.view(), price.view());
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

    struct PricesPathImplNode
    {
        static constexpr auto name = "prices_path_impl_node";

        static void eval(Scalar<"path", Str> path,
                         In<"keys", TSS<Int>, InputValidity::Unchecked> keys,
                         Out<TSD<Int, TS<Int>>> out)
        {
            if (!keys.valid()) { return; }

            const Int multiplier = path.value() == "premium" ? Int{100} : Int{10};
            auto mutation = out.begin_mutation(out.evaluation_time());
            for (Int removed : keys.removed()) { static_cast<void>(mutation.erase(Value{removed}.view())); }
            for (Int key : keys.values())
            {
                Value key_value{key};
                Value price{key * multiplier};
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

    struct ReferencePricesAltImpl
    {
        [[maybe_unused]] static constexpr auto name = "reference_prices_alt_impl";

        static Port<TSD<Int, TS<Int>>> compose(Wiring &w)
        {
            return wire<ReferencePricesAltImplNode>(w).as<TSD<Int, TS<Int>>>();
        }
    };

    struct ReferencePricePathInjectionGraph
    {
        [[maybe_unused]] static constexpr auto name = "reference_price_path_injection_graph";

        static Port<TS<Int>> compose(Wiring &w)
        {
            service::register_reference_service<ReferencePricesService, ReferencePricesPathImplNode>(
                w, service::path("premium"));
            auto prices = service::reference_service<ReferencePricesService>(w, service::path("premium"));
            return wire<stdlib::getitem_>(w, prices, Int{7}).as<TS<Int>>();
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

    struct ReferencePricePathClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "reference_price_path_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> instrument)
        {
            service::register_reference_service<ReferencePricesService, ReferencePricesImpl>(
                w, service::path("primary"));
            service::register_reference_service<ReferencePricesService, ReferencePricesAltImpl>(
                w, service::path("secondary"));
            auto prices = service::reference_service<ReferencePricesService>(w, service::path("secondary"));
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

    struct PathPriceClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "path_price_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> instrument)
        {
            service::register_subscription_service<PricesService, PricesPathImplNode>(w, service::path("premium"));
            auto prices = service::subscription_service<PricesService>(w, service::path("premium"));
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

TEST_CASE("service wiring: reference service paths keep shared outputs separate")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<ReferencePricePathClientGraph>(values<Int>(7, none, 8)),
                 values<Int>(700, none, 800));
}

TEST_CASE("service wiring: reference implementation can receive the service path scalar")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<ReferencePricePathInjectionGraph>(), values<Int>(777));
}

TEST_CASE("service wiring: subscription client reads implementation output by reference")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<PriceClientGraph>(values<Int>(7, none, 8)),
                 values<Int>(none, 70, none, 80));
}

TEST_CASE("service wiring: subscription service supports explicit paths")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<PathPriceClientGraph>(values<Int>(7, none, 8)),
                 values<Int>(none, 700, none, 800));
}

TEST_CASE("service wiring: implementation registration is separate from client use")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<RegisteredPriceClientGraph>(values<Int>(7, none, 8)),
                 values<Int>(none, 70, none, 80));
}
