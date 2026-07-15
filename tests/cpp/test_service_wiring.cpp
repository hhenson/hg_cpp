#include <catch2/catch_test_macros.hpp>

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/service_wiring.h>
#include <hgraph/types/static_node.h>

#include <stdexcept>
#include <string>

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

    struct AddOneService
    {
        static constexpr std::string_view name{"add_one"};
        using request_schema  = TS<Int>;
        using response_schema = TS<Int>;
    };

    struct AddTenService
    {
        static constexpr std::string_view name{"add_ten"};
        using request_schema  = TS<Int>;
        using response_schema = TS<Int>;
    };

    struct BaseValueService
    {
        static constexpr std::string_view name{"base_value"};
        using output_schema = TS<Int>;
    };

    struct DerivedValueService
    {
        static constexpr std::string_view name{"derived_value"};
        using output_schema = TS<Int>;
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

    struct TenSourceNode
    {
        static constexpr auto name              = "ten_source_node";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<TS<Int>> out) { out.set(Int{10}); }
    };

    struct AddOneValueNode
    {
        static constexpr auto name = "add_one_value_node";

        static void eval(In<"value", TS<Int>> value, Out<TS<Int>> out)
        {
            out.set(value.value() + Int{1});
        }
    };

    struct BaseValueImpl
    {
        [[maybe_unused]] static constexpr auto name = "base_value_impl";

        static Port<TS<Int>> compose(Wiring &w)
        {
            return wire<TenSourceNode>(w);
        }
    };

    struct DerivedValueImpl
    {
        [[maybe_unused]] static constexpr auto name = "derived_value_impl";

        static Port<TS<Int>> compose(Wiring &w)
        {
            auto base = wire<BaseValueService>(w);
            return wire<AddOneValueNode>(w, base);
        }
    };

    struct TypedReferencePricesPathImplNode
    {
        static constexpr auto name              = "typed_reference_prices_path_impl_node";
        static constexpr bool schedule_on_start = true;

        static void eval(Scalar<"path", Str> path, Out<TSD<Int, TS<Int>>> out)
        {
            auto mutation = out.begin_mutation(out.evaluation_time());
            Value key{Int{7}};
            Value price{path.value().find("premium") != std::string::npos ? Int{777} : Int{70}};
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

    struct AddOneImplNode
    {
        static constexpr auto name = "add_one_impl_node";

        static void eval(In<"requests", TSD<Int, TS<Int>>, InputValidity::Unchecked> requests,
                         Out<TSD<Int, TS<Int>>> out)
        {
            if (!requests.modified()) { return; }

            auto mutation = out.begin_mutation(out.evaluation_time());
            for (const auto &[request_id, request] : requests.removed_items())
            {
                (void)request;
                static_cast<void>(mutation.erase(request_id));
            }
            for (const auto &[request_id, request] : requests.modified_items())
            {
                if (!request.valid())
                {
                    static_cast<void>(mutation.erase(request_id));
                    continue;
                }

                Value response{request.value() + Int{1}};
                mutation.set(request_id, response.view());
            }
        }
    };

    struct AddOnePathImplNode
    {
        static constexpr auto name = "add_one_path_impl_node";

        static void eval(Scalar<"path", Str> path,
                         In<"requests", TSD<Int, TS<Int>>, InputValidity::Unchecked> requests,
                         Out<TSD<Int, TS<Int>>> out)
        {
            if (!requests.modified()) { return; }

            const Int addend = path.value() == "premium" ? Int{100} : Int{1};
            auto mutation = out.begin_mutation(out.evaluation_time());
            for (const auto &[request_id, request] : requests.removed_items())
            {
                (void)request;
                static_cast<void>(mutation.erase(request_id));
            }
            for (const auto &[request_id, request] : requests.modified_items())
            {
                if (!request.valid())
                {
                    static_cast<void>(mutation.erase(request_id));
                    continue;
                }

                Value response{request.value() + addend};
                mutation.set(request_id, response.view());
            }
        }
    };

    struct AddTenImplNode
    {
        static constexpr auto name = "add_ten_impl_node";

        static void eval(In<"requests", TSD<Int, TS<Int>>, InputValidity::Unchecked> requests,
                         Out<TSD<Int, TS<Int>>> out)
        {
            if (!requests.modified()) { return; }

            auto mutation = out.begin_mutation(out.evaluation_time());
            for (const auto &[request_id, request] : requests.removed_items())
            {
                (void)request;
                static_cast<void>(mutation.erase(request_id));
            }
            for (const auto &[request_id, request] : requests.modified_items())
            {
                if (!request.valid())
                {
                    static_cast<void>(mutation.erase(request_id));
                    continue;
                }

                Value response{request.value() + Int{10}};
                mutation.set(request_id, response.view());
            }
        }
    };

    struct AddTwentyServiceAdaptor : service_adaptor::interface
    {
        static constexpr std::string_view name{"add_twenty_adaptor"};
        using input_schema  = TS<Int>;
        using output_schema = TS<Int>;
    };

    struct AddThirtyServiceAdaptor : service_adaptor::interface
    {
        static constexpr std::string_view name{"add_thirty_adaptor"};
        using input_schema  = TS<Int>;
        using output_schema = TS<Int>;
    };

    struct GenericAddOneService
    {
        static constexpr std::string_view name{"generic_add_one"};
        using request_schema  = TS<ScalarVar<"NUMBER", Int, Float>>;
        using response_schema = TS<ScalarVar<"NUMBER", Int, Float>>;
    };

    struct GenericServiceAdaptor : service_adaptor::interface
    {
        static constexpr std::string_view name{"generic_service_adaptor"};
        using input_schema  = TS<ScalarVar<"T">>;
        using output_schema = TS<ScalarVar<"T">>;
    };

    struct AddTwentyServiceAdaptorImplNode
    {
        static constexpr auto name = "add_twenty_service_adaptor_impl_node";

        static void eval(In<"requests", TSD<Int, TS<Int>>, InputValidity::Unchecked> requests,
                         Out<TSD<Int, TS<Int>>> out)
        {
            if (!requests.modified()) { return; }

            auto mutation = out.begin_mutation(out.evaluation_time());
            for (const auto &[request_id, request] : requests.removed_items())
            {
                (void)request;
                static_cast<void>(mutation.erase(request_id));
            }
            for (const auto &[request_id, request] : requests.modified_items())
            {
                if (!request.valid())
                {
                    static_cast<void>(mutation.erase(request_id));
                    continue;
                }

                Value response{request.value() + Int{20}};
                mutation.set(request_id, response.view());
            }
        }
    };

    struct AddThirtyServiceAdaptorImplNode
    {
        static constexpr auto name = "add_thirty_service_adaptor_impl_node";

        static void eval(In<"requests", TSD<Int, TS<Int>>, InputValidity::Unchecked> requests,
                         Out<TSD<Int, TS<Int>>> out)
        {
            if (!requests.modified()) { return; }

            auto mutation = out.begin_mutation(out.evaluation_time());
            for (const auto &[request_id, request] : requests.modified_items())
            {
                if (!request.valid())
                {
                    static_cast<void>(mutation.erase(request_id));
                    continue;
                }

                Value response{request.value() + Int{30}};
                mutation.set(request_id, response.view());
            }
        }
    };

    struct AddTwentyServiceAdaptorImpl
    {
        [[maybe_unused]] static constexpr auto name = "add_twenty_service_adaptor_impl";

        static void compose(Wiring &w, Scalar<"path", Str> path)
        {
            const auto custom = service_adaptor::path(path.value());
            auto requests = service_adaptor::from_graph<AddTwentyServiceAdaptor>(w, custom);
            auto replies = wire<AddTwentyServiceAdaptorImplNode>(w, requests).as<TSD<Int, TS<Int>>>();
            service_adaptor::to_graph<AddTwentyServiceAdaptor>(w, custom, replies);
        }
    };

    struct MultiServiceAdaptorImpl
    {
        [[maybe_unused]] static constexpr auto name = "multi_service_adaptor_impl";

        static void compose(Wiring &w, Scalar<"path", Str> path)
        {
            const auto custom = service_adaptor::path(path.value());
            auto add_twenty_requests = service_adaptor::from_graph<AddTwentyServiceAdaptor>(w, custom);
            auto add_thirty_requests = service_adaptor::from_graph<AddThirtyServiceAdaptor>(w, custom);
            auto add_twenty_replies = wire<AddTwentyServiceAdaptorImplNode>(w, add_twenty_requests)
                .as<TSD<Int, TS<Int>>>();
            auto add_thirty_replies = wire<AddThirtyServiceAdaptorImplNode>(w, add_thirty_requests)
                .as<TSD<Int, TS<Int>>>();
            service_adaptor::to_graph<AddTwentyServiceAdaptor>(w, custom, add_twenty_replies);
            service_adaptor::to_graph<AddThirtyServiceAdaptor>(w, custom, add_thirty_replies);
        }
    };

    struct GenericServiceAdaptorImpl
    {
        [[maybe_unused]] static constexpr auto name = "generic_service_adaptor_impl";

        static Port<TSD<Int, TS<Int>>> compose(Wiring &w, Port<TSD<Int, TS<ScalarVar<"T">>>> requests)
        {
            return wire<AddTwentyServiceAdaptorImplNode>(w, requests.as<TSD<Int, TS<Int>>>())
                .as<TSD<Int, TS<Int>>>();
        }
    };

    struct MissingServiceAdaptorOutputImpl
    {
        [[maybe_unused]] static constexpr auto name = "missing_service_adaptor_output_impl";

        static void compose(Wiring &w, Scalar<"path", Str> path)
        {
            const auto custom = service_adaptor::path(path.value());
            (void)service_adaptor::from_graph<AddTwentyServiceAdaptor>(w, custom);
        }
    };

    struct MissingMultiServiceOutputImpl
    {
        [[maybe_unused]] static constexpr auto name = "missing_multi_service_output_impl";

        static void compose(Wiring &w, Scalar<"path", Str> path)
        {
            const auto custom = service::path(path.value());
            (void)service::impl_input<AddOneService>(w, custom);
        }
    };

    struct GenericAddOneImpl
    {
        [[maybe_unused]] static constexpr auto name = "generic_add_one_impl";

        static Port<TSD<Int, TS<Int>>> compose(
            Wiring &w, Port<TSD<Int, TS<ScalarVar<"NUMBER", Int, Float>>>> requests)
        {
            return wire<AddOneImplNode>(w, requests.as<TSD<Int, TS<Int>>>()).as<TSD<Int, TS<Int>>>();
        }
    };

    struct AddHalfImplNode
    {
        static constexpr auto name = "add_half_impl_node";

        static void eval(In<"requests", TSD<Int, TS<Float>>, InputValidity::Unchecked> requests,
                         Out<TSD<Int, TS<Float>>> out)
        {
            if (!requests.modified()) { return; }

            auto mutation = out.begin_mutation(out.evaluation_time());
            for (const auto &[request_id, request] : requests.removed_items())
            {
                (void)request;
                static_cast<void>(mutation.erase(request_id));
            }
            for (const auto &[request_id, request] : requests.modified_items())
            {
                if (!request.valid())
                {
                    static_cast<void>(mutation.erase(request_id));
                    continue;
                }
                Value response{request.value() + Float{0.5}};
                mutation.set(request_id, response.view());
            }
        }
    };

    struct GenericAddHalfImpl
    {
        [[maybe_unused]] static constexpr auto name = "generic_add_half_impl";

        static Port<TSD<Int, TS<Float>>> compose(
            Wiring &w, Port<TSD<Int, TS<ScalarVar<"NUMBER", Int, Float>>>> requests)
        {
            return wire<AddHalfImplNode>(w, requests.as<TSD<Int, TS<Float>>>())
                .as<TSD<Int, TS<Float>>>();
        }
    };

    template <typename T>
    struct TemplateAddService
    {
        static constexpr std::string_view name{"template_add"};
        using request_schema  = TS<T>;
        using response_schema = TS<T>;
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
            auto prices = wire<ReferencePricesService>(w, service::path("premium"));
            return wire<stdlib::getitem_>(w, prices, Int{7}).as<TS<Int>>();
        }
    };

    struct ReferenceServiceDependencyGraph
    {
        [[maybe_unused]] static constexpr auto name = "reference_service_dependency_graph";

        static Port<TS<Int>> compose(Wiring &w)
        {
            service::register_reference_service<DerivedValueService, DerivedValueImpl>(w);
            service::register_reference_service<BaseValueService, BaseValueImpl>(w);
            return wire<DerivedValueService>(w);
        }
    };

    struct TypedReferencePricePathGraph
    {
        [[maybe_unused]] static constexpr auto name = "typed_reference_price_path_graph";

        static Port<TS<Int>> compose(Wiring &w)
        {
            service::register_reference_service<ReferencePricesService, TypedReferencePricesPathImplNode>(
                w, service::path("typed_prices", arg<"tier">(Str{"standard"})));
            service::register_reference_service<ReferencePricesService, TypedReferencePricesPathImplNode>(
                w, service::path("typed_prices", arg<"tier">(Str{"premium"})));
            service::register_reference_service<ReferencePricesService, TypedReferencePricesPathImplNode>(
                w, service::path("typed_prices", arg<"tier">(Str{"premium/special, value"})));
            auto prices = wire<ReferencePricesService>(
                w, service::path("typed_prices", arg<"tier">(Str{"premium/special, value"})));
            return wire<stdlib::getitem_>(w, prices, Int{7}).as<TS<Int>>();
        }
    };

    struct DuplicateReferenceServiceGraph
    {
        [[maybe_unused]] static constexpr auto name = "duplicate_reference_service_graph";

        static void compose(Wiring &w)
        {
            const auto custom = service::path("duplicate");
            service::register_reference_service<ReferencePricesService, ReferencePricesImpl>(w, custom);
            service::register_reference_service<ReferencePricesService, ReferencePricesAltImpl>(w, custom);
        }
    };

    struct ReferencePriceClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "reference_price_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> instrument)
        {
            service::register_reference_service<ReferencePricesService, ReferencePricesImpl>(w);
            auto prices = wire<ReferencePricesService>(w);
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
            auto prices = wire<ReferencePricesService>(w, service::path("secondary"));
            return wire<stdlib::getitem_>(w, prices, instrument).as<TS<Int>>();
        }
    };

    struct PriceClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "price_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> instrument)
        {
            service::register_subscription_service<PricesService, PricesImpl>(w);
            return wire<PricesService>(w, instrument);
        }
    };

    struct PathPriceClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "path_price_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> instrument)
        {
            service::register_subscription_service<PricesService, PricesPathImplNode>(w, service::path("premium"));
            return wire<PricesService>(w, service::path("premium"), instrument);
        }
    };

    struct RegisteredPriceClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "registered_price_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> instrument)
        {
            service::register_subscription_service<PricesService, PricesImpl>(w);
            return wire<PricesService>(w, instrument);
        }
    };

    struct AddOneClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "add_one_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> request)
        {
            service::register_request_reply_service<AddOneService, AddOneImplNode>(w);
            return wire<AddOneService>(w, request);
        }
    };

    struct AddOnePathClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "add_one_path_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> request)
        {
            service::register_request_reply_service<AddOneService, AddOnePathImplNode>(
                w, service::path("premium"));
            return wire<AddOneService>(w, service::path("premium"), request);
        }
    };

    struct AddOneTwoClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "add_one_two_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> lhs_request, Port<TS<Int>> rhs_request)
        {
            service::register_request_reply_service<AddOneService, AddOneImplNode>(w);
            auto lhs_reply = wire<AddOneService>(w, lhs_request);
            auto rhs_reply = wire<AddOneService>(w, rhs_request);
            return wire<stdlib::add_>(w, lhs_reply, rhs_reply).as<TS<Int>>();
        }
    };

    struct MissingServiceImplementationGraph
    {
        [[maybe_unused]] static constexpr auto name = "missing_service_implementation_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> request)
        {
            return wire<AddOneService>(w, request);
        }
    };

    struct IllegalServiceStubGraph
    {
        [[maybe_unused]] static constexpr auto name = "illegal_service_stub_graph";

        static void compose(Wiring &w)
        {
            (void)service::impl_input<AddOneService>(w);
        }
    };

    struct MissingMultiServiceStubGraph
    {
        [[maybe_unused]] static constexpr auto name = "missing_multi_service_stub_graph";

        static void compose(Wiring &w)
        {
            service::register_services<MissingMultiServiceOutputImpl, AddOneService>(w, service::path("missing"));
        }
    };

    struct MultiRequestReplyImpl
    {
        [[maybe_unused]] static constexpr auto name = "multi_request_reply_impl";

        static void compose(Wiring &w, Scalar<"path", Str> path)
        {
            const auto custom = service::path(path.value());
            auto add_one_requests = service::impl_input<AddOneService>(w, custom);
            auto add_ten_requests = service::impl_input<AddTenService>(w, custom);
            auto add_one_replies = wire<AddOneImplNode>(w, add_one_requests).as<TSD<Int, TS<Int>>>();
            auto add_ten_replies = wire<AddTenImplNode>(w, add_ten_requests).as<TSD<Int, TS<Int>>>();
            service::impl_output<AddOneService>(w, custom, add_one_replies);
            service::impl_output<AddTenService>(w, custom, add_ten_replies);
        }
    };

    struct MultiServiceClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "multi_service_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> request)
        {
            const auto custom = service::path("multi");
            service::register_services<MultiRequestReplyImpl, AddOneService, AddTenService>(w, custom);
            auto add_one = wire<AddOneService>(w, custom, request);
            auto add_ten = wire<AddTenService>(w, custom, request);
            return wire<stdlib::add_>(w, add_one, add_ten).as<TS<Int>>();
        }
    };

    struct ServiceAdaptorTwoClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "service_adaptor_two_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> lhs_request, Port<TS<Int>> rhs_request)
        {
            const auto custom = service_adaptor::path("multi_client");
            service_adaptor::register_service_adaptor<AddTwentyServiceAdaptor, AddTwentyServiceAdaptorImpl>(
                w, custom);
            auto lhs_reply = wire<AddTwentyServiceAdaptor>(w, custom, lhs_request);
            auto rhs_reply = wire<AddTwentyServiceAdaptor>(w, custom, rhs_request);
            return wire<stdlib::add_>(w, lhs_reply, rhs_reply).as<TS<Int>>();
        }
    };

    struct ServiceAdaptorImplTwoClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "service_adaptor_impl_two_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> lhs_request, Port<TS<Int>> rhs_request)
        {
            const auto custom = service_adaptor::path("multi_client_impl");
            service_adaptor::register_service_adaptor_impl<AddTwentyServiceAdaptor, AddTwentyServiceAdaptorImplNode>(
                w, custom);
            auto lhs_reply = wire<AddTwentyServiceAdaptor>(w, custom, lhs_request);
            auto rhs_reply = wire<AddTwentyServiceAdaptor>(w, custom, rhs_request);
            return wire<stdlib::add_>(w, lhs_reply, rhs_reply).as<TS<Int>>();
        }
    };

    struct MultiServiceAdaptorClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "multi_service_adaptor_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> request)
        {
            const auto custom = service_adaptor::path("multi_service_adaptor");
            service_adaptor::register_service_adaptors<
                MultiServiceAdaptorImpl, AddTwentyServiceAdaptor, AddThirtyServiceAdaptor>(w, custom);
            auto add_twenty = wire<AddTwentyServiceAdaptor>(w, custom, request);
            auto add_thirty = wire<AddThirtyServiceAdaptor>(w, custom, request);
            return wire<stdlib::add_>(w, add_twenty, add_thirty).as<TS<Int>>();
        }
    };

    struct GenericServiceAdaptorClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "generic_service_adaptor_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> request)
        {
            service_adaptor::register_service_adaptor_impl<
                GenericServiceAdaptor, GenericServiceAdaptorImpl>(
                    w, service_adaptor::path("generic_service_adaptor", arg<"T">(scalar_type<Int>())));
            return wire<GenericServiceAdaptor>(w, service_adaptor::path("generic_service_adaptor"), request)
                .as<TS<Int>>();
        }
    };

    struct MissingServiceAdaptorImplementationGraph
    {
        [[maybe_unused]] static constexpr auto name = "missing_service_adaptor_implementation_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> request)
        {
            return wire<AddTwentyServiceAdaptor>(w, service_adaptor::path("missing_service_adaptor"), request);
        }
    };

    struct IllegalServiceAdaptorStubGraph
    {
        [[maybe_unused]] static constexpr auto name = "illegal_service_adaptor_stub_graph";

        static void compose(Wiring &w)
        {
            (void)service_adaptor::from_graph<AddTwentyServiceAdaptor>(w);
        }
    };

    struct MissingServiceAdaptorStubGraph
    {
        [[maybe_unused]] static constexpr auto name = "missing_service_adaptor_stub_graph";

        static void compose(Wiring &w)
        {
            service_adaptor::register_service_adaptor<
                AddTwentyServiceAdaptor, MissingServiceAdaptorOutputImpl>(
                    w, service_adaptor::path("missing_stub"));
        }
    };

    struct TemplateServiceClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "template_service_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> request)
        {
            const auto typed = service::path("template", arg<"T">(Str{"Int"}));
            service::register_request_reply_service<TemplateAddService<Int>, AddOneImplNode>(w, typed);
            return wire<TemplateAddService<Int>>(w, typed, request);
        }
    };

    struct GenericServiceClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "generic_service_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> request)
        {
            service::register_request_reply_service<GenericAddOneService, GenericAddOneImpl>(
                w, service::path("generic", arg<"NUMBER">(scalar_type<Int>())));
            return wire<GenericAddOneService>(w, service::path("generic"), request).as<TS<Int>>();
        }
    };

    struct GenericFloatServiceClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "generic_float_service_client_graph";

        static Port<TS<Float>> compose(Wiring &w, Port<TS<Float>> request)
        {
            service::register_request_reply_service<GenericAddOneService, GenericAddHalfImpl>(
                w, service::path("generic", arg<"NUMBER">(scalar_type<Float>())));
            return wire<GenericAddOneService>(w, service::path("generic"), request).as<TS<Float>>();
        }
    };

    struct GenericStringServiceClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "generic_string_service_client_graph";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Str>> request)
        {
            return wire<GenericAddOneService>(w, service::path("generic"), request).as<TS<Str>>();
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

TEST_CASE("service wiring: service implementation can depend on a later registered service")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<ReferenceServiceDependencyGraph>(), values<Int>(11));
}

TEST_CASE("service wiring: scalar-qualified paths keep implementations separate")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<TypedReferencePricePathGraph>(), values<Int>(777));
}

TEST_CASE("service wiring: duplicate implementation registrations are rejected")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_THROWS_AS(build_graph<DuplicateReferenceServiceGraph>(), std::invalid_argument);
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

TEST_CASE("service wiring: request/reply client receives keyed implementation response")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<AddOneClientGraph>(values<Int>(1)), values<Int>(none, 2));
}

TEST_CASE("service wiring: request/reply service supports explicit paths")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<AddOnePathClientGraph>(values<Int>(7)), values<Int>(none, 107));
}

TEST_CASE("service wiring: request/reply source emits cumulative client requests")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<AddOneTwoClientGraph>(values<Int>(1), values<Int>(10)),
                 values<Int>(none, 13));
}

TEST_CASE("service wiring: validates missing implementations and illegal stubs")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_THROWS_AS((void)eval_node<MissingServiceImplementationGraph>(values<Int>(1)), std::invalid_argument);
    CHECK_THROWS_AS(build_graph<IllegalServiceStubGraph>(), std::invalid_argument);
    CHECK_THROWS_AS(build_graph<MissingMultiServiceStubGraph>(), std::invalid_argument);
}

TEST_CASE("service wiring: multi-interface implementation graph wires explicit stubs")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<MultiServiceClientGraph>(values<Int>(1)), values<Int>(none, 13));
}

TEST_CASE("service wiring: service adaptors collect multiple client requests")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<ServiceAdaptorTwoClientGraph>(values<Int>(1), values<Int>(10)),
                 values<Int>(none, 51));
}

TEST_CASE("service wiring: service_adaptor_impl auto-wires single-interface implementations")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<ServiceAdaptorImplTwoClientGraph>(values<Int>(1), values<Int>(10)),
                 values<Int>(none, 51));
}

TEST_CASE("service wiring: multi-interface service adaptors wire explicit stubs")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<MultiServiceAdaptorClientGraph>(values<Int>(1)), values<Int>(none, 52));
}

TEST_CASE("service wiring: service adaptors validate missing implementations and stubs")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_THROWS_AS((void)eval_node<MissingServiceAdaptorImplementationGraph>(values<Int>(1)), std::invalid_argument);
    CHECK_THROWS_AS(build_graph<IllegalServiceAdaptorStubGraph>(), std::invalid_argument);
    CHECK_THROWS_AS(build_graph<MissingServiceAdaptorStubGraph>(), std::invalid_argument);
}

TEST_CASE("service wiring: templated service descriptors bind as concrete interfaces")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<TemplateServiceClientGraph>(values<Int>(3)), values<Int>(none, 4));
}

TEST_CASE("service wiring: generic service descriptors resolve from client inputs")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<GenericServiceClientGraph>(values<Int>(3)), values<Int>(none, 4));
    CHECK_OUTPUT(eval_node<GenericFloatServiceClientGraph>(values<Float>(1.5)),
                 values<Float>(none, 2.0));
    CHECK_OUTPUT(eval_node<GenericServiceAdaptorClientGraph>(values<Int>(3)), values<Int>(none, 23));
    CHECK_THROWS_AS((void)eval_node<GenericStringServiceClientGraph>(values<Str>("not numeric")),
                    std::invalid_argument);
}
