#ifndef HGRAPH_TYPES_SERVICE_WIRING_H
#define HGRAPH_TYPES_SERVICE_WIRING_H

#include <hgraph/lib/std/operators/container.h>
#include <hgraph/runtime/service_node.h>
#include <hgraph/runtime/shared_output_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_schema.h>

#include <array>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <typeindex>
#include <utility>

namespace hgraph::service
{
    /**
     * C++ subscription service wiring.
     *
     * ``Service`` is a descriptor type:
     *
     * .. code-block:: cpp
     *
     *    struct Prices {
     *        static constexpr std::string_view name{"prices"};
     *        using key_type = Str;
     *        using value_schema = TS<Int>;
     *    };
     *
     * ``subscription_service_impl<Prices, Impl>(w)`` wires ``Impl`` with a
     * ``TSS<key_type>`` subscription input and captures its
     * ``TSD<key_type, value_schema>`` output as a shared reference. Calling the
     * returned handle with a key port records that client's subscription and
     * returns the selected service value by reference; no service value is copied
     * by the wiring layer.
     */

    namespace detail
    {
        template <typename Service>
        using key_type_t = typename Service::key_type;

        template <typename Service>
        using value_schema_t = typename Service::value_schema;

        template <typename Service>
        using output_schema_t = TSD<key_type_t<Service>, value_schema_t<Service>>;

        template <typename Service>
        [[nodiscard]] std::string service_base_path()
        {
            std::string_view name{Service::name};
            if (name.empty()) { throw std::invalid_argument("subscription service name must not be empty"); }

            std::string path{"subs_svc://"};
            path.append(name);
            return path;
        }

        template <typename Service>
        [[nodiscard]] std::string subscriptions_path()
        {
            std::string path = service_base_path<Service>();
            path.append("/subs");
            return path;
        }

        template <typename Service>
        [[nodiscard]] std::string output_path()
        {
            std::string path = service_base_path<Service>();
            path.append("/out");
            return path;
        }

        template <typename Service>
        struct subscription_source_marker
        {
        };

        template <typename Service>
        struct subscription_capture_marker
        {
        };

        template <typename Service>
        struct shared_output_source_marker
        {
        };

        template <typename Service, typename Impl>
        struct shared_output_capture_marker
        {
        };

        template <typename Service>
        [[nodiscard]] Port<TSS<key_type_t<Service>>> subscription_source(Wiring &w)
        {
            using key_type = key_type_t<Service>;
            static_assert(scalar_descriptor<key_type>::is_concrete(),
                          "subscription service key_type must be a concrete scalar type");

            const auto *key_meta = scalar_descriptor<key_type>::value_meta();
            const auto *out_meta = schema_descriptor<TSS<key_type>>::ts_meta();

            WiringNodeSchema schema;
            schema.output = out_meta;

            WiringPortRef port = w.add_node(
                std::type_index(typeid(subscription_source_marker<Service>)), schema,
                std::span<const WiringPortRef>{}, Value{}, [path = subscriptions_path<Service>(), key_meta]() {
                    return make_subscription_key_source_node(path, *key_meta);
                });
            return Port<TSS<key_type>>{w, std::move(port)};
        }

        template <typename Service>
        [[nodiscard]] Port<REF<output_schema_t<Service>>> shared_output_source(Wiring &w)
        {
            using output_schema = output_schema_t<Service>;
            static_assert(schema_descriptor<output_schema>::is_concrete(),
                          "subscription service output schema must be concrete");

            const auto *target_meta = schema_descriptor<output_schema>::ts_meta();
            const auto *ref_meta    = schema_descriptor<REF<output_schema>>::ts_meta();

            WiringNodeSchema schema;
            schema.output = ref_meta;
            schema.state  = ref_meta->value_schema;

            WiringPortRef port = w.add_node(
                std::type_index(typeid(shared_output_source_marker<Service>)), schema,
                std::span<const WiringPortRef>{}, Value{}, [path = output_path<Service>(), target_meta]() {
                    return make_shared_output_source_node(path, *target_meta);
                });
            return Port<REF<output_schema>>{w, std::move(port)};
        }

        template <typename Service>
        void capture_subscription_key(Wiring &w,
                                      Port<TS<key_type_t<Service>>> key,
                                      Port<TSS<key_type_t<Service>>> subscriptions)
        {
            using key_type = key_type_t<Service>;

            std::array<WiringPortRef, 2> inputs{key.erased(), subscriptions.erased()};
            NodeBuilder builder = make_subscription_key_capture_node(
                subscriptions_path<Service>(), *scalar_descriptor<key_type>::value_meta());
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                builder.binding().type_meta->input_schema,
                std::span<const WiringPortRef>{inputs.data(), inputs.size()}));

            static_cast<void>(w.add_node(std::type_index(typeid(subscription_capture_marker<Service>)),
                                         std::move(builder),
                                         std::span<const WiringPortRef>{inputs.data(), inputs.size()},
                                         Value{}));
        }

        template <typename Service, typename Impl>
        void capture_service_output(Wiring &w,
                                    Port<output_schema_t<Service>> output,
                                    Port<REF<output_schema_t<Service>>> shared_output)
        {
            using output_schema = output_schema_t<Service>;

            std::array<WiringPortRef, 2> inputs{output.erased(), shared_output.erased()};
            NodeBuilder builder = make_shared_output_capture_node(
                output_path<Service>(), *schema_descriptor<output_schema>::ts_meta());
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                builder.binding().type_meta->input_schema,
                std::span<const WiringPortRef>{inputs.data(), inputs.size()}));

            static_cast<void>(w.add_node(std::type_index(typeid(shared_output_capture_marker<Service, Impl>)),
                                         std::move(builder),
                                         std::span<const WiringPortRef>{inputs.data(), inputs.size()},
                                         Value{}));
        }
    }  // namespace detail

    template <typename Service>
    class SubscriptionService
    {
      public:
        using key_type      = detail::key_type_t<Service>;
        using value_schema  = detail::value_schema_t<Service>;
        using output_schema = detail::output_schema_t<Service>;

        SubscriptionService() noexcept = default;
        SubscriptionService(Wiring &w,
                            Port<TSS<key_type>> subscriptions,
                            Port<REF<output_schema>> output) noexcept
            : wiring_(&w),
              subscriptions_(std::move(subscriptions)),
              output_(std::move(output))
        {
        }

        [[nodiscard]] Port<TSS<key_type>> subscriptions() const { return subscriptions_; }
        [[nodiscard]] Port<REF<output_schema>> output() const { return output_; }

        [[nodiscard]] Port<value_schema> operator()(Port<TS<key_type>> key) const
        {
            if (wiring_ == nullptr) { throw std::logic_error("subscription service handle is not bound"); }
            detail::capture_subscription_key<Service>(*wiring_, key, subscriptions_);
            return wire<stdlib::getitem_>(*wiring_, output_.template as<output_schema>(), key)
                .template as<value_schema>();
        }

      private:
        Wiring                  *wiring_{nullptr};
        Port<TSS<key_type>>      subscriptions_{};
        Port<REF<output_schema>> output_{};
    };

    template <typename Service, typename Impl, typename... Args>
    [[nodiscard]] SubscriptionService<Service> subscription_service_impl(Wiring &w, const Args &...args)
    {
        using output_schema = detail::output_schema_t<Service>;

        auto subscriptions = detail::subscription_source<Service>(w);
        auto shared_output = detail::shared_output_source<Service>(w);
        auto output        = wire<Impl>(w, subscriptions, args...).template as<output_schema>();
        detail::capture_service_output<Service, Impl>(w, output, shared_output);
        return SubscriptionService<Service>{w, subscriptions, shared_output};
    }
}  // namespace hgraph::service

#endif  // HGRAPH_TYPES_SERVICE_WIRING_H
