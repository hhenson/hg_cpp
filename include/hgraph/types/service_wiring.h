#ifndef HGRAPH_TYPES_SERVICE_WIRING_H
#define HGRAPH_TYPES_SERVICE_WIRING_H

#include <hgraph/lib/std/operators/container.h>
#include <hgraph/runtime/service_node.h>
#include <hgraph/runtime/shared_output_node.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_schema.h>

#include <array>
#include <atomic>
#include <concepts>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <typeindex>
#include <type_traits>
#include <utility>

namespace hgraph::service
{
    struct ServicePath
    {
        std::string value{};
    };

    [[nodiscard]] inline ServicePath path(std::string_view value)
    {
        if (value.empty()) { throw std::invalid_argument("service path must not be empty"); }
        return ServicePath{std::string{value}};
    }

    /**
     * C++ service wiring.
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
     * Reference services expose one shared output:
     *
     * .. code-block:: cpp
     *
     *    struct Accounts {
     *        static constexpr std::string_view name{"accounts"};
     *        using output_schema = TSS<Str>;
     *    };
     *
     *    register_reference_service<Accounts, StaticAccounts>(w);
     *    auto accounts = wire<Accounts>(w);
     *
     * Use ``service::path("name")`` as the first argument after ``w`` to bind
     * or call a non-default service path.
     *
     * Subscription services add a key stream:
     *
     * ``register_subscription_service<Prices, Impl>(w)`` wires ``Impl`` with a
     * ``TSS<key_type>`` subscription input and captures its
     * ``TSD<key_type, value_schema>`` output as a shared reference.
     * ``wire<Prices>(w, key)`` records that client's subscription and returns the
     * selected service value by reference; no service value is copied by the
     * wiring layer.
     *
     * Request/reply services collect client requests through a feedback-style
     * source/capture pair:
     *
     * .. code-block:: cpp
     *
     *    struct AddOne {
     *        static constexpr std::string_view name{"add_one"};
     *        using request_schema = TS<Int>;
     *        using response_schema = TS<Int>;
     *    };
     *
     *    register_request_reply_service<AddOne, AddOneImpl>(w);
     *    auto reply = wire<AddOne>(w, request);
     *
     * The request source owns ``TSD<Int, request_schema>`` plus a mutable
     * request-delta state. Client capture sinks update that state, and the
     * source emits the cumulative delta on the next scheduled tick.
     */

    namespace detail
    {
        template <typename>
        inline constexpr bool always_false_v = false;

        template <typename T>
        struct is_service_path : std::false_type
        {
        };

        template <>
        struct is_service_path<ServicePath> : std::true_type
        {
        };

        template <typename T>
        inline constexpr bool is_service_path_v = is_service_path<std::remove_cvref_t<T>>::value;

        template <typename Service, typename = void>
        struct has_key_value_schema : std::false_type
        {
        };

        template <typename Service>
        struct has_key_value_schema<Service, std::void_t<typename Service::key_type, typename Service::value_schema>>
            : std::true_type
        {
        };

        template <typename Service, typename = void>
        struct has_reference_output_schema : std::false_type
        {
        };

        template <typename Service>
        struct has_reference_output_schema<Service, std::void_t<typename Service::output_schema>> : std::true_type
        {
        };

        template <typename Service, typename = void>
        struct has_request_reply_schema : std::false_type
        {
        };

        template <typename Service>
        struct has_request_reply_schema<
            Service,
            std::void_t<typename Service::request_schema, typename Service::response_schema>>
            : std::true_type
        {
        };

        template <typename Service>
        concept reference_service_interface =
            has_reference_output_schema<Service>::value &&
            !has_key_value_schema<Service>::value &&
            !has_request_reply_schema<Service>::value;

        template <typename Service>
        concept subscription_service_interface =
            has_key_value_schema<Service>::value &&
            !has_reference_output_schema<Service>::value &&
            !has_request_reply_schema<Service>::value;

        template <typename Service>
        concept request_reply_service_interface =
            has_request_reply_schema<Service>::value &&
            !has_reference_output_schema<Service>::value &&
            !has_key_value_schema<Service>::value;

        template <typename Service>
        concept service_interface =
            reference_service_interface<Service> ||
            subscription_service_interface<Service> ||
            request_reply_service_interface<Service>;

        template <typename Service>
        using key_type_t = typename Service::key_type;

        template <typename Service>
        using value_schema_t = typename Service::value_schema;

        template <typename Service>
        using reference_output_schema_t = typename Service::output_schema;

        template <typename Service>
        using output_schema_t = TSD<key_type_t<Service>, value_schema_t<Service>>;

        template <typename Service>
        using request_schema_t = typename Service::request_schema;

        template <typename Service>
        using response_schema_t = typename Service::response_schema;

        template <typename Service>
        using request_input_schema_t = TSD<Int, request_schema_t<Service>>;

        template <typename Service>
        using request_output_schema_t = TSD<Int, response_schema_t<Service>>;

        template <typename T>
        concept graph_implementation = requires { &T::compose; };

        template <typename T>
        concept node_implementation = requires { &T::eval; };

        template <typename Impl, bool IsGraph = graph_implementation<Impl>, bool IsNode = node_implementation<Impl>>
        struct implementation_params
        {
            using type = std::tuple<>;
        };

        template <typename Impl>
        struct implementation_params<Impl, true, false>
        {
            using type = typename StaticGraphSignature<Impl>::param_types;
        };

        template <typename Impl>
        struct implementation_params<Impl, false, true>
        {
            using type = typename StaticNodeSignature<Impl>::wire_param_types;
        };

        template <typename Impl>
        using implementation_params_t = typename implementation_params<Impl>::type;

        template <typename T>
        struct is_path_scalar : std::false_type
        {
        };

        template <fixed_string Name, typename T>
        struct is_path_scalar<Scalar<Name, T>>
            : std::bool_constant<Name.sv() == std::string_view{"path"} && std::same_as<T, Str>>
        {
        };

        template <typename T>
        struct is_service_input_param : std::false_type
        {
        };

        template <fixed_string Name, typename S, auto... Policies>
        struct is_service_input_param<In<Name, S, Policies...>> : std::true_type
        {
        };

        template <fixed_string Name, typename S>
        struct is_service_input_param<NamedPort<Name, S>> : std::true_type
        {
        };

        template <typename S>
        struct is_service_input_param<Port<S>> : std::true_type
        {
        };

        template <typename Params, std::size_t I, bool Done>
        struct first_service_input_param_impl;

        template <typename Params, std::size_t I, bool Found>
        struct first_service_input_param_choice;

        template <typename Params, std::size_t I>
        struct first_service_input_param_impl<Params, I, true>
        {
            using type = void;
        };

        template <typename Params, std::size_t I>
        struct first_service_input_param_impl<Params, I, false>
        {
            using candidate = std::tuple_element_t<I, Params>;
            using type = typename first_service_input_param_choice<
                Params, I, is_service_input_param<candidate>::value>::type;
        };

        template <typename Params, std::size_t I>
        struct first_service_input_param_choice<Params, I, true>
        {
            using type = std::tuple_element_t<I, Params>;
        };

        template <typename Params, std::size_t I>
        struct first_service_input_param_choice<Params, I, false>
        {
            using type = typename first_service_input_param_impl<
                Params, I + 1, (I + 1 >= std::tuple_size_v<Params>)>::type;
        };

        template <typename Params>
        using first_service_input_param =
            first_service_input_param_impl<Params, 0, (std::tuple_size_v<Params> == 0)>;

        template <typename Params, std::size_t... I>
        [[nodiscard]] consteval bool has_path_scalar(std::index_sequence<I...>)
        {
            return (false || ... || is_path_scalar<std::tuple_element_t<I, Params>>::value);
        }

        template <typename Impl>
        [[nodiscard]] consteval bool implementation_accepts_path()
        {
            using params = implementation_params_t<Impl>;
            return has_path_scalar<params>(std::make_index_sequence<std::tuple_size_v<params>>{});
        }

        template <typename Impl, typename OutputSchema, typename... Args>
        [[nodiscard]] Port<OutputSchema> wire_service_impl(Wiring &w, const ServicePath &user_path, const Args &...args)
        {
            if constexpr (implementation_accepts_path<Impl>())
            {
                return wire<Impl>(w, args..., arg<"path">(Str{user_path.value})).template as<OutputSchema>();
            }
            else
            {
                return wire<Impl>(w, args...).template as<OutputSchema>();
            }
        }

        template <typename Impl, typename PortT>
        [[nodiscard]] auto service_input_arg(const PortT &port)
        {
            using input_param = typename first_service_input_param<implementation_params_t<Impl>>::type;
            if constexpr (!std::is_void_v<input_param> && requires { input_param::field_name; })
            {
                return arg<input_param::field_name>(port);
            }
            else
            {
                return port;
            }
        }

        template <typename Service>
        [[nodiscard]] std::string service_name(std::string_view error_prefix)
        {
            std::string_view name{Service::name};
            if (name.empty())
            {
                throw std::invalid_argument(std::string{error_prefix} + " service name must not be empty");
            }
            return std::string{name};
        }

        template <typename Service>
        [[nodiscard]] ServicePath default_service_path()
        {
            if constexpr (requires { std::string_view{Service::default_path}; })
            {
                return path(std::string_view{Service::default_path});
            }
            else
            {
                std::string user_path = service_name<Service>("default");
                user_path.append("_default");
                return ServicePath{std::move(user_path)};
            }
        }

        template <typename Service>
        [[nodiscard]] std::string service_full_path(std::string_view prefix,
                                                    std::string_view error_prefix,
                                                    const ServicePath &user_path)
        {
            const std::string name = service_name<Service>(error_prefix);
            if (user_path.value.starts_with(prefix)) { return user_path.value; }
            std::string path{prefix};
            path.append(user_path.value);
            path.push_back('/');
            path.append(name);
            return path;
        }

        template <typename Service>
        [[nodiscard]] std::string reference_base_path(const ServicePath &user_path)
        {
            return service_full_path<Service>("ref_svc://", "reference", user_path);
        }

        template <typename Service>
        [[nodiscard]] std::string subscription_base_path(const ServicePath &user_path)
        {
            return service_full_path<Service>("subs_svc://", "subscription", user_path);
        }

        template <typename Service>
        [[nodiscard]] std::string request_reply_base_path(const ServicePath &user_path)
        {
            return service_full_path<Service>("reqrepl_svc://", "request-reply", user_path);
        }

        template <typename Service>
        [[nodiscard]] std::string reference_output_path(const ServicePath &user_path)
        {
            return reference_base_path<Service>(user_path);
        }

        template <typename Service>
        [[nodiscard]] std::string subscriptions_path(const ServicePath &user_path)
        {
            std::string path = subscription_base_path<Service>(user_path);
            path.append("/subs");
            return path;
        }

        template <typename Service>
        [[nodiscard]] std::string output_path(const ServicePath &user_path)
        {
            std::string path = subscription_base_path<Service>(user_path);
            path.append("/out");
            return path;
        }

        template <typename Service>
        [[nodiscard]] std::string request_input_path(const ServicePath &user_path)
        {
            std::string path = request_reply_base_path<Service>(user_path);
            path.append("/request");
            return path;
        }

        template <typename Service>
        [[nodiscard]] std::string request_reply_output_path(const ServicePath &user_path)
        {
            std::string path = request_reply_base_path<Service>(user_path);
            path.append("/replies");
            return path;
        }

        [[nodiscard]] inline Int next_request_id() noexcept
        {
            static std::atomic<Int> next{0};
            return next.fetch_add(1, std::memory_order_relaxed) + 1;
        }

        [[nodiscard]] inline const ValueTypeMetaData *request_input_state_schema(
            const TSValueTypeMetaData &request_schema)
        {
            if (request_schema.delta_value_schema == nullptr)
            {
                throw std::invalid_argument("request/reply service request schema must have a delta schema");
            }

            auto       &registry          = TypeRegistry::instance();
            const auto *request_id_schema = registry.register_scalar<Int>("int");
            const auto *removed_schema    = registry.mutable_set(request_id_schema);
            const auto *modified_schema   = registry.mutable_map(request_id_schema, request_schema.delta_value_schema);
            return registry.un_named_bundle({{"removed", removed_schema}, {"modified", modified_schema}});
        }

        [[nodiscard]] inline Value path_key_value(const std::string &full_path)
        {
            // Used only by Wiring's intern key; service source nodes do not carry path scalars at runtime.
            return Value{Str{full_path}};
        }

        template <typename Service>
        struct reference_output_source_marker
        {
        };

        template <typename Service, typename Impl>
        struct reference_output_capture_marker
        {
        };

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
        struct request_input_source_marker
        {
        };

        template <typename Service>
        struct request_input_capture_marker
        {
        };

        template <typename Service>
        struct request_reply_output_source_marker
        {
        };

        template <typename Service, typename Impl>
        struct request_reply_output_capture_marker
        {
        };

        template <typename Service>
        [[nodiscard]] Port<REF<reference_output_schema_t<Service>>> reference_shared_output_source(
            Wiring &w,
            const ServicePath &user_path)
        {
            using output_schema = reference_output_schema_t<Service>;
            static_assert(schema_descriptor<output_schema>::is_concrete(),
                          "reference service output schema must be concrete");

            std::string full_path = reference_output_path<Service>(user_path);
            const auto *target_meta = schema_descriptor<output_schema>::ts_meta();
            const auto *ref_meta    = schema_descriptor<REF<output_schema>>::ts_meta();

            WiringNodeSchema schema;
            schema.output = ref_meta;
            schema.state  = ref_meta->value_schema;

            WiringPortRef port = w.add_node(
                std::type_index(typeid(reference_output_source_marker<Service>)), schema,
                std::span<const WiringPortRef>{}, path_key_value(full_path),
                [path = std::move(full_path), target_meta]() {
                    return make_shared_output_source_node(path, *target_meta);
                });
            return Port<REF<output_schema>>{w, std::move(port)};
        }

        template <typename Service>
        [[nodiscard]] Port<TSS<key_type_t<Service>>> subscription_source(Wiring &w, const ServicePath &user_path)
        {
            using key_type = key_type_t<Service>;
            static_assert(scalar_descriptor<key_type>::is_concrete(),
                          "subscription service key_type must be a concrete scalar type");

            std::string full_path = subscriptions_path<Service>(user_path);
            const auto *key_meta = scalar_descriptor<key_type>::value_meta();
            const auto *out_meta = schema_descriptor<TSS<key_type>>::ts_meta();

            WiringNodeSchema schema;
            schema.output = out_meta;

            WiringPortRef port = w.add_node(
                std::type_index(typeid(subscription_source_marker<Service>)), schema,
                std::span<const WiringPortRef>{}, path_key_value(full_path),
                [path = std::move(full_path), key_meta]() {
                    return make_subscription_key_source_node(path, *key_meta);
                });
            return Port<TSS<key_type>>{w, std::move(port)};
        }

        template <typename Service>
        [[nodiscard]] Port<REF<output_schema_t<Service>>> shared_output_source(Wiring &w,
                                                                               const ServicePath &user_path)
        {
            using output_schema = output_schema_t<Service>;
            static_assert(schema_descriptor<output_schema>::is_concrete(),
                          "subscription service output schema must be concrete");

            std::string full_path = output_path<Service>(user_path);
            const auto *target_meta = schema_descriptor<output_schema>::ts_meta();
            const auto *ref_meta    = schema_descriptor<REF<output_schema>>::ts_meta();

            WiringNodeSchema schema;
            schema.output = ref_meta;
            schema.state  = ref_meta->value_schema;

            WiringPortRef port = w.add_node(
                std::type_index(typeid(shared_output_source_marker<Service>)), schema,
                std::span<const WiringPortRef>{}, path_key_value(full_path),
                [path = std::move(full_path), target_meta]() {
                    return make_shared_output_source_node(path, *target_meta);
                });
            return Port<REF<output_schema>>{w, std::move(port)};
        }

        template <typename Service>
        [[nodiscard]] Port<request_input_schema_t<Service>> request_input_source(
            Wiring &w,
            const ServicePath &user_path)
        {
            using input_schema = request_input_schema_t<Service>;
            using request_schema = request_schema_t<Service>;
            static_assert(schema_descriptor<input_schema>::is_concrete(),
                          "request/reply service request schema must be concrete");

            std::string full_path = request_input_path<Service>(user_path);
            const auto *request_meta = schema_descriptor<request_schema>::ts_meta();
            const auto *out_meta     = schema_descriptor<input_schema>::ts_meta();

            WiringNodeSchema schema;
            schema.output = out_meta;
            schema.state  = request_input_state_schema(*request_meta);

            WiringPortRef port = w.add_node(
                std::type_index(typeid(request_input_source_marker<Service>)), schema,
                std::span<const WiringPortRef>{}, path_key_value(full_path),
                [path = std::move(full_path), request_meta]() {
                    return make_request_input_source_node(path, *request_meta);
                });
            return Port<input_schema>{w, std::move(port)};
        }

        template <typename Service>
        [[nodiscard]] Port<REF<request_output_schema_t<Service>>> request_reply_output_source(
            Wiring &w,
            const ServicePath &user_path)
        {
            using output_schema = request_output_schema_t<Service>;
            static_assert(schema_descriptor<output_schema>::is_concrete(),
                          "request/reply service response schema must be concrete");

            std::string full_path = request_reply_output_path<Service>(user_path);
            const auto *target_meta = schema_descriptor<output_schema>::ts_meta();
            const auto *ref_meta    = schema_descriptor<REF<output_schema>>::ts_meta();

            WiringNodeSchema schema;
            schema.output = ref_meta;
            schema.state  = ref_meta->value_schema;

            WiringPortRef port = w.add_node(
                std::type_index(typeid(request_reply_output_source_marker<Service>)), schema,
                std::span<const WiringPortRef>{}, path_key_value(full_path),
                [path = std::move(full_path), target_meta]() {
                    return make_shared_output_source_node(path, *target_meta);
                });
            return Port<REF<output_schema>>{w, std::move(port)};
        }

        template <typename Service>
        void capture_subscription_key(Wiring &w,
                                      Port<TS<key_type_t<Service>>> key,
                                      Port<TSS<key_type_t<Service>>> subscriptions,
                                      const ServicePath &user_path)
        {
            using key_type = key_type_t<Service>;

            std::array<WiringPortRef, 2> inputs{key.erased(), subscriptions.erased()};
            NodeBuilder builder = make_subscription_key_capture_node(
                subscriptions_path<Service>(user_path), *scalar_descriptor<key_type>::value_meta());
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                builder.binding().type_meta->input_schema,
                std::span<const WiringPortRef>{inputs.data(), inputs.size()}));

            static_cast<void>(w.add_node(std::type_index(typeid(subscription_capture_marker<Service>)),
                                         std::move(builder),
                                         std::span<const WiringPortRef>{inputs.data(), inputs.size()},
                                         Value{}));
        }

        template <typename Service>
        void capture_request_input(Wiring &w,
                                   Port<request_schema_t<Service>> request,
                                   Port<request_input_schema_t<Service>> requests,
                                   const ServicePath &user_path,
                                   Int request_id)
        {
            using request_schema = request_schema_t<Service>;

            std::array<WiringPortRef, 2> inputs{request.erased(), requests.erased()};
            NodeBuilder builder = make_request_input_capture_node(
                request_input_path<Service>(user_path), *schema_descriptor<request_schema>::ts_meta(), request_id);
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                builder.binding().type_meta->input_schema,
                std::span<const WiringPortRef>{inputs.data(), inputs.size()}));

            static_cast<void>(w.add_node(std::type_index(typeid(request_input_capture_marker<Service>)),
                                         std::move(builder),
                                         std::span<const WiringPortRef>{inputs.data(), inputs.size()},
                                         Value{request_id}));
        }

        template <typename Service, typename Impl>
        void capture_reference_service_output(Wiring &w,
                                              Port<reference_output_schema_t<Service>> output,
                                              Port<REF<reference_output_schema_t<Service>>> shared_output,
                                              const ServicePath &user_path)
        {
            using output_schema = reference_output_schema_t<Service>;

            std::array<WiringPortRef, 2> inputs{output.erased(), shared_output.erased()};
            NodeBuilder builder = make_shared_output_capture_node(
                reference_output_path<Service>(user_path), *schema_descriptor<output_schema>::ts_meta());
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                builder.binding().type_meta->input_schema,
                std::span<const WiringPortRef>{inputs.data(), inputs.size()}));

            static_cast<void>(w.add_node(std::type_index(typeid(reference_output_capture_marker<Service, Impl>)),
                                         std::move(builder),
                                         std::span<const WiringPortRef>{inputs.data(), inputs.size()},
                                         Value{}));
        }

        template <typename Service, typename Impl>
        void capture_service_output(Wiring &w,
                                    Port<output_schema_t<Service>> output,
                                    Port<REF<output_schema_t<Service>>> shared_output,
                                    const ServicePath &user_path)
        {
            using output_schema = output_schema_t<Service>;

            std::array<WiringPortRef, 2> inputs{output.erased(), shared_output.erased()};
            NodeBuilder builder = make_shared_output_capture_node(
                output_path<Service>(user_path), *schema_descriptor<output_schema>::ts_meta());
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                builder.binding().type_meta->input_schema,
                std::span<const WiringPortRef>{inputs.data(), inputs.size()}));

            static_cast<void>(w.add_node(std::type_index(typeid(shared_output_capture_marker<Service, Impl>)),
                                         std::move(builder),
                                         std::span<const WiringPortRef>{inputs.data(), inputs.size()},
                                         Value{}));
        }

        template <typename Service, typename Impl>
        void capture_request_reply_service_output(
            Wiring &w,
            Port<request_output_schema_t<Service>> output,
            Port<REF<request_output_schema_t<Service>>> shared_output,
            const ServicePath &user_path)
        {
            using output_schema = request_output_schema_t<Service>;

            std::array<WiringPortRef, 2> inputs{output.erased(), shared_output.erased()};
            NodeBuilder builder = make_shared_output_capture_node(
                request_reply_output_path<Service>(user_path), *schema_descriptor<output_schema>::ts_meta());
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                builder.binding().type_meta->input_schema,
                std::span<const WiringPortRef>{inputs.data(), inputs.size()}));

            static_cast<void>(w.add_node(
                std::type_index(typeid(request_reply_output_capture_marker<Service, Impl>)),
                std::move(builder),
                std::span<const WiringPortRef>{inputs.data(), inputs.size()},
                Value{}));
        }

        template <typename Impl, typename... Args>
        void wire_service_graph(Wiring &w, const ServicePath &user_path, const Args &...args)
        {
            if constexpr (implementation_accepts_path<Impl>())
            {
                static_cast<void>(wire<Impl>(w, args..., arg<"path">(Str{user_path.value})));
            }
            else
            {
                static_cast<void>(wire<Impl>(w, args...));
            }
        }
    }  // namespace detail

    template <typename Service>
    [[nodiscard]] Port<typename Service::output_schema> reference_service(Wiring &w, ServicePath user_path)
    {
        using output_schema = detail::reference_output_schema_t<Service>;
        return detail::reference_shared_output_source<Service>(w, user_path).template as<output_schema>();
    }

    template <typename Service>
    [[nodiscard]] Port<typename Service::output_schema> reference_service(Wiring &w)
    {
        return reference_service<Service>(w, detail::default_service_path<Service>());
    }

    template <typename Service, typename Impl, typename... Args>
    void register_reference_service(Wiring &w, ServicePath user_path, const Args &...args)
    {
        using output_schema = detail::reference_output_schema_t<Service>;

        auto shared_output = detail::reference_shared_output_source<Service>(w, user_path);
        auto output        = detail::wire_service_impl<Impl, output_schema>(w, user_path, args...);
        detail::capture_reference_service_output<Service, Impl>(w, output, shared_output, user_path);
    }

    template <typename Service, typename Impl, typename... Args>
    void register_reference_service(Wiring &w, const Args &...args)
    {
        register_reference_service<Service, Impl>(w, detail::default_service_path<Service>(), args...);
    }

    template <typename Service>
        requires detail::subscription_service_interface<Service>
    [[nodiscard]] Port<TSS<detail::key_type_t<Service>>> impl_input(Wiring &w, ServicePath user_path)
    {
        return detail::subscription_source<Service>(w, user_path);
    }

    template <typename Service>
        requires detail::subscription_service_interface<Service>
    [[nodiscard]] Port<TSS<detail::key_type_t<Service>>> impl_input(Wiring &w)
    {
        return impl_input<Service>(w, detail::default_service_path<Service>());
    }

    template <typename Service>
        requires detail::request_reply_service_interface<Service>
    [[nodiscard]] Port<detail::request_input_schema_t<Service>> impl_input(Wiring &w, ServicePath user_path)
    {
        return detail::request_input_source<Service>(w, user_path);
    }

    template <typename Service>
        requires detail::request_reply_service_interface<Service>
    [[nodiscard]] Port<detail::request_input_schema_t<Service>> impl_input(Wiring &w)
    {
        return impl_input<Service>(w, detail::default_service_path<Service>());
    }

    struct explicit_impl_output_marker
    {
    };

    template <typename Service>
        requires detail::reference_service_interface<Service>
    void impl_output(Wiring &w,
                     ServicePath user_path,
                     Port<detail::reference_output_schema_t<Service>> output)
    {
        auto shared_output = detail::reference_shared_output_source<Service>(w, user_path);
        detail::capture_reference_service_output<Service, explicit_impl_output_marker>(
            w, std::move(output), shared_output, user_path);
    }

    template <typename Service>
        requires detail::reference_service_interface<Service>
    void impl_output(Wiring &w, Port<detail::reference_output_schema_t<Service>> output)
    {
        impl_output<Service>(w, detail::default_service_path<Service>(), std::move(output));
    }

    template <typename Service>
        requires detail::subscription_service_interface<Service>
    void impl_output(Wiring &w,
                     ServicePath user_path,
                     Port<detail::output_schema_t<Service>> output)
    {
        auto shared_output = detail::shared_output_source<Service>(w, user_path);
        detail::capture_service_output<Service, explicit_impl_output_marker>(
            w, std::move(output), shared_output, user_path);
    }

    template <typename Service>
        requires detail::subscription_service_interface<Service>
    void impl_output(Wiring &w, Port<detail::output_schema_t<Service>> output)
    {
        impl_output<Service>(w, detail::default_service_path<Service>(), std::move(output));
    }

    template <typename Service>
        requires detail::request_reply_service_interface<Service>
    void impl_output(Wiring &w,
                     ServicePath user_path,
                     Port<detail::request_output_schema_t<Service>> output)
    {
        auto shared_output = detail::request_reply_output_source<Service>(w, user_path);
        detail::capture_request_reply_service_output<Service, explicit_impl_output_marker>(
            w, std::move(output), shared_output, user_path);
    }

    template <typename Service>
        requires detail::request_reply_service_interface<Service>
    void impl_output(Wiring &w, Port<detail::request_output_schema_t<Service>> output)
    {
        impl_output<Service>(w, detail::default_service_path<Service>(), std::move(output));
    }

    template <typename Impl, typename... Services, typename... Args>
    void register_services(Wiring &w, ServicePath user_path, const Args &...args)
    {
        static_assert(sizeof...(Services) > 0,
                      "register_services requires at least one service interface");
        static_assert((detail::service_interface<Services> && ...),
                      "register_services requires service descriptor types");
        detail::wire_service_graph<Impl>(w, user_path, args...);
    }

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
                            Port<REF<output_schema>> output,
                            ServicePath user_path) noexcept
            : wiring_(&w),
              subscriptions_(std::move(subscriptions)),
              output_(std::move(output)),
              path_(std::move(user_path))
        {
        }

        [[nodiscard]] Port<TSS<key_type>> subscriptions() const { return subscriptions_; }
        [[nodiscard]] Port<REF<output_schema>> output() const { return output_; }

        [[nodiscard]] Port<value_schema> operator()(Port<TS<key_type>> key) const
        {
            if (wiring_ == nullptr) { throw std::logic_error("subscription service handle is not bound"); }
            detail::capture_subscription_key<Service>(*wiring_, key, subscriptions_, path_);
            return wire<stdlib::getitem_>(*wiring_, output_.template as<output_schema>(), key)
                .template as<value_schema>();
        }

      private:
        Wiring                  *wiring_{nullptr};
        Port<TSS<key_type>>      subscriptions_{};
        Port<REF<output_schema>> output_{};
        ServicePath              path_{};
    };

    template <typename Service>
    [[nodiscard]] SubscriptionService<Service> subscription_service(Wiring &w, ServicePath user_path)
    {
        auto subscriptions = detail::subscription_source<Service>(w, user_path);
        auto shared_output = detail::shared_output_source<Service>(w, user_path);
        return SubscriptionService<Service>{w, subscriptions, shared_output, std::move(user_path)};
    }

    template <typename Service>
    [[nodiscard]] SubscriptionService<Service> subscription_service(Wiring &w)
    {
        return subscription_service<Service>(w, detail::default_service_path<Service>());
    }

    template <typename Service, typename Impl, typename... Args>
    void register_subscription_service(Wiring &w, ServicePath user_path, const Args &...args)
    {
        using output_schema = detail::output_schema_t<Service>;

        auto subscriptions = detail::subscription_source<Service>(w, user_path);
        auto shared_output = detail::shared_output_source<Service>(w, user_path);
        auto output = detail::wire_service_impl<Impl, output_schema>(
            w, user_path, detail::service_input_arg<Impl>(subscriptions), args...);
        detail::capture_service_output<Service, Impl>(w, output, shared_output, user_path);
    }

    template <typename Service, typename Impl, typename... Args>
    void register_subscription_service(Wiring &w, const Args &...args)
    {
        register_subscription_service<Service, Impl>(w, detail::default_service_path<Service>(), args...);
    }

    template <typename Service, typename Impl, typename... Args>
    [[nodiscard]] SubscriptionService<Service> subscription_service_impl(Wiring &w,
                                                                         ServicePath user_path,
                                                                         const Args &...args)
    {
        register_subscription_service<Service, Impl>(w, user_path, args...);
        return subscription_service<Service>(w, std::move(user_path));
    }

    template <typename Service, typename Impl, typename... Args>
    [[nodiscard]] SubscriptionService<Service> subscription_service_impl(Wiring &w, const Args &...args)
    {
        return subscription_service_impl<Service, Impl>(w, detail::default_service_path<Service>(), args...);
    }

    template <typename Service>
    [[nodiscard]] Port<typename Service::response_schema> request_reply_service(
        Wiring &w,
        Port<typename Service::request_schema> request,
        ServicePath user_path)
    {
        using output_schema   = detail::request_output_schema_t<Service>;
        using response_schema = detail::response_schema_t<Service>;

        const Int request_id = detail::next_request_id();
        auto requests        = detail::request_input_source<Service>(w, user_path);
        auto replies         = detail::request_reply_output_source<Service>(w, user_path);

        detail::capture_request_input<Service>(w, std::move(request), requests, user_path, request_id);
        return wire<stdlib::getitem_>(w, replies.template as<output_schema>(), request_id)
            .template as<response_schema>();
    }

    template <typename Service>
    [[nodiscard]] Port<typename Service::response_schema> request_reply_service(
        Wiring &w,
        Port<typename Service::request_schema> request)
    {
        return request_reply_service<Service>(
            w, std::move(request), detail::default_service_path<Service>());
    }

    template <typename Service, typename Impl, typename... Args>
    void register_request_reply_service(Wiring &w, ServicePath user_path, const Args &...args)
    {
        using output_schema = detail::request_output_schema_t<Service>;

        auto requests = detail::request_input_source<Service>(w, user_path);
        auto replies  = detail::request_reply_output_source<Service>(w, user_path);
        auto output = detail::wire_service_impl<Impl, output_schema>(
            w, user_path, detail::service_input_arg<Impl>(requests), args...);
        detail::capture_request_reply_service_output<Service, Impl>(w, output, replies, user_path);
    }

    template <typename Service, typename Impl, typename... Args>
    void register_request_reply_service(Wiring &w, const Args &...args)
    {
        register_request_reply_service<Service, Impl>(w, detail::default_service_path<Service>(), args...);
    }

    template <typename Service, typename Impl, typename... Args>
    [[nodiscard]] Port<typename Service::response_schema> request_reply_service_impl(
        Wiring &w,
        Port<typename Service::request_schema> request,
        ServicePath user_path,
        const Args &...args)
    {
        register_request_reply_service<Service, Impl>(w, user_path, args...);
        return request_reply_service<Service>(w, std::move(request), std::move(user_path));
    }

    template <typename Service, typename Impl, typename... Args>
    [[nodiscard]] Port<typename Service::response_schema> request_reply_service_impl(
        Wiring &w,
        Port<typename Service::request_schema> request,
        const Args &...args)
    {
        return request_reply_service_impl<Service, Impl>(
            w, std::move(request), detail::default_service_path<Service>(), args...);
    }
}  // namespace hgraph::service

namespace hgraph::graph_wiring_detail
{
    template <typename Service, typename OutSchema, typename... Args>
        requires service::detail::service_interface<Service>
    struct wire_customization<Service, OutSchema, Args...>
    {
        static constexpr bool enabled = true;

        static auto wire(Wiring &w, const Args &...args)
        {
            static_assert(std::is_void_v<OutSchema>,
                          "wire<Service, OutSchema>: service output schema is defined by the service descriptor");

            auto arg_tuple = std::forward_as_tuple(args...);
            if constexpr (service::detail::reference_service_interface<Service>)
            {
                static_assert(sizeof...(Args) <= 1,
                              "wire<ReferenceService>: expected zero arguments or service::path(...)");
                if constexpr (sizeof...(Args) == 0)
                {
                    return service::reference_service<Service>(w);
                }
                else
                {
                    using A0 = std::tuple_element_t<0, std::tuple<Args...>>;
                    static_assert(service::detail::is_service_path_v<A0>,
                                  "wire<ReferenceService>: first argument must be service::path(...)");
                    return service::reference_service<Service>(w, std::get<0>(arg_tuple));
                }
            }
            else if constexpr (service::detail::subscription_service_interface<Service>)
            {
                static_assert(sizeof...(Args) == 1 || sizeof...(Args) == 2,
                              "wire<SubscriptionService>: expected key or service::path(...), key");
                if constexpr (sizeof...(Args) == 1)
                {
                    using A0 = std::tuple_element_t<0, std::tuple<Args...>>;
                    static_assert(!service::detail::is_service_path_v<A0>,
                                  "wire<SubscriptionService>: missing key after service::path(...)");
                    return service::subscription_service<Service>(w)(std::get<0>(arg_tuple));
                }
                else
                {
                    using A0 = std::tuple_element_t<0, std::tuple<Args...>>;
                    static_assert(service::detail::is_service_path_v<A0>,
                                  "wire<SubscriptionService>: first argument must be service::path(...)");
                    return service::subscription_service<Service>(w, std::get<0>(arg_tuple))(std::get<1>(arg_tuple));
                }
            }
            else if constexpr (service::detail::request_reply_service_interface<Service>)
            {
                static_assert(sizeof...(Args) == 1 || sizeof...(Args) == 2,
                              "wire<RequestReplyService>: expected request or service::path(...), request");
                if constexpr (sizeof...(Args) == 1)
                {
                    using A0 = std::tuple_element_t<0, std::tuple<Args...>>;
                    static_assert(!service::detail::is_service_path_v<A0>,
                                  "wire<RequestReplyService>: missing request after service::path(...)");
                    return service::request_reply_service<Service>(w, std::get<0>(arg_tuple));
                }
                else
                {
                    using A0 = std::tuple_element_t<0, std::tuple<Args...>>;
                    static_assert(service::detail::is_service_path_v<A0>,
                                  "wire<RequestReplyService>: first argument must be service::path(...)");
                    return service::request_reply_service<Service>(w, std::get<1>(arg_tuple), std::get<0>(arg_tuple));
                }
            }
            else
            {
                static_assert(service::detail::always_false_v<Service>, "unsupported service descriptor");
            }
        }
    };
}  // namespace hgraph::graph_wiring_detail

#endif  // HGRAPH_TYPES_SERVICE_WIRING_H
