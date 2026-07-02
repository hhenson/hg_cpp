#ifndef HGRAPH_TYPES_ADAPTOR_WIRING_H
#define HGRAPH_TYPES_ADAPTOR_WIRING_H

#include <hgraph/runtime/shared_output_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/static_schema.h>

#include <array>
#include <concepts>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <typeindex>
#include <type_traits>
#include <utility>

namespace hgraph::adaptor
{
    struct interface
    {
    };

    struct AdaptorPath
    {
        std::string value{};
    };

    [[nodiscard]] inline AdaptorPath path(std::string_view value)
    {
        if (value.empty()) { throw std::invalid_argument("adaptor path must not be empty"); }
        return AdaptorPath{std::string{value}};
    }

    /**
     * C++ adaptor wiring.
     *
     * ``Interface`` is a descriptor type:
     *
     * .. code-block:: cpp
     *
     *    struct Loopback {
     *        static constexpr std::string_view name{"loopback"};
     *        using input_schema = TS<Int>;
     *        using output_schema = TS<Int>;
     *    };
     *
     * ``register_adaptor<Loopback, Impl>(w)`` wires ``Impl`` with an
     * implementation-side graph. Client code calls ``wire<Loopback>(w, input)``;
     * adaptor implementations call ``from_graph<Loopback>(w)`` to fetch client
     * input and ``to_graph<Loopback>(w, output)`` to publish their output.
     * Source-only and sink-only descriptors omit ``input_schema`` or
     * ``output_schema`` respectively.
     */
    namespace detail
    {
        template <typename Interface, typename = void>
        struct has_input_schema : std::false_type
        {
        };

        template <typename Interface>
        struct has_input_schema<Interface, std::void_t<typename Interface::input_schema>> : std::true_type
        {
        };

        template <typename Interface, typename = void>
        struct has_output_schema : std::false_type
        {
        };

        template <typename Interface>
        struct has_output_schema<Interface, std::void_t<typename Interface::output_schema>> : std::true_type
        {
        };

        template <typename Interface>
        using input_schema_t = typename Interface::input_schema;

        template <typename Interface>
        using output_schema_t = typename Interface::output_schema;

        template <typename Interface>
        concept adaptor_interface =
            std::derived_from<Interface, interface> &&
            (has_input_schema<Interface>::value || has_output_schema<Interface>::value);

        template <typename Impl>
        concept graph_implementation = requires { &Impl::compose; };

        template <typename Impl>
        concept node_implementation = requires { &Impl::eval; };

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

        template <typename Interface>
        [[nodiscard]] std::string adaptor_name()
        {
            std::string_view name{Interface::name};
            if (name.empty()) { throw std::invalid_argument("adaptor name must not be empty"); }
            return std::string{name};
        }

        template <typename Interface>
        [[nodiscard]] AdaptorPath default_adaptor_path()
        {
            if constexpr (requires { std::string_view{Interface::default_path}; })
            {
                return path(std::string_view{Interface::default_path});
            }
            else
            {
                std::string value = adaptor_name<Interface>();
                value.append("_default");
                return AdaptorPath{std::move(value)};
            }
        }

        template <typename Interface>
        [[nodiscard]] std::string adaptor_base_path(const AdaptorPath &user_path)
        {
            constexpr std::string_view prefix{"adaptor://"};
            if (user_path.value.starts_with(prefix)) { return user_path.value; }
            std::string full{prefix};
            full.append(user_path.value);
            full.push_back('/');
            full.append(adaptor_name<Interface>());
            return full;
        }

        template <typename Interface>
        [[nodiscard]] std::string adaptor_from_graph_path(const AdaptorPath &user_path)
        {
            std::string full = adaptor_base_path<Interface>(user_path);
            full.append("/from_graph");
            return full;
        }

        template <typename Interface>
        [[nodiscard]] std::string adaptor_to_graph_path(const AdaptorPath &user_path)
        {
            std::string full = adaptor_base_path<Interface>(user_path);
            full.append("/to_graph");
            return full;
        }

        [[nodiscard]] inline Value path_key_value(const std::string &full_path)
        {
            return Value{Str{full_path}};
        }

        template <typename Interface>
        struct input_stub_source_marker
        {
        };

        template <typename Interface>
        struct input_capture_marker
        {
        };

        template <typename Interface>
        struct output_source_marker
        {
        };

        template <typename Interface>
        struct output_capture_marker
        {
        };

        template <typename Interface>
        [[nodiscard]] Port<REF<input_schema_t<Interface>>> input_stub_source(Wiring &w, const AdaptorPath &user_path)
        {
            using input_schema = input_schema_t<Interface>;
            static_assert(schema_descriptor<input_schema>::is_concrete(),
                          "adaptor input schema must be concrete");

            std::string full_path = adaptor_from_graph_path<Interface>(user_path);
            const auto *input_meta = schema_descriptor<input_schema>::ts_meta();
            const auto *ref_meta = schema_descriptor<REF<input_schema>>::ts_meta();

            WiringNodeSchema schema;
            schema.output = ref_meta;
            schema.state = ref_meta->value_schema;

            WiringPortRef port = w.add_node(
                std::type_index(typeid(input_stub_source_marker<Interface>)), schema,
                std::span<const WiringPortRef>{}, path_key_value(full_path),
                [path = std::move(full_path), input_meta]() {
                    return make_shared_output_source_node(path, *input_meta, false);
                });
            return Port<REF<input_schema>>{w, std::move(port)};
        }

        template <typename Interface>
        void capture_input(Wiring &w,
                           Port<input_schema_t<Interface>> input,
                           Port<REF<input_schema_t<Interface>>> source,
                           const AdaptorPath &user_path)
        {
            using input_schema = input_schema_t<Interface>;

            std::array<WiringPortRef, 2> sources{input.erased(), source.erased()};
            std::array<WiringInputRef, 2> inputs{{
                WiringInputRef{.source = sources[0]},
                WiringInputRef{.source = sources[1], .rank_dependency = false},
            }};
            NodeBuilder builder = make_shared_output_capture_node(
                adaptor_from_graph_path<Interface>(user_path), *schema_descriptor<input_schema>::ts_meta());
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                builder.binding().type_meta->input_schema,
                std::span<const WiringPortRef>{sources.data(), sources.size()}));

            WiringPortRef capture = w.add_node(std::type_index(typeid(input_capture_marker<Interface>)),
                                               std::move(builder),
                                               std::span<const WiringInputRef>{inputs.data(), inputs.size()},
                                               Value{});
            w.add_rank_dependency(source.node(), capture.peered_node());
        }

        template <typename Interface>
        [[nodiscard]] Port<REF<output_schema_t<Interface>>> output_source(Wiring &w, const AdaptorPath &user_path)
        {
            using output_schema = output_schema_t<Interface>;
            static_assert(schema_descriptor<output_schema>::is_concrete(),
                          "adaptor output schema must be concrete");

            std::string full_path = adaptor_to_graph_path<Interface>(user_path);
            const auto *target_meta = schema_descriptor<output_schema>::ts_meta();
            const auto *ref_meta = schema_descriptor<REF<output_schema>>::ts_meta();

            WiringNodeSchema schema;
            schema.output = ref_meta;
            schema.state = ref_meta->value_schema;

            WiringPortRef port = w.add_node(
                std::type_index(typeid(output_source_marker<Interface>)), schema,
                std::span<const WiringPortRef>{}, path_key_value(full_path),
                [path = std::move(full_path), target_meta]() {
                    return make_shared_output_source_node(path, *target_meta);
                });
            return Port<REF<output_schema>>{w, std::move(port)};
        }

        template <typename Interface>
        void capture_output(Wiring &w,
                            Port<output_schema_t<Interface>> output,
                            Port<REF<output_schema_t<Interface>>> shared_output,
                            const AdaptorPath &user_path)
        {
            using output_schema = output_schema_t<Interface>;

            std::array<WiringPortRef, 2> sources{output.erased(), shared_output.erased()};
            std::array<WiringInputRef, 2> inputs{{
                WiringInputRef{.source = sources[0]},
                WiringInputRef{.source = sources[1], .rank_dependency = false},
            }};
            NodeBuilder builder = make_shared_output_capture_node(
                adaptor_to_graph_path<Interface>(user_path), *schema_descriptor<output_schema>::ts_meta());
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                builder.binding().type_meta->input_schema,
                std::span<const WiringPortRef>{sources.data(), sources.size()}));

            WiringPortRef capture = w.add_node(std::type_index(typeid(output_capture_marker<Interface>)),
                                               std::move(builder),
                                               std::span<const WiringInputRef>{inputs.data(), inputs.size()},
                                               Value{});
            w.add_rank_dependency(shared_output.node(), capture.peered_node());
        }

        template <typename Impl, typename... Args>
        void wire_impl(Wiring &w, const AdaptorPath &user_path, const Args &...args)
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

        template <typename Interface>
        void client_from_graph(
            Wiring &w,
            AdaptorPath user_path,
            Port<input_schema_t<Interface>> input)
        {
            auto source = input_stub_source<Interface>(w, user_path);
            capture_input<Interface>(w, std::move(input), source, user_path);
        }

        template <typename Interface>
        [[nodiscard]] Port<output_schema_t<Interface>> client_to_graph(Wiring &w, AdaptorPath user_path)
        {
            return output_source<Interface>(w, user_path).template as<output_schema_t<Interface>>();
        }
    }  // namespace detail

    template <typename Interface, typename Impl, typename... Args>
    void register_adaptor(Wiring &w, AdaptorPath user_path, const Args &...args)
    {
        static_assert(detail::adaptor_interface<Interface>,
                      "register_adaptor requires a type derived from adaptor::interface");
        detail::wire_impl<Impl>(w, user_path, args...);
    }

    template <typename Interface, typename Impl, typename... Args>
    void register_adaptor(Wiring &w, const Args &...args)
    {
        register_adaptor<Interface, Impl>(w, detail::default_adaptor_path<Interface>(), args...);
    }

    template <typename Impl, typename... Interfaces, typename... Args>
    void register_adaptors(Wiring &w, AdaptorPath user_path, const Args &...args)
    {
        static_assert(sizeof...(Interfaces) > 0,
                      "register_adaptors requires at least one adaptor interface");
        static_assert((detail::adaptor_interface<Interfaces> && ...),
                      "register_adaptors requires adaptor::interface descriptor types");
        detail::wire_impl<Impl>(w, user_path, args...);
    }

    template <typename Interface>
        requires detail::has_input_schema<Interface>::value
    [[nodiscard]] Port<detail::input_schema_t<Interface>> from_graph(Wiring &w, AdaptorPath user_path)
    {
        return detail::input_stub_source<Interface>(w, user_path).template as<detail::input_schema_t<Interface>>();
    }

    template <typename Interface>
        requires detail::has_input_schema<Interface>::value
    [[nodiscard]] Port<detail::input_schema_t<Interface>> from_graph(Wiring &w)
    {
        return from_graph<Interface>(w, detail::default_adaptor_path<Interface>());
    }

    template <typename Interface>
        requires detail::has_output_schema<Interface>::value
    void to_graph(Wiring &w,
                  AdaptorPath user_path,
                  Port<detail::output_schema_t<Interface>> output)
    {
        auto shared_output = detail::output_source<Interface>(w, user_path);
        detail::capture_output<Interface>(w, std::move(output), shared_output, user_path);
    }

    template <typename Interface>
        requires detail::has_output_schema<Interface>::value
    void to_graph(Wiring &w, Port<detail::output_schema_t<Interface>> output)
    {
        to_graph<Interface>(w, detail::default_adaptor_path<Interface>(), std::move(output));
    }

    template <typename Interface>
        requires(detail::has_input_schema<Interface>::value &&
                 detail::has_output_schema<Interface>::value)
    [[nodiscard]] Port<detail::output_schema_t<Interface>> adaptor(
        Wiring &w,
        AdaptorPath user_path,
        Port<detail::input_schema_t<Interface>> input)
    {
        detail::client_from_graph<Interface>(w, user_path, std::move(input));
        return detail::client_to_graph<Interface>(w, std::move(user_path));
    }

    template <typename Interface>
        requires(detail::has_input_schema<Interface>::value &&
                 detail::has_output_schema<Interface>::value)
    [[nodiscard]] Port<detail::output_schema_t<Interface>> adaptor(
        Wiring &w,
        Port<detail::input_schema_t<Interface>> input)
    {
        return adaptor<Interface>(w, detail::default_adaptor_path<Interface>(), std::move(input));
    }

    template <typename Interface>
        requires(!detail::has_input_schema<Interface>::value &&
                 detail::has_output_schema<Interface>::value)
    [[nodiscard]] Port<detail::output_schema_t<Interface>> adaptor(Wiring &w, AdaptorPath user_path)
    {
        return detail::client_to_graph<Interface>(w, std::move(user_path));
    }

    template <typename Interface>
        requires(!detail::has_input_schema<Interface>::value &&
                 detail::has_output_schema<Interface>::value)
    [[nodiscard]] Port<detail::output_schema_t<Interface>> adaptor(Wiring &w)
    {
        return adaptor<Interface>(w, detail::default_adaptor_path<Interface>());
    }

    template <typename Interface>
        requires(detail::has_input_schema<Interface>::value &&
                 !detail::has_output_schema<Interface>::value)
    void adaptor(Wiring &w, AdaptorPath user_path, Port<detail::input_schema_t<Interface>> input)
    {
        detail::client_from_graph<Interface>(w, std::move(user_path), std::move(input));
    }

    template <typename Interface>
        requires(detail::has_input_schema<Interface>::value &&
                 !detail::has_output_schema<Interface>::value)
    void adaptor(Wiring &w, Port<detail::input_schema_t<Interface>> input)
    {
        adaptor<Interface>(w, detail::default_adaptor_path<Interface>(), std::move(input));
    }

    template <typename Interface>
        requires(detail::has_input_schema<Interface>::value &&
                 !detail::has_output_schema<Interface>::value)
    void sink_adaptor(Wiring &w, AdaptorPath user_path, Port<detail::input_schema_t<Interface>> input)
    {
        adaptor<Interface>(w, std::move(user_path), std::move(input));
    }

    template <typename Interface>
        requires(detail::has_input_schema<Interface>::value &&
                 !detail::has_output_schema<Interface>::value)
    void sink_adaptor(Wiring &w, Port<detail::input_schema_t<Interface>> input)
    {
        sink_adaptor<Interface>(w, detail::default_adaptor_path<Interface>(), std::move(input));
    }
}  // namespace hgraph::adaptor

namespace hgraph::graph_wiring_detail
{
    template <typename Interface, typename OutSchema, typename... Args>
        requires adaptor::detail::adaptor_interface<Interface>
    struct wire_customization<Interface, OutSchema, Args...>
    {
        static constexpr bool enabled = true;

        static auto wire(Wiring &w, const Args &...args)
        {
            static_assert(std::is_void_v<OutSchema>,
                          "wire<Adaptor, OutSchema>: adaptor output schema is defined by the adaptor interface");
            return adaptor::adaptor<Interface>(w, args...);
        }
    };
}  // namespace hgraph::graph_wiring_detail

#endif  // HGRAPH_TYPES_ADAPTOR_WIRING_H
