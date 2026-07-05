#include <hgraph/types/service_runtime.h>

#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/adaptor_wiring.h>
#include <hgraph/util/scope.h>
#include <hgraph/types/service_wiring.h>

#include <array>
#include <algorithm>
#include <memory>
#include <stdexcept>
#include <unordered_map>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] bool schemas_match(const RuntimeServiceDescriptor &lhs, const RuntimeServiceDescriptor &rhs)
        {
            return lhs.flavour == rhs.flavour && lhs.output_schema == rhs.output_schema &&
                   lhs.key_type == rhs.key_type && lhs.value_schema == rhs.value_schema &&
                   lhs.request_schema == rhs.request_schema && lhs.response_schema == rhs.response_schema &&
                   lhs.input_schema == rhs.input_schema;
        }

        /** Immortal (registry rule): descriptor addresses must stay stable. */
        [[nodiscard]] std::unordered_map<std::string, RuntimeServiceDescriptor *> &descriptor_registry()
        {
            static auto *registry = new std::unordered_map<std::string, RuntimeServiceDescriptor *>{};
            return *registry;
        }

        [[nodiscard]] std::string user_or_default_path(const RuntimeServiceDescriptor &descriptor,
                                                       std::string_view path)
        {
            if (!path.empty()) { return std::string{path}; }
            if (!descriptor.default_path.empty()) { return descriptor.default_path; }
            return descriptor.name + "_default";
        }

        // The name-qualified full-path grammar — IDENTICAL to the template
        // side (service_full_path): "<prefix><user-path>/<name>".
        [[nodiscard]] std::string full_path(std::string_view prefix, const RuntimeServiceDescriptor &descriptor,
                                            std::string_view user_path)
        {
            std::string resolved = user_or_default_path(descriptor, user_path);
            if (resolved.starts_with(prefix)) { return resolved; }
            std::string result{prefix};
            result.append(resolved);
            result.push_back('/');
            result.append(descriptor.name);
            return result;
        }

        [[nodiscard]] std::string reference_base(const RuntimeServiceDescriptor &d, std::string_view p)
        {
            return full_path("ref_svc://", d, p);
        }

        [[nodiscard]] std::string subscription_base(const RuntimeServiceDescriptor &d, std::string_view p)
        {
            return full_path("subs_svc://", d, p);
        }

        [[nodiscard]] std::string request_reply_base(const RuntimeServiceDescriptor &d, std::string_view p)
        {
            return full_path("reqrepl_svc://", d, p);
        }

        void require_flavour(const RuntimeServiceDescriptor &descriptor, ServiceFlavour flavour, const char *what)
        {
            if (descriptor.flavour != flavour)
            {
                throw std::invalid_argument("service '" + descriptor.name + "': not a " + what + " service");
            }
        }

        // --- the erased node factories (the template detail bodies over
        // runtime metas; same role markers, same node makers) ---

        [[nodiscard]] WiringPortRef shared_output_source_node(Wiring &w, std::type_index role,
                                                              const TSValueTypeMetaData *target_meta,
                                                              std::string path)
        {
            const auto *ref_meta = TypeRegistry::instance().ref(target_meta);
            WiringNodeSchema schema;
            schema.output = ref_meta;
            schema.state  = ref_meta->value_schema;
            return w.add_node(role, schema, std::span<const WiringPortRef>{},
                              service::detail::path_key_value(path),
                              [path, target_meta]() { return make_shared_output_source_node(path, *target_meta); });
        }

        [[nodiscard]] const WiringInstance *shared_output_capture_node(Wiring &w, std::type_index role,
                                                                       const TSValueTypeMetaData *output_meta,
                                                                       const std::string &path,
                                                                       const WiringPortRef &output,
                                                                       const WiringPortRef &shared_output)
        {
            std::array<WiringPortRef, 2>  sources{output, shared_output};
            std::array<WiringInputRef, 2> inputs{{
                WiringInputRef{.source = sources[0]},
                WiringInputRef{.source = sources[1], .rank_dependency = false},
            }};
            NodeBuilder builder = make_shared_output_capture_node(path, *output_meta);
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                builder.binding().type_meta->input_schema,
                std::span<const WiringPortRef>{sources.data(), sources.size()}));
            WiringPortRef capture = w.add_node(role, std::move(builder),
                                               std::span<const WiringInputRef>{inputs.data(), inputs.size()},
                                               Value{});
            // Shared-output relays are rank-correct and same-cycle (see the
            // template counterpart in service_wiring.h).
            w.add_same_cycle_pair(capture.peered_node(), shared_output.peered_node());
            return capture.peered_node();
        }

        [[nodiscard]] WiringPortRef wire_impl(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                              const WiredFn &impl, std::span<const WiringPortRef> inputs)
        {
            if (!impl.valid())
            {
                throw std::invalid_argument("service '" + descriptor.name + "': implementation is not wirable");
            }
            return impl.wire(w, inputs);
        }
    }  // namespace

    const RuntimeServiceDescriptor &intern_service_descriptor(RuntimeServiceDescriptor descriptor)
    {
        if (descriptor.name.empty()) { throw std::invalid_argument("service descriptor requires a name"); }
        auto &registry = descriptor_registry();
        auto  found    = registry.find(descriptor.name);
        if (found != registry.end())
        {
            if (!schemas_match(*found->second, descriptor))
            {
                throw std::invalid_argument("service '" + descriptor.name +
                                            "' is already interned with different schemas");
            }
            return *found->second;
        }
        auto *record = new RuntimeServiceDescriptor{std::move(descriptor)};
        registry.emplace(record->name, record);
        return *record;
    }

    const RuntimeServiceDescriptor *find_service_descriptor(std::string_view name)
    {
        const auto &registry = descriptor_registry();
        const auto  found    = registry.find(std::string{name});
        return found != registry.end() ? found->second : nullptr;
    }

    std::vector<std::string> service_descriptor_names()
    {
        std::vector<std::string> names;
        names.reserve(descriptor_registry().size());
        for (const auto &[name, descriptor] : descriptor_registry()) { names.push_back(name); }
        std::sort(names.begin(), names.end());
        return names;
    }

    // ------------------------------------------------------------------
    // Reference
    // ------------------------------------------------------------------

    WiringPortRef reference_service_client(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                           std::string_view path)
    {
        require_flavour(descriptor, ServiceFlavour::Reference, "reference");
        const std::string base = reference_base(descriptor, path);
        w.register_service_client_path(base, "reference service");
        WiringPortRef source = shared_output_source_node(
            w, std::type_index(typeid(service::detail::reference_output_source_marker)),
            descriptor.output_schema, base);
        w.register_service_client_rank(base, "reference service", source.peered_node(), true);
        // Clients read the impl output by REF; the typed facade (Port::as)
        // erases to a descriptive-schema patch - input binding inserts the
        // REF adaptation exactly as for the template path.
        source.schema = descriptor.output_schema;
        return source;
    }

    void register_reference_service_impl(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                         std::string_view path, const WiredFn &impl)
    {
        require_flavour(descriptor, ServiceFlavour::Reference, "reference");
        const std::string base = reference_base(descriptor, path);
        w.register_built_service_path(base, "reference service");
        WiringPortRef shared = shared_output_source_node(
            w, std::type_index(typeid(service::detail::reference_output_source_marker)),
            descriptor.output_schema, base);
        WiringPortRef output = wire_impl(w, descriptor, impl, {});
        const WiringInstance *capture = shared_output_capture_node(
            w, std::type_index(typeid(service::detail::reference_output_capture_marker)),
            output.schema != nullptr ? output.schema : descriptor.output_schema, base, output, shared);
        w.register_service_rank_anchor(base, capture);
    }

    // ------------------------------------------------------------------
    // Subscription
    // ------------------------------------------------------------------

    WiringPortRef subscription_service_subscribe(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                                 std::string_view path, const WiringPortRef &key)
    {
        require_flavour(descriptor, ServiceFlavour::Subscription, "subscription");
        const std::string base       = subscription_base(descriptor, path);
        const std::string subs_path  = base + "/subs";
        const std::string out_path   = base + "/out";
        w.register_service_client_path(base, "subscription service");

        const auto *subs_meta = TypeRegistry::instance().tss(descriptor.key_type);
        WiringNodeSchema subs_schema;
        subs_schema.output = subs_meta;
        WiringPortRef subscriptions = w.add_node(
            std::type_index(typeid(service::detail::subscription_source_marker)), subs_schema,
            std::span<const WiringPortRef>{}, service::detail::path_key_value(subs_path),
            [subs_path, key_meta = descriptor.key_type]() {
                return make_subscription_key_source_node(subs_path, *key_meta);
            });

        const auto *dict_meta = TypeRegistry::instance().tsd(descriptor.key_type, descriptor.value_schema);
        WiringPortRef shared = shared_output_source_node(
            w, std::type_index(typeid(service::detail::shared_output_source_marker)), dict_meta, out_path);

        w.register_service_rank_anchor(subs_path, subscriptions.peered_node());
        w.register_service_client_rank(out_path, "subscription service", shared.peered_node(), true);

        // The subscription capture (a rank-free next-cycle forwarder).
        std::array<WiringPortRef, 2>  sources{key, subscriptions};
        std::array<WiringInputRef, 2> inputs{{
            WiringInputRef{.source = sources[0]},
            WiringInputRef{.source = sources[1], .rank_dependency = false},
        }};
        NodeBuilder builder = make_subscription_key_capture_node(subs_path, *descriptor.key_type);
        builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
            builder.binding().type_meta->input_schema,
            std::span<const WiringPortRef>{sources.data(), sources.size()}));
        WiringPortRef capture = w.add_node(
            std::type_index(typeid(service::detail::subscription_capture_marker)), std::move(builder),
            std::span<const WiringInputRef>{inputs.data(), inputs.size()}, Value{});
        w.register_service_client_rank(subs_path, "subscription service", capture.peered_node(), false);

        // The subscribed value: the shared TSD (descriptive-schema patched,
        // as Port::as does) keyed at the subscribed key.
        WiringPortRef dict = shared;
        dict.schema        = dict_meta;
        WiringArg dict_arg;
        dict_arg.kind = WiringArg::Kind::TimeSeries;
        dict_arg.port = dict;
        WiringArg key_arg;
        key_arg.kind = WiringArg::Kind::TimeSeries;
        key_arg.port = key;
        std::array<WiringArg, 2> item_args{dict_arg, key_arg};
        ResolvedOperatorCall resolved = OperatorRegistry::instance().resolve(
            "getitem_", std::span<const WiringArg>{item_args.data(), item_args.size()});
        return resolved.impl->wire(w, resolved.map, resolved.args, resolved.kwargs).output;
    }

    void register_subscription_service_impl(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                            std::string_view path, const WiredFn &impl)
    {
        require_flavour(descriptor, ServiceFlavour::Subscription, "subscription");
        const std::string base      = subscription_base(descriptor, path);
        const std::string subs_path = base + "/subs";
        const std::string out_path  = base + "/out";
        w.register_built_service_path(base, "subscription service");

        const auto *subs_meta = TypeRegistry::instance().tss(descriptor.key_type);
        WiringNodeSchema subs_schema;
        subs_schema.output = subs_meta;
        WiringPortRef subscriptions = w.add_node(
            std::type_index(typeid(service::detail::subscription_source_marker)), subs_schema,
            std::span<const WiringPortRef>{}, service::detail::path_key_value(subs_path),
            [subs_path, key_meta = descriptor.key_type]() {
                return make_subscription_key_source_node(subs_path, *key_meta);
            });

        const auto *dict_meta = TypeRegistry::instance().tsd(descriptor.key_type, descriptor.value_schema);
        WiringPortRef shared = shared_output_source_node(
            w, std::type_index(typeid(service::detail::shared_output_source_marker)), dict_meta, out_path);
        w.register_service_rank_anchor(subs_path, subscriptions.peered_node());

        std::array<WiringPortRef, 1> impl_inputs{subscriptions};
        WiringPortRef output = wire_impl(w, descriptor, impl, impl_inputs);
        const WiringInstance *capture = shared_output_capture_node(
            w, std::type_index(typeid(service::detail::shared_output_capture_marker)),
            output.schema != nullptr ? output.schema : dict_meta, out_path, output, shared);
        w.register_service_rank_anchor(out_path, capture);
    }

    // ------------------------------------------------------------------
    // Request/reply
    // ------------------------------------------------------------------

    namespace
    {
        [[nodiscard]] WiringPortRef request_input_source_node(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                                              const std::string &request_path)
        {
            const auto *dict_meta = TypeRegistry::instance().tsd(scalar_descriptor<Int>::value_meta(),
                                                                 descriptor.request_schema);
            WiringNodeSchema schema;
            schema.output = dict_meta;
            schema.state  = service::detail::request_input_state_schema(*descriptor.request_schema);
            return w.add_node(
                std::type_index(typeid(service::detail::request_input_source_marker)), schema,
                std::span<const WiringPortRef>{}, service::detail::path_key_value(request_path),
                [request_path, request_meta = descriptor.request_schema]() {
                    return make_request_input_source_node(request_path, *request_meta);
                });
        }

        [[nodiscard]] WiringPortRef reply_output_source_node(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                                             const std::string &replies_path)
        {
            const auto *dict_meta = TypeRegistry::instance().tsd(scalar_descriptor<Int>::value_meta(),
                                                                 descriptor.response_schema);
            return shared_output_source_node(
                w, std::type_index(typeid(service::detail::request_reply_output_source_marker)), dict_meta,
                replies_path);
        }
    }  // namespace

    WiringPortRef request_reply_service_call(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                             std::string_view path, const WiringPortRef &request)
    {
        require_flavour(descriptor, ServiceFlavour::RequestReply, "request/reply");
        const std::string base         = request_reply_base(descriptor, path);
        const std::string request_path = base + "/request";
        const std::string replies_path = base + "/replies";
        w.register_service_client_path(base, "request/reply service");

        const Int request_id = service::detail::next_request_id();
        WiringPortRef requests = request_input_source_node(w, descriptor, request_path);
        WiringPortRef replies  = reply_output_source_node(w, descriptor, replies_path);
        w.register_service_rank_anchor(request_path, requests.peered_node());

        std::array<WiringPortRef, 2>  sources{request, requests};
        std::array<WiringInputRef, 2> inputs{{
            WiringInputRef{.source = sources[0]},
            WiringInputRef{.source = sources[1], .rank_dependency = false},
        }};
        NodeBuilder builder =
            make_request_input_capture_node(request_path, *descriptor.request_schema, request_id);
        builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
            builder.binding().type_meta->input_schema,
            std::span<const WiringPortRef>{sources.data(), sources.size()}));
        WiringPortRef capture = w.add_node(
            std::type_index(typeid(service::detail::request_input_capture_marker)), std::move(builder),
            std::span<const WiringInputRef>{inputs.data(), inputs.size()}, Value{request_id});
        w.register_service_client_rank(request_path, "request/reply service", capture.peered_node(), false);
        w.register_service_client_rank(replies_path, "request/reply service", replies.peered_node(), true);

        WiringPortRef dict = replies;
        dict.schema        = TypeRegistry::instance().tsd(scalar_descriptor<Int>::value_meta(),
                                                          descriptor.response_schema);
        WiringArg dict_arg;
        dict_arg.kind = WiringArg::Kind::TimeSeries;
        dict_arg.port = dict;
        WiringArg id_arg;
        id_arg.kind         = WiringArg::Kind::Scalar;
        id_arg.scalar_value = Value{request_id};
        id_arg.scalar_meta  = id_arg.scalar_value.schema();
        std::array<WiringArg, 2> item_args{dict_arg, id_arg};
        ResolvedOperatorCall resolved = OperatorRegistry::instance().resolve(
            "getitem_", std::span<const WiringArg>{item_args.data(), item_args.size()});
        return resolved.impl->wire(w, resolved.map, resolved.args, resolved.kwargs).output;
    }

    void register_request_reply_service_impl(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                             std::string_view path, const WiredFn &impl)
    {
        require_flavour(descriptor, ServiceFlavour::RequestReply, "request/reply");
        const std::string base         = request_reply_base(descriptor, path);
        const std::string request_path = base + "/request";
        const std::string replies_path = base + "/replies";
        w.register_built_service_path(base, "request/reply service");

        WiringPortRef requests = request_input_source_node(w, descriptor, request_path);
        WiringPortRef replies  = reply_output_source_node(w, descriptor, replies_path);
        w.register_service_rank_anchor(request_path, requests.peered_node());

        std::array<WiringPortRef, 1> impl_inputs{requests};
        WiringPortRef output = wire_impl(w, descriptor, impl, impl_inputs);
        const auto *dict_meta = TypeRegistry::instance().tsd(scalar_descriptor<Int>::value_meta(),
                                                             descriptor.response_schema);
        const WiringInstance *capture = shared_output_capture_node(
            w, std::type_index(typeid(service::detail::request_reply_output_capture_marker)),
            output.schema != nullptr ? output.schema : dict_meta, replies_path, output, replies);
        w.register_service_rank_anchor(replies_path, capture);
    }

    // ------------------------------------------------------------------
    // Multi-interface implementations (register_services + impl_input /
    // impl_output, erased)
    // ------------------------------------------------------------------

    namespace
    {
        [[nodiscard]] std::string flavour_base(const RuntimeServiceDescriptor &d, std::string_view p)
        {
            switch (d.flavour)
            {
                case ServiceFlavour::Reference: return reference_base(d, p);
                case ServiceFlavour::Subscription: return subscription_base(d, p);
                case ServiceFlavour::RequestReply: return request_reply_base(d, p);
                case ServiceFlavour::Adaptor: break;
            }
            throw std::invalid_argument("service '" + d.name + "': not a service flavour");
        }
    }  // namespace

    WiringPortRef service_impl_input(Wiring &w, const RuntimeServiceDescriptor &descriptor, std::string_view path)
    {
        switch (descriptor.flavour)
        {
            case ServiceFlavour::Subscription: {
                const std::string endpoint = subscription_base(descriptor, path) + "/subs";
                w.register_service_implementation_stub(endpoint, "subscription service");
                const auto *subs_meta = TypeRegistry::instance().tss(descriptor.key_type);
                WiringNodeSchema schema;
                schema.output = subs_meta;
                WiringPortRef source = w.add_node(
                    std::type_index(typeid(service::detail::subscription_source_marker)), schema,
                    std::span<const WiringPortRef>{}, service::detail::path_key_value(endpoint),
                    [endpoint, key_meta = descriptor.key_type]() {
                        return make_subscription_key_source_node(endpoint, *key_meta);
                    });
                w.register_service_rank_anchor(endpoint, source.peered_node());
                return source;
            }
            case ServiceFlavour::RequestReply: {
                const std::string endpoint = request_reply_base(descriptor, path) + "/request";
                w.register_service_implementation_stub(endpoint, "request/reply service");
                WiringPortRef source = request_input_source_node(w, descriptor, endpoint);
                w.register_service_rank_anchor(endpoint, source.peered_node());
                return source;
            }
            default:
                throw std::invalid_argument("service '" + descriptor.name + "': flavour has no impl input");
        }
    }

    void service_impl_output(Wiring &w, const RuntimeServiceDescriptor &descriptor, std::string_view path,
                             const WiringPortRef &out)
    {
        switch (descriptor.flavour)
        {
            case ServiceFlavour::Reference: {
                const std::string base = reference_base(descriptor, path);
                w.register_service_implementation_stub(base, "reference service");
                WiringPortRef shared = shared_output_source_node(
                    w, std::type_index(typeid(service::detail::reference_output_source_marker)),
                    descriptor.output_schema, base);
                const WiringInstance *capture = shared_output_capture_node(
                    w, std::type_index(typeid(service::detail::reference_output_capture_marker)),
                    out.schema != nullptr ? out.schema : descriptor.output_schema, base, out, shared);
                w.register_service_rank_anchor(base, capture);
                return;
            }
            case ServiceFlavour::Subscription: {
                const std::string endpoint = subscription_base(descriptor, path) + "/out";
                w.register_service_implementation_stub(endpoint, "subscription service");
                const auto *dict_meta =
                    TypeRegistry::instance().tsd(descriptor.key_type, descriptor.value_schema);
                WiringPortRef shared = shared_output_source_node(
                    w, std::type_index(typeid(service::detail::shared_output_source_marker)), dict_meta, endpoint);
                const WiringInstance *capture = shared_output_capture_node(
                    w, std::type_index(typeid(service::detail::shared_output_capture_marker)),
                    out.schema != nullptr ? out.schema : dict_meta, endpoint, out, shared);
                w.register_service_rank_anchor(endpoint, capture);
                return;
            }
            case ServiceFlavour::RequestReply: {
                const std::string endpoint = request_reply_base(descriptor, path) + "/replies";
                w.register_service_implementation_stub(endpoint, "request/reply service");
                WiringPortRef shared = reply_output_source_node(w, descriptor, endpoint);
                const auto *dict_meta = TypeRegistry::instance().tsd(scalar_descriptor<Int>::value_meta(),
                                                                     descriptor.response_schema);
                const WiringInstance *capture = shared_output_capture_node(
                    w, std::type_index(typeid(service::detail::request_reply_output_capture_marker)),
                    out.schema != nullptr ? out.schema : dict_meta, endpoint, out, shared);
                w.register_service_rank_anchor(endpoint, capture);
                return;
            }
            default:
                throw std::invalid_argument("service '" + descriptor.name + "': flavour has no impl output");
        }
    }

    void register_multi_service_impl(Wiring &w, std::span<const RuntimeServiceDescriptor *const> descriptors,
                                     std::string_view path, const WiredFn &impl)
    {
        if (descriptors.empty())
        {
            throw std::invalid_argument("register_multi_service_impl requires at least one interface");
        }
        std::vector<WiringServiceImplementationEndpoint> required_endpoints;
        std::string description{"multi-service implementation:"};
        for (const RuntimeServiceDescriptor *descriptor : descriptors)
        {
            const std::string base = flavour_base(*descriptor, path);
            const char *what = descriptor->flavour == ServiceFlavour::Reference    ? "reference service"
                               : descriptor->flavour == ServiceFlavour::Subscription ? "subscription service"
                                                                                     : "request/reply service";
            w.register_built_service_path(base, what);
            description += ' ';
            description += base;
            switch (descriptor->flavour)
            {
                case ServiceFlavour::Reference:
                    required_endpoints.push_back(WiringServiceImplementationEndpoint{base, {}});
                    break;
                case ServiceFlavour::Subscription:
                    required_endpoints.push_back(WiringServiceImplementationEndpoint{base + "/subs", {}});
                    required_endpoints.push_back(WiringServiceImplementationEndpoint{base + "/out", {}});
                    break;
                case ServiceFlavour::RequestReply:
                    required_endpoints.push_back(WiringServiceImplementationEndpoint{base + "/request", {}});
                    required_endpoints.push_back(WiringServiceImplementationEndpoint{base + "/replies", {}});
                    break;
                default: break;
            }
        }
        w.begin_service_implementation(std::move(description), std::move(required_endpoints));
        auto unwind = UnwindCleanupGuard([&] { w.cancel_service_implementation(); });
        static_cast<void>(wire_impl(w, *descriptors.front(), impl, {}));
        unwind.release();
        w.end_service_implementation();
    }

    // ------------------------------------------------------------------
    // Adaptors (adaptor_wiring.h erased; same recipe as the services)
    // ------------------------------------------------------------------

    namespace
    {
        [[nodiscard]] std::string adaptor_base(const RuntimeServiceDescriptor &d, std::string_view p)
        {
            return full_path("adaptor://", d, p);
        }
    }  // namespace

    WiringPortRef adaptor_from_graph(Wiring &w, const RuntimeServiceDescriptor &descriptor, std::string_view path)
    {
        require_flavour(descriptor, ServiceFlavour::Adaptor, "adaptor");
        if (descriptor.input_schema == nullptr)
        {
            throw std::invalid_argument("adaptor '" + descriptor.name + "' has no input schema");
        }
        const std::string endpoint = adaptor_base(descriptor, path) + "/from_graph";
        w.register_service_implementation_stub(endpoint, "adaptor");
        const auto *ref_meta = TypeRegistry::instance().ref(descriptor.input_schema);
        WiringNodeSchema schema;
        schema.output = ref_meta;
        schema.state  = ref_meta->value_schema;
        WiringPortRef source = w.add_node(
            std::type_index(typeid(adaptor::detail::input_stub_source_marker)), schema,
            std::span<const WiringPortRef>{}, adaptor::detail::path_key_value(endpoint),
            [endpoint, input_meta = descriptor.input_schema]() {
                return make_shared_output_source_node(endpoint, *input_meta, false);
            });
        w.register_service_rank_anchor(endpoint, source.peered_node());
        source.schema = descriptor.input_schema;   // the erased Port::as facade
        return source;
    }

    void adaptor_to_graph(Wiring &w, const RuntimeServiceDescriptor &descriptor, std::string_view path,
                          const WiringPortRef &out)
    {
        require_flavour(descriptor, ServiceFlavour::Adaptor, "adaptor");
        if (descriptor.output_schema == nullptr)
        {
            throw std::invalid_argument("adaptor '" + descriptor.name + "' has no output schema");
        }
        const std::string endpoint = adaptor_base(descriptor, path) + "/to_graph";
        w.register_service_implementation_stub(endpoint, "adaptor");
        WiringPortRef shared = shared_output_source_node(
            w, std::type_index(typeid(adaptor::detail::output_source_marker)), descriptor.output_schema, endpoint);
        const WiringInstance *capture = shared_output_capture_node(
            w, std::type_index(typeid(adaptor::detail::output_capture_marker)),
            out.schema != nullptr ? out.schema : descriptor.output_schema, endpoint, out, shared);
        w.register_service_rank_anchor(endpoint, capture);
    }

    WiringPortRef adaptor_client(Wiring &w, const RuntimeServiceDescriptor &descriptor, std::string_view path,
                                 const WiringPortRef *in)
    {
        require_flavour(descriptor, ServiceFlavour::Adaptor, "adaptor");
        const std::string base = adaptor_base(descriptor, path);
        w.register_service_client_path(base, "adaptor");
        if (descriptor.input_schema != nullptr)
        {
            if (in == nullptr)
            {
                throw std::invalid_argument("adaptor '" + descriptor.name + "' requires an input");
            }
            const std::string endpoint = base + "/from_graph";
            const auto *ref_meta = TypeRegistry::instance().ref(descriptor.input_schema);
            WiringNodeSchema schema;
            schema.output = ref_meta;
            schema.state  = ref_meta->value_schema;
            WiringPortRef source = w.add_node(
                std::type_index(typeid(adaptor::detail::input_stub_source_marker)), schema,
                std::span<const WiringPortRef>{}, adaptor::detail::path_key_value(endpoint),
                [endpoint, input_meta = descriptor.input_schema]() {
                    return make_shared_output_source_node(endpoint, *input_meta, false);
                });
            w.register_service_rank_anchor(endpoint, source.peered_node());
            const WiringInstance *capture = shared_output_capture_node(
                w, std::type_index(typeid(adaptor::detail::input_capture_marker)),
                in->schema != nullptr ? in->schema : descriptor.input_schema, endpoint, *in, source);
            w.register_service_client_rank(endpoint, "adaptor", capture, false);
        }
        if (descriptor.output_schema != nullptr)
        {
            const std::string endpoint = base + "/to_graph";
            WiringPortRef shared = shared_output_source_node(
                w, std::type_index(typeid(adaptor::detail::output_source_marker)), descriptor.output_schema,
                endpoint);
            w.register_service_client_rank(endpoint, "adaptor", shared.peered_node(), true);
            shared.schema = descriptor.output_schema;   // the erased Port::as facade
            return shared;
        }
        return WiringPortRef{};
    }

    void register_adaptor_impl(Wiring &w, const RuntimeServiceDescriptor &descriptor, std::string_view path,
                               const WiredFn &impl)
    {
        require_flavour(descriptor, ServiceFlavour::Adaptor, "adaptor");
        const std::string base = adaptor_base(descriptor, path);
        w.register_built_service_path(base, "adaptor");
        std::vector<WiringServiceImplementationEndpoint> required_endpoints;
        if (descriptor.input_schema != nullptr)
        {
            required_endpoints.push_back(WiringServiceImplementationEndpoint{base + "/from_graph", {}});
        }
        if (descriptor.output_schema != nullptr)
        {
            required_endpoints.push_back(WiringServiceImplementationEndpoint{base + "/to_graph", {}});
        }
        w.begin_service_implementation("adaptor " + base, std::move(required_endpoints));
        auto unwind = UnwindCleanupGuard([&] { w.cancel_service_implementation(); });
        static_cast<void>(wire_impl(w, descriptor, impl, {}));
        unwind.release();
        w.end_service_implementation();
    }
}  // namespace hgraph