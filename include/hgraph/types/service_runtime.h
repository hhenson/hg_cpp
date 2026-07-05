#ifndef HGRAPH_TYPES_SERVICE_RUNTIME_H
#define HGRAPH_TYPES_SERVICE_RUNTIME_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/wired_fn.h>

#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace hgraph
{
    class Wiring;
    struct WiringPortRef;

    /**
     * Runtime service identity (design record: services.rst *Runtime service
     * identity*; rulings 2026-07-05). A ``RuntimeServiceDescriptor`` is the
     * erased form of a service interface: C++ descriptor types synthesise one
     * at first use, the Python bridge builds one from a stub's annotations.
     * Descriptors intern BY NAME (immortal registry); re-registration with
     * matching schemas returns the interned record, mismatches throw.
     *
     * Node identity rides the name-qualified full path (the path scalar) plus
     * the per-ROLE markers — the same key the C++ templates use — so a Python
     * implementation can serve a C++ interface stub on the same path (the Q1
     * direction; the reverse is not supported).
     */
    enum class ServiceFlavour : std::uint8_t
    {
        Reference,
        Subscription,
        RequestReply,
        Adaptor,
    };

    struct RuntimeServiceDescriptor
    {
        std::string                name;
        ServiceFlavour             flavour{ServiceFlavour::Reference};
        const TSValueTypeMetaData *output_schema{nullptr};     ///< reference
        const ValueTypeMetaData   *key_type{nullptr};          ///< subscription
        const TSValueTypeMetaData *value_schema{nullptr};      ///< subscription
        const TSValueTypeMetaData *request_schema{nullptr};    ///< request/reply
        const TSValueTypeMetaData *response_schema{nullptr};   ///< request/reply
        const TSValueTypeMetaData *input_schema{nullptr};      ///< adaptor (optional)
        // (adaptor output reuses output_schema; either may be null for
        //  sink-/source-only adaptors)
        std::string                default_path;
    };

    /** Intern by name; schema-match enforced on re-registration. The returned
        pointer is stable for the process lifetime. */
    HGRAPH_EXPORT const RuntimeServiceDescriptor &intern_service_descriptor(RuntimeServiceDescriptor descriptor);

    /** The interned descriptor for ``name``, or null. */
    [[nodiscard]] HGRAPH_EXPORT const RuntimeServiceDescriptor *find_service_descriptor(std::string_view name);

    /** Every interned descriptor name (sorted) — discovery for the bridge. */
    [[nodiscard]] HGRAPH_EXPORT std::vector<std::string> service_descriptor_names();

    // ------------------------------------------------------------------
    // The erased flavour flows (the runtime counterparts of the template
    // client/register functions; identical path grammar, role markers and
    // node makers). ``path`` is the USER path ("" = the descriptor's
    // default). Implementations arrive as WiredFn graph callables; note the
    // WiredFn contract is ts-only, so path injection into implementations is
    // not available through this surface.
    // ------------------------------------------------------------------

    [[nodiscard]] HGRAPH_EXPORT WiringPortRef reference_service_client(Wiring &w,
                                                                       const RuntimeServiceDescriptor &descriptor,
                                                                       std::string_view path);
    HGRAPH_EXPORT void register_reference_service_impl(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                                       std::string_view path, const WiredFn &impl);

    [[nodiscard]] HGRAPH_EXPORT WiringPortRef subscription_service_subscribe(
        Wiring &w, const RuntimeServiceDescriptor &descriptor, std::string_view path, const WiringPortRef &key);
    HGRAPH_EXPORT void register_subscription_service_impl(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                                          std::string_view path, const WiredFn &impl);

    [[nodiscard]] HGRAPH_EXPORT WiringPortRef request_reply_service_call(
        Wiring &w, const RuntimeServiceDescriptor &descriptor, std::string_view path, const WiringPortRef &request);
    HGRAPH_EXPORT void register_request_reply_service_impl(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                                           std::string_view path, const WiredFn &impl);

    /**
     * Multi-interface implementations (the ``register_services`` +
     * ``impl_input``/``impl_output`` shape, erased): ONE graph implements
     * several interfaces by fetching each interface's input inside its body
     * and publishing each output explicitly.
     */
    [[nodiscard]] HGRAPH_EXPORT WiringPortRef service_impl_input(Wiring &w,
                                                                 const RuntimeServiceDescriptor &descriptor,
                                                                 std::string_view path);
    HGRAPH_EXPORT void service_impl_output(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                           std::string_view path, const WiringPortRef &out);
    HGRAPH_EXPORT void register_multi_service_impl(Wiring &w,
                                                   std::span<const RuntimeServiceDescriptor *const> descriptors,
                                                   std::string_view path, const WiredFn &impl);

    /** Adaptor client: publishes ``in`` (when the interface has an input)
        and returns the adaptor output ref (empty for sink-only adaptors). */
    [[nodiscard]] HGRAPH_EXPORT WiringPortRef adaptor_client(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                                             std::string_view path, const WiringPortRef *in);
    /** Impl-side: the client input (called inside a registered impl). */
    [[nodiscard]] HGRAPH_EXPORT WiringPortRef adaptor_from_graph(Wiring &w,
                                                                 const RuntimeServiceDescriptor &descriptor,
                                                                 std::string_view path);
    /** Impl-side: publish the adaptor output back to clients. */
    HGRAPH_EXPORT void adaptor_to_graph(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                        std::string_view path, const WiringPortRef &out);
    HGRAPH_EXPORT void register_adaptor_impl(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                             std::string_view path, const WiredFn &impl);
}  // namespace hgraph

#endif  // HGRAPH_TYPES_SERVICE_RUNTIME_H
