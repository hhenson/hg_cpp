#ifndef HGRAPH_TYPES_METADATA_TYPE_REALIZATION_H
#define HGRAPH_TYPES_METADATA_TYPE_REALIZATION_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/value/value_type_ref.h>

#include <cstdint>
#include <memory>
#include <vector>

namespace hgraph
{
    class TypeRegistry;

    /**
     * Immutable graph-wiring snapshot of the registered Bundle hierarchy.
     *
     * Canonical exact bindings remain process-wide. A snapshot adds a
     * graph-specific closed-union binding for a declared Bundle that had
     * registered children when the snapshot was captured, and for an abstract
     * Bundle whose construction must be resolved to a concrete descendant.
     * Later registrations cannot change its alternatives or storage size.
     */
    class HGRAPH_EXPORT TypeRealizationSnapshot
    {
      public:
        [[nodiscard]] static std::shared_ptr<const TypeRealizationSnapshot>
        capture(TypeRegistry &registry);

        [[nodiscard]] std::uint64_t generation() const noexcept;
        [[nodiscard]] bool is_polymorphic(const ValueTypeMetaData *schema) const noexcept;
        [[nodiscard]] std::vector<const ValueTypeMetaData *>
        alternatives(const ValueTypeMetaData *schema) const;
        /** Realize one exact schema without replacing it by its closed union. */
        [[nodiscard]] ValueTypeRef exact_type_for(const ValueTypeMetaData *schema) const;
        [[nodiscard]] ValueTypeRef type_for(const ValueTypeMetaData *schema) const;

      private:
        struct Impl;
        explicit TypeRealizationSnapshot(std::shared_ptr<Impl> impl) noexcept;

        std::shared_ptr<Impl> impl_;
    };

    /** Thread-local construction scope used while a graph creates its nodes. */
    class HGRAPH_EXPORT TypeRealizationScope
    {
      public:
        explicit TypeRealizationScope(const TypeRealizationSnapshot *snapshot) noexcept;
        TypeRealizationScope(const TypeRealizationScope &) = delete;
        TypeRealizationScope &operator=(const TypeRealizationScope &) = delete;
        ~TypeRealizationScope() noexcept;

      private:
        const TypeRealizationSnapshot *previous_{nullptr};
    };

    [[nodiscard]] HGRAPH_EXPORT const TypeRealizationSnapshot *active_type_realization() noexcept;

    /** Resolve storage for a wiring-time value. Reuse an enclosing graph's
        closed-union snapshot, or capture the current registered hierarchy. */
    [[nodiscard]] HGRAPH_EXPORT ValueTypeRef
    value_type_for_wiring(const ValueTypeMetaData *schema);

    /** Test-only registry teardown hook; call after TypeRecordRegistry reset. */
    HGRAPH_EXPORT void clear_type_realization_snapshots() noexcept;
}

#endif
