#ifndef HGRAPH_CPP_ROOT_VALUE_CONTAINER_OPS_H
#define HGRAPH_CPP_ROOT_VALUE_CONTAINER_OPS_H

#include <hgraph/types/metadata/type_binding.h>
#include <hgraph/types/value/value_ops.h>
#include <hgraph/types/value/value_range.h>
#include <hgraph/types/value/value_view.h>

#include <cstddef>

namespace hgraph
{
    /**
     * Per-kind ops surfaces for the value-layer container kinds.
     *
     * This header defines only the *type* surfaces — the layout-agnostic
     * vtables that views call through. Each storage flavour
     * (compact value-layer, slot-store-backed time-series, etc.) lives
     * in its own ``*_container_ops.h`` and supplies the matching
     * thunks plus a canonical-binding factory. The compact value-layer
     * implementation lives in ``compact_container_ops.h``.
     *
     * Each container kind extends the base ``ValueOps`` (hash / equals
     * / compare / to_string) with the read accessors the typed views
     * need — ``size``, ``element_at``, ``contains``, ``front``,
     * ``head``, etc. The typed view receives a pointer to the
     * kind-specific ops sub-class (downcast from the binding's
     * ``const ValueOps *``) and calls through it without ever casting
     * the storage data pointer to a concrete C++ storage class.
     *
     * Order semantics:
     *
     * - ``List`` / ``CyclicBuffer`` / ``Queue`` — order-dependent
     *   (lexicographic hash and compare).
     * - ``Set`` — order-independent (xor of element hashes;
     *   subset+superset for equality).
     * - ``Map`` — order-independent (xor of (key, value) pair hashes;
     *   same key set with same key→value for equality).
     */

    // -----------------------------------------------------------------
    // Per-kind ops hierarchy — mirrors the specialised-view hierarchy.
    //
    //   ValueOps              — hash, equals, compare, to_string
    //     IndexedValueOps     — adds size, element_at, element_binding
    //                           (used by every kind whose view exposes
    //                            indexed iteration)
    //       ListValueOps      — no additions
    //       CyclicBufferValueOps — adds head
    //       QueueValueOps     — adds front
    //       SetValueOps       — adds contains
    //       MapValueOps       — adds contains plus the value side
    //                           (value_at by key, value_at_index,
    //                            value_binding). ``element_at`` /
    //                           ``element_binding`` on the indexed
    //                           base point at the *key* surface, so
    //                           the map iterator walks keys via the
    //                           base ops and pairs them with values
    //                           via the derived ops.
    //
    // Views always reach the kind-specific surface by static-casting
    // the binding's ``const ValueOps *`` to the matching sub-class.
    // No view ever casts the storage data pointer to a concrete C++
    // storage class — that keeps the views layout-agnostic.
    // -----------------------------------------------------------------

    // Forward-declared because ``MapValueOps::key_set`` returns a
    // ``SetView`` by value. The view definition lives in
    // ``specialized_views.h``.
    class SetView;

    struct IndexedValueOps : ValueOps
    {
        const void *context{nullptr};
        std::size_t (*size)(const void *context, const void *memory) noexcept = nullptr;
        const void *(*element_at)(const void *context, const void *memory, std::size_t index) = nullptr;
        const ValueTypeBinding *(*element_binding)(const void *context, const void *memory,
                                                   std::size_t index) noexcept = nullptr;

        /**
         * Iteration is exposed as a single op that returns a
         * ``Range<ValueView>``. The range carries an opaque
         * ``context`` (typically the storage pointer), an ordinal
         * ``limit`` (the high water mark of the index space), an
         * optional ``predicate`` filter (null for dense layouts;
         * non-null for slot-stored layouts that need to skip dead
         * slots) and a ``projector`` that builds a ``ValueView`` from
         * ``(context, index)``. Range-based views call
         * ``make_range(context, memory)`` and use the range's own
         * iterator surface, which keeps iteration layout-agnostic.
         */
        Range<ValueView> (*make_range)(const void *context, const void *memory) = nullptr;
    };

    struct ListValueOps : IndexedValueOps
    {
    };

    struct CyclicBufferValueOps : IndexedValueOps
    {
        std::size_t (*head)(const void *memory) noexcept = nullptr;
    };

    struct QueueValueOps : IndexedValueOps
    {
        const void *(*front)(const void *memory) = nullptr;
    };

    struct SetValueOps : IndexedValueOps
    {
        bool (*contains)(const void *memory, const void *key) = nullptr;
    };

    struct MapValueOps : IndexedValueOps
    {
        bool (*contains)(const void *memory, const void *key)                = nullptr;
        const void *(*value_at)(const void *memory, const void *key)         = nullptr;
        const void *(*value_at_index)(const void *context, const void *memory, std::size_t index) = nullptr;
        const ValueTypeBinding *(*value_binding)(const void *context, const void *memory) noexcept = nullptr;
        /**
         * Three iteration surfaces. Each follows the predicate /
         * projector pattern; the bound thunks decide what the
         * projector returns.
         *
         * - ``make_keys_range`` — yields ``ValueView`` over keys
         *   only. Equivalent to the ``IndexedValueOps::make_range``
         *   inherited from the base (the map's indexed surface
         *   walks keys), but exposed on ``MapValueOps`` for
         *   discoverability.
         * - ``make_values_range`` — yields ``ValueView`` over
         *   values only.
         * - ``make_kv_range`` — yields ``std::pair<ValueView,
         *   ValueView>`` (key, value) pairs.
         */
        Range<ValueView> (*make_keys_range)(const void *memory)                  = nullptr;
        Range<ValueView> (*make_values_range)(const void *memory)                = nullptr;
        KeyValueRange<ValueView, ValueView> (*make_kv_range)(const void *memory) = nullptr;
        /**
         * Build a read-only ``SetView`` over the map's keys. The
         * returned view is a *wrapper* over the map's memory — it
         * uses set-shaped ops that delegate into the map's read
         * accessors, so the implementation does not require unifying
         * the map and set storage layouts. Each map kind installs its
         * own ``key_set`` thunk paired with a SetValueOps adapter
         * that knows that map's layout.
         */
        SetView (*key_set)(const ValueTypeBinding *map_binding, const void *memory) = nullptr;
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_VALUE_CONTAINER_OPS_H
