#ifndef HGRAPH_CPP_ROOT_VALUE_PLAN_FACTORY_H
#define HGRAPH_CPP_ROOT_VALUE_PLAN_FACTORY_H

#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/utils/memory_utils.h>

#include <mutex>
#include <unordered_map>

namespace hgraph
{
    /**
     * Factory that maps a value-layer ``ValueTypeMetaData`` schema to its
     * canonical ``MemoryUtils::StoragePlan``.
     *
     * The schema describes *what* a value is; the plan describes *how* it
     * is laid out in memory. The factory bridges the two so callers never
     * have to reach into ``MemoryUtils`` directly when all they have is a
     * schema pointer. Results are cached against the schema, so repeated
     * lookups are O(1) and pointer-stable for the lifetime of the
     * registry.
     *
     * **Atomic kinds.** Atomic schemas have no intrinsic layout
     * information on the schema side; the canonical plan must be supplied
     * by the registry at registration time via ``register_atomic``. The
     * intended path is ``TypeRegistry::register_scalar<T>``, which passes
     * ``&MemoryUtils::plan_for<T>()`` through to the factory.
     *
     * **Composite kinds.** Tuple, bundle, and fixed-size list plans are
     * synthesised on first use by recursively resolving component
     * schemas and feeding the resulting plans into
     * ``MemoryUtils::tuple_plan`` / ``named_tuple_plan`` / ``array_plan``.
     * Because those builders intern their results, the factory's cache
     * lines up with the global plan cache.
     *
     * **Container kinds (Set, Map, CyclicBuffer, Queue, dynamic List).**
     * These require the value-layer's container storage shapes
     * (``DynamicListStorage``, ``SetStorage``, ``MapStorage``, etc.).
     * Until that layer is ported, the factory throws
     * ``std::logic_error`` for these kinds.
     *
     * The factory is a process-wide singleton via ``instance()``;
     * non-copyable and non-movable.
     */
    class ValuePlanFactory
    {
      public:
        /** Singleton accessor; returns a reference to the process-wide factory. */
        static ValuePlanFactory &instance();

        ValuePlanFactory(const ValuePlanFactory &)            = delete;
        ValuePlanFactory &operator=(const ValuePlanFactory &) = delete;
        ValuePlanFactory(ValuePlanFactory &&)                 = delete;
        ValuePlanFactory &operator=(ValuePlanFactory &&)      = delete;

        /**
         * Pair an atomic schema with its canonical storage plan.
         *
         * Called by ``TypeRegistry::register_scalar`` so that
         * ``plan_for(atomic_schema)`` can return the matching
         * ``MemoryUtils::plan_for<T>()`` afterwards. Re-registration with
         * the same plan is a no-op; re-registration with a different plan
         * throws.
         */
        void register_atomic(const ValueTypeMetaData *schema, const MemoryUtils::StoragePlan *plan);

        /**
         * Look up or synthesise the canonical plan for ``schema``.
         *
         * Returns ``nullptr`` when ``schema`` is null. For atomic schemas
         * not previously registered, throws. For tuple / bundle / fixed
         * list schemas, recursively resolves component plans and
         * synthesises via ``MemoryUtils`` builders. For other container
         * kinds, throws (value-layer support not yet ported).
         */
        const MemoryUtils::StoragePlan *plan_for(const ValueTypeMetaData *schema);

        /** Look up only; never synthesises. Returns ``nullptr`` when missing. */
        const MemoryUtils::StoragePlan *find(const ValueTypeMetaData *schema) const;

      private:
        ValuePlanFactory() = default;

        const MemoryUtils::StoragePlan *synthesise(const ValueTypeMetaData *schema);

        mutable std::mutex                                                              mutex_;
        std::unordered_map<const ValueTypeMetaData *, const MemoryUtils::StoragePlan *> cache_;
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_VALUE_PLAN_FACTORY_H
