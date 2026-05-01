#ifndef HGRAPH_CPP_ROOT_TS_VALUE_PLAN_FACTORY_H
#define HGRAPH_CPP_ROOT_TS_VALUE_PLAN_FACTORY_H

#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/utils/memory_utils.h>

#include <mutex>
#include <unordered_map>

namespace hgraph
{
    /**
     * Factory that maps a time-series ``TSValueTypeMetaData`` schema to
     * its canonical ``MemoryUtils::StoragePlan``.
     *
     * Sibling of ``ValuePlanFactory`` for the time-series layer. The
     * runtime layout of a time-series combines:
     *
     * - the underlying value-layer payload (resolved through the
     *   ``ValuePlanFactory`` for ``TS[T]``, the value side of ``TSD``,
     *   and so on),
     * - the per-instance time-series state tree
     *   (``last_modified_time``, observers, kind-specific state), and
     * - kind-specific storage shapes (``KeySlotStore`` / ``ValueSlotStore``
     *   for ``TSS`` and ``TSD``, slot-backed dynamic storage for dynamic
     *   ``TSL``, and so on).
     *
     * Implementation of those storage shapes lives in the value layer
     * and the time-series layer; until those layers are ported, the
     * factory's interface is in place but ``plan_for`` throws
     * ``std::logic_error`` for every TS kind. The interface exists now
     * so callers can be written against the final shape.
     *
     * The factory is a process-wide singleton via ``instance()``;
     * non-copyable and non-movable.
     */
    class TSValuePlanFactory
    {
      public:
        /** Singleton accessor; returns a reference to the process-wide factory. */
        static TSValuePlanFactory &instance();

        TSValuePlanFactory(const TSValuePlanFactory &)            = delete;
        TSValuePlanFactory &operator=(const TSValuePlanFactory &) = delete;
        TSValuePlanFactory(TSValuePlanFactory &&)                 = delete;
        TSValuePlanFactory &operator=(TSValuePlanFactory &&)      = delete;

        /**
         * Look up or synthesise the canonical plan for ``schema``.
         *
         * Returns ``nullptr`` when ``schema`` is null. For all TS kinds
         * currently throws ``std::logic_error`` because the underlying
         * value/TS-layer storage shapes are not yet ported.
         */
        const MemoryUtils::StoragePlan *plan_for(const TSValueTypeMetaData *schema);

        /** Look up only; never synthesises. Returns ``nullptr`` when missing. */
        const MemoryUtils::StoragePlan *find(const TSValueTypeMetaData *schema) const;

      private:
        TSValuePlanFactory() = default;

        [[noreturn]] static void unsupported(const TSValueTypeMetaData *schema);

        mutable std::mutex                                                                mutex_;
        std::unordered_map<const TSValueTypeMetaData *, const MemoryUtils::StoragePlan *> cache_;
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_VALUE_PLAN_FACTORY_H
