#ifndef HGRAPH_CPP_ROOT_TS_DATA_PLAN_FACTORY_H
#define HGRAPH_CPP_ROOT_TS_DATA_PLAN_FACTORY_H

#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/time_series/ts_data.h>
#include <hgraph/types/utils/memory_utils.h>

#include <mutex>
#include <unordered_map>

namespace hgraph
{
    /**
     * Factory that maps a time-series ``TSValueTypeMetaData`` schema to the
     * canonical ``MemoryUtils::StoragePlan`` and default ``TSDataBinding``
     * for the TS data component.
     *
     * ``TSOutput`` and ``TSInput`` are the top-level runtime time-series
     * holders. They combine endpoint state (modification scope, validity,
     * subscribers, binding, and scheduling) with a payload/delta component
     * named ``TSData``. This factory resolves only the ``TSData`` memory plan.
     * Endpoint construction is handled by the reusable input/output builders
     * that compose this data plan with the separate endpoint state.
     *
     * Atomic TSData uses the compact value storage plan with mutable ops
     * enabled. Fixed ``TSB`` and fixed-size ``TSL`` allocate the complete
     * current value as the first canonical value-layer region, then store
     * child/parent tracking in a separate auxiliary tree. Keyed collection
     * TSData uses slot-oriented storage so current payload and delta
     * bookkeeping stay aligned by stable slot id.
     *
     * Implemented synthesis paths cover atomic TSData (``TS<T>``, ``REF<T>``,
     * and ``SIGNAL``), fixed structured TSData (``TSB`` and fixed-size
     * ``TSL``), and keyed slot TSData (``TSS`` and ``TSD``). Dynamic ``TSL``
     * currently throws ``std::logic_error`` until its slot-oriented storage is
     * implemented.
     *
     * The factory is a process-wide singleton via ``instance()``;
     * non-copyable and non-movable.
     */
    class TSDataPlanFactory
    {
      public:
        /** Singleton accessor; returns a reference to the process-wide factory. */
        static TSDataPlanFactory &instance();

        TSDataPlanFactory(const TSDataPlanFactory &)            = delete;
        TSDataPlanFactory &operator=(const TSDataPlanFactory &) = delete;
        TSDataPlanFactory(TSDataPlanFactory &&)                 = delete;
        TSDataPlanFactory &operator=(TSDataPlanFactory &&)      = delete;

        /**
         * Look up or synthesise the canonical plan for ``schema``.
         *
         * Returns ``nullptr`` when ``schema`` is null. Dynamic ``TSL``
         * currently throws ``std::logic_error`` until its slot-oriented
         * TSData store is implemented.
         */
        const MemoryUtils::StoragePlan *plan_for(const TSValueTypeMetaData *schema);

        /** Look up only; never synthesises. Returns ``nullptr`` when missing. */
        const MemoryUtils::StoragePlan *find(const TSValueTypeMetaData *schema) const;

        /**
         * Look up or synthesise the canonical default TSData binding for
         * ``schema``. Returns ``nullptr`` when ``schema`` is null.
         */
        const TSDataBinding *binding_for(const TSValueTypeMetaData *schema);

        /** Binding lookup only; never synthesises. Returns ``nullptr`` when missing. */
        const TSDataBinding *find_binding(const TSValueTypeMetaData *schema) const;

        /**
         * Drop every cached schema → plan mapping. Test-only helper used to
         * isolate tests from each other; production code must not call it.
         */
        void reset() noexcept;

      private:
        TSDataPlanFactory() = default;

        const MemoryUtils::StoragePlan *synthesise(const TSValueTypeMetaData *schema);
        const TSDataBinding            *synthesise_binding(const TSValueTypeMetaData *schema);

        mutable std::mutex                                                                mutex_;
        std::unordered_map<const TSValueTypeMetaData *, const MemoryUtils::StoragePlan *> cache_;
        std::unordered_map<const TSValueTypeMetaData *, const TSDataBinding *>             binding_cache_;
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_DATA_PLAN_FACTORY_H
