// types/registry_reset.h — the ONE canonical, ordered teardown of every
// process-wide registry/factory. The ordering is load-bearing: several
// registries intern by *pointer* into artifacts another registry owns, so a
// borrower must always be cleared before its lender frees the pointees (a
// missing/misordered clear historically caused stale-pointer reuse and memory
// corruption in aggregate test runs). Any NEW pointer-keyed registry or
// binding cache MUST be added to reset_all_registries() — never grow a second
// teardown sequence elsewhere. Reset is a test-only facility; production
// processes never reset.
#ifndef HGRAPH_TYPES_REGISTRY_RESET_H
#define HGRAPH_TYPES_REGISTRY_RESET_H

#include <hgraph/hgraph_export.h>

namespace hgraph
{
    /**
     * Reset every process-wide registry/factory, in dependency order:
     *
     * 1. ``TypeRecordRegistry`` — common records borrow every other metadata
     *    object and are therefore invalidated first.
     * 2. ``OperatorRegistry`` — candidate patterns borrow interned schema
     *    pointers.
     * 3. ``ValuePlanFactory`` / ``TSDataPlanFactory`` — plans borrow schema
     *    pointers (and clear the MemoryUtils synthesised composite/array plan
     *    registries).
     * 4. ``TSInputBuilderFactory`` — input builders borrow schema + plan
     *    pointers.
     * 5. Compact-container plan registries — lifecycle contexts reference the
     *    bindings below.
     * 6. ``TSDataBinding`` / ``ValueTypeBinding`` — borrow schema + plan + ops
     *    pointers.
     * 7. ``TypeRegistry`` — last, because it owns the schemas everyone above
     *    borrows; its reset re-seeds the standard scalar/TS vocabulary.
     */
    HGRAPH_EXPORT void reset_all_registries() noexcept;
}  // namespace hgraph

#endif  // HGRAPH_TYPES_REGISTRY_RESET_H
