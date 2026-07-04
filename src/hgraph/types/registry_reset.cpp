#include <hgraph/types/value/json_codec.h>
#include <hgraph/types/value/table_codec.h>
#include <hgraph/types/registry_reset.h>

#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/type_binding.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/time_series/ts_data.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/value/compact_storage.h>
#include <hgraph/types/value/value_ops.h>

namespace hgraph
{
    void reset_all_registries() noexcept
    {
        // Ordering contract: borrowers before lenders. See the header for the
        // full rationale; the type registry must be LAST because it owns the
        // schemas every other registry borrows by pointer.
        // Force TypeRegistry construction FIRST: instance() seeds the standard
        // vocabulary on first touch, populating the plan caches below. If that
        // first touch happened at the reset() call at the END of this
        // function, the clears would run BEFORE the first seed instead of
        // between the seeds, leaving prior-generation entries to collide with
        // re-seeded metas on reused addresses (the stale-pointer-reuse class;
        // conflict manifests as "already registered with a different plan").
        TypeRegistry &type_registry = TypeRegistry::instance();

        OperatorRegistry::instance().reset();
        clear_json_converters();   // interns by meta/binding pointer — must precede the lenders below
        clear_table_converters();  // same rule (also captures record_replay config keys)
        ValuePlanFactory::instance().reset();
        TSDataPlanFactory::instance().reset();
        TSInputBuilderFactory::reset();
        clear_compact_container_plans();
        TSDataBinding::clear();
        ValueTypeBinding::clear();
        type_registry.reset();  // re-seeds the standard scalar/TS vocabulary
    }
}  // namespace hgraph
