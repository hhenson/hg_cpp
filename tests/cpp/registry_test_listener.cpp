// Catch2 event listener that resets every process-wide registry/factory
// before each test case. Tests that mutate the registries (intern atomic
// scalars, register named bundles, etc.) would otherwise leak state into
// later cases — most visibly through the named-bundle / named-tsb
// uniqueness checks, which see prior registrations as conflicts.
//
// Linking this translation unit into the test executable activates the
// listener via Catch2's CATCH_REGISTER_LISTENER macro; no test code needs
// to call anything explicitly.

#include <catch2/catch_session.hpp>
#include <catch2/reporters/catch_reporter_event_listener.hpp>
#include <catch2/reporters/catch_reporter_registrars.hpp>

#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/type_binding.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/time_series/ts_data.h>
#include <hgraph/types/value/compact_storage.h>
#include <hgraph/types/value/value_ops.h>

namespace
{
    void reset_registries() noexcept
    {
        // Plan factories hold borrowed schema pointers; bindings hold
        // borrowed schema + plan + ops pointers; compact-container plan
        // registries hold lifecycle context that references those
        // bindings. Clear all of them before the type registry's reset
        // destroys the underlying schemas.
        hgraph::ValuePlanFactory::instance().reset();
        hgraph::TSDataPlanFactory::instance().reset();
        hgraph::clear_compact_container_plans();
        hgraph::TSDataBinding::clear();
        hgraph::ValueTypeBinding::clear();
        hgraph::TypeRegistry::instance().reset();
    }

    class RegistryResetListener final : public Catch::EventListenerBase
    {
      public:
        using Catch::EventListenerBase::EventListenerBase;

        void testCaseStarting(const Catch::TestCaseInfo &) override { reset_registries(); }
        void testCaseEnded(const Catch::TestCaseStats &) override { reset_registries(); }
    };
}  // namespace

CATCH_REGISTER_LISTENER(RegistryResetListener)
