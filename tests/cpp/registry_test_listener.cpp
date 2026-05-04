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

#include <hgraph/types/metadata/ts_value_plan_factory.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>

namespace
{
    void reset_registries() noexcept
    {
        // Reset plan factories first, then the type registry. Plan factories
        // hold borrowed schema pointers that the type-registry reset will
        // invalidate; clearing the factories first avoids dangling lookups.
        hgraph::ValuePlanFactory::instance().reset();
        hgraph::TSValuePlanFactory::instance().reset();
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
