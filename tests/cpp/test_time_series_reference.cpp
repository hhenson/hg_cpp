// Tests for the TimeSeriesReference struct — the C++ value type that
// backs the canonical ``TimeSeriesReference`` atomic schema in the type
// registry.

#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/time_series_reference.h>
#include <hgraph/types/utils/memory_utils.h>

#include <unordered_set>
#include <vector>

TEST_CASE("TimeSeriesReference: default-constructed is empty with no target schema")
{
    using namespace hgraph;
    TimeSeriesReference ref;
    REQUIRE(ref.kind() == TimeSeriesReference::Kind::EMPTY);
    REQUIRE(ref.is_empty());
    REQUIRE_FALSE(ref.is_peered());
    REQUIRE_FALSE(ref.is_non_peered());
    REQUIRE(ref.target_schema() == nullptr);
}

TEST_CASE("TimeSeriesReference: empty reference can record an expected target schema")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);

    TimeSeriesReference ref{ts_int};
    REQUIRE(ref.is_empty());
    REQUIRE(ref.target_schema() == ts_int);
}

TEST_CASE("TimeSeriesReference: peered reference carries kind and target schema")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);

    auto ref = TimeSeriesReference::peered(ts_int);
    REQUIRE(ref.is_peered());
    REQUIRE(ref.target_schema() == ts_int);
}

TEST_CASE("TimeSeriesReference: non-peered reference holds sub-references")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsl      = registry.tsl(ts_int, /*fixed_size=*/2);

    std::vector<TimeSeriesReference> items{
        TimeSeriesReference::peered(ts_int),
        TimeSeriesReference::peered(ts_int),
    };
    TimeSeriesReference composite{tsl, items};

    REQUIRE(composite.is_non_peered());
    REQUIRE(composite.target_schema() == tsl);
    REQUIRE(composite.items().size() == 2);
    REQUIRE(composite[0].target_schema() == ts_int);
    REQUIRE(composite[1].is_peered());
}

TEST_CASE("TimeSeriesReference: items() and operator[] throw for non-NON_PEERED references")
{
    using namespace hgraph;
    TimeSeriesReference empty;
    REQUIRE_THROWS_AS(empty.items(), std::logic_error);
    REQUIRE_THROWS_AS(empty[0], std::logic_error);
}

TEST_CASE("TimeSeriesReference: operator[] bounds-checks NON_PEERED indexes")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);

    std::vector<TimeSeriesReference> items{TimeSeriesReference::peered(ts_int)};
    TimeSeriesReference              composite{ts_int, items};

    REQUIRE(composite[0].is_peered());
    REQUIRE_THROWS_AS(composite[1], std::out_of_range);
}

TEST_CASE("TimeSeriesReference: equality and hash respect kind, target_schema, and sub-items")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);

    const TimeSeriesReference empty_a;
    const TimeSeriesReference empty_b;
    REQUIRE(empty_a == empty_b);
    REQUIRE(empty_a.hash() == empty_b.hash());

    const TimeSeriesReference empty_with_schema{ts_int};
    REQUIRE_FALSE(empty_a == empty_with_schema);

    const auto peered_a = TimeSeriesReference::peered(ts_int);
    const auto peered_b = TimeSeriesReference::peered(ts_int);
    REQUIRE(peered_a == peered_b);
    REQUIRE(peered_a.hash() == peered_b.hash());

    const TimeSeriesReference composite_a{ts_int, {TimeSeriesReference::peered(ts_int)}};
    const TimeSeriesReference composite_b{ts_int, {TimeSeriesReference::peered(ts_int)}};
    const TimeSeriesReference composite_diff{ts_int, {empty_a}};
    REQUIRE(composite_a == composite_b);
    REQUIRE_FALSE(composite_a == composite_diff);

    // hash should distinguish between kinds.
    REQUIRE(empty_a.hash() != peered_a.hash());

    // unordered_set should accept TimeSeriesReference via std::hash specialisation.
    std::unordered_set<TimeSeriesReference> seen;
    seen.insert(empty_a);
    seen.insert(peered_a);
    REQUIRE(seen.size() == 2);
}

TEST_CASE("TimeSeriesReference: empty_reference() returns a stable singleton")
{
    using namespace hgraph;
    const auto &a = TimeSeriesReference::empty_reference();
    const auto &b = TimeSeriesReference::empty_reference();
    REQUIRE(&a == &b);
    REQUIRE(a.is_empty());
    REQUIRE(a.target_schema() == nullptr);
}

TEST_CASE("TimeSeriesReference: registry pairs the type with a real plan via ValuePlanFactory")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = ValuePlanFactory::instance();

    // Trigger registration by asking for any REF schema.
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    (void)registry.ref(ts_int);

    const auto *ref_atom = registry.value_type("TimeSeriesReference");
    REQUIRE(ref_atom != nullptr);
    REQUIRE(ref_atom->kind == ValueTypeKind::Atomic);

    // Unlike the previous synthetic atomic, the registered type now has a
    // canonical plan paired with it.
    const auto *plan = factory.find(ref_atom);
    REQUIRE(plan != nullptr);
    REQUIRE(plan == &MemoryUtils::plan_for<TimeSeriesReference>());
}
