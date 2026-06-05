// Tests for BundleBuilder — assembling a compact (immutable) Bundle Value from
// prebuilt field Values via whole-value copy-assign at the field offsets. This is
// the construction path for the canonical delta bundles (e.g. the TSS delta
// Bundle{added: Set<T>, removed: Set<T>}), which have immutable container fields
// that cannot be populated through begin_mutation.

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

#include <hgraph/lib/std/value_util.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>

namespace
{
    using namespace hgraph;

    // Build the canonical TSS-delta bundle: Bundle{added: Set<std::int32_t>, removed: Set<std::int32_t>}.
    Value make_delta_bundle(std::initializer_list<std::int32_t> added, std::initializer_list<std::int32_t> removed)
    {
        auto       &registry      = TypeRegistry::instance();
        const auto *int_meta       = registry.register_scalar<std::int32_t>("int32");
        const auto *set_meta        = registry.set(int_meta);
        const auto *bundle_schema   = registry.un_named_bundle({{"added", set_meta}, {"removed", set_meta}});
        const auto *bundle_binding  = ValuePlanFactory::instance().binding_for(bundle_schema);

        Value added_set   = stdlib::make_set<std::int32_t>(added);
        Value removed_set = stdlib::make_set<std::int32_t>(removed);

        BundleBuilder builder{*bundle_binding};
        builder.set("added", added_set.view());
        builder.set("removed", removed_set.view());
        return builder.build();
    }
}  // namespace

TEST_CASE("BundleBuilder: assembles a Bundle{Set,Set} and reads its fields back")
{
    using namespace hgraph;

    Value bundle = make_delta_bundle({1, 2}, {});
    REQUIRE(bundle.has_value());

    const auto view  = bundle.view().as_bundle();
    const auto added = view.field("added").as_set();
    CHECK(added.size() == 2);
    CHECK(added.contains(Value{std::int32_t{1}}.view()));
    CHECK(added.contains(Value{std::int32_t{2}}.view()));
    CHECK(view.field("removed").as_set().size() == 0);
}

TEST_CASE("BundleBuilder: equality is content-based and order-independent (Value::equals)")
{
    using namespace hgraph;

    // Same content, different element insertion order -> equal.
    CHECK(make_delta_bundle({1, 2}, {3}).equals(make_delta_bundle({2, 1}, {3})));

    // Different content -> not equal.
    CHECK_FALSE(make_delta_bundle({1, 2}, {3}).equals(make_delta_bundle({1, 2}, {4})));
    CHECK_FALSE(make_delta_bundle({1}, {}).equals(make_delta_bundle({1, 2}, {})));
}

TEST_CASE("BundleBuilder: the built value matches the canonical un_named_bundle schema")
{
    using namespace hgraph;
    auto       &registry     = TypeRegistry::instance();
    const auto *int_meta      = registry.register_scalar<std::int32_t>("int32");
    const auto *set_meta       = registry.set(int_meta);
    const auto *bundle_schema  = registry.un_named_bundle({{"added", set_meta}, {"removed", set_meta}});

    Value bundle = make_delta_bundle({5}, {6});
    // The built value's schema is exactly the interned canonical bundle schema, so
    // it compares (Value::equals) against a runtime-produced delta of the same shape.
    CHECK(bundle.schema() == bundle_schema);
    CHECK_FALSE(bundle.view().to_string().empty());
}
