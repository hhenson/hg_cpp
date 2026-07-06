// Tests for BundleBuilder — assembling a compact (immutable) Bundle Value from
// prebuilt field Values via whole-value copy/move-assign at the field offsets.
// This is the construction path for the canonical delta bundles (e.g. the TSS
// delta Bundle{added: Set<T>, removed: Set<T>}), which have immutable container
// fields that cannot be populated through begin_mutation.

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <utility>

#include <hgraph/lib/std/value_util.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>

namespace
{
    using namespace hgraph;

    struct BundleMoveCountingScalar
    {
        inline static int copy_constructs{0};
        inline static int copy_assigns{0};

        int value{0};

        BundleMoveCountingScalar() = default;
        explicit BundleMoveCountingScalar(int value_)
            : value(value_)
        {
        }
        BundleMoveCountingScalar(const BundleMoveCountingScalar &other)
            : value(other.value)
        {
            ++copy_constructs;
        }
        BundleMoveCountingScalar(BundleMoveCountingScalar &&) noexcept = default;

        BundleMoveCountingScalar &operator=(const BundleMoveCountingScalar &other)
        {
            value = other.value;
            ++copy_assigns;
            return *this;
        }
        BundleMoveCountingScalar &operator=(BundleMoveCountingScalar &&) noexcept = default;
    };

    void reset_bundle_move_counting_scalar_counts()
    {
        BundleMoveCountingScalar::copy_constructs = 0;
        BundleMoveCountingScalar::copy_assigns    = 0;
    }

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

TEST_CASE("BundleBuilder: bundle fields are unset until explicitly set")
{
    using namespace hgraph;

    auto       &registry      = TypeRegistry::instance();
    const auto *int_meta       = registry.register_scalar<std::int32_t>("int32");
    const auto *str_meta       = registry.register_scalar<Str>("str");
    const auto *bundle_schema  = registry.un_named_bundle({{"count", int_meta}, {"label", str_meta}});
    const auto *bundle_binding = ValuePlanFactory::instance().binding_for(bundle_schema);
    REQUIRE(bundle_binding != nullptr);

    Value empty{*bundle_binding};
    auto  empty_view = empty.as_bundle();
    CHECK(empty_view.size() == 2);
    CHECK_FALSE(empty_view.at("count").has_value());
    CHECK_FALSE(empty_view.at("label").has_value());
    CHECK_FALSE(empty_view.has_field("___validity"));

    BundleBuilder builder{*bundle_binding};
    CHECK(builder.size() == 2);
    builder.set("count", Value{std::int32_t{42}});
    Value partial = builder.build();

    auto partial_view = partial.as_bundle();
    CHECK(partial_view.size() == 2);
    CHECK(partial_view.at("count").checked_as<std::int32_t>() == 42);
    CHECK_FALSE(partial_view.at("label").has_value());

    BundleBuilder rejecting_builder{*bundle_binding};
    Value         typed_null{*str_meta};
    CHECK_THROWS_AS(rejecting_builder.set("label", typed_null.view()), std::invalid_argument);
}

TEST_CASE("BundleBuilder: moves owned field values")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *field_meta = registry.register_scalar<BundleMoveCountingScalar>("BundleMoveCountingScalar");
    const auto *bundle_schema = registry.un_named_bundle({{"field", field_meta}});
    const auto *bundle_binding = ValuePlanFactory::instance().binding_for(bundle_schema);
    REQUIRE(bundle_binding != nullptr);

    BundleBuilder builder{*bundle_binding};
    Value         field{BundleMoveCountingScalar{13}};

    reset_bundle_move_counting_scalar_counts();
    builder.set("field", std::move(field));
    Value bundle = builder.build();

    auto        field_view = bundle.view().as_bundle().field("field");
    const auto &stored     = field_view.checked_as<BundleMoveCountingScalar>();
    REQUIRE(stored.value == 13);
    REQUIRE(BundleMoveCountingScalar::copy_constructs == 0);
    REQUIRE(BundleMoveCountingScalar::copy_assigns == 0);
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
