// Tests for the value-layer mutable (structurally-mutable) Set — a growable,
// slot-store-backed set distinct from the immutable compact set. Covers the
// Mutable schema axis, distinct interning, add/remove/contains/clear,
// order-independent equality, and copy independence.

#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/value.h>

namespace
{
    using namespace hgraph;

    Value make_mutable_set(const ValueTypeMetaData *element_meta)
    {
        const auto *schema  = TypeRegistry::instance().mutable_set(element_meta);
        const auto *binding = ValuePlanFactory::instance().binding_for(schema);
        REQUIRE(binding != nullptr);
        return Value{*binding};
    }
}  // namespace

TEST_CASE("mutable set: the Mutable schema axis interns distinctly from the immutable set")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");

    const ValueTypeMetaData *immutable = registry.set(int_meta);
    const ValueTypeMetaData *mutable_  = registry.mutable_set(int_meta);

    REQUIRE(mutable_ != nullptr);
    CHECK(mutable_->kind == ValueTypeKind::Set);
    CHECK(mutable_->is_mutable());
    CHECK_FALSE(immutable->is_mutable());
    CHECK(mutable_ != immutable);
    CHECK(registry.mutable_set(int_meta) == mutable_);  // interned singleton per element
}

TEST_CASE("mutable set: add, contains, remove, clear")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");

    Value set = make_mutable_set(int_meta);
    CHECK(set.view().is_set());
    CHECK(set.as_set().size() == 0);

    {
        auto m = set.as_set().begin_mutation();
        CHECK(m.add(Value{1}.view()));
        CHECK(m.add(Value{2}.view()));
        CHECK_FALSE(m.add(Value{1}.view()));  // duplicate: no change
    }
    {
        auto view = set.as_set();
        CHECK(view.size() == 2);
        CHECK(view.contains(Value{1}.view()));
        CHECK_FALSE(view.contains(Value{3}.view()));
    }
    {
        auto m = set.as_set().begin_mutation();
        CHECK(m.remove(Value{1}.view()));
        CHECK_FALSE(m.remove(Value{9}.view()));  // absent: no change
    }
    CHECK(set.as_set().size() == 1);
    CHECK(set.as_set().contains(Value{2}.view()));

    set.as_set().begin_mutation().clear();
    CHECK(set.as_set().size() == 0);
}

TEST_CASE("mutable set: equality is order-independent and a copy is independent")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");

    Value a = make_mutable_set(int_meta);
    {
        auto m = a.as_set().begin_mutation();
        (void)m.add(Value{1}.view());
        (void)m.add(Value{2}.view());
        (void)m.add(Value{3}.view());
    }
    Value b = make_mutable_set(int_meta);
    {
        auto m = b.as_set().begin_mutation();
        (void)m.add(Value{3}.view());  // different insertion order
        (void)m.add(Value{1}.view());
        (void)m.add(Value{2}.view());
    }
    CHECK(a.equals(b));  // order-independent

    Value c = a;  // deep copy
    c.as_set().begin_mutation().clear();
    CHECK(c.as_set().size() == 0);
    CHECK(a.as_set().size() == 3);  // original unaffected
}
