// Tests for the value-layer mutable (structurally-mutable) List — a growable,
// slot-store-backed dynamic list distinct from the immutable compact list.
// Covers the Mutable schema axis, distinct interning, build/append/read,
// set/pop/clear, copy independence, and holding heterogeneous Any elements.

#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/value.h>

#include <string>

namespace
{
    using namespace hgraph;

    Value make_mutable_list(const ValueTypeMetaData *element_meta)
    {
        const auto *schema  = TypeRegistry::instance().mutable_list(element_meta);
        const auto *binding = ValuePlanFactory::instance().binding_for(schema);
        REQUIRE(binding != nullptr);
        return Value{*binding};
    }

    Value make_any(const Value &inner)
    {
        const auto *binding = ValuePlanFactory::instance().binding_for(TypeRegistry::instance().any());
        Value       any{*binding};
        any.as_any().begin_mutation().set(inner.view());
        return any;
    }
}  // namespace

TEST_CASE("mutable list: the Mutable schema axis interns distinctly from the immutable list")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");

    const ValueTypeMetaData *immutable = registry.list(int_meta);
    const ValueTypeMetaData *mutable_  = registry.mutable_list(int_meta);

    REQUIRE(mutable_ != nullptr);
    CHECK(mutable_->kind == ValueTypeKind::List);
    CHECK(mutable_->is_mutable());
    CHECK_FALSE(immutable->is_mutable());
    CHECK(mutable_ != immutable);                       // distinct schemas
    CHECK(registry.mutable_list(int_meta) == mutable_);  // interned singleton per element
}

TEST_CASE("mutable list: build empty, append, and read back")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");

    Value list = make_mutable_list(int_meta);
    REQUIRE(list.has_value());
    CHECK(list.view().is_list());
    CHECK(list.as_list().size() == 0);
    CHECK(list.to_string() == "[]");

    {
        auto mutation = list.as_list().begin_mutation();
        mutation.push_back(Value{1}.view());
        mutation.push_back(Value{2}.view());
        mutation.push_back(Value{3}.view());
    }

    auto view = list.as_list();
    REQUIRE(view.size() == 3);
    CHECK(view.at(0).checked_as<int>() == 1);
    CHECK(view.at(1).checked_as<int>() == 2);
    CHECK(view.at(2).checked_as<int>() == 3);
    CHECK(list.to_string() == "[1, 2, 3]");
}

TEST_CASE("mutable list: set, pop_back and clear")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");

    Value list = make_mutable_list(int_meta);
    {
        auto m = list.as_list().begin_mutation();
        m.push_back(Value{10}.view());
        m.push_back(Value{20}.view());
        m.push_back(Value{30}.view());
        m.set(1, Value{99}.view());
        m.pop_back();
    }
    {
        auto view = list.as_list();
        REQUIRE(view.size() == 2);
        CHECK(view.at(0).checked_as<int>() == 10);
        CHECK(view.at(1).checked_as<int>() == 99);
    }
    list.as_list().begin_mutation().clear();
    CHECK(list.as_list().size() == 0);
    CHECK(list.to_string() == "[]");
}

TEST_CASE("mutable list: erase by index shifts later elements down")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");

    Value list = make_mutable_list(int_meta);
    {
        auto m = list.as_list().begin_mutation();
        m.push_back(Value{10}.view());
        m.push_back(Value{20}.view());
        m.push_back(Value{30}.view());
        m.push_back(Value{40}.view());
        m.erase(1);  // remove 20 -> [10, 30, 40]
    }
    {
        auto view = list.as_list();
        REQUIRE(view.size() == 3);
        CHECK(view.at(0).checked_as<int>() == 10);
        CHECK(view.at(1).checked_as<int>() == 30);
        CHECK(view.at(2).checked_as<int>() == 40);
    }

    {
        auto m = list.as_list().begin_mutation();
        m.erase(0);  // remove 10 -> [30, 40]
        m.erase(1);  // remove 40 -> [30]
    }
    {
        auto view = list.as_list();
        REQUIRE(view.size() == 1);
        CHECK(view.at(0).checked_as<int>() == 30);
    }

    CHECK_THROWS(list.as_list().begin_mutation().erase(5));  // out of range
}

TEST_CASE("mutable list: equality and a copy is independent")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");

    Value a = make_mutable_list(int_meta);
    {
        auto m = a.as_list().begin_mutation();
        m.push_back(Value{1}.view());
        m.push_back(Value{2}.view());
    }

    Value b = a;  // deep copy
    CHECK(a.equals(b));

    // Mutating the copy leaves the original unchanged.
    b.as_list().begin_mutation().push_back(Value{3}.view());
    CHECK(b.as_list().size() == 3);
    CHECK(a.as_list().size() == 2);
    CHECK_FALSE(a.equals(b));
}

TEST_CASE("mutable list: holds heterogeneous Any elements")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<int>("int");
    (void)registry.register_scalar<std::string>("str");
    const auto *any_meta = registry.any();

    Value list = make_mutable_list(any_meta);
    {
        Value an_int = make_any(Value{7});
        Value a_str  = make_any(Value{std::string{"hi"}});
        auto  m      = list.as_list().begin_mutation();
        m.push_back(an_int.view());
        m.push_back(a_str.view());
    }

    auto view = list.as_list();
    REQUIRE(view.size() == 2);
    CHECK(view.at(0).as_any().get().checked_as<int>() == 7);
    CHECK(view.at(1).as_any().get().checked_as<std::string>() == "hi");
}
