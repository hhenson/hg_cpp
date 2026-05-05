// Tests for the value-layer plumbing: ``ValueOps`` synthesis,
// ``ValueTypeBinding`` interning, ``StorageHandle`` SBO behaviour, and the
// owning ``Value`` + non-owning ``ValueView`` round-trip for atomic kinds.
// Composite kinds (Tuple, Bundle, List, Set, Map, …) and view casting come
// in a follow-on slice once the per-kind storage shapes are ported.

#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/metadata/type_binding.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_ops.h>
#include <hgraph/types/value/value_view.h>

#include <cstdint>
#include <stdexcept>
#include <string>

TEST_CASE("ValueOps: ops_for<T> returns a stable canonical vtable")
{
    using namespace hgraph;
    REQUIRE(&ops_for<int>() == &ops_for<int>());
    REQUIRE(&ops_for<int>() != &ops_for<double>());

    const ValueOps &ops = ops_for<int>();
    REQUIRE(ops.hash != nullptr);
    REQUIRE(ops.equals != nullptr);
    REQUIRE(ops.compare != nullptr);
    REQUIRE(ops.to_string != nullptr);

    int a = 42;
    int b = 42;
    int c = 7;
    REQUIRE(ops.equals(&a, &b));
    REQUIRE_FALSE(ops.equals(&a, &c));
    REQUIRE(ops.compare(&a, &b) == 0);
    REQUIRE(ops.compare(&c, &a) < 0);
    REQUIRE(ops.compare(&a, &c) > 0);
    REQUIRE(ops.hash(&a) == ops.hash(&b));
    REQUIRE(ops.to_string(&a) == "42");
}

TEST_CASE("ValueOps: bool to_string and string round-trip use type-specific paths")
{
    using namespace hgraph;
    bool t = true;
    bool f = false;
    REQUIRE(ops_for<bool>().to_string(&t) == "true");
    REQUIRE(ops_for<bool>().to_string(&f) == "false");

    std::string s{"hello"};
    REQUIRE(ops_for<std::string>().to_string(&s) == "hello");
}

TEST_CASE("TypeRegistry::register_scalar pairs the schema with a binding")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *meta     = registry.register_scalar<int>("int");

    const auto *binding = registry.scalar_binding<int>();
    REQUIRE(binding != nullptr);
    REQUIRE(binding->valid());
    REQUIRE(binding->type_meta == meta);
    REQUIRE(binding->plan() == &MemoryUtils::plan_for<int>());
    REQUIRE(binding->ops == &ops_for<int>());

    // Idempotency: re-registering returns the same binding pointer.
    (void)registry.register_scalar<int>("int");
    REQUIRE(registry.scalar_binding<int>() == binding);
}

TEST_CASE("TypeRegistry::scalar_binding returns null for unregistered types")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    REQUIRE(registry.scalar_binding<int>() == nullptr);  // no register_scalar yet
}

// ``StorageHandle`` itself has its own coverage in ``test_memory_utils.cpp``;
// here we exercise the value-layer round-trip that uses it through
// ``Value`` and ``ValueTypeBinding``.

TEST_CASE("Value: atomic round-trip — construct, view, hash/equals/to_string")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    (void)registry.register_scalar<int>("int");

    Value v{42};
    REQUIRE(v.has_value());
    REQUIRE(v.schema() != nullptr);
    REQUIRE(v.schema()->kind == ValueTypeKind::Atomic);

    // Owning-handle accessors.
    REQUIRE(v.as<int>() == 42);
    REQUIRE(*v.try_as<int>() == 42);
    REQUIRE(v.try_as<double>() == nullptr);  // type mismatch via view -> still fine, atomic but wrong T
    REQUIRE(v.to_string() == "42");

    // ValueView round-trip.
    ValueView view = v.view();
    REQUIRE(view.valid());
    REQUIRE(view.is_atomic());
    REQUIRE(view.checked_as<int>() == 42);
    REQUIRE(view.hash() == ops_for<int>().hash(view.data()));
    REQUIRE(view.to_string() == "42");

    // Mutate through the view; the owning Value sees the new value.
    view.as<int>() = 99;
    REQUIRE(v.as<int>() == 99);
}

TEST_CASE("Value: equality and ordering through bound ValueOps")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    (void)registry.register_scalar<int>("int");

    Value a{10};
    Value b{10};
    Value c{20};

    REQUIRE(a.equals(b));
    REQUIRE_FALSE(a.equals(c));
    REQUIRE(a.compare(b) == 0);
    REQUIRE(a.compare(c) < 0);
    REQUIRE(c.compare(a) > 0);
    REQUIRE(a.hash() == b.hash());
    REQUIRE(a.hash() != c.hash());
}

TEST_CASE("Value: default-constructed Value has no payload")
{
    using namespace hgraph;
    Value v;
    REQUIRE_FALSE(v.has_value());
    REQUIRE(v.schema() == nullptr);
    REQUIRE(v.view().valid() == false);
}

TEST_CASE("Value: Value(binding) builds a default-valued payload of the bound type")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    (void)registry.register_scalar<double>("double");

    const ValueTypeBinding *binding = registry.scalar_binding<double>();
    REQUIRE(binding != nullptr);

    Value v{*binding};
    REQUIRE(v.has_value());
    REQUIRE(v.schema() != nullptr);
    REQUIRE(v.schema()->kind == ValueTypeKind::Atomic);
    REQUIRE(v.as<double>() == 0.0);  // default-constructed double
}

TEST_CASE("Value: move construction transfers ownership")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    (void)registry.register_scalar<int>("int");

    Value original{123};
    Value moved{std::move(original)};
    REQUIRE_FALSE(original.has_value());
    REQUIRE(moved.has_value());
    REQUIRE(moved.as<int>() == 123);
}

TEST_CASE("Value: throws when a scalar type has not been registered")
{
    using namespace hgraph;
    REQUIRE_THROWS_AS(Value(42), std::logic_error);
}
