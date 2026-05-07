// Tests for the value-layer plumbing: ``ValueOps`` synthesis,
// ``ValueTypeBinding`` interning, ``StorageHandle`` SBO behaviour, and the
// owning ``Value`` + non-owning ``ValueView`` round-trip for atomic and
// structured value-layer kinds.

#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/metadata/type_binding.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_ops.h>
#include <hgraph/types/value/value_view.h>

#if HGRAPH_ENABLE_PYTHON_USER_NODES
#include <Python.h>
#endif

#include <compare>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <type_traits>

#if HGRAPH_ENABLE_PYTHON_USER_NODES
#ifndef HGRAPH_TEST_PYTHON_EXECUTABLE
#define HGRAPH_TEST_PYTHON_EXECUTABLE "python3"
#endif

namespace
{
    struct UnsupportedPythonScalar
    {
        int value{0};
    };

    [[nodiscard]] std::string python_init_error_message(PyStatus status, const char *stage)
    {
        std::string message{stage};
        message += ": ";
        message += status.err_msg != nullptr ? status.err_msg : "unknown Python initialization failure";
        return message;
    }

    class PythonInterpreterGuard
    {
      public:
        PythonInterpreterGuard()
        {
            if (!Py_IsInitialized())
            {
                PyConfig config;
                PyConfig_InitPythonConfig(&config);

                PyStatus status =
                    PyConfig_SetBytesString(&config, &config.program_name, HGRAPH_TEST_PYTHON_EXECUTABLE);
                if (PyStatus_Exception(status))
                {
                    const auto message = python_init_error_message(status, "failed to set Python program_name");
                    PyConfig_Clear(&config);
                    throw std::runtime_error(message);
                }

                status = PyConfig_SetBytesString(&config, &config.executable, HGRAPH_TEST_PYTHON_EXECUTABLE);
                if (PyStatus_Exception(status))
                {
                    const auto message = python_init_error_message(status, "failed to set Python executable");
                    PyConfig_Clear(&config);
                    throw std::runtime_error(message);
                }

                status = Py_InitializeFromConfig(&config);
                const auto message = PyStatus_Exception(status)
                                         ? python_init_error_message(status, "failed to initialize Python")
                                         : std::string{};
                PyConfig_Clear(&config);
                if (!message.empty()) { throw std::runtime_error(message); }
                owned_ = true;
            }
        }

        PythonInterpreterGuard(const PythonInterpreterGuard &)            = delete;
        PythonInterpreterGuard &operator=(const PythonInterpreterGuard &) = delete;

        ~PythonInterpreterGuard()
        {
            if (owned_) { (void)Py_FinalizeEx(); }
        }

      private:
        bool owned_{false};
    };
}  // namespace
#endif

TEST_CASE("ValueOps: ops_for<T> returns a stable canonical vtable")
{
    using namespace hgraph;
    REQUIRE(&ops_for<int>() == &ops_for<int>());
    REQUIRE(&ops_for<int>() != &ops_for<double>());

    const ValueOps &ops = ops_for<int>();
    REQUIRE(ops.hash_impl != nullptr);
    REQUIRE(ops.equals_impl != nullptr);
    REQUIRE(ops.compare_impl != nullptr);
    REQUIRE(ops.to_string_impl != nullptr);

    int a = 42;
    int b = 42;
    int c = 7;
    STATIC_REQUIRE(std::is_same_v<decltype(ops.compare(&a, &b)), std::partial_ordering>);
    REQUIRE(ops.equals(&a, &b));
    REQUIRE_FALSE(ops.equals(&a, &c));
    REQUIRE(std::is_eq(ops.compare(&a, &b)));
    REQUIRE(std::is_lt(ops.compare(&c, &a)));
    REQUIRE(std::is_gt(ops.compare(&a, &c)));
    REQUIRE(ops.hash(&a) == ops.hash(&b));
    REQUIRE(ops.to_string(&a) == "42");
}

TEST_CASE("ValueOps: floating compare preserves unordered comparison results")
{
    using namespace hgraph;
    const ValueOps &ops = ops_for<double>();

    double value = 1.0;
    double nan   = std::numeric_limits<double>::quiet_NaN();

    REQUIRE(ops.compare(&nan, &value) == std::partial_ordering::unordered);
    REQUIRE(ops.compare(&value, &nan) == std::partial_ordering::unordered);
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

    view.set<int>(123);
    REQUIRE(v.as<int>() == 123);
    REQUIRE(view.is_scalar_type<int>());
    REQUIRE_FALSE(view.is_scalar_type<double>());
    REQUIRE(view.is_type(v.schema()));
}

TEST_CASE("Value: equality and ordering through bound ValueOps")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    (void)registry.register_scalar<double>("double");

    Value a{10};
    Value b{10};
    Value c{20};
    Value d{10.0};

    STATIC_REQUIRE(std::is_same_v<decltype(a.compare(b)), std::partial_ordering>);
    REQUIRE(a.equals(b));
    REQUIRE_FALSE(a.equals(c));
    REQUIRE(std::is_eq(a.compare(b)));
    REQUIRE(std::is_lt(a.compare(c)));
    REQUIRE(std::is_gt(c.compare(a)));
    REQUIRE(a.compare(d) == std::partial_ordering::unordered);
    REQUIRE(a.hash() == b.hash());
    REQUIRE(a.hash() != c.hash());

    Value empty;
    REQUIRE(std::is_eq(empty.compare(empty)));
    REQUIRE(std::is_lt(empty.compare(a)));
    REQUIRE(std::is_gt(a.compare(empty)));
    REQUIRE(std::is_eq(ValueView{}.compare(ValueView{})));
    REQUIRE(std::is_lt(ValueView{}.compare(a.view())));
    REQUIRE(std::is_gt(a.view().compare(ValueView{})));
    REQUIRE(a.view().compare(d.view()) == std::partial_ordering::unordered);

    Value null_int{*int_meta};
    Value null_int_2{*int_meta};
    const auto *double_meta = registry.value_type("double");
    Value null_double{*double_meta};
    REQUIRE(null_int.equals(null_int_2));
    REQUIRE_FALSE(null_int.equals(null_double));
    REQUIRE(std::is_eq(null_int.compare(null_int_2)));
    REQUIRE(null_int.compare(null_double) == std::partial_ordering::unordered);
    REQUIRE(std::is_lt(null_int.compare(a)));
    REQUIRE(std::is_gt(a.compare(null_int)));
}

TEST_CASE("ValueView: clone and copy_from preserve binding and payload")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    (void)registry.register_scalar<double>("double");

    Value source{42};
    Value cloned = source.view().clone();
    REQUIRE(cloned.binding() == source.binding());
    REQUIRE(cloned.as<int>() == 42);
    cloned.as<int>() = 7;
    REQUIRE(source.as<int>() == 42);

    Value target{0};
    target.view().copy_from(source.view());
    REQUIRE(target.as<int>() == 42);

    Value other_type{3.0};
    REQUIRE_FALSE(target.view().try_copy_from(other_type.view()));
    REQUIRE_THROWS_AS(target.view().copy_from(other_type.view()), std::invalid_argument);

    Value typed_null{*int_meta};
    Value typed_null_clone = typed_null.view().clone();
    REQUIRE_FALSE(typed_null_clone.has_value());
    REQUIRE(typed_null_clone.binding() == typed_null.binding());
    REQUIRE_THROWS_AS(ValueView{}.clone(), std::logic_error);
}

TEST_CASE("Value: default-constructed Value has no payload")
{
    using namespace hgraph;
    Value v;
    REQUIRE_FALSE(v.has_value());
    REQUIRE(v.schema() == nullptr);
    REQUIRE(v.view().valid() == false);
}

TEST_CASE("Value: Value(schema) preserves binding in typed-null state")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");

    Value v{*int_meta};
    REQUIRE_FALSE(v.has_value());
    REQUIRE(v.schema() == int_meta);
    REQUIRE(v.binding() == ValuePlanFactory::instance().binding_for(int_meta));
    REQUIRE_FALSE(v.view().valid());

    Value copy = v;
    REQUIRE_FALSE(copy.has_value());
    REQUIRE(copy.schema() == int_meta);
    REQUIRE(copy.binding() == v.binding());
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

    v.reset();
    REQUIRE_FALSE(v.has_value());
    REQUIRE(v.schema() != nullptr);
    REQUIRE(v.schema()->kind == ValueTypeKind::Atomic);
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

#if HGRAPH_ENABLE_PYTHON_USER_NODES
TEST_CASE("Value: Python conversion round-trips scalars and compact containers")
{
    using namespace hgraph;
    PythonInterpreterGuard guard;

    auto       &registry = TypeRegistry::instance();
    auto       &factory  = ValuePlanFactory::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *str_meta = registry.register_scalar<std::string>("string");

    Value scalar{*factory.binding_for(int_meta)};
    scalar.from_python(nb::cast(42));
    REQUIRE(scalar.as<int>() == 42);
    REQUIRE(nb::cast<int>(scalar.to_python()) == 42);

    nb::list py_list;
    py_list.append(nb::cast(1));
    py_list.append(nb::cast(2));
    py_list.append(nb::cast(3));

    Value list{*factory.binding_for(registry.list(int_meta, 0))};
    list.from_python(py_list);
    REQUIRE(list.as_list().size() == 3);
    REQUIRE(list.as_list().back().checked_as<int>() == 3);
    REQUIRE(nb::len(nb::cast<nb::list>(list.to_python())) == 3);

    Value fixed_list{*factory.binding_for(registry.list(int_meta, 3))};
    fixed_list.from_python(py_list);
    REQUIRE(nb::cast<int>(nb::cast<nb::list>(fixed_list.to_python())[2]) == 3);

    nb::tuple py_tuple = nb::make_tuple(nb::cast(5), nb::cast(std::string{"five"}));
    Value tuple{*factory.binding_for(registry.tuple({int_meta, str_meta}))};
    tuple.from_python(py_tuple);
    nb::tuple tuple_out = nb::cast<nb::tuple>(tuple.to_python());
    REQUIRE(nb::cast<int>(tuple_out[0]) == 5);
    REQUIRE(nb::cast<std::string>(tuple_out[1]) == "five");

    nb::dict py_bundle;
    py_bundle[nb::str("count")] = nb::cast(7);
    py_bundle[nb::str("name")]  = nb::cast(std::string{"seven"});

    const auto *bundle_meta = registry.bundle("PythonRoundTripBundle", {{"count", int_meta}, {"name", str_meta}});
    Value       bundle{*factory.binding_for(bundle_meta)};
    bundle.from_python(py_bundle);
    REQUIRE(bundle.as_bundle().field("count").checked_as<int>() == 7);
    REQUIRE(bundle.as_bundle().field("name").checked_as<std::string>() == "seven");
    nb::dict bundle_out = nb::cast<nb::dict>(bundle.to_python());
    REQUIRE(nb::cast<int>(bundle_out[nb::str("count")]) == 7);
    REQUIRE(nb::cast<std::string>(bundle_out[nb::str("name")]) == "seven");

    nb::dict py_map;
    py_map[nb::str("a")] = nb::cast(10);
    py_map[nb::str("b")] = nb::cast(20);

    Value map{*factory.binding_for(registry.map(str_meta, int_meta))};
    map.from_python(py_map);
    REQUIRE(map.as_map().size() == 2);
    const std::string a{"a"};
    REQUIRE(map.as_map().at(ValueView{registry.scalar_binding<std::string>(), const_cast<std::string *>(&a)})
                .checked_as<int>() == 10);
    REQUIRE(nb::cast<int>(nb::cast<nb::dict>(map.to_python())[nb::str("b")]) == 20);
}

TEST_CASE("ValueOps: unsupported scalar to_python raises")
{
    using namespace hgraph;
    PythonInterpreterGuard guard;

    UnsupportedPythonScalar value{7};
    REQUIRE_THROWS_AS(ops_for<UnsupportedPythonScalar>().to_python(&value), std::logic_error);
}
#endif
