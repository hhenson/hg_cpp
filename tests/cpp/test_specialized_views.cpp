// Tests for the read-only specialised views (``ListView``,
// ``CyclicBufferView``, ``QueueView``, ``SetView``, ``MapView``) and
// the per-kind ``ValueOps`` (``compact_list_ops`` / ``compact_set_ops``
// / ``compact_map_ops`` / ``compact_cyclic_buffer_ops`` /
// ``compact_queue_ops``).
//
// These tests exercise both the storage/ops layer directly and the
// ``Value`` / ``ValueView`` casting surface that resolves canonical
// bindings through ``ValuePlanFactory``.

#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/value/compact_container_ops.h>
#include <hgraph/types/value/compact_storage.h>
#include <hgraph/types/value/container_ops.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/value/value_view.h>

#include <compare>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace
{
    template <typename T>
    [[nodiscard]] const T &as_const(const void *memory) noexcept
    {
        return *static_cast<const T *>(memory);
    }
}  // namespace

TEST_CASE("ListView: size, at, iteration over a built list")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<int>("int");
    (void)registry.register_scalar<std::string>("string");
    const auto *element_binding = registry.scalar_binding<int>();
    const auto &list_binding    = compact_list_binding(*element_binding);

    ListBuilder builder{*element_binding};
    builder.push_back<int>(10);
    builder.push_back<int>(20);
    builder.push_back<int>(30);
    auto storage = builder.build_storage();

    ListView view{ValueView{&list_binding, &storage}};
    REQUIRE(view.valid());
    REQUIRE(view.size() == 3);
    REQUIRE_FALSE(view.empty());
    REQUIRE_FALSE(view.is_fixed());
    REQUIRE(view.element_schema() == element_binding->type_meta);
    REQUIRE(view.front().checked_as<int>() == 10);
    REQUIRE(view.back().checked_as<int>() == 30);
    REQUIRE(view.at(0).checked_as<int>() == 10);
    REQUIRE(view[2].checked_as<int>() == 30);

    int sum = 0;
    for (const auto element : view) { sum += element.checked_as<int>(); }
    REQUIRE(sum == 60);

    int values_sum = 0;
    for (const auto element : view.values()) { values_sum += element.checked_as<int>(); }
    REQUIRE(values_sum == 60);
}

TEST_CASE("compact_list_ops: hash / equals / compare / to_string walk elements")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<int>("int");
    const auto *element_binding = registry.scalar_binding<int>();

    ListBuilder a{*element_binding};
    a.push_back<int>(1); a.push_back<int>(2); a.push_back<int>(3);
    auto storage_a = a.build_storage();

    ListBuilder b{*element_binding};
    b.push_back<int>(1); b.push_back<int>(2); b.push_back<int>(3);
    auto storage_b = b.build_storage();

    ListBuilder c{*element_binding};
    c.push_back<int>(1); c.push_back<int>(2); c.push_back<int>(4);
    auto storage_c = c.build_storage();

    const ValueOps &ops = compact_list_ops();

    REQUIRE(ops.hash(&storage_a) == ops.hash(&storage_b));
    REQUIRE(ops.equals(&storage_a, &storage_b));
    REQUIRE_FALSE(ops.equals(&storage_a, &storage_c));
    REQUIRE(std::is_lt(ops.compare(&storage_a, &storage_c)));
    REQUIRE(std::is_gt(ops.compare(&storage_c, &storage_a)));
    REQUIRE(std::is_eq(ops.compare(&storage_a, &storage_b)));
    REQUIRE(ops.to_string(&storage_a) == "[1, 2, 3]");
}

TEST_CASE("compact container compares order null storage consistently")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<int>("int");
    (void)registry.register_scalar<std::string>("string");
    const auto *int_binding = registry.scalar_binding<int>();
    const auto *str_binding = registry.scalar_binding<std::string>();

    auto assert_null_order = [](const ValueOps &ops, const void *storage) {
        REQUIRE(std::is_eq(ops.compare(nullptr, nullptr)));
        REQUIRE(std::is_lt(ops.compare(nullptr, storage)));
        REQUIRE(std::is_gt(ops.compare(storage, nullptr)));
    };

    ListBuilder list_builder{*int_binding};
    list_builder.push_back<int>(1);
    auto list_storage = list_builder.build_storage();
    assert_null_order(compact_list_ops(), &list_storage);

    CyclicBufferBuilder cyclic_builder{*int_binding, 2};
    cyclic_builder.push_back<int>(1);
    auto cyclic_storage = cyclic_builder.build_storage();
    assert_null_order(compact_cyclic_buffer_ops(), &cyclic_storage);

    QueueBuilder queue_builder{*int_binding, 2};
    queue_builder.push<int>(1);
    auto queue_storage = queue_builder.build_storage();
    assert_null_order(compact_queue_ops(), &queue_storage);

    SetBuilder set_builder{*int_binding};
    set_builder.insert<int>(1);
    auto set_storage = set_builder.build_storage();
    assert_null_order(compact_set_ops(), &set_storage);

    MapBuilder map_builder{*str_binding, *int_binding};
    map_builder.set_item<std::string, int>(std::string{"one"}, 1);
    auto map_storage = map_builder.build_storage();
    assert_null_order(compact_map_ops(), &map_storage);
}

TEST_CASE("CyclicBufferView: head, ring iteration, empty")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<int>("int");
    const auto *element_binding = registry.scalar_binding<int>();
    const auto &binding         = compact_cyclic_buffer_binding(*element_binding, 3);

    CyclicBufferBuilder builder{*element_binding, 3};
    builder.push_back<int>(1);
    builder.push_back<int>(2);
    builder.push_back<int>(3);
    builder.push_back<int>(4);  // rotates: oldest (1) drops out
    builder.push_back<int>(5);
    auto storage = builder.build_storage();

    CyclicBufferView view{ValueView{&binding, &storage}};
    REQUIRE(view.size() == 3);
    REQUIRE(view.capacity() == 3);
    REQUIRE(view.element_schema() == element_binding->type_meta);
    REQUIRE_FALSE(view.empty());
    // Read in ring order: oldest first.
    REQUIRE(view.front().checked_as<int>() == 3);
    REQUIRE(view.back().checked_as<int>() == 5);
    REQUIRE(view.at(0).checked_as<int>() == 3);
    REQUIRE(view.at(1).checked_as<int>() == 4);
    REQUIRE(view.at(2).checked_as<int>() == 5);
}

TEST_CASE("QueueView: front, size, iteration")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<int>("int");
    const auto *element_binding = registry.scalar_binding<int>();
    const auto &binding         = compact_queue_binding(*element_binding, /*max_capacity=*/0);

    QueueBuilder builder{*element_binding};
    builder.push<int>(100);
    builder.push<int>(200);
    builder.push<int>(300);
    auto storage = builder.build_storage();

    QueueView view{ValueView{&binding, &storage}};
    REQUIRE_FALSE(view.empty());
    REQUIRE(view.size() == 3);
    REQUIRE_FALSE(view.has_max_capacity());
    REQUIRE(view.element_schema() == element_binding->type_meta);
    REQUIRE(view.front().checked_as<int>() == 100);
    REQUIRE(view.back().checked_as<int>() == 300);
    REQUIRE(view.at(0).checked_as<int>() == 100);
    REQUIRE(view.at(2).checked_as<int>() == 300);
}

TEST_CASE("SetView: contains, size, iteration; ops are order-independent")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<int>("int");
    const auto *element_binding = registry.scalar_binding<int>();
    const auto &binding         = compact_set_binding(*element_binding);

    SetBuilder a{*element_binding};
    a.insert<int>(7); a.insert<int>(11); a.insert<int>(13);
    auto storage_a = a.build_storage();

    SetView view_a{ValueView{&binding, &storage_a}};
    REQUIRE(view_a.size() == 3);
    REQUIRE(view_a.element_schema() == element_binding->type_meta);

    int seven = 7;
    int twelve = 12;
    std::string wrong_type{"7"};
    REQUIRE(view_a.contains(ValueView{element_binding, &seven}));
    REQUIRE_FALSE(view_a.contains(ValueView{element_binding, &twelve}));
    REQUIRE_FALSE(view_a.contains(ValueView{registry.scalar_binding<std::string>(), &wrong_type}));

    int sum = 0;
    for (const auto member : view_a) { sum += member.checked_as<int>(); }
    REQUIRE(sum == (7 + 11 + 13));

    int values_sum = 0;
    for (const auto member : view_a.values()) { values_sum += member.checked_as<int>(); }
    REQUIRE(values_sum == (7 + 11 + 13));

    // Sets compare order-independently via the ops table.
    SetBuilder b{*element_binding};
    b.insert<int>(13); b.insert<int>(7); b.insert<int>(11);  // different insertion order
    auto storage_b = b.build_storage();

    const ValueOps &ops = compact_set_ops();
    REQUIRE(ops.hash(&storage_a) == ops.hash(&storage_b));
    REQUIRE(ops.equals(&storage_a, &storage_b));
    REQUIRE(std::is_eq(ops.compare(&storage_a, &storage_b)));

    SetBuilder c{*element_binding};
    c.insert<int>(7); c.insert<int>(11); c.insert<int>(17);
    auto storage_c = c.build_storage();
    REQUIRE_FALSE(ops.equals(&storage_a, &storage_c));
    REQUIRE(ops.compare(&storage_a, &storage_c) == std::partial_ordering::unordered);

    SetBuilder small{*element_binding};
    small.insert<int>(7); small.insert<int>(11);
    auto storage_small = small.build_storage();
    REQUIRE(std::is_lt(ops.compare(&storage_small, &storage_a)));
    REQUIRE(std::is_gt(ops.compare(&storage_a, &storage_small)));
}

TEST_CASE("MapView: contains, at, iteration; ops are order-independent over keys")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::string>("string");
    (void)registry.register_scalar<int>("int");
    const auto *key_binding   = registry.scalar_binding<std::string>();
    const auto *value_binding = registry.scalar_binding<int>();
    const auto &binding       = compact_map_binding(*key_binding, *value_binding);

    MapBuilder a{*key_binding, *value_binding};
    a.set_item<std::string, int>(std::string{"alpha"}, 1);
    a.set_item<std::string, int>(std::string{"beta"}, 2);
    a.set_item<std::string, int>(std::string{"gamma"}, 3);
    auto storage_a = a.build_storage();

    MapView view{ValueView{&binding, &storage_a}};
    REQUIRE(view.size() == 3);
    REQUIRE(view.key_schema() == key_binding->type_meta);
    REQUIRE(view.value_schema() == value_binding->type_meta);

    const std::string alpha{"alpha"};
    const std::string beta{"beta"};
    const std::string delta{"delta"};
    int wrong_key_type = 1;
    REQUIRE(view.contains(ValueView{key_binding, const_cast<std::string *>(&alpha)}));
    REQUIRE_FALSE(view.contains(ValueView{key_binding, const_cast<std::string *>(&delta)}));
    REQUIRE_FALSE(view.contains(ValueView{value_binding, &wrong_key_type}));

    REQUIRE(view.at(ValueView{key_binding, const_cast<std::string *>(&beta)}).checked_as<int>() == 2);
    REQUIRE_THROWS_AS(view.at(ValueView{key_binding, const_cast<std::string *>(&delta)}),
                      std::out_of_range);
    REQUIRE_THROWS_AS(view.at(ValueView{value_binding, &wrong_key_type}), std::out_of_range);

    // Iterate; verify we see all three entries (in some order). The
    // map view yields ``std::pair<ValueView, ValueView>`` (key,
    // value) through its KeyValueRange.
    int total = 0;
    int count = 0;
    for (const auto entry : view)
    {
        total += entry.second.checked_as<int>();
        ++count;
    }
    REQUIRE(count == 3);
    REQUIRE(total == (1 + 2 + 3));

    int item_total = 0;
    for (const auto entry : view.items()) { item_total += entry.second.checked_as<int>(); }
    REQUIRE(item_total == (1 + 2 + 3));

    // Keys-only range.
    int key_count = 0;
    for (const auto key : view.keys())
    {
        REQUIRE(key.valid());
        ++key_count;
    }
    REQUIRE(key_count == 3);

    // Values-only range.
    int value_total = 0;
    for (const auto val : view.values()) { value_total += val.checked_as<int>(); }
    REQUIRE(value_total == (1 + 2 + 3));

    // Order-independent: same map built in different order has the same hash / equals.
    MapBuilder b{*key_binding, *value_binding};
    b.set_item<std::string, int>(std::string{"gamma"}, 3);
    b.set_item<std::string, int>(std::string{"alpha"}, 1);
    b.set_item<std::string, int>(std::string{"beta"}, 2);
    auto storage_b = b.build_storage();

    const ValueOps &ops = compact_map_ops();
    REQUIRE(ops.hash(&storage_a) == ops.hash(&storage_b));
    REQUIRE(ops.equals(&storage_a, &storage_b));
    REQUIRE(std::is_eq(ops.compare(&storage_a, &storage_b)));

    MapBuilder c{*key_binding, *value_binding};
    c.set_item<std::string, int>(std::string{"gamma"}, 30);
    c.set_item<std::string, int>(std::string{"alpha"}, 1);
    c.set_item<std::string, int>(std::string{"beta"}, 2);
    auto storage_c = c.build_storage();
    REQUIRE_FALSE(ops.equals(&storage_a, &storage_c));
    REQUIRE(ops.compare(&storage_a, &storage_c) == std::partial_ordering::unordered);
}

TEST_CASE("compact bindings: same inputs return the same canonical binding pointer")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<int>("int");
    (void)registry.register_scalar<std::string>("string");
    const auto *int_binding = registry.scalar_binding<int>();
    const auto *str_binding = registry.scalar_binding<std::string>();

    REQUIRE(&compact_list_binding(*int_binding) == &compact_list_binding(*int_binding));
    REQUIRE(&compact_set_binding(*int_binding) == &compact_set_binding(*int_binding));
    REQUIRE(&compact_map_binding(*str_binding, *int_binding) ==
            &compact_map_binding(*str_binding, *int_binding));
    REQUIRE(&compact_cyclic_buffer_binding(*int_binding, 4) ==
            &compact_cyclic_buffer_binding(*int_binding, 4));
    REQUIRE(&compact_queue_binding(*int_binding, 0) == &compact_queue_binding(*int_binding, 0));

    // Different inputs → different bindings.
    REQUIRE(&compact_list_binding(*int_binding) != &compact_list_binding(*str_binding));
    REQUIRE(&compact_cyclic_buffer_binding(*int_binding, 4) !=
            &compact_cyclic_buffer_binding(*int_binding, 8));
}

TEST_CASE("MapView::key_set: returns a SetView wrapping the map's keys")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::string>("string");
    (void)registry.register_scalar<int>("int");
    const auto *key_binding   = registry.scalar_binding<std::string>();
    const auto *value_binding = registry.scalar_binding<int>();
    const auto &binding       = compact_map_binding(*key_binding, *value_binding);

    MapBuilder b{*key_binding, *value_binding};
    b.set_item<std::string, int>(std::string{"a"}, 1);
    b.set_item<std::string, int>(std::string{"b"}, 2);
    b.set_item<std::string, int>(std::string{"c"}, 3);
    auto storage = b.build_storage();

    MapView map_view{ValueView{&binding, &storage}};
    SetView keys = map_view.key_set();

    REQUIRE(keys.valid());
    REQUIRE(keys.size() == 3);

    const std::string a{"a"};
    const std::string b_key{"b"};
    const std::string z{"z"};
    REQUIRE(keys.contains(ValueView{key_binding, const_cast<std::string *>(&a)}));
    REQUIRE(keys.contains(ValueView{key_binding, const_cast<std::string *>(&b_key)}));
    REQUIRE_FALSE(keys.contains(ValueView{key_binding, const_cast<std::string *>(&z)}));

    int count = 0;
    for (auto member : keys)
    {
        REQUIRE_THROWS_AS(member.checked_mutable_as<std::string>() = "mutated", std::logic_error);
        ++count;
    }
    REQUIRE(count == 3);

    MapBuilder smaller{*key_binding, *value_binding};
    smaller.set_item<std::string, int>(std::string{"a"}, 1);
    smaller.set_item<std::string, int>(std::string{"b"}, 2);
    auto smaller_storage = smaller.build_storage();
    SetView smaller_keys = MapView{ValueView{&binding, &smaller_storage}}.key_set();

    REQUIRE(std::is_lt(smaller_keys.compare(keys)));
    REQUIRE(std::is_gt(keys.compare(smaller_keys)));
}

TEST_CASE("Value and ValueView expose direct specialized casts for compact containers")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<int>("int");
    (void)registry.register_scalar<std::string>("string");
    const auto *int_binding = registry.scalar_binding<int>();
    const auto *str_binding = registry.scalar_binding<std::string>();

    ListBuilder list_builder{*int_binding};
    list_builder.push_back<int>(4);
    list_builder.push_back<int>(5);
    Value list_value = list_builder.build();
    auto  list_view  = list_value.view();
    const auto indexed_list = list_view.as_indexed_view();
    REQUIRE(indexed_list.size() == 2);
    REQUIRE(indexed_list.at(1).checked_as<int>() == 5);
    REQUIRE(list_view.try_as_indexed_view().has_value());
    REQUIRE(list_value.as_indexed_view().size() == 2);
    REQUIRE(list_value.as_list().size() == 2);
    REQUIRE(list_value.as_list().at(1).checked_as<int>() == 5);
    REQUIRE(list_value.view().try_as_list().has_value());
    REQUIRE_FALSE(list_value.view().try_as_map().has_value());
    REQUIRE_FALSE(list_value.view().can_begin_mutation());
    REQUIRE_THROWS_AS(list_value.view().begin_mutation(), std::logic_error);
    REQUIRE_THROWS_AS(list_value.as_list().begin_mutation(), std::logic_error);

    SetBuilder set_builder{*int_binding};
    set_builder.insert<int>(7);
    set_builder.insert<int>(11);
    Value set_value = set_builder.build();
    int seven = 7;
    REQUIRE(set_value.as_set().contains(ValueView{int_binding, &seven}));
    REQUIRE_FALSE(set_value.view().can_begin_mutation());
    REQUIRE_THROWS_AS(set_value.view().begin_mutation(), std::logic_error);

    MapBuilder map_builder{*str_binding, *int_binding};
    map_builder.set_item<std::string, int>(std::string{"x"}, 10);
    Value map_value = map_builder.build();
    const std::string x{"x"};
    REQUIRE(map_value.as_map().at(ValueView{str_binding, const_cast<std::string *>(&x)}).checked_as<int>() == 10);
    REQUIRE_FALSE(map_value.view().can_begin_mutation());
    REQUIRE_THROWS_AS(map_value.view().begin_mutation(), std::logic_error);

    CyclicBufferBuilder cyclic_builder{*int_binding, 2};
    cyclic_builder.push_back<int>(1);
    cyclic_builder.push_back<int>(2);
    cyclic_builder.push_back<int>(3);
    Value cyclic_value = cyclic_builder.build();
    REQUIRE(cyclic_value.as_cyclic_buffer().size() == 2);
    REQUIRE(cyclic_value.as_cyclic_buffer().at(0).checked_as<int>() == 2);
    REQUIRE(cyclic_value.as_cyclic_buffer().full());
    REQUIRE_FALSE(cyclic_value.view().can_begin_mutation());
    REQUIRE_THROWS_AS(cyclic_value.as_cyclic_buffer().begin_mutation(), std::logic_error);

    QueueBuilder queue_builder{*int_binding, 2};
    queue_builder.push<int>(8);
    queue_builder.push<int>(9);
    Value queue_value = queue_builder.build();
    REQUIRE(queue_value.as_queue().front().checked_as<int>() == 8);
    REQUIRE(queue_value.as_queue().full());
    REQUIRE_FALSE(queue_value.view().can_begin_mutation());
    REQUIRE_THROWS_AS(queue_value.as_queue().begin_mutation(), std::logic_error);

    Value atomic_value{3};
    REQUIRE_FALSE(atomic_value.view().try_as_indexed_view().has_value());
    REQUIRE_THROWS_AS(atomic_value.view().as_indexed_view(), std::logic_error);
}

TEST_CASE("TupleView, BundleView and fixed ListView read structured MemoryUtils storage")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = ValuePlanFactory::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *str_meta = registry.register_scalar<std::string>("string");

    const auto *tuple_meta = registry.tuple({int_meta, str_meta});
    const auto *tuple_binding = factory.binding_for(tuple_meta);
    REQUIRE(tuple_binding != nullptr);
    Value tuple_value{*tuple_binding};
    TupleView tuple = tuple_value.as_tuple();
    REQUIRE(tuple.size() == 2);
    auto readonly_tuple_child = tuple.at(0);
    REQUIRE_FALSE(readonly_tuple_child.mutable_payload());
    REQUIRE_THROWS_AS(readonly_tuple_child.checked_mutable_as<int>() = 42, std::logic_error);
    auto mutable_tuple = tuple_value.as_tuple().begin_mutation();
    mutable_tuple.at(0).checked_mutable_as<int>() = 42;
    mutable_tuple.at(1).checked_mutable_as<std::string>() = "forty-two";
    REQUIRE(tuple[0].checked_as<int>() == 42);
    REQUIRE(tuple[1].checked_as<std::string>() == "forty-two");
    REQUIRE(tuple_value.to_string() == "(42, forty-two)");

    const auto *bundle_meta = registry.bundle("SpecializedViewBundle", {{"count", int_meta}, {"name", str_meta}});
    const auto *bundle_binding = factory.binding_for(bundle_meta);
    REQUIRE(bundle_binding != nullptr);
    Value bundle_value{*bundle_binding};
    BundleView bundle = bundle_value.as_bundle();
    REQUIRE(bundle.size() == 2);
    REQUIRE(bundle.has_field("count"));
    REQUIRE_FALSE(bundle.has_field("missing"));
    auto readonly_bundle_child = bundle["count"];
    REQUIRE_FALSE(readonly_bundle_child.mutable_payload());
    REQUIRE_THROWS_AS(readonly_bundle_child.checked_mutable_as<int>() = 3, std::logic_error);
    auto mutable_bundle = bundle_value.as_bundle().begin_mutation();
    mutable_bundle["count"].checked_mutable_as<int>() = 3;
    mutable_bundle["name"].checked_mutable_as<std::string>() = "items";
    REQUIRE(bundle.field("count").checked_as<int>() == 3);
    REQUIRE(bundle.at("count").checked_as<int>() == 3);
    REQUIRE(bundle.at("name").checked_as<std::string>() == "items");
    REQUIRE(bundle_value.to_string() == "{count: 3, name: items}");

    const auto *fixed_list_meta = registry.list(int_meta, 3);
    const auto *fixed_list_binding = factory.binding_for(fixed_list_meta);
    REQUIRE(fixed_list_binding != nullptr);
    Value fixed_list_value{*fixed_list_binding};
    ListView fixed_list = fixed_list_value.as_list();
    REQUIRE(fixed_list.size() == 3);
    REQUIRE(fixed_list.is_fixed());
    auto readonly_list_child = fixed_list.at(0);
    REQUIRE_FALSE(readonly_list_child.mutable_payload());
    REQUIRE_THROWS_AS(readonly_list_child.checked_mutable_as<int>() = 1, std::logic_error);
    auto mutable_fixed_list = fixed_list_value.as_list().begin_mutation();
    mutable_fixed_list.at(0).checked_mutable_as<int>() = 1;
    mutable_fixed_list.at(1).checked_mutable_as<int>() = 2;
    mutable_fixed_list.at(2).checked_mutable_as<int>() = 3;
    for (auto element : mutable_fixed_list) { element.checked_mutable_as<int>() += 10; }
    int sum = 0;
    for (const auto element : fixed_list) { sum += element.checked_as<int>(); }
    REQUIRE(sum == 36);
    REQUIRE(fixed_list_value.to_string() == "[11, 12, 13]");
}

TEST_CASE("ValueView semantic fallback compares fixed and compact lists by index")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = ValuePlanFactory::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *int_binding = registry.scalar_binding<int>();

    const auto *fixed_list_meta = registry.list(int_meta, 3);
    const auto *fixed_list_binding = factory.binding_for(fixed_list_meta);
    REQUIRE(fixed_list_binding != nullptr);

    Value fixed{*fixed_list_binding};
    auto fixed_mutation = fixed.as_list().begin_mutation();
    fixed_mutation.at(0).checked_mutable_as<int>() = 1;
    fixed_mutation.at(1).checked_mutable_as<int>() = 2;
    fixed_mutation.at(2).checked_mutable_as<int>() = 3;

    ListBuilder same_builder{*int_binding};
    same_builder.push_back<int>(1);
    same_builder.push_back<int>(2);
    same_builder.push_back<int>(3);
    Value same = same_builder.build();

    ListBuilder different_builder{*int_binding};
    different_builder.push_back<int>(1);
    different_builder.push_back<int>(2);
    different_builder.push_back<int>(4);
    Value different = different_builder.build();

    REQUIRE(fixed.binding() != same.binding());
    REQUIRE(fixed.view().equals(same.view()));
    REQUIRE(std::is_eq(fixed.view().compare(same.view())));
    REQUIRE_FALSE(fixed.view().equals(different.view()));
    REQUIRE(std::is_lt(fixed.view().compare(different.view())));
}

TEST_CASE("ValueView semantic fallback compares named and structural bundles by index")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = ValuePlanFactory::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *str_meta = registry.register_scalar<std::string>("string");

    const auto fields = std::vector<std::pair<std::string, const ValueTypeMetaData *>>{
        {"count", int_meta},
        {"name", str_meta},
    };
    const auto *structural_meta = registry.un_named_bundle(fields);
    const auto *named_meta      = registry.bundle("SemanticFallbackBundle", fields);
    const auto *structural_binding = factory.binding_for(structural_meta);
    const auto *named_binding      = factory.binding_for(named_meta);
    REQUIRE(structural_binding != nullptr);
    REQUIRE(named_binding != nullptr);

    Value structural{*structural_binding};
    auto structural_mutation = structural.as_bundle().begin_mutation();
    structural_mutation.at("count").checked_mutable_as<int>() = 7;
    structural_mutation.at("name").checked_mutable_as<std::string>() = "seven";

    Value named{*named_binding};
    auto named_mutation = named.as_bundle().begin_mutation();
    named_mutation.at("count").checked_mutable_as<int>() = 7;
    named_mutation.at("name").checked_mutable_as<std::string>() = "seven";

    REQUIRE(structural.binding() != named.binding());
    REQUIRE(structural.view().equals(named.view()));
    REQUIRE(std::is_eq(structural.view().compare(named.view())));

    named.as_bundle().begin_mutation().at("count").checked_mutable_as<int>() = 8;
    REQUIRE_FALSE(structural.view().equals(named.view()));
    REQUIRE(std::is_lt(structural.view().compare(named.view())));
}

TEST_CASE("ValueView semantic fallback compares set-compatible lookup surfaces")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::string>("string");
    (void)registry.register_scalar<int>("int");
    const auto *key_binding   = registry.scalar_binding<std::string>();
    const auto *value_binding = registry.scalar_binding<int>();

    MapBuilder map_builder{*key_binding, *value_binding};
    map_builder.set_item<std::string, int>(std::string{"a"}, 1);
    map_builder.set_item<std::string, int>(std::string{"b"}, 2);
    Value map_value = map_builder.build();
    SetView keys = map_value.as_map().key_set();

    SetBuilder set_builder{*key_binding};
    set_builder.insert<std::string>(std::string{"b"});
    set_builder.insert<std::string>(std::string{"a"});
    Value set_value = set_builder.build();

    SetBuilder different_builder{*key_binding};
    different_builder.insert<std::string>(std::string{"a"});
    different_builder.insert<std::string>(std::string{"c"});
    Value different = different_builder.build();

    REQUIRE(keys.binding() != set_value.binding());
    REQUIRE(keys.equals(set_value.view()));
    REQUIRE(std::is_eq(keys.compare(set_value.view())));
    REQUIRE_FALSE(keys.equals(different.view()));
    REQUIRE(keys.compare(different.view()) == std::partial_ordering::unordered);

    SetBuilder smaller_builder{*key_binding};
    smaller_builder.insert<std::string>(std::string{"a"});
    Value smaller = smaller_builder.build();
    REQUIRE(std::is_lt(smaller.view().compare(keys)));
    REQUIRE(std::is_gt(keys.compare(smaller.view())));
}

TEST_CASE("ValueView semantic fallback compares maps with equivalent value layouts")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = ValuePlanFactory::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    (void)registry.register_scalar<std::string>("string");
    const auto *key_binding = registry.scalar_binding<std::string>();
    const auto *int_binding = registry.scalar_binding<int>();

    const auto *fixed_list_meta    = registry.list(int_meta, 3);
    const auto *fixed_list_binding = factory.binding_for(fixed_list_meta);
    REQUIRE(fixed_list_binding != nullptr);

    auto make_fixed_list = [&](int a, int b, int c) {
        Value value{*fixed_list_binding};
        auto mutation = value.as_list().begin_mutation();
        mutation.at(0).checked_mutable_as<int>() = a;
        mutation.at(1).checked_mutable_as<int>() = b;
        mutation.at(2).checked_mutable_as<int>() = c;
        return value;
    };

    auto make_compact_list = [&](int a, int b, int c) {
        ListBuilder builder{*int_binding};
        builder.push_back<int>(a);
        builder.push_back<int>(b);
        builder.push_back<int>(c);
        return builder.build();
    };

    const std::string alpha{"alpha"};
    const std::string beta{"beta"};

    Value fixed_alpha = make_fixed_list(1, 2, 3);
    Value fixed_beta  = make_fixed_list(4, 5, 6);
    MapBuilder fixed_map_builder{*key_binding, *fixed_list_binding};
    fixed_map_builder.set_item_copy(&alpha, fixed_alpha.view().data());
    fixed_map_builder.set_item_copy(&beta, fixed_beta.view().data());
    Value fixed_map = fixed_map_builder.build();

    Value compact_alpha = make_compact_list(1, 2, 3);
    Value compact_beta  = make_compact_list(4, 5, 6);
    MapBuilder compact_map_builder{*key_binding, *compact_alpha.binding()};
    compact_map_builder.set_item_copy(&beta, compact_beta.view().data());
    compact_map_builder.set_item_copy(&alpha, compact_alpha.view().data());
    Value compact_map = compact_map_builder.build();

    Value different_beta = make_compact_list(4, 5, 7);
    MapBuilder different_map_builder{*key_binding, *compact_alpha.binding()};
    different_map_builder.set_item_copy(&alpha, compact_alpha.view().data());
    different_map_builder.set_item_copy(&beta, different_beta.view().data());
    Value different_map = different_map_builder.build();

    REQUIRE(fixed_map.binding() != compact_map.binding());
    REQUIRE(fixed_map.view().equals(compact_map.view()));
    REQUIRE(std::is_eq(fixed_map.view().compare(compact_map.view())));
    REQUIRE_FALSE(fixed_map.view().equals(different_map.view()));
    REQUIRE(fixed_map.view().compare(different_map.view()) == std::partial_ordering::unordered);
}
