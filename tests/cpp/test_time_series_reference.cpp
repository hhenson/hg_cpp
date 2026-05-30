// Tests for the TimeSeriesReference struct — the C++ value type that
// backs the canonical ``TimeSeriesReference`` atomic schema in the type
// registry.

#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series_reference.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/value/value.h>

#include <array>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

namespace
{
    template <std::size_t Count>
    [[nodiscard]] std::array<hgraph::engine_time_t, Count> sequential_times(
        hgraph::engine_time_t start = hgraph::MIN_ST,
        hgraph::engine_time_delta_t step = hgraph::engine_time_delta_t{1})
    {
        std::array<hgraph::engine_time_t, Count> result{};
        auto                                     next = start;
        for (auto &time : result)
        {
            time = next;
            next += step;
        }
        return result;
    }

    template <typename Range>
    std::size_t range_count(const Range &range)
    {
        std::size_t count = 0;
        for (auto it = range.begin(); it != range.end(); ++it) { ++count; }
        return count;
    }

    template <typename Range>
    [[nodiscard]] auto collect_range(const Range &range)
    {
        using value_type = std::decay_t<decltype(*range.begin())>;
        std::vector<value_type> result;
        for (auto value : range) { result.emplace_back(std::move(value)); }
        return result;
    }

    void set_output_value(hgraph::TSOutput &output, int value, hgraph::engine_time_t time)
    {
        hgraph::Value stored{value};
        auto          mutation = output.begin_mutation(time);
        REQUIRE(mutation.copy_value_from(stored.view()));
    }

    void set_output_reference(hgraph::TSOutput &output,
                              hgraph::TimeSeriesReference reference,
                              hgraph::engine_time_t time)
    {
        hgraph::Value stored{std::move(reference)};
        auto          mutation = output.begin_mutation(time);
        REQUIRE(mutation.copy_value_from(stored.view()));
    }
}

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

    TSOutput output{*ts_int};
    auto     ref = TimeSeriesReference::peered(output.view());
    REQUIRE(ref.is_peered());
    REQUIRE(ref.has_output());
    REQUIRE(ref.target_schema() == ts_int);
    REQUIRE(ref == TimeSeriesReference{output.view()});
}

TEST_CASE("TimeSeriesReference: non-peered reference holds sub-references")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsl      = registry.tsl(ts_int, /*fixed_size=*/2);

    TSOutput lhs{*ts_int};
    TSOutput rhs{*ts_int};

    auto composite = TimeSeriesReference::non_peered(
        tsl,
        {
            TimeSeriesReference{lhs.view()},
            TimeSeriesReference{rhs.view()},
        });

    REQUIRE(composite.is_non_peered());
    REQUIRE(composite.target_schema() == tsl);
    REQUIRE(composite.items().size() == 2);
    REQUIRE(composite[0].target_schema() == ts_int);
    REQUIRE(composite[1].is_peered());
    REQUIRE(composite[1] == TimeSeriesReference{rhs.view()});
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

    TSOutput output{*ts_int};
    auto     composite = TimeSeriesReference::non_peered(ts_int, {TimeSeriesReference{output.view()}});

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

    TSOutput first{*ts_int};
    TSOutput second{*ts_int};

    const auto peered_a = TimeSeriesReference::peered(first.view());
    const auto peered_b = TimeSeriesReference::peered(first.view());
    const auto peered_other = TimeSeriesReference::peered(second.view());
    REQUIRE(peered_a == peered_b);
    REQUIRE(peered_a.hash() == peered_b.hash());
    REQUIRE_FALSE(peered_a == peered_other);

    const auto composite_a = TimeSeriesReference::non_peered(ts_int, {TimeSeriesReference{first.view()}});
    const auto composite_b = TimeSeriesReference::non_peered(ts_int, {TimeSeriesReference{first.view()}});
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

TEST_CASE("TimeSeriesReference: input view construction handles peered terminals and non-peered prefixes")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *list     = registry.tsl(ts_int, 2);
    const auto *root     = registry.tsb("TimeSeriesReferenceInputRoot", {{"leaf", ts_int}, {"items", list}});

    const auto endpoint_schema = TSEndpointSchema::non_peered(
        root,
        {
            TSEndpointSchema::peered(ts_int),
            TSEndpointSchema::non_peered_list(list, TSEndpointSchema::peered(ts_int)),
        });

    TSOutput leaf_output{*ts_int};
    TSOutput item_output{*ts_int};
    TSOutput second_item_output{*ts_int};
    TSInput  input{TSInputBuilderFactory::checked_builder_for(*root, endpoint_schema)};

    auto root_view = input.view();
    auto bundle    = root_view.as_bundle();
    auto leaf      = bundle.field("leaf");
    leaf.bind_output(leaf_output.view());

    auto items      = bundle.field("items");
    auto list_view  = items.as_list();
    auto first_item = list_view[0];
    first_item.bind_output(item_output.view());

    const TimeSeriesReference leaf_ref = leaf.reference();
    REQUIRE(leaf_ref.is_peered());
    REQUIRE(leaf_ref == TimeSeriesReference{leaf_output.view()});

    const TimeSeriesReference missing_ref = list_view[1].reference();
    REQUIRE(missing_ref.is_empty());
    REQUIRE(missing_ref.target_schema() == ts_int);

    const TimeSeriesReference partial_root_ref{root_view};
    REQUIRE(partial_root_ref.is_non_peered());
    REQUIRE(partial_root_ref[1].is_non_peered());
    REQUIRE(partial_root_ref[1][1].is_empty());
    REQUIRE(partial_root_ref[1][1].target_schema() == ts_int);

    list_view[1].bind_output(second_item_output.view());

    const TimeSeriesReference root_ref{root_view};
    REQUIRE(root_ref.is_non_peered());
    REQUIRE(root_ref.target_schema() == root);
    REQUIRE(root_ref.items().size() == 2);
    REQUIRE(root_ref[0] == TimeSeriesReference{leaf_output.view()});
    REQUIRE(root_ref[1].is_non_peered());
    REQUIRE(root_ref[1].items().size() == 2);
    REQUIRE(root_ref[1][0] == TimeSeriesReference{item_output.view()});
    REQUIRE(root_ref[1][1] == TimeSeriesReference{second_item_output.view()});
}

TEST_CASE("TimeSeriesReference: target link negotiates output as REF alternative")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *ref_int  = registry.ref(ts_int);
    const auto *root     = registry.tsb("TimeSeriesReferenceAlternativeInputRoot", {{"ref", ref_int}});

    const auto endpoint_schema = TSEndpointSchema::non_peered(
        root,
        {
            TSEndpointSchema::peered(ref_int),
        });

    TSOutput target{*ts_int};
    Value    value{17};
    const auto t1 = MIN_ST;
    {
        auto mutation = target.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(value.view()));
    }

    TSInput input{TSInputBuilderFactory::checked_builder_for(*root, endpoint_schema)};
    auto    root_view = input.view(nullptr, t1);
    auto    root_bundle = root_view.as_bundle();
    auto    ref_input = root_bundle.field("ref");

    ref_input.bind_output(target.view(t1));

    REQUIRE(ref_input.bound());
    REQUIRE(ref_input.valid());
    REQUIRE(ref_input.modified());

    const auto &reference = ref_input.value().checked_as<TimeSeriesReference>();
    REQUIRE(reference.is_peered());
    REQUIRE(reference.target_schema() == ts_int);
    REQUIRE(reference == TimeSeriesReference{target.view(t1)});

    Value next_value{18};
    const auto t2 = t1 + engine_time_delta_t{1};
    {
        auto mutation = target.begin_mutation(t2);
        REQUIRE(mutation.copy_value_from(next_value.view()));
    }

    auto t2_root_view = input.view(nullptr, t2);
    auto t2_bundle = t2_root_view.as_bundle();
    auto t2_ref_input = t2_bundle.field("ref");
    REQUIRE(t2_ref_input.valid());
    REQUIRE_FALSE(t2_ref_input.modified());
    const auto t2_reference_value = t2_ref_input.value().clone();
    REQUIRE(t2_reference_value.as<TimeSeriesReference>() == TimeSeriesReference{target.view(t2)});
}

TEST_CASE("TimeSeriesReference: to-REF alternative samples at bind time and ignores source value ticks")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *ref_int  = registry.ref(ts_int);
    const auto *root     = registry.tsb("TimeSeriesReferenceProjectionTimingRoot", {{"ref", ref_int}});

    const auto endpoint_schema = TSEndpointSchema::non_peered(
        root,
        {
            TSEndpointSchema::peered(ref_int),
        });

    TSOutput target{*ts_int};
    {
        Value value{17};
        auto  mutation = target.begin_mutation(MIN_ST);
        REQUIRE(mutation.copy_value_from(value.view()));
    }

    TSInput input{TSInputBuilderFactory::checked_builder_for(*root, endpoint_schema)};
    const auto bind_time = MIN_ST + engine_time_delta_t{1};
    auto       root_view = input.view(nullptr, bind_time);
    auto       root_bundle = root_view.as_bundle();
    auto       ref_input = root_bundle.field("ref");
    ref_input.bind_output(target.view(bind_time));

    REQUIRE(ref_input.valid());
    REQUIRE(ref_input.modified());
    REQUIRE(ref_input.last_modified_time() == bind_time);

    auto reference_value = ref_input.value().clone();
    REQUIRE(reference_value.as<TimeSeriesReference>() == TimeSeriesReference{target.view(bind_time)});

    const auto source_tick_time = bind_time + engine_time_delta_t{1};
    {
        Value value{18};
        auto  mutation = target.begin_mutation(source_tick_time);
        REQUIRE(mutation.copy_value_from(value.view()));
    }

    auto after_tick_root = input.view(nullptr, source_tick_time);
    auto after_tick_bundle = after_tick_root.as_bundle();
    auto after_tick_ref  = after_tick_bundle.field("ref");
    REQUIRE(after_tick_ref.valid());
    REQUIRE_FALSE(after_tick_ref.modified());
    REQUIRE(after_tick_ref.last_modified_time() == bind_time);

    auto after_tick_reference = after_tick_ref.value().clone();
    REQUIRE(after_tick_reference.as<TimeSeriesReference>() == TimeSeriesReference{target.view(source_tick_time)});
}

TEST_CASE("TimeSeriesReference: output alternatives are keyed by starting view and requested schema")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *named_bundle      = registry.tsb("TimeSeriesReferenceAlternativeKeyNamed", {{"value", ts_int}});
    const auto *structural_bundle = registry.un_named_tsb({{"value", ts_int}});
    const auto *ref_named         = registry.ref(named_bundle);
    const auto *ref_structural    = registry.ref(structural_bundle);

    TSOutput target{*named_bundle};
    {
        Value value{17};
        auto  target_data = target.data_view();
        auto  bundle = target_data.as_bundle();
        auto  child = bundle.field("value");
        auto  mutation = child.begin_mutation(MIN_ST);
        REQUIRE(mutation.copy_value_from(value.view()));
    }

    auto source = target.view(MIN_ST);
    auto named_ref_handle = source.binding_for(*ref_named);
    auto structural_ref_handle = source.binding_for(*ref_structural);

    REQUIRE(named_ref_handle.schema() == ref_named);
    REQUIRE(structural_ref_handle.schema() == ref_structural);
    REQUIRE(named_ref_handle.data_view().data() != structural_ref_handle.data_view().data());

    const auto named_ref_value = named_ref_handle.view(MIN_ST).value().clone();
    const auto structural_ref_value = structural_ref_handle.view(MIN_ST).value().clone();

    REQUIRE(named_ref_value.as<TimeSeriesReference>().target_schema() == named_bundle);
    REQUIRE(structural_ref_value.as<TimeSeriesReference>().target_schema() == structural_bundle);
}

TEST_CASE("TimeSeriesReference: to-REF alternative stores fixed list children through TSData")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *ref_int  = registry.ref(ts_int);
    const auto *source_schema = registry.tsl(ts_int, 2);
    const auto *requested_schema = registry.tsl(ref_int, 2);

    TSOutput target{*source_schema};
    auto     target_data = target.data_view();
    auto     source_list = target_data.as_list();
    {
        Value one{1};
        auto  mutation = source_list.at(0).begin_mutation(MIN_ST);
        REQUIRE(mutation.copy_value_from(one.view()));
    }
    {
        Value two{2};
        auto  mutation = source_list.at(1).begin_mutation(MIN_ST);
        REQUIRE(mutation.copy_value_from(two.view()));
    }

    auto handle = target.view(MIN_ST).binding_for(*requested_schema);
    REQUIRE(handle.schema() == requested_schema);

    auto projected_view = handle.view(MIN_ST);
    REQUIRE(projected_view.valid());
    REQUIRE(projected_view.modified());
    auto projected_list = projected_view.as_list();
    REQUIRE(projected_list.size() == 2);

    auto source_view = target.view(MIN_ST);
    auto source_output_list = source_view.as_list();
    const auto &first = projected_list.at(0).value().checked_as<TimeSeriesReference>();
    const auto &second = projected_list.at(1).value().checked_as<TimeSeriesReference>();
    REQUIRE(first.target_schema() == ts_int);
    REQUIRE(second.target_schema() == ts_int);
    REQUIRE(first == TimeSeriesReference{source_output_list.at(0)});
    REQUIRE(second == TimeSeriesReference{source_output_list.at(1)});
}

TEST_CASE("TimeSeriesReference: to-REF TSD alternative keeps keys synchronized")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *ref_int  = registry.ref(ts_int);
    const auto *source_schema = registry.tsd(int_meta, ts_int);
    const auto *requested_schema = registry.tsd(int_meta, ref_int);

    TSOutput target{*source_schema};
    Value    key_one{1};
    Value    key_two{2};
    {
        Value value{11};
        auto  data = target.data_view();
        auto  dict = data.as_dict();
        auto  mutation = dict.begin_mutation(MIN_ST);
        auto  child = mutation.at(key_one.view());
        auto  child_mutation = child.begin_mutation(MIN_ST);
        REQUIRE(child_mutation.copy_value_from(value.view()));
    }

    auto handle       = target.view(MIN_ST).binding_for(*requested_schema);
    auto initial_view = handle.view(MIN_ST);
    auto initial_dict = initial_view.as_dict();
    REQUIRE(initial_dict.contains(key_one.view()));
    REQUIRE(range_count(initial_dict.items()) == 1);

    const auto t2 = MIN_ST + engine_time_delta_t{1};
    {
        Value value{22};
        auto  data = target.data_view();
        auto  dict = data.as_dict();
        auto  mutation = dict.begin_mutation(t2);
        auto  child = mutation.at(key_two.view());
        auto  child_mutation = child.begin_mutation(t2);
        REQUIRE(child_mutation.copy_value_from(value.view()));
    }

    auto add_view  = handle.view(t2);
    auto after_add = add_view.as_dict();
    REQUIRE(after_add.modified());
    REQUIRE(after_add.contains(key_one.view()));
    REQUIRE(after_add.contains(key_two.view()));
    REQUIRE(range_count(after_add.items()) == 2);
    auto source_after_add = target.view(t2);
    auto source_after_add_dict = source_after_add.as_dict();
    REQUIRE(after_add.at(key_one.view()).value().checked_as<TimeSeriesReference>() ==
            TimeSeriesReference{source_after_add_dict.at(key_one.view())});
    REQUIRE(after_add.at(key_two.view()).value().checked_as<TimeSeriesReference>() ==
            TimeSeriesReference{source_after_add_dict.at(key_two.view())});

    auto value_snapshot = add_view.value().clone();
    auto value_map = value_snapshot.as_map();
    REQUIRE(value_map.contains(key_one.view()));
    REQUIRE(value_map.contains(key_two.view()));
    REQUIRE(value_map.at(key_two.view()).checked_as<TimeSeriesReference>() ==
            TimeSeriesReference{source_after_add_dict.at(key_two.view())});

    const auto t3 = t2 + engine_time_delta_t{1};
    {
        auto data = target.data_view();
        auto dict = data.as_dict();
        auto mutation = dict.begin_mutation(t3);
        REQUIRE(mutation.erase(key_one.view()));
    }

    auto remove_view  = handle.view(t3);
    auto after_remove = remove_view.as_dict();
    REQUIRE(after_remove.modified());
    REQUIRE_FALSE(after_remove.contains(key_one.view()));
    REQUIRE(after_remove.contains(key_two.view()));
    REQUIRE(range_count(after_remove.items()) == 1);
    auto delta_snapshot = remove_view.delta_value().clone();
    auto delta_bundle = delta_snapshot.as_bundle();
    auto removed_keys = delta_bundle.field("removed").as_set();
    REQUIRE(removed_keys.contains(key_one.view()));
}

TEST_CASE("TimeSeriesReference: to-REF TSD path constructs normal child structures before REF leaves")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *ref_int  = registry.ref(ts_int);
    const auto *source_list = registry.tsl(ts_int, 2);
    const auto *requested_list = registry.tsl(ref_int, 2);
    const auto *source_bundle = registry.tsb("TimeSeriesReferenceTSDPathSourceBundle", {{"items", source_list}});
    const auto *requested_bundle =
        registry.tsb("TimeSeriesReferenceTSDPathRequestedBundle", {{"items", requested_list}});
    const auto *source_schema = registry.tsd(int_meta, source_bundle);
    const auto *requested_schema = registry.tsd(int_meta, requested_bundle);

    TSOutput target{*source_schema};
    Value    key_one{1};
    Value    key_two{2};

    {
        auto data = target.data_view();
        auto dict = data.as_dict();
        auto dict_mutation = dict.begin_mutation(MIN_ST);
        auto child = dict_mutation.at(key_one.view());
        auto bundle = child.as_bundle();
        auto items_child = bundle.field("items");
        auto items = items_child.as_list();
        Value first{11};
        Value second{12};
        REQUIRE(items.at(0).begin_mutation(MIN_ST).copy_value_from(first.view()));
        REQUIRE(items.at(1).begin_mutation(MIN_ST).copy_value_from(second.view()));
    }

    auto target_view = target.view(MIN_ST);
    auto handle = target_view.binding_for(*requested_schema);
    auto handle_view = handle.view(MIN_ST);
    auto projected = handle_view.as_dict();
    REQUIRE(projected.contains(key_one.view()));
    REQUIRE(projected.at(key_one.view()).schema() == requested_bundle);

    auto projected_child = projected.at(key_one.view());
    auto projected_bundle = projected_child.as_bundle();
    auto projected_items_child = projected_bundle.field("items");
    auto projected_items = projected_items_child.as_list();
    REQUIRE(projected_items.at(0).schema() == ref_int);
    REQUIRE(projected_items.at(1).schema() == ref_int);

    auto source_view = target.view(MIN_ST);
    auto source = source_view.as_dict();
    auto source_child = source.at(key_one.view());
    auto source_bundle_view = source_child.as_bundle();
    auto source_items_child = source_bundle_view.field("items");
    auto source_items = source_items_child.as_list();
    REQUIRE(projected_items.at(0).value().checked_as<TimeSeriesReference>() ==
            TimeSeriesReference{source_items.at(0)});
    REQUIRE(projected_items.at(1).value().checked_as<TimeSeriesReference>() ==
            TimeSeriesReference{source_items.at(1)});

    const auto t2 = MIN_ST + engine_time_delta_t{1};
    {
        auto data = target.data_view();
        auto dict = data.as_dict();
        auto dict_mutation = dict.begin_mutation(t2);
        auto child = dict_mutation.at(key_two.view());
        auto bundle = child.as_bundle();
        auto items_child = bundle.field("items");
        auto items = items_child.as_list();
        Value first{21};
        Value second{22};
        REQUIRE(items.at(0).begin_mutation(t2).copy_value_from(first.view()));
        REQUIRE(items.at(1).begin_mutation(t2).copy_value_from(second.view()));
    }

    auto after_add_view = handle.view(t2);
    auto after_add = after_add_view.as_dict();
    REQUIRE(after_add.modified());
    REQUIRE(after_add.contains(key_one.view()));
    REQUIRE(after_add.contains(key_two.view()));
    REQUIRE(range_count(after_add.items()) == 2);
    REQUIRE(after_add.at(key_two.view()).schema() == requested_bundle);

    auto added_child = after_add.at(key_two.view());
    auto added_bundle = added_child.as_bundle();
    auto added_items_child = added_bundle.field("items");
    auto added_items = added_items_child.as_list();
    auto source_after_add_view = target.view(t2);
    auto source_after_add = source_after_add_view.as_dict();
    auto source_added_child = source_after_add.at(key_two.view());
    auto source_added_bundle = source_added_child.as_bundle();
    auto source_added_items_child = source_added_bundle.field("items");
    auto source_added_items = source_added_items_child.as_list();
    REQUIRE(added_items.at(0).value().checked_as<TimeSeriesReference>() ==
            TimeSeriesReference{source_added_items.at(0)});
    REQUIRE(added_items.at(1).value().checked_as<TimeSeriesReference>() ==
            TimeSeriesReference{source_added_items.at(1)});
}

TEST_CASE("TimeSeriesReference: to-REF nested TSD alternative keeps each proxy subscribed")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *ref_int  = registry.ref(ts_int);
    const auto *source_inner_schema = registry.tsd(int_meta, ts_int);
    const auto *requested_inner_schema = registry.tsd(int_meta, ref_int);
    const auto *source_schema = registry.tsd(int_meta, source_inner_schema);
    const auto *requested_schema = registry.tsd(int_meta, requested_inner_schema);

    TSOutput target{*source_schema};
    Value    outer_key{1};
    Value    inner_key_one{11};
    Value    inner_key_two{22};

    {
        Value value{11};
        auto  target_data = target.data_view();
        auto  source_dict = target_data.as_dict();
        auto  outer_mutation = source_dict.begin_mutation(MIN_ST);
        auto  inner = outer_mutation.at(outer_key.view());
        auto  inner_dict = inner.as_dict();
        auto  inner_mutation = inner_dict.begin_mutation(MIN_ST);
        auto  child = inner_mutation.at(inner_key_one.view());
        auto  child_mutation = child.begin_mutation(MIN_ST);
        REQUIRE(child_mutation.copy_value_from(value.view()));
    }

    auto handle = target.view(MIN_ST).binding_for(*requested_schema);
    auto projected_view = handle.view(MIN_ST);
    auto projected_dict = projected_view.as_dict();
    REQUIRE(projected_dict.contains(outer_key.view()));
    auto projected_inner_child = projected_dict.at(outer_key.view());
    auto projected_inner = projected_inner_child.as_dict();
    REQUIRE(projected_inner.contains(inner_key_one.view()));
    REQUIRE(range_count(projected_inner.items()) == 1);
    const auto t2 = MIN_ST + engine_time_delta_t{1};
    {
        Value value{22};
        auto  target_data = target.data_view();
        auto  source_dict = target_data.as_dict();
        auto  outer_child = source_dict.at(outer_key.view());
        auto  inner_dict = outer_child.as_dict();
        auto  inner_mutation = inner_dict.begin_mutation(t2);
        auto  child = inner_mutation.at(inner_key_two.view());
        auto  child_mutation = child.begin_mutation(t2);
        REQUIRE(child_mutation.copy_value_from(value.view()));
    }

    auto after_add_view = handle.view(t2);
    auto after_add_dict = after_add_view.as_dict();
    auto after_add_inner_child = after_add_dict.at(outer_key.view());
    auto after_add_inner = after_add_inner_child.as_dict();
    REQUIRE(after_add_inner.modified());
    REQUIRE(after_add_inner.contains(inner_key_one.view()));
    REQUIRE(after_add_inner.contains(inner_key_two.view()));
    REQUIRE(range_count(after_add_inner.items()) == 2);

    auto source_after_add = target.view(t2);
    auto source_after_add_dict = source_after_add.as_dict();
    auto source_after_add_inner_child = source_after_add_dict.at(outer_key.view());
    auto source_after_add_inner = source_after_add_inner_child.as_dict();
    REQUIRE(after_add_inner.at(inner_key_two.view()).value().checked_as<TimeSeriesReference>() ==
            TimeSeriesReference{source_after_add_inner.at(inner_key_two.view())});
}

TEST_CASE("TimeSeriesReference: from-REF alternative binds peered target and follows target ticks")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *ref_int  = registry.ref(ts_int);

    TSOutput first_target{*ts_int};
    TSOutput second_target{*ts_int};
    TSOutput ref_output{*ref_int};

    const auto [t1, t2, t3, t4, t5] = sequential_times<5>();

    set_output_value(first_target, 11, t1);
    set_output_reference(ref_output, TimeSeriesReference{first_target.view(t1)}, t2);

    auto handle = ref_output.view(t2).binding_for(*ts_int);
    auto dereferenced = handle.view(t2);
    REQUIRE(dereferenced.valid());
    REQUIRE(dereferenced.modified());
    REQUIRE(dereferenced.value().checked_as<int>() == 11);
    REQUIRE(dereferenced.last_modified_time() == t2);

    set_output_value(first_target, 12, t3);
    auto after_target_tick = handle.view(t3);
    REQUIRE(after_target_tick.valid());
    REQUIRE(after_target_tick.modified());
    REQUIRE(after_target_tick.value().checked_as<int>() == 12);

    set_output_value(second_target, 21, t4);
    set_output_reference(ref_output, TimeSeriesReference{second_target.view(t4)}, t4);
    auto after_rebind = handle.view(t4);
    REQUIRE(after_rebind.valid());
    REQUIRE(after_rebind.modified());
    REQUIRE(after_rebind.value().checked_as<int>() == 21);
    REQUIRE(after_rebind.last_modified_time() == t4);

    set_output_reference(ref_output, TimeSeriesReference::empty(ts_int), t5);
    auto after_unbind = handle.view(t5);
    REQUIRE_FALSE(after_unbind.valid());
    REQUIRE(after_unbind.modified());
    REQUIRE(after_unbind.last_modified_time() == t5);
}

TEST_CASE("TimeSeriesReference: from-REF alternative expands non-peered bundle references")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *bundle_schema =
        registry.tsb("TimeSeriesReferenceFromRefNonPeeredBundle", {{"lhs", ts_int}, {"rhs", ts_int}});
    const auto *ref_bundle = registry.ref(bundle_schema);

    TSOutput lhs_target{*ts_int};
    TSOutput rhs_target{*ts_int};
    TSOutput ref_output{*ref_bundle};

    const auto [t1, t2, t3] = sequential_times<3>();

    set_output_value(lhs_target, 1, t1);
    set_output_value(rhs_target, 2, t1);
    set_output_reference(ref_output,
                         TimeSeriesReference::non_peered(
                             bundle_schema,
                             {
                                 TimeSeriesReference{lhs_target.view(t1)},
                                 TimeSeriesReference{rhs_target.view(t1)},
                             }),
                         t2);

    auto handle = ref_output.view(t2).binding_for(*bundle_schema);
    auto dereferenced_view = handle.view(t2);
    auto bundle = dereferenced_view.as_bundle();
    auto lhs = bundle.field("lhs");
    auto rhs = bundle.field("rhs");
    REQUIRE(dereferenced_view.valid());
    REQUIRE(dereferenced_view.modified());
    REQUIRE(lhs.value().checked_as<int>() == 1);
    REQUIRE(rhs.value().checked_as<int>() == 2);

    set_output_value(rhs_target, 3, t3);
    auto after_rhs_tick_view = handle.view(t3);
    auto after_rhs_tick = after_rhs_tick_view.as_bundle();
    REQUIRE(after_rhs_tick_view.modified());
    auto modified_items = collect_range(after_rhs_tick.modified_items());
    REQUIRE(modified_items.size() == 1);
    REQUIRE(std::string{modified_items[0].first} == "rhs");
    REQUIRE(modified_items[0].second.value().checked_as<int>() == 3);
}

TEST_CASE("TimeSeriesReference: from-REF alternative expands non-peered fixed-list references")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *list_schema = registry.tsl(ts_int, 2);
    const auto *ref_list = registry.ref(list_schema);

    TSOutput first_target{*ts_int};
    TSOutput second_target{*ts_int};
    TSOutput ref_output{*ref_list};

    const auto [t1, t2, t3] = sequential_times<3>();

    set_output_value(first_target, 4, t1);
    set_output_value(second_target, 5, t1);
    set_output_reference(ref_output,
                         TimeSeriesReference::non_peered(
                             list_schema,
                             {
                                 TimeSeriesReference{first_target.view(t1)},
                                 TimeSeriesReference{second_target.view(t1)},
                             }),
                         t2);

    auto handle = ref_output.view(t2).binding_for(*list_schema);
    auto dereferenced_view = handle.view(t2);
    auto list = dereferenced_view.as_list();
    REQUIRE(dereferenced_view.valid());
    REQUIRE(dereferenced_view.modified());
    REQUIRE(list[0].value().checked_as<int>() == 4);
    REQUIRE(list[1].value().checked_as<int>() == 5);

    set_output_value(first_target, 6, t3);
    auto after_first_tick_view = handle.view(t3);
    auto after_first_tick = after_first_tick_view.as_list();
    REQUIRE(after_first_tick_view.modified());
    auto modified_items = collect_range(after_first_tick.modified_items());
    REQUIRE(modified_items.size() == 1);
    REQUIRE(modified_items[0].first == 0);
    REQUIRE(modified_items[0].second.value().checked_as<int>() == 6);
}

TEST_CASE("TimeSeriesReference: from-REF alternative splits peered bundle references into child links")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *bundle_schema =
        registry.tsb("TimeSeriesReferenceFromRefPeeredBundle", {{"lhs", ts_int}, {"rhs", ts_int}});
    const auto *ref_bundle = registry.ref(bundle_schema);

    TSOutput target{*bundle_schema};
    TSOutput ref_output{*ref_bundle};

    const auto [t1, t2, t3] = sequential_times<3>();

    {
        auto data = target.data_view();
        auto bundle = data.as_bundle();
        auto lhs = bundle.field("lhs");
        auto rhs = bundle.field("rhs");
        Value lhs_value{5};
        Value rhs_value{6};
        REQUIRE(lhs.begin_mutation(t1).copy_value_from(lhs_value.view()));
        REQUIRE(rhs.begin_mutation(t1).copy_value_from(rhs_value.view()));
    }

    set_output_reference(ref_output, TimeSeriesReference{target.view(t1)}, t2);

    auto handle = ref_output.view(t2).binding_for(*bundle_schema);
    auto dereferenced_view = handle.view(t2);
    auto bundle = dereferenced_view.as_bundle();
    REQUIRE(bundle.field("lhs").value().checked_as<int>() == 5);
    REQUIRE(bundle.field("rhs").value().checked_as<int>() == 6);

    {
        auto target_view = target.view(t3);
        auto target_bundle = target_view.as_bundle();
        auto rhs = target_bundle.field("rhs");
        Value rhs_value{7};
        REQUIRE(rhs.begin_mutation(t3).copy_value_from(rhs_value.view()));
    }

    auto after_rhs_tick_view = handle.view(t3);
    auto after_rhs_tick = after_rhs_tick_view.as_bundle();
    REQUIRE(after_rhs_tick_view.modified());
    auto modified_items = collect_range(after_rhs_tick.modified_items());
    REQUIRE(modified_items.size() == 1);
    REQUIRE(std::string{modified_items[0].first} == "rhs");
    REQUIRE(modified_items[0].second.value().checked_as<int>() == 7);
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
