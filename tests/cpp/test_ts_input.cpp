#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/value/value.h>

#include <catch2/catch_test_macros.hpp>

#include <stdexcept>
#include <string>
#include <vector>

namespace
{
    struct RecordingNotifiable : hgraph::Notifiable
    {
        std::vector<hgraph::engine_time_t> notified{};

        void notify(hgraph::engine_time_t modified_time) override
        {
            notified.push_back(modified_time);
        }
    };

    [[nodiscard]] hgraph::TSEndpointSchema nested_input_schema(const hgraph::TSValueTypeMetaData *root,
                                                               const hgraph::TSValueTypeMetaData *nested,
                                                               const hgraph::TSValueTypeMetaData *scalar)
    {
        return hgraph::TSEndpointSchema::non_peered(
            root,
            {
                hgraph::TSEndpointSchema::peered(scalar),
                hgraph::TSEndpointSchema::non_peered(
                    nested,
                    {
                        hgraph::TSEndpointSchema::peered(scalar),
                    }),
            });
    }

    void set_output(hgraph::TSOutput &output, int value, hgraph::engine_time_t time)
    {
        hgraph::Value wrapped{value};
        auto mutation = output.begin_mutation(time);
        REQUIRE(mutation.copy_value_from(wrapped.view()));
    }

    void set_list_output(hgraph::TSOutput &output, std::size_t index, int value, hgraph::engine_time_t time)
    {
        hgraph::Value wrapped{value};
        auto          view = output.view(time);
        auto          list = view.as_list();
        auto          mutation = list[index].begin_mutation(time);
        REQUIRE(mutation.copy_value_from(wrapped.view()));
    }

    template <typename Range>
    [[nodiscard]] auto collect_range(const Range &range)
    {
        using value_type = std::decay_t<decltype(*range.begin())>;
        std::vector<value_type> result;
        for (auto value : range) { result.push_back(value); }
        return result;
    }

    template <typename Range>
    [[nodiscard]] std::size_t range_size(const Range &range)
    {
        std::size_t result = 0;
        for (auto it = range.begin(); it != range.end(); ++it) { ++result; }
        return result;
    }
}

TEST_CASE("TSInput builds a non-peered TSB root with nested peered terminals")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *nested   = registry.tsb("TSInputNested", {{"x", ts_int}});
    const auto *root     = registry.tsb("TSInputRoot", {{"a", ts_int}, {"nested", nested}});

    const auto &builder = TSInputBuilderFactory::checked_builder_for(
        *root,
        nested_input_schema(root, nested, ts_int));
    TSOutput output{*ts_int};
    TSOutput replacement{*ts_int};
    const auto t1 = MIN_ST;
    set_output(output, 42, t1);
    set_output(replacement, 99, t1);

    TSInput     input   = builder.make_input();

    REQUIRE(input.has_value());
    REQUIRE(input.schema() == root);

    auto input_root_view = input.view();
    REQUIRE_FALSE(input_root_view.is_bindable());
    REQUIRE(input_root_view.bound());
    auto root_view = input_root_view.as_bundle();
    REQUIRE_FALSE(root_view.is_bindable());
    REQUIRE(root_view.bound());
    REQUIRE(root_view.size() == 2);
    REQUIRE(root_view.has_field("a"));
    REQUIRE(root_view.has_field("nested"));

    auto scalar = root_view.field("a");
    REQUIRE(scalar.is_bindable());
    REQUIRE_FALSE(scalar.bound());
    REQUIRE_FALSE(scalar.valid());
    scalar.bind_output(output.view(t1));
    REQUIRE(scalar.bound());
    REQUIRE(input_root_view.bound());
    REQUIRE(root_view.bound());
    REQUIRE(scalar.valid());
    REQUIRE(scalar.value().checked_as<int>() == 42);

    scalar.bind_output(replacement.view(t1));
    REQUIRE(scalar.bound());
    REQUIRE(scalar.valid());
    REQUIRE(scalar.value().checked_as<int>() == 99);

    auto nested_input_view = input.view();
    auto nested_root = nested_input_view.as_bundle();
    auto nested_field = nested_root.field("nested");
    REQUIRE_FALSE(nested_field.is_bindable());
    REQUIRE(nested_field.bound());
    auto nested_bundle = nested_field.as_bundle();
    REQUIRE_FALSE(nested_bundle.is_bindable());
    REQUIRE(nested_bundle.bound());
    auto nested_leaf = nested_bundle.field("x");
    REQUIRE(nested_leaf.is_bindable());
    REQUIRE_FALSE(nested_leaf.bound());
    REQUIRE_FALSE(nested_leaf.valid());
    nested_leaf.bind_output(output.view(t1));
    REQUIRE(nested_leaf.bound());
    REQUIRE(nested_field.bound());
    REQUIRE(nested_bundle.bound());
    REQUIRE(nested_leaf.valid());
    REQUIRE(nested_leaf.value().checked_as<int>() == 42);
}

TEST_CASE("TSInput construction uses generic endpoint annotations")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *list     = registry.tsl(ts_int, 2);
    const auto *root     = registry.tsb("TSInputAnnotatedRoot", {{"items", list}});

    const auto list_annotation = TSEndpointSchema::non_peered_list(
        list,
        TSEndpointSchema::peered(ts_int));
    REQUIRE(list_annotation.is_non_peered());
    REQUIRE(list_annotation.child_count() == 2);
    REQUIRE(list_annotation.child(0).is_peered());
    REQUIRE(list_annotation.child(1).schema() == ts_int);

    const auto input_annotation = TSEndpointSchema::non_peered(root, {list_annotation});
    const auto plan = TSInputPlanFactory::compile(*root, input_annotation);
    REQUIRE(plan.schema().kind == TSTypeKind::TSB);
    REQUIRE(plan.endpoint_schema().child(0).child_count() == 2);

    const auto input_with_peered_child = TSEndpointSchema::non_peered(
        root,
        {
            TSEndpointSchema::peered(list),
        });
    REQUIRE_NOTHROW(TSInputPlanFactory::compile(*root, input_with_peered_child));

    REQUIRE_THROWS_AS(
        TSEndpointSchema::non_peered(
            root,
            {
                TSEndpointSchema::peered(ts_int),
            }),
        std::invalid_argument);
}

TEST_CASE("TSInput active non-peered prefixes schedule through peered terminal notifications")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *list     = registry.tsl(ts_int, 2);
    const auto *root     = registry.tsb("TSInputListRoot", {{"items", list}});

    const auto input_schema = TSEndpointSchema::non_peered(
        root,
        {
            TSEndpointSchema::non_peered_list(list, TSEndpointSchema::peered(ts_int)),
        });

    TSOutput lhs{*ts_int};
    TSOutput rhs{*ts_int};
    TSInput  input{TSInputBuilderFactory::checked_builder_for(*root, input_schema)};

    auto input_root_view = input.view();
    auto input_root = input_root_view.as_bundle();
    auto list_view = input_root.field("items");
    auto list_children = list_view.as_list();
    list_children[0].bind_output(lhs.view());
    list_children[1].bind_output(rhs.view());

    RecordingNotifiable recorder;
    auto active_root_view = input.view(&recorder);
    auto active_root = active_root_view.as_bundle();
    auto active_list = active_root.field("items");
    active_list.make_active();
    REQUIRE(active_list.active());

    const auto t1 = MIN_ST + engine_time_delta_t{1};
    const auto t2 = t1 + engine_time_delta_t{1};
    set_output(lhs, 1, t1);
    set_output(rhs, 2, t2);

    REQUIRE(recorder.notified == std::vector<engine_time_t>{t1, t2});

    active_list.make_passive();
    REQUIRE_FALSE(active_list.active());

    const auto t3 = t2 + engine_time_delta_t{1};
    set_output(lhs, 3, t3);
    REQUIRE(recorder.notified == std::vector<engine_time_t>{t1, t2});
}

TEST_CASE("TSInput target binding updates non-peered bundle and list prefixes")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *list     = registry.tsl(ts_int, 2);
    const auto *root     = registry.tsb("TSInputRecursiveBindingRoot", {{"items", list}});

    const auto input_schema = TSEndpointSchema::non_peered(
        root,
        {
            TSEndpointSchema::non_peered_list(list, TSEndpointSchema::peered(ts_int)),
        });

    TSOutput first_output{*ts_int};
    TSOutput second_output{*ts_int};
    TSOutput root_output{*root};
    TSInput  input{TSInputBuilderFactory::checked_builder_for(*root, input_schema)};

    const auto t1 = MIN_ST + engine_time_delta_t{20};
    const auto t2 = t1 + engine_time_delta_t{1};

    set_output(first_output, 10, t1);

    auto input_root = input.view(nullptr, t1);
    REQUIRE(input_root.evaluation_time() == t1);
    REQUIRE(input_root.binding() != nullptr);
    REQUIRE_FALSE(input_root.is_bindable());
    REQUIRE(input_root.bound());
    REQUIRE_FALSE(input_root.valid());
    REQUIRE_THROWS_AS(input_root.bind_output(root_output.view(t1)), std::logic_error);

    auto bundle = input_root.as_bundle();
    auto items = bundle.field("items");
    REQUIRE_FALSE(items.is_bindable());
    REQUIRE(items.bound());
    auto list_view = items.as_list();
    REQUIRE_FALSE(list_view.is_bindable());
    REQUIRE(list_view.bound());
    REQUIRE(list_view[0].is_bindable());
    REQUIRE_FALSE(list_view[0].bound());
    list_view[0].bind_output(first_output.view(t1));

    REQUIRE(input_root.valid());
    REQUIRE_FALSE(input_root.all_valid());
    REQUIRE(input_root.modified());
    REQUIRE(input_root.last_modified_time() == t1);

    const auto keys = collect_range(bundle.keys());
    REQUIRE(keys.size() == 1);
    REQUIRE(std::string{keys[0]} == "items");
    REQUIRE(range_size(bundle.values()) == 1);
    REQUIRE(range_size(bundle.valid_items()) == 1);
    auto bundle_modified_items = collect_range(bundle.modified_items());
    REQUIRE(bundle_modified_items.size() == 1);
    REQUIRE(std::string{bundle_modified_items[0].first} == "items");
    REQUIRE(input_root.value().is_bundle());

    REQUIRE(items.binding() != nullptr);
    REQUIRE(items.valid());
    REQUIRE_FALSE(items.all_valid());
    REQUIRE(items.last_modified_time() == t1);

    REQUIRE(list_view.size() == 2);
    REQUIRE(range_size(list_view.values()) == 2);
    auto list_valid_items = collect_range(list_view.valid_items());
    REQUIRE(list_valid_items.size() == 1);
    REQUIRE(list_valid_items[0].first == 0);
    REQUIRE(list_valid_items[0].second.value().checked_as<int>() == 10);
    REQUIRE(list_view[0].binding() != nullptr);
    REQUIRE(list_view[0].evaluation_time() == t1);
    auto list_modified_items = collect_range(list_view.modified_items());
    REQUIRE(list_modified_items.size() == 1);
    REQUIRE(list_modified_items[0].first == 0);
    REQUIRE(list_view[0].valid());
    REQUIRE_FALSE(list_view[1].valid());

    set_output(second_output, 20, t2);

    auto t2_root = input.view(nullptr, t2);
    auto t2_bundle = t2_root.as_bundle();
    auto t2_items  = t2_bundle.field("items");
    auto t2_list   = t2_items.as_list();
    REQUIRE(t2_list[1].is_bindable());
    REQUIRE_FALSE(t2_list[1].bound());
    t2_list[1].bind_output(second_output.view(t2));

    REQUIRE(t2_root.valid());
    REQUIRE(t2_root.all_valid());
    REQUIRE(t2_root.modified());
    REQUIRE(t2_root.last_modified_time() == t2);

    REQUIRE(range_size(t2_list.valid_values()) == 2);
    REQUIRE(range_size(t2_list.modified_values()) == 1);
    auto t2_modified_items = collect_range(t2_list.modified_items());
    REQUIRE(t2_modified_items.size() == 1);
    REQUIRE(t2_modified_items[0].first == 1);
    REQUIRE(t2_modified_items[0].second.value().checked_as<int>() == 20);

    REQUIRE_THROWS_AS(t2_root.unbind_output(), std::logic_error);
    t2_list[0].unbind_output();
    t2_list[1].unbind_output();
    REQUIRE_FALSE(t2_root.valid());
    REQUIRE_FALSE(t2_items.valid());
}

TEST_CASE("TSInput data views project non-peered prefixes")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *list     = registry.tsl(ts_int, 2);
    const auto *root_schema = registry.tsb("TSInputDataViewNonPeeredRoot", {{"items", list}});

    const auto input_schema = TSEndpointSchema::non_peered(
        root_schema,
        {
            TSEndpointSchema::non_peered_list(list, TSEndpointSchema::peered(ts_int)),
        });

    TSOutput first_output{*ts_int};
    TSInput  input{TSInputBuilderFactory::checked_builder_for(*root_schema, input_schema)};

    const auto t1 = MIN_ST + engine_time_delta_t{40};
    set_output(first_output, 11, t1);

    auto root_view = input.view(nullptr, t1);
    auto bundle = root_view.as_bundle();
    auto items = bundle.field("items");
    auto list_view = items.as_list();
    list_view[0].bind_output(first_output.view(t1));

    auto root_data = root_view.data_view();
    REQUIRE(root_data.valid());
    REQUIRE(root_data.schema() == input.schema());
    REQUIRE(root_data.binding() == root_view.binding());
    REQUIRE(root_data.has_current_value());
    REQUIRE_FALSE(root_data.all_valid());
    REQUIRE(root_data.modified(t1));

    auto bundle_data = root_data.as_bundle();
    REQUIRE(bundle_data.size() == 1);
    auto items_data = bundle_data.field("items");
    REQUIRE(items_data.valid());
    REQUIRE(items_data.schema() == list);
    REQUIRE(items_data.has_current_value());
    REQUIRE_FALSE(items_data.all_valid());
    REQUIRE(items_data.modified(t1));

    auto list_data = items_data.as_list();
    REQUIRE(list_data.size() == 2);
    REQUIRE(range_size(list_data.valid_items()) == 1);
    auto first_child = list_data[0];
    REQUIRE(first_child.valid());
    REQUIRE(first_child.schema() == ts_int);
    REQUIRE(first_child.value().checked_as<int>() == 11);

    auto second_child = list_data[1];
    REQUIRE_FALSE(second_child.valid());
    REQUIRE(second_child.schema() == ts_int);
}

TEST_CASE("TSInput data views step through target links and rebinds")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *list     = registry.tsl(ts_int, 2);
    const auto *root_schema = registry.tsb("TSInputDataViewTargetLinkRoot", {{"items", list}});

    const auto input_schema = TSEndpointSchema::non_peered(
        root_schema,
        {
            TSEndpointSchema::peered(list),
        });

    TSOutput first_output{*list};
    TSOutput second_output{*list};
    TSInput  input{TSInputBuilderFactory::checked_builder_for(*root_schema, input_schema)};

    const auto t1 = MIN_ST + engine_time_delta_t{50};
    const auto t2 = t1 + engine_time_delta_t{1};
    set_list_output(first_output, 0, 10, t1);
    set_list_output(first_output, 1, 20, t1);
    set_list_output(second_output, 0, 100, t2);
    set_list_output(second_output, 1, 200, t2);

    auto root_view = input.view(nullptr, t1);
    auto bundle = root_view.as_bundle();
    auto items = bundle.field("items");
    items.bind_output(first_output.view(t1));

    auto list_view = items.as_list();
    auto cached_child = list_view[1];
    REQUIRE(cached_child.schema() == ts_int);
    REQUIRE(cached_child.data_view().schema() == ts_int);
    REQUIRE(cached_child.value().checked_as<int>() == 20);

    auto root_data = root_view.data_view();
    auto root_data_bundle = root_data.as_bundle();
    auto target_data = root_data_bundle.field("items");
    auto target_data_list = target_data.as_list();
    auto target_child_data = target_data_list[1];
    REQUIRE(target_child_data.schema() == ts_int);
    REQUIRE(target_child_data.value().checked_as<int>() == 20);

    auto rebound_root = input.view(nullptr, t2);
    auto rebound_bundle = rebound_root.as_bundle();
    auto rebound_items = rebound_bundle.field("items");
    rebound_items.bind_output(second_output.view(t2));

    REQUIRE(cached_child.schema() == ts_int);
    REQUIRE(cached_child.data_view().schema() == ts_int);
    REQUIRE(cached_child.value().checked_as<int>() == 200);

    auto rebound_root_data = input.view(nullptr, t2).data_view();
    auto rebound_root_data_bundle = rebound_root_data.as_bundle();
    auto rebound_target_data = rebound_root_data_bundle.field("items");
    auto rebound_target_data_list = rebound_target_data.as_list();
    auto rebound_target_child_data = rebound_target_data_list[1];
    REQUIRE(rebound_target_child_data.schema() == ts_int);
    REQUIRE(rebound_target_child_data.value().checked_as<int>() == 200);
}

TEST_CASE("TSInput binding rejects non-peered views and incompatible output schemas")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *dbl_meta = registry.register_scalar<double>("double");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *ts_dbl   = registry.ts(dbl_meta);
    const auto *root     = registry.tsb("TSInputBindingValidationRoot", {{"value", ts_int}});
    const auto *bad_root = registry.tsb("TSInputBindingValidationBadRoot", {{"value", ts_dbl}});

    const auto input_schema = TSEndpointSchema::non_peered(
        root,
        {
            TSEndpointSchema::peered(ts_int),
        });

    TSOutput wrong_root{*bad_root};
    TSOutput wrong_leaf{*ts_dbl};
    TSInput  input{TSInputBuilderFactory::checked_builder_for(*root, input_schema)};

    auto root_view = input.view();
    REQUIRE_FALSE(root_view.is_bindable());
    REQUIRE_THROWS_AS(root_view.bind_output(wrong_root.view()), std::logic_error);

    auto root_bundle = root_view.as_bundle();
    auto leaf        = root_bundle.field("value");
    REQUIRE_THROWS_AS(leaf.bind_output(wrong_leaf.view()), std::invalid_argument);
}

TEST_CASE("TSInput active root bubbles output modifications through non-peered prefixes")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *list     = registry.tsl(ts_int, 2);
    const auto *root     = registry.tsb("TSInputRootBubblingRoot", {{"items", list}});

    const auto input_schema = TSEndpointSchema::non_peered(
        root,
        {
            TSEndpointSchema::non_peered_list(list, TSEndpointSchema::peered(ts_int)),
        });

    TSOutput first_output{*ts_int};
    TSOutput second_output{*ts_int};
    TSInput  input{TSInputBuilderFactory::checked_builder_for(*root, input_schema)};

    auto root_binding_view = input.view();
    auto binding_bundle = root_binding_view.as_bundle();
    auto binding_items = binding_bundle.field("items");
    auto binding_list = binding_items.as_list();
    binding_list[0].bind_output(first_output.view());
    binding_list[1].bind_output(second_output.view());

    RecordingNotifiable recorder;
    auto active_root = input.view(&recorder);
    active_root.make_active();
    REQUIRE(active_root.active());

    const auto t1 = MIN_ST + engine_time_delta_t{30};
    const auto t2 = t1 + engine_time_delta_t{1};
    set_output(first_output, 1, t1);
    set_output(second_output, 2, t2);

    REQUIRE(recorder.notified == std::vector<engine_time_t>{t1, t2});

    auto t2_root = input.view(nullptr, t2);
    REQUIRE(t2_root.modified());
    REQUIRE(t2_root.last_modified_time() == t2);

    auto bundle = t2_root.as_bundle();
    auto modified_bundle_items = collect_range(bundle.modified_items());
    REQUIRE(modified_bundle_items.size() == 1);
    REQUIRE(std::string{modified_bundle_items[0].first} == "items");

    auto modified_list = modified_bundle_items[0].second.as_list();
    auto modified_list_items = collect_range(modified_list.modified_items());
    REQUIRE(modified_list_items.size() == 1);
    REQUIRE(modified_list_items[0].first == 1);

    active_root.make_passive();
    REQUIRE_FALSE(active_root.active());

    const auto t3 = t2 + engine_time_delta_t{1};
    set_output(first_output, 3, t3);
    REQUIRE(recorder.notified == std::vector<engine_time_t>{t1, t2});
}

TEST_CASE("TSInput peered collection descendants can be activated independently")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *list     = registry.tsl(ts_int, 2);
    const auto *root     = registry.tsb("TSInputBoundListRoot", {{"items", list}});

    const auto input_schema = TSEndpointSchema::non_peered(
        root,
        {
            TSEndpointSchema::peered(list),
        });

    TSOutput output{*list};
    TSInput  input{TSInputBuilderFactory::checked_builder_for(*root, input_schema)};

    auto output_view = output.view();
    auto output_list = output_view.as_list();
    {
        auto first = output_list[0].begin_mutation(MIN_ST);
        hgraph::Value one{1};
        REQUIRE(first.copy_value_from(one.view()));
    }
    {
        auto second = output_list[1].begin_mutation(MIN_ST);
        hgraph::Value two{2};
        REQUIRE(second.copy_value_from(two.view()));
    }

    auto input_root_view = input.view();
    auto input_root = input_root_view.as_bundle();
    auto input_items = input_root.field("items");
    REQUIRE(input_items.is_bindable());
    REQUIRE_FALSE(input_items.bound());
    input_items.bind_output(output.view());
    REQUIRE(input_items.bound());

    RecordingNotifiable recorder;
    auto active_root_view = input.view(&recorder);
    auto active_root = active_root_view.as_bundle();
    auto active_items = active_root.field("items");
    auto active_list = active_items.as_list();
    REQUIRE(active_items.is_bindable());
    REQUIRE(active_items.bound());
    REQUIRE(active_list.is_bindable());
    REQUIRE(active_list.bound());
    auto second = active_list[1];
    REQUIRE(second.bound());
    REQUIRE(second.is_bindable());
    REQUIRE_NOTHROW(second.bind_output(output.view()));
    second.make_active();
    REQUIRE(second.active());

    const auto t1 = MIN_ST + engine_time_delta_t{10};
    const auto t2 = t1 + engine_time_delta_t{1};
    {
        auto view = output.view();
        auto list_view = view.as_list();
        auto first_mutation = list_view[0].begin_mutation(t1);
        hgraph::Value value{10};
        REQUIRE(first_mutation.copy_value_from(value.view()));
    }
    REQUIRE(recorder.notified.empty());

    {
        auto view = output.view();
        auto list_view = view.as_list();
        auto second_mutation = list_view[1].begin_mutation(t2);
        hgraph::Value value{20};
        REQUIRE(second_mutation.copy_value_from(value.view()));
    }
    REQUIRE(recorder.notified == std::vector<engine_time_t>{t2});

    second.make_passive();
}

TEST_CASE("TSInput shape casts return endpoint views for slot collections")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tss      = registry.tss(int_meta);
    const auto *tsd      = registry.tsd(int_meta, ts_int);
    const auto *root     = registry.tsb("TSInputSlotRoot", {{"set", tss}, {"dict", tsd}});

    const auto input_schema = TSEndpointSchema::non_peered(
        root,
        {
            TSEndpointSchema::peered(tss),
            TSEndpointSchema::peered(tsd),
        });

    TSOutput set_output{*tss};
    TSOutput dict_output{*tsd};
    TSInput  input{TSInputBuilderFactory::checked_builder_for(*root, input_schema)};

    const auto t1 = MIN_ST;
    const auto t2 = t1 + engine_time_delta_t{1};
    Value      one{1};
    Value      two{2};
    Value      key{7};
    Value      value{42};
    Value      replacement{84};

    {
        auto set_view = set_output.view(t1);
        auto set = set_view.as_set();
        auto mutation = set.begin_mutation(t1);
        REQUIRE(mutation.add(one.view()));
    }
    {
        auto dict_view = dict_output.view(t1);
        auto dict = dict_view.as_dict();
        auto mutation = dict.begin_mutation(t1);
        auto child = mutation.at(key.view());
        auto child_mutation = child.begin_mutation(t1);
        REQUIRE(child_mutation.copy_value_from(value.view()));
    }

    auto root_view = input.view(nullptr, t1);
    auto bundle = root_view.as_bundle();
    auto set_field = bundle.field("set");
    auto dict_field = bundle.field("dict");
    auto set_input = set_field.as_set();
    auto dict_input = dict_field.as_dict();

    set_input.bind_output(set_output.view(t1));
    dict_input.bind_output(dict_output.view(t1));

    REQUIRE(set_input.valid());
    REQUIRE(set_input.contains(one.view()));
    REQUIRE(dict_input.valid());
    REQUIRE(dict_input.contains(key.view()));
    REQUIRE(range_size(dict_input.values()) == 1);
    auto dict_child = dict_input.at(key.view());
    REQUIRE(dict_child.valid());
    REQUIRE(dict_child.value().checked_as<int>() == 42);

    RecordingNotifiable recorder;
    auto active_root_view = input.view(&recorder, t2);
    auto active_bundle = active_root_view.as_bundle();
    auto active_set_field = active_bundle.field("set");
    auto active_dict_field = active_bundle.field("dict");
    auto active_set = active_set_field.as_set();
    auto active_dict = active_dict_field.as_dict();
    auto active_dict_child = active_dict.at(key.view());
    active_set.make_active();
    active_dict_child.make_active();
    REQUIRE(active_set.active());
    REQUIRE(active_dict_child.active());

    {
        auto set_view = set_output.view(t2);
        auto set = set_view.as_set();
        auto mutation = set.begin_mutation(t2);
        REQUIRE(mutation.add(two.view()));
    }
    {
        auto dict_view = dict_output.view(t2);
        auto dict = dict_view.as_dict();
        auto child = dict.at(key.view());
        auto mutation = child.begin_mutation(t2);
        REQUIRE(mutation.copy_value_from(replacement.view()));
    }

    REQUIRE(recorder.notified == std::vector<engine_time_t>{t2, t2});

    active_set.make_passive();
    active_dict_child.make_passive();
}
