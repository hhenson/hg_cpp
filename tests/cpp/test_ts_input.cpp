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
    TSInput     input   = builder.make_input();

    REQUIRE(input.has_value());
    REQUIRE(input.schema() == root);

    auto input_root_view = input.view();
    auto root_view = input_root_view.as_bundle();
    REQUIRE(root_view.size() == 2);
    REQUIRE(root_view.has_field("a"));
    REQUIRE(root_view.has_field("nested"));

    TSOutput output{*ts_int};
    const auto t1 = MIN_ST;
    set_output(output, 42, t1);

    auto scalar = root_view.field("a");
    REQUIRE_FALSE(scalar.valid());
    scalar.bind_output(output.view(t1));
    REQUIRE(scalar.valid());
    REQUIRE(scalar.value().checked_as<int>() == 42);

    auto nested_input_view = input.view();
    auto nested_root = nested_input_view.as_bundle();
    auto nested_field = nested_root.field("nested");
    auto nested_bundle = nested_field.as_bundle();
    auto nested_leaf = nested_bundle.field("x");
    REQUIRE_FALSE(nested_leaf.valid());
    nested_leaf.bind_output(output.view(t1));
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

    TSInput input{TSInputBuilderFactory::checked_builder_for(*root, input_schema)};

    TSOutput lhs{*ts_int};
    TSOutput rhs{*ts_int};

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

    TSInput  input{TSInputBuilderFactory::checked_builder_for(*root, input_schema)};
    TSOutput output{*list};

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
    input_items.bind_output(output.view());

    RecordingNotifiable recorder;
    auto active_root_view = input.view(&recorder);
    auto active_root = active_root_view.as_bundle();
    auto active_items = active_root.field("items");
    auto active_list = active_items.as_list();
    auto second = active_list[1];
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

TEST_CASE("TSInput peered terminals invalidate cached input views when output storage is destroyed")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *root     = registry.tsb("TSInputInvalidationRoot", {{"value", ts_int}});

    const auto input_schema = TSEndpointSchema::non_peered(
        root,
        {
            TSEndpointSchema::peered(ts_int),
        });

    TSInput input{TSInputBuilderFactory::checked_builder_for(*root, input_schema)};
    auto    root_view = input.view();
    auto    bundle = root_view.as_bundle();
    auto    value = bundle.field("value");

    {
        TSOutput output{*ts_int};
        set_output(output, 7, MIN_ST);
        value.bind_output(output.view(MIN_ST));
        REQUIRE(value.valid());
        REQUIRE(value.value().checked_as<int>() == 7);
    }

    REQUIRE_FALSE(value.bound());
    REQUIRE_FALSE(value.valid());
    REQUIRE(value.schema() == ts_int);
    REQUIRE_THROWS_AS(value.value(), std::logic_error);
}
