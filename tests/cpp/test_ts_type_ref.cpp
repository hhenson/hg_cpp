#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/type_record_registry.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/registry_reset.h>
#include <hgraph/lib/testing/mock_runtime.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/value/value.h>

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace
{
    struct RecordingNotifier final : hgraph::Notifiable
    {
        void notify(hgraph::DateTime time) override { notifications.push_back(time); }
        std::vector<hgraph::DateTime> notifications{};
    };

    struct InvalidationRecorder final : hgraph::Notifiable
    {
        void notify(hgraph::DateTime) override {}
        void source_invalidated(const hgraph::TSDataTracking *source) noexcept override
        {
            ++invalidations;
            invalidated_source = source;
        }

        std::size_t invalidations{0};
        const hgraph::TSDataTracking *invalidated_source{nullptr};
    };

    struct ReentrantInvalidationRecorder final : hgraph::Notifiable
    {
        void notify(hgraph::DateTime) override {}
        void source_invalidated(const hgraph::TSDataTracking *source) noexcept override
        {
            ++invalidations;
            const_cast<hgraph::TSDataTracking *>(source)->observers.invalidate(source);
        }

        std::size_t invalidations{0};
    };

    struct ForwardingClearRecorder final : hgraph::SlotObserver
    {
        void on_capacity(std::size_t, std::size_t) override {}
        void on_insert(std::size_t) override {}
        void on_remove(std::size_t) override {}
        void on_erase(std::size_t) override {}
        void on_clear() override
        {
            ++clears;
            saw_empty_target = forwarding != nullptr &&
                               !forwarding->view(evaluation_time).forwarding_target().bound();
        }

        hgraph::TSOutput *forwarding{nullptr};
        hgraph::DateTime evaluation_time{hgraph::MIN_ST};
        std::size_t clears{0};
        bool saw_empty_target{false};
    };

    struct LifetimeSource
    {
        static constexpr auto name = "scalar_lifetime_source";
        static constexpr bool schedule_on_start = true;
        static void eval(hgraph::Out<hgraph::TS<hgraph::Int>> out) { out.set(hgraph::Int{1}); }
    };

    struct LifetimeSink
    {
        static constexpr auto name = "scalar_lifetime_sink";
        static void eval(hgraph::In<"value", hgraph::TS<hgraph::Int>>) {}
    };

    [[nodiscard]] hgraph::TSInput scalar_input(const hgraph::TSValueTypeMetaData *schema)
    {
        return hgraph::TSInput{
            hgraph::TSInputBuilderFactory::checked_builder_for(*schema, hgraph::TSEndpointSchema::peered(schema))};
    }
}

TEST_CASE("time-series schemas expose canonical SchemaHeader prefixes and numeric kinds")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const auto *signal = registry.signal();
    const auto *tss = registry.tss(integer);
    const auto *tsd = registry.tsd(integer, ts);
    const auto *tsl = registry.tsl(ts, 2);
    const auto *tsw = registry.tsw(integer, 3, 1);
    const auto *structural = registry.un_named_tsb({{"value", ts}});
    const auto *named = registry.tsb("RoleHeaderBundle", {{"value", ts}});
    const auto *ref = registry.ref(ts);

    const std::array schemas{ts, tss, tsd, tsl, tsw, structural, ref, signal, named};
    for (const auto *schema : schemas)
    {
        REQUIRE(schema != nullptr);
        REQUIRE(reinterpret_cast<const SchemaHeader *>(schema) == &schema->header);
        REQUIRE(schema->header.valid());
        REQUIRE(schema->header.family == TypeFamily::TimeSeries);
        REQUIRE(schema->header.kind == static_cast<TypeKind>(schema->kind));
        REQUIRE_FALSE(schema->name().empty());
    }
    REQUIRE(structural->wrapped_un_named_tsb() == nullptr);
    REQUIRE(named->wrapped_un_named_tsb() == structural);
    REQUIRE(std::string{named->name()} == "RoleHeaderBundle");
}

TEST_CASE("scalar time-series role records are canonical, exact, and thread stable")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    auto &factory = TSDataPlanFactory::instance();
    const auto *schema = registry.ts(registry.register_scalar<std::int32_t>("int32"));

    const auto data = factory.data_type_for(schema);
    const auto output = factory.output_type_for(schema);
    const auto input_builder = TSInputBuilderFactory::checked_builder_for(*schema, TSEndpointSchema::owned(schema));
    TSInput input{input_builder};
    const auto input_type = input.type_ref();

    for (const auto type : {data.as_role(), input_type.as_role(), output.as_role()})
    {
        REQUIRE(type.valid());
        REQUIRE(type.schema() == schema);
        REQUIRE(type.record()->schema == &schema->header);
        REQUIRE(type.record()->ops_abi_version == TS_DATA_OPS_ABI_VERSION);
        REQUIRE(type.record()->implementation_name().empty());
        REQUIRE(type.record()->debug == nullptr);
        REQUIRE(type.ops_ref().kind == TSTypeKind::TS);
        REQUIRE(has_capability(type.capabilities(), TypeCapabilities::Viewable));
        REQUIRE_FALSE(has_capability(type.capabilities(), TypeCapabilities::HasChildren));
        REQUIRE_FALSE(has_capability(type.capabilities(), TypeCapabilities::Hashable));
        REQUIRE_FALSE(has_capability(type.capabilities(), TypeCapabilities::Equatable));
        REQUIRE_FALSE(has_capability(type.capabilities(), TypeCapabilities::Comparable));
    }
    REQUIRE(data.record() != output.record());
    REQUIRE(data.record() != input_type.record());
    REQUIRE(output.record() != input_type.record());
    REQUIRE(has_capability(data.capabilities(), TypeCapabilities::Mutable));
    REQUIRE(has_capability(output.capabilities(), TypeCapabilities::Mutable));
    REQUIRE_FALSE(has_capability(input_type.capabilities(), TypeCapabilities::Mutable));
    REQUIRE_THROWS_AS(factory.binding_for(schema), std::logic_error);
    REQUIRE(factory.find_binding(schema) == nullptr);

    std::array<const TypeRecord *, 8> records{};
    std::array<std::thread, 8> threads{};
    for (std::size_t i = 0; i < threads.size(); ++i)
        threads[i] = std::thread([&, i] { records[i] = factory.data_type_for(schema).record(); });
    for (auto &thread : threads) thread.join();
    for (const auto *record : records) REQUIRE(record == data.record());
}

TEST_CASE("scalar role references validate AnyPtr boundaries and enforce role mutation")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    auto &factory = TSDataPlanFactory::instance();
    const auto *schema = registry.ts(registry.register_scalar<std::int32_t>("int32"));
    const auto data_type = factory.data_type_for(schema);
    const auto output_type = factory.output_type_for(schema);

    REQUIRE(TSDataTypeRef::checked(data_type.typed_null().to_any()) == data_type);
    REQUIRE_THROWS_AS(TSInputTypeRef::checked(data_type.typed_null().to_any()), std::invalid_argument);
    REQUIRE_THROWS_AS(TSDataTypeRef::checked(output_type.typed_null().to_any()), std::invalid_argument);
    const auto value_type = registry.scalar_type<std::int32_t>();
    REQUIRE_THROWS_AS(TSRoleTypeRef::checked(value_type.typed_null().to_any()), std::invalid_argument);

    const auto input_role = checked_ts_role_type(
        intern_ts_type(*schema, TypeRole::Input, data_type.checked_plan(), data_type.ops_ref()),
        std::integral_constant<TypeRole, TypeRole::Input>{});
    REQUIRE_THROWS_AS(input_role.writable(nullptr), std::logic_error);
    REQUIRE_THROWS_AS(TSInputTypeRef::checked(AnyPtr::writable(*input_role.record(), nullptr)),
                      std::invalid_argument);
    REQUIRE_THROWS_AS(TSData{*factory.legacy_binding_for(schema)}, std::invalid_argument);

    TSData data{data_type};
    Value forty_two{std::int32_t{42}};
    {
        auto mutation = data.view().begin_mutation(MIN_ST);
        REQUIRE(mutation.copy_value_from(forty_two.view()));
    }
    REQUIRE(data.view().value().checked_as<std::int32_t>() == 42);

    TSInput owned{TSInputBuilderFactory::checked_builder_for(*schema, TSEndpointSchema::owned(schema))};
    REQUIRE(owned.binding() == nullptr);
    REQUIRE_THROWS_AS(owned.view(nullptr, MIN_ST).data_view().begin_mutation(MIN_ST), std::logic_error);
}

TEST_CASE("time-series role validation rejects common header kind mismatches")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    auto &factory = TSDataPlanFactory::instance();
    const auto *ts = registry.ts(registry.register_scalar<std::int32_t>("int32"));
    const auto ts_data = factory.data_type_for(ts);

    TSValueTypeMetaData malformed_for_intern = *ts;
    malformed_for_intern.header.kind = static_cast<TypeKind>(TSTypeKind::TSS);
    REQUIRE(malformed_for_intern.header.valid());
    REQUIRE(malformed_for_intern.kind == TSTypeKind::TS);
    REQUIRE(ts_data.ops_ref().kind == TSTypeKind::TS);
    REQUIRE_THROWS_AS(
        intern_ts_type(malformed_for_intern, TypeRole::Data, ts_data.checked_plan(), ts_data.ops_ref()),
        std::invalid_argument);

    // TypeRecordRegistry validates only the common record contract, so retain
    // this deliberately malformed family schema for the registry lifetime and
    // exercise the time-series checked narrowing boundary explicitly.
    static auto *malformed_record_schema = new TSValueTypeMetaData{*ts};
    malformed_record_schema->header.kind = static_cast<TypeKind>(TSTypeKind::TSS);
    const TypeRecordDefinition definition{
        .key = TypeRecordKey{
            .schema = &malformed_record_schema->header,
            .role = TypeRole::Data,
            .plan = &ts_data.checked_plan(),
            .ops = &ts_data.ops_ref(),
            .debug = nullptr,
        },
        .ops_abi_version = TS_DATA_OPS_ABI_VERSION,
        .capabilities = ts_type_capabilities(TypeRole::Data, ts_data.checked_plan(), ts_data.ops_ref()),
        .implementation_label = {},
    };
    const TypeRecord &malformed_record = TypeRecordRegistry::instance().intern(definition);
    const AnyPtr malformed_pointer = AnyPtr::typed_null(malformed_record);
    REQUIRE(malformed_pointer.well_formed());
    REQUIRE_THROWS_AS(TSRoleTypeRef::checked(malformed_pointer), std::invalid_argument);
    REQUIRE_THROWS_AS(TSDataTypeRef::checked(malformed_pointer), std::invalid_argument);

    const auto malformed_role = TSStorageTypeRef::from_raw_bits(
        reinterpret_cast<std::uintptr_t>(&malformed_record)).type_ref();
    REQUIRE_FALSE(malformed_role.valid());
    REQUIRE_THROWS_AS(TSDataTypeRef::checked(malformed_role), std::invalid_argument);

    for (const auto *canonical : {ts, registry.signal()})
    {
        const auto type = factory.data_type_for(canonical);
        REQUIRE(type.valid());
        REQUIRE(TSRoleTypeRef::checked(type.typed_null().to_any()).valid());
        REQUIRE(TSDataTypeRef::checked(type.as_role()).valid());
    }
}

TEST_CASE("scalar output and peered input preserve binding, timing, and subscriptions")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    const auto *schema = registry.ts(registry.register_scalar<std::int32_t>("int32"));
    TSOutput first{schema};
    TSOutput second{schema};
    TSInput input{TSInputBuilderFactory::checked_builder_for(*schema, TSEndpointSchema::peered(schema))};
    RecordingNotifier notifier;
    auto in = input.view(&notifier, MIN_ST);

    REQUIRE(first.binding() == nullptr);
    REQUIRE(input.binding() == nullptr);
    REQUIRE(first.schema() == schema);
    REQUIRE(first.data_view().schema() == schema);
    REQUIRE(first.view(MIN_ST).schema() == schema);
    REQUIRE(first.type_ref().record() != input.type_ref().record());
    REQUIRE(in.is_bindable());
    REQUIRE_FALSE(in.bound());
    in.bind_output(first.view(MIN_ST));
    in.make_active();
    REQUIRE(in.bound());
    REQUIRE(first.data_view().observer_count() == 2);

    Value one{std::int32_t{1}};
    {
        auto mutation = first.begin_mutation(MIN_ST);
        REQUIRE(mutation.copy_value_from(one.view()));
    }
    REQUIRE(in.value().checked_as<std::int32_t>() == 1);
    REQUIRE(in.modified());
    REQUIRE(in.delta_value().checked_as<std::int32_t>() == 1);
    REQUIRE(notifier.notifications == std::vector<DateTime>{MIN_ST});

    in.bind_output(second.view(MIN_ST + TimeDelta{1}));
    REQUIRE(first.data_view().observer_count() == 0);
    REQUIRE(second.data_view().observer_count() == 2);
    in.unbind_output();
    REQUIRE(second.data_view().observer_count() == 0);
    REQUIRE_FALSE(in.bound());
}

TEST_CASE("scalar output invalidation detaches passive inputs and supports rebind")
{
    using namespace hgraph;
    const auto *schema = TypeRegistry::instance().ts(
        TypeRegistry::instance().register_scalar<std::int32_t>("int32"));
    TSInput input = scalar_input(schema);
    auto in = input.view(nullptr, MIN_ST);

    std::optional<TSOutput> first{std::in_place, schema};
    in.bind_output(first->view(MIN_ST));
    REQUIRE(in.bound());
    REQUIRE(first->data_view().observer_count() == 1);

    first.reset();
    REQUIRE_FALSE(in.bound());
    REQUIRE_FALSE(in.valid());

    TSOutput replacement{schema};
    Value value{std::int32_t{42}};
    {
        auto mutation = replacement.begin_mutation(MIN_ST + TimeDelta{1});
        REQUIRE(mutation.copy_value_from(value.view()));
    }
    in.bind_output(replacement.view(MIN_ST + TimeDelta{1}));
    REQUIRE(in.bound());
    REQUIRE(in.valid());
    REQUIRE(in.value().checked_as<std::int32_t>() == 42);
    REQUIRE(replacement.data_view().observer_count() == 1);
}

TEST_CASE("scalar output invalidation preserves active topology without scheduling")
{
    using namespace hgraph;
    const auto *schema = TypeRegistry::instance().ts(
        TypeRegistry::instance().register_scalar<std::int32_t>("int32"));
    TSInput input = scalar_input(schema);
    RecordingNotifier scheduling;
    auto in = input.view(&scheduling, MIN_ST);

    auto first = std::make_unique<TSOutput>(schema);
    in.bind_output(first->view(MIN_ST));
    in.make_active();
    REQUIRE(in.active());
    REQUIRE(first->data_view().observer_count() == 2);

    first.reset();
    REQUIRE_FALSE(in.bound());
    REQUIRE(in.active());
    REQUIRE(scheduling.notifications.empty());

    TSOutput replacement{schema};
    Value one{std::int32_t{1}};
    Value two{std::int32_t{2}};
    {
        auto mutation = replacement.begin_mutation(MIN_ST + TimeDelta{1});
        REQUIRE(mutation.copy_value_from(one.view()));
    }
    in.bind_output(replacement.view(MIN_ST + TimeDelta{1}));
    REQUIRE(in.active());
    REQUIRE(replacement.data_view().observer_count() == 2);

    scheduling.notifications.clear();
    {
        auto mutation = replacement.begin_mutation(MIN_ST + TimeDelta{2});
        REQUIRE(mutation.copy_value_from(two.view()));
    }
    REQUIRE(scheduling.notifications == std::vector<DateTime>{MIN_ST + TimeDelta{2}});

    in.make_passive();
    REQUIRE_FALSE(in.active());
    REQUIRE(replacement.data_view().observer_count() == 1);
    scheduling.notifications.clear();
    {
        auto mutation = replacement.begin_mutation(MIN_ST + TimeDelta{3});
        REQUIRE(mutation.copy_value_from(one.view()));
    }
    REQUIRE(scheduling.notifications.empty());
}

TEST_CASE("root output invalidation detaches direct inputs for migrated and legacy shapes")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const std::array schemas{
        registry.signal(),
        registry.tss(integer),
        registry.tsl(ts, 2),
        registry.tsw(integer, 3, 1),
    };

    for (const auto *schema : schemas)
    {
        for (const bool active : {false, true})
        {
            CAPTURE(static_cast<int>(schema->kind), active);
            TSInput input{
                TSInputBuilderFactory::checked_builder_for(*schema, TSEndpointSchema::peered(schema))};
            RecordingNotifier scheduling;
            auto in = input.view(&scheduling, MIN_ST);
            std::optional<TSOutput> output{std::in_place, schema};
            in.bind_output(output->view(MIN_ST));
            if (active) { in.make_active(); }

            REQUIRE(in.bound());
            REQUIRE(in.active() == active);
            REQUIRE(output->data_view().observer_count() == (active ? 2 : 1));
            scheduling.notifications.clear();

            output.reset();
            REQUIRE_FALSE(in.bound());
            REQUIRE(in.active() == active);
            REQUIRE(scheduling.notifications.empty());
        }
    }
}

TEST_CASE("SIGNAL inputs release legacy root outputs before input teardown")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const auto *signal = registry.signal();
    const std::array legacy_roots{
        registry.tss(integer),
        registry.tsl(ts, 2),
        registry.tsw(integer, 3, 1),
    };

    for (const auto *root_schema : legacy_roots)
    {
        for (const bool active : {false, true})
        {
            CAPTURE(static_cast<int>(root_schema->kind), active);
            TSInput input{
                TSInputBuilderFactory::checked_builder_for(*signal, TSEndpointSchema::peered(signal))};
            RecordingNotifier scheduling;
            auto in = input.view(&scheduling, MIN_ST);
            std::optional<TSOutput> output{std::in_place, root_schema};
            in.bind_output(output->view(MIN_ST));
            if (active) { in.make_active(); }

            REQUIRE(in.bound());
            REQUIRE(in.bound_output().handle().bound());
            REQUIRE(in.active() == active);
            REQUIRE(output->data_view().observer_count() == (active ? 2 : 1));
            scheduling.notifications.clear();

            output.reset();
            REQUIRE_FALSE(in.bound());
            REQUIRE_FALSE(in.bound_output().handle().bound());
            REQUIRE(in.active() == active);
            REQUIRE(scheduling.notifications.empty());
        }
    }
}

TEST_CASE("root invalidation clears borrowed target state before slot callbacks")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    const auto *schema = registry.tss(registry.register_scalar<std::int32_t>("int32"));
    TSOutput forwarding{TSEndpointSchema::peered(schema)};
    ForwardingClearRecorder observer;
    observer.forwarding = &forwarding;
    observer.evaluation_time = MIN_ST;
    std::optional<TSOutput> producer{std::in_place, schema};
    auto forwarding_view = forwarding.view(MIN_ST);
    forwarding_view.bind_forwarding_target(producer->view(MIN_ST));
    auto forwarding_data = forwarding_view.data_view().borrowed_ref();
    auto forwarding_set = forwarding_data.as_set();
    forwarding_set.subscribe_slot_observer(&observer);

    producer.reset();
    REQUIRE(observer.clears == 1);
    REQUIRE(observer.saw_empty_target);
    REQUIRE_FALSE(forwarding_view.forwarding_target().bound());

    forwarding_set.unsubscribe_slot_observer(&observer);
}

TEST_CASE("scalar input teardown and move keep producer subscriptions coherent")
{
    using namespace hgraph;
    const auto *schema = TypeRegistry::instance().ts(
        TypeRegistry::instance().register_scalar<std::int32_t>("int32"));
    TSOutput output{schema};
    RecordingNotifier scheduling;

    {
        TSInput input = scalar_input(schema);
        auto in = input.view(&scheduling, MIN_ST);
        in.bind_output(output.view(MIN_ST));
        in.make_active();
        REQUIRE(output.data_view().observer_count() == 2);

        TSInput moved{std::move(input)};
        auto moved_view = moved.view(&scheduling, MIN_ST);
        REQUIRE(moved_view.bound());
        REQUIRE(moved_view.active());
        REQUIRE(output.data_view().observer_count() == 2);
    }

    REQUIRE(output.data_view().observer_count() == 0);
    Value value{std::int32_t{7}};
    {
        auto mutation = output.begin_mutation(MIN_ST);
        REQUIRE(mutation.copy_value_from(value.view()));
    }
}

TEST_CASE("scalar output copy and move replacement invalidate published bindings")
{
    using namespace hgraph;
    const auto *schema = TypeRegistry::instance().ts(
        TypeRegistry::instance().register_scalar<std::int32_t>("int32"));
    Value one{std::int32_t{1}};
    Value two{std::int32_t{2}};

    TSOutput source{schema};
    {
        auto mutation = source.begin_mutation(MIN_ST);
        REQUIRE(mutation.copy_value_from(one.view()));
    }
    TSInput source_input = scalar_input(schema);
    auto source_view = source_input.view(nullptr, MIN_ST);
    source_view.bind_output(source.view(MIN_ST));

    TSOutput moved{std::move(source)};
    REQUIRE_FALSE(source_view.bound());
    REQUIRE(moved.view(MIN_ST).valid());
    REQUIRE(moved.data_view().observer_count() == 0);

    TSOutput destination{schema};
    TSInput destination_input = scalar_input(schema);
    auto destination_view = destination_input.view(nullptr, MIN_ST);
    destination_view.bind_output(destination.view(MIN_ST));

    TSOutput replacement{schema};
    {
        auto mutation = replacement.begin_mutation(MIN_ST + TimeDelta{1});
        REQUIRE(mutation.copy_value_from(two.view()));
    }
    TSInput replacement_input = scalar_input(schema);
    auto replacement_view = replacement_input.view(nullptr, MIN_ST + TimeDelta{1});
    replacement_view.bind_output(replacement.view(MIN_ST + TimeDelta{1}));

    destination = std::move(replacement);
    REQUIRE_FALSE(destination_view.bound());
    REQUIRE_FALSE(replacement_view.bound());
    REQUIRE(destination.view(MIN_ST + TimeDelta{1}).value().checked_as<std::int32_t>() == 2);
    REQUIRE(destination.data_view().observer_count() == 0);

    TSOutput copy_source{schema};
    {
        auto mutation = copy_source.begin_mutation(MIN_ST + TimeDelta{2});
        REQUIRE(mutation.copy_value_from(one.view()));
    }
    TSInput copy_source_input = scalar_input(schema);
    auto copy_source_view = copy_source_input.view(nullptr, MIN_ST + TimeDelta{2});
    copy_source_view.bind_output(copy_source.view(MIN_ST + TimeDelta{2}));
    TSInput copied_destination_input = scalar_input(schema);
    auto copied_destination_view = copied_destination_input.view(nullptr, MIN_ST + TimeDelta{2});
    copied_destination_view.bind_output(destination.view(MIN_ST + TimeDelta{2}));

    destination = copy_source;
    REQUIRE_FALSE(copied_destination_view.bound());
    REQUIRE(copy_source_view.bound());
    REQUIRE(copy_source.data_view().observer_count() == 1);
    REQUIRE(destination.data_view().observer_count() == 0);
}

TEST_CASE("scalar observer invalidation is detached and reentrant")
{
    using namespace hgraph;
    TSDataTracking tracking;
    ReentrantInvalidationRecorder first;
    InvalidationRecorder second;
    tracking.observers.subscribe(&first);
    tracking.observers.subscribe(&second);

    tracking.observers.invalidate(&tracking);
    REQUIRE(first.invalidations == 1);
    REQUIRE(second.invalidations == 1);
    REQUIRE(second.invalidated_source == &tracking);
    REQUIRE(tracking.observers.empty());
}

TEST_CASE("scalar reverse member teardown and normal graph stop are safe")
{
    using namespace hgraph;
    const auto *schema = TypeRegistry::instance().ts(
        TypeRegistry::instance().register_scalar<std::int32_t>("int32"));
    struct InputBeforeOutput
    {
        explicit InputBeforeOutput(const TSValueTypeMetaData *type)
            : input{scalar_input(type)}, output{type}
        {
            input.view(nullptr, MIN_ST).bind_output(output.view(MIN_ST));
        }
        TSInput input;
        TSOutput output;
    };
    { InputBeforeOutput reverse{schema}; }

    GraphBuilder builder;
    builder.add_node(NodeBuilder{}.implementation<LifetimeSource>());
    builder.add_node(NodeBuilder{}.implementation<LifetimeSink>());
    builder.add_edge(GraphEdge{.source_node = 0, .target_node = 1, .target_path = {0}});
    testing::MockRootGraph graph{builder};
    auto view = graph.graph();
    view.start(MIN_ST);
    view.evaluate(MIN_ST);
    view.stop();
    REQUIRE_FALSE(view.started());
}

TEST_CASE("scalar SIGNAL role storage preserves typed-null and tick delta semantics")
{
    using namespace hgraph;
    const auto *signal = TypeRegistry::instance().signal();
    TSOutput output{signal};
    REQUIRE(output.type_ref().schema() == signal);
    REQUIRE_FALSE(output.view(MIN_ST).valid());
    REQUIRE_FALSE(output.view(MIN_ST).delta_value().has_value());

    Value tick{true};
    {
        auto mutation = output.begin_mutation(MIN_ST);
        REQUIRE(mutation.copy_value_from(tick.view()));
    }
    REQUIRE(output.view(MIN_ST).valid());
    REQUIRE(output.view(MIN_ST).delta_value().checked_as<bool>());
    REQUIRE_FALSE(output.view(MIN_ST + TimeDelta{1}).delta_value().has_value());
}

TEST_CASE("scalar role caches reset after owners are destroyed and reseed cleanly")
{
    using namespace hgraph;
    {
        auto &registry = TypeRegistry::instance();
        const auto *schema = registry.ts(registry.register_scalar<std::int32_t>("int32"));
        TSOutput output{schema};
        TSInput input{TSInputBuilderFactory::checked_builder_for(*schema, TSEndpointSchema::peered(schema))};
        input.view(nullptr, MIN_ST).bind_output(output.view(MIN_ST));
    }

    reset_all_registries();

    auto &registry = TypeRegistry::instance();
    const auto *schema = registry.ts(registry.register_scalar<std::int32_t>("int32"));
    const auto data = TSDataPlanFactory::instance().data_type_for(schema);
    const auto output = TSDataPlanFactory::instance().output_type_for(schema);
    REQUIRE(schema->header.valid());
    REQUIRE(data.valid());
    REQUIRE(output.valid());
    REQUIRE(data.schema() == schema);
    REQUIRE(output.schema() == schema);
}
