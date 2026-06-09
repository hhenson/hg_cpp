#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/utils/memory_utils.h>

#include <cstdint>
#include <string>

TEST_CASE("TypeRegistry::register_scalar returns canonical metadata")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *a        = registry.register_scalar<std::int32_t>("int32");
    const auto *b        = registry.register_scalar<std::int32_t>("int32");
    REQUIRE(a == b);
    REQUIRE(a != nullptr);
    REQUIRE(a->kind == ValueTypeKind::Atomic);
}

TEST_CASE("TypeRegistry::register_scalar populates the value_type alias")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *meta     = registry.register_scalar<double>("double");
    REQUIRE(registry.value_type("double") == meta);
}

TEST_CASE("TypeRegistry aliases reject conflicting schema bindings")
{
    using namespace hgraph;
    auto       &registry     = TypeRegistry::instance();
    const auto *standard_int = registry.value_type("int");
    REQUIRE(standard_int == registry.scalar_binding<std::int64_t>()->type_meta);

    const auto *int32_meta = registry.register_scalar<std::int32_t>("int32");
    REQUIRE(int32_meta != standard_int);
    REQUIRE_THROWS_AS(registry.register_scalar<std::int32_t>("int"), std::invalid_argument);
    REQUIRE_THROWS_AS(registry.register_value_type_alias("int", int32_meta), std::invalid_argument);
    REQUIRE(registry.value_type("int") == standard_int);

    const auto *standard_ts_int = registry.time_series_type("TS[int]");
    REQUIRE(standard_ts_int == registry.ts(standard_int));
    REQUIRE_THROWS_AS(registry.register_time_series_type_alias("TS[int]", registry.ts(int32_meta)),
                      std::invalid_argument);
    REQUIRE(registry.time_series_type("TS[int]") == standard_ts_int);
}

TEST_CASE("TypeRegistry::register_scalar wires the canonical plan into ValuePlanFactory")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = ValuePlanFactory::instance();
    const auto *meta     = registry.register_scalar<long long>("long long");
    REQUIRE(factory.find(meta) == &MemoryUtils::plan_for<long long>());
}

TEST_CASE("TypeRegistry: different scalar types yield different metadata")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta  = registry.register_scalar<std::int32_t>("int32");
    const auto *long_meta = registry.register_scalar<long>("long");
    REQUIRE(int_meta != long_meta);
}

TEST_CASE("TypeRegistry::tuple interns by component identity and is order-sensitive")
{
    using namespace hgraph;
    auto       &registry   = TypeRegistry::instance();
    const auto *int_meta   = registry.register_scalar<std::int32_t>("int32");
    const auto *float_meta = registry.register_scalar<float>("float32");

    const auto *t_if = registry.tuple({int_meta, float_meta});
    const auto *t_if_again = registry.tuple({int_meta, float_meta});
    const auto *t_fi = registry.tuple({float_meta, int_meta});

    REQUIRE(t_if == t_if_again);
    REQUIRE(t_if != t_fi);
    REQUIRE(t_if->kind == ValueTypeKind::Tuple);
    REQUIRE(t_if->field_count == 2);
    REQUIRE(t_if->fields[0].type == int_meta);
    REQUIRE(t_if->fields[1].type == float_meta);
    REQUIRE(t_if->fields[0].name == nullptr);
    REQUIRE(t_if->fields[1].name == nullptr);
}

TEST_CASE("TypeRegistry::bundle interns by structural identity and registers the alias")
{
    using namespace hgraph;
    auto       &registry   = TypeRegistry::instance();
    const auto *int_meta   = registry.register_scalar<std::int32_t>("int32");
    const auto *str_meta   = registry.register_scalar<std::string>("string");
    const auto *bundle_meta = registry.bundle("TestBundleA", {{"x", int_meta}, {"y", str_meta}});

    REQUIRE(bundle_meta != nullptr);
    REQUIRE(bundle_meta->kind == ValueTypeKind::Bundle);
    REQUIRE(bundle_meta->field_count == 2);
    REQUIRE(std::string(bundle_meta->fields[0].name) == "x");
    REQUIRE(std::string(bundle_meta->fields[1].name) == "y");

    REQUIRE(registry.bundle("TestBundleA", {{"x", int_meta}, {"y", str_meta}}) == bundle_meta);
    REQUIRE(registry.value_type("TestBundleA") == bundle_meta);
}

TEST_CASE("TypeRegistry: un_named_bundle and bundle distinguish structural vs nominal identity")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *str_meta = registry.register_scalar<std::string>("string");

    const std::vector<std::pair<std::string, const ValueTypeMetaData *>> fields{
        {"x", int_meta}, {"y", str_meta}};

    // Two un_named_bundle calls with the same field list return the same canonical pointer.
    const auto *u1 = registry.un_named_bundle(fields);
    const auto *u2 = registry.un_named_bundle(fields);
    REQUIRE(u1 == u2);
    REQUIRE(u1->is_un_named_bundle());
    REQUIRE_FALSE(u1->is_named_bundle());
    REQUIRE(u1->display_name == nullptr);
    REQUIRE(u1->wrapped_un_named == nullptr);

    // bundle(name, fields) wraps an un_named_bundle; same name + same fields → same pointer.
    const auto *named_a1 = registry.bundle("BundleNamedA", fields);
    const auto *named_a2 = registry.bundle("BundleNamedA", fields);
    REQUIRE(named_a1 == named_a2);
    REQUIRE(named_a1->is_named_bundle());
    REQUIRE_FALSE(named_a1->is_un_named_bundle());
    REQUIRE(named_a1->wrapped_un_named == u1);  // wraps the un-named twin
    REQUIRE(std::string(named_a1->name()) == std::string("BundleNamedA"));
    REQUIRE(named_a1->fields == u1->fields);     // shares the field array
    REQUIRE(named_a1->field_count == u1->field_count);

    // Different names with the same fields are DISTINCT named schemas (nominal identity).
    const auto *named_b = registry.bundle("BundleNamedB", fields);
    REQUIRE(named_b != named_a1);
    REQUIRE(named_b->wrapped_un_named == u1);    // both wrap the same un-named

    // Named ≠ un-named, even though they share fields.
    REQUIRE(named_a1 != u1);

    // bundle(...) requires a non-empty name.
    REQUIRE_THROWS_AS(registry.bundle("", fields), std::invalid_argument);

    // named_bundle() lookup: returns the named meta, or nullptr when the
    // name doesn't exist or doesn't resolve to a named bundle.
    REQUIRE(registry.named_bundle("BundleNamedA") == named_a1);
    REQUIRE(registry.named_bundle("BundleNamedB") == named_b);
    REQUIRE(registry.named_bundle("DoesNotExist") == nullptr);
    // The atomic "int" registered earlier shares the value-name space but is not a named bundle.
    REQUIRE(registry.named_bundle("int") == nullptr);

    // Bundle name namespace is unique: same name + same fields is idempotent,
    // same name + different fields is rejected.
    const std::vector<std::pair<std::string, const ValueTypeMetaData *>> different_fields{
        {"x", int_meta}, {"z", int_meta}};
    REQUIRE_NOTHROW(registry.bundle("BundleNamedA", fields));            // same shape -> OK
    REQUIRE_THROWS_AS(registry.bundle("BundleNamedA", different_fields), // different shape -> conflict
                      std::invalid_argument);
}

TEST_CASE("TypeRegistry::list distinguishes fixed and dynamic forms")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");

    const auto *fixed   = registry.list(int_meta, 4, false);
    const auto *dynamic = registry.list(int_meta, 0, false);
    const auto *fixed2  = registry.list(int_meta, 4, false);

    REQUIRE(fixed != dynamic);
    REQUIRE(fixed == fixed2);
    REQUIRE(fixed->is_fixed_size());
    REQUIRE(fixed->fixed_size == 4);
    REQUIRE(!dynamic->is_fixed_size());
}

TEST_CASE("TypeRegistry: set, map, cyclic_buffer and queue intern correctly")
{
    using namespace hgraph;
    auto       &registry  = TypeRegistry::instance();
    const auto *int_meta  = registry.register_scalar<std::int32_t>("int32");

    const auto *s = registry.set(int_meta);
    REQUIRE(s->kind == ValueTypeKind::Set);
    REQUIRE(s == registry.set(int_meta));
    REQUIRE(s->element_type == int_meta);

    const auto *m = registry.map(int_meta, int_meta);
    REQUIRE(m->kind == ValueTypeKind::Map);
    REQUIRE(m == registry.map(int_meta, int_meta));
    REQUIRE(m->key_type == int_meta);
    REQUIRE(m->element_type == int_meta);

    const auto *cb = registry.cyclic_buffer(int_meta, 8);
    REQUIRE(cb->kind == ValueTypeKind::CyclicBuffer);
    REQUIRE(cb->fixed_size == 8);
    REQUIRE(cb->is_hashable());
    REQUIRE(cb->is_equatable());
    REQUIRE(cb->is_comparable());
    REQUIRE(cb->is_buffer_compatible());
    REQUIRE(cb == registry.cyclic_buffer(int_meta, 8));

    const auto *q = registry.queue(int_meta, 16);
    REQUIRE(q->kind == ValueTypeKind::Queue);
    REQUIRE(q->fixed_size == 16);
    REQUIRE(q->is_hashable());
    REQUIRE(q->is_equatable());
    REQUIRE(q->is_comparable());
    REQUIRE(q->is_buffer_compatible());
    REQUIRE(q == registry.queue(int_meta, 16));
}

TEST_CASE("TypeRegistry::ts / tss / tsd / tsl / tsw intern correctly")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");

    const auto *ts_int = registry.ts(int_meta);
    REQUIRE(ts_int->kind == TSTypeKind::TS);
    REQUIRE(ts_int == registry.ts(int_meta));

    const auto *tss = registry.tss(int_meta);
    REQUIRE(tss->kind == TSTypeKind::TSS);
    REQUIRE(tss == registry.tss(int_meta));

    const auto *tsd = registry.tsd(int_meta, ts_int);
    REQUIRE(tsd->kind == TSTypeKind::TSD);
    REQUIRE(tsd->key_type() == int_meta);
    REQUIRE(tsd->element_ts() == ts_int);

    const auto *tsl_fixed = registry.tsl(ts_int, 4);
    REQUIRE(tsl_fixed->kind == TSTypeKind::TSL);
    REQUIRE(tsl_fixed->fixed_size() == 4);
    REQUIRE(tsl_fixed->element_ts() == ts_int);

    const auto *tsl_dynamic = registry.tsl(ts_int, 0);
    REQUIRE(tsl_dynamic != tsl_fixed);
    REQUIRE(tsl_dynamic->fixed_size() == 0);

    const auto *tsw_tick = registry.tsw(int_meta, 10, 5);
    REQUIRE(tsw_tick->kind == TSTypeKind::TSW);
    REQUIRE(!tsw_tick->is_duration_based());
    REQUIRE(tsw_tick->period() == 10);
    REQUIRE(tsw_tick->min_period() == 5);

    const auto *tsw_dur = registry.tsw_duration(
        int_meta, TimeDelta{1000}, TimeDelta{500});
    REQUIRE(tsw_dur->kind == TSTypeKind::TSW);
    REQUIRE(tsw_dur->is_duration_based());
    REQUIRE(tsw_dur->time_range() == TimeDelta{1000});
    REQUIRE(tsw_dur->min_time_range() == TimeDelta{500});
    REQUIRE(tsw_tick != tsw_dur);
}

TEST_CASE("TypeRegistry::tsb stores fields and registers the alias")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    const auto *tsb = registry.tsb("TestTSBundleA", {{"a", ts_int}, {"b", ts_int}});
    REQUIRE(tsb->kind == TSTypeKind::TSB);
    REQUIRE(tsb->field_count() == 2);
    REQUIRE(std::string(tsb->fields()[0].name) == "a");
    REQUIRE(std::string(tsb->fields()[1].name) == "b");
    REQUIRE(tsb->fields()[0].type == ts_int);

    REQUIRE(registry.time_series_type("TestTSBundleA") == tsb);
    REQUIRE(registry.tsb("TestTSBundleA", {{"a", ts_int}, {"b", ts_int}}) == tsb);
}

TEST_CASE("TypeRegistry: un_named_tsb and tsb distinguish structural vs nominal identity")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    const std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields{
        {"a", ts_int}, {"b", ts_int}};

    // Same field list -> same canonical un-named TSB.
    const auto *u1 = registry.un_named_tsb(fields);
    const auto *u2 = registry.un_named_tsb(fields);
    REQUIRE(u1 == u2);
    REQUIRE(u1->is_un_named_tsb());
    REQUIRE_FALSE(u1->is_named_tsb());
    REQUIRE(u1->display_name == nullptr);
    REQUIRE(u1->wrapped_un_named_tsb() == nullptr);
    // The un-named TSB's value-side bundle is the matching un-named Bundle.
    REQUIRE(u1->value_type != nullptr);
    REQUIRE(u1->value_type->is_un_named_bundle());

    // tsb(name, fields) wraps the un-named TSB.
    const auto *named_x1 = registry.tsb("TSBNamedX", fields);
    const auto *named_x2 = registry.tsb("TSBNamedX", fields);
    REQUIRE(named_x1 == named_x2);
    REQUIRE(named_x1->is_named_tsb());
    REQUIRE(named_x1->wrapped_un_named_tsb() == u1);
    REQUIRE(std::string(named_x1->name()) == std::string("TSBNamedX"));
    // Field array shared with the un-named twin.
    REQUIRE(named_x1->fields() == u1->fields());
    REQUIRE(named_x1->field_count() == u1->field_count());
    // Value-side bundle is the matching named Bundle (nominal identity flows through).
    REQUIRE(named_x1->value_type != nullptr);
    REQUIRE(named_x1->value_type->is_named_bundle());

    // Different names with the same fields are distinct named TSBs.
    const auto *named_y = registry.tsb("TSBNamedY", fields);
    REQUIRE(named_y != named_x1);
    REQUIRE(named_y->wrapped_un_named_tsb() == u1);  // share the un-named twin
    REQUIRE(named_y->value_type != named_x1->value_type);

    // Named TSB ≠ un-named TSB.
    REQUIRE(named_x1 != u1);

    // tsb(...) requires a non-empty name.
    REQUIRE_THROWS_AS(registry.tsb("", fields), std::invalid_argument);

    // named_tsb() lookup.
    REQUIRE(registry.named_tsb("TSBNamedX") == named_x1);
    REQUIRE(registry.named_tsb("TSBNamedY") == named_y);
    REQUIRE(registry.named_tsb("DoesNotExist") == nullptr);
    // SIGNAL is registered in the TS name space but isn't a named TSB.
    (void)registry.signal();
    REQUIRE(registry.named_tsb("SIGNAL") == nullptr);

    // TSB name namespace is unique: same name + same fields is idempotent,
    // same name + different fields is rejected.
    const std::vector<std::pair<std::string, const TSValueTypeMetaData *>> different_fields{
        {"a", ts_int}, {"c", ts_int}};
    REQUIRE_NOTHROW(registry.tsb("TSBNamedX", fields));            // same shape -> OK
    REQUIRE_THROWS_AS(registry.tsb("TSBNamedX", different_fields), // different shape -> conflict
                      std::invalid_argument);
}

TEST_CASE("TypeRegistry::signal returns a single canonical instance")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *s1       = registry.signal();
    const auto *s2       = registry.signal();
    REQUIRE(s1 == s2);
    REQUIRE(s1->kind == TSTypeKind::SIGNAL);
    REQUIRE(registry.time_series_type("SIGNAL") == s1);
}

TEST_CASE("TypeRegistry::ref creates the TimeSeriesReference singleton")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    const auto *r1 = registry.ref(ts_int);
    const auto *r2 = registry.ref(ts_int);
    REQUIRE(r1 == r2);
    REQUIRE(r1->kind == TSTypeKind::REF);
    REQUIRE(r1->referenced_ts() == ts_int);
    REQUIRE(registry.ref(r1) == r1);
}

TEST_CASE("TypeRegistry::contains_ref recurses through composite TS kinds")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *ref_int  = registry.ref(ts_int);

    REQUIRE(!TypeRegistry::contains_ref(ts_int));
    REQUIRE(TypeRegistry::contains_ref(ref_int));

    const auto *bundle_with_ref    = registry.tsb("TestRefBundleA", {{"r", ref_int}});
    const auto *bundle_without_ref = registry.tsb("TestRefBundleB", {{"v", ts_int}});
    REQUIRE(TypeRegistry::contains_ref(bundle_with_ref));
    REQUIRE(!TypeRegistry::contains_ref(bundle_without_ref));

    const auto *list_of_refs = registry.tsl(ref_int);
    REQUIRE(TypeRegistry::contains_ref(list_of_refs));

    const auto *dict_with_ref = registry.tsd(int_meta, ref_int);
    REQUIRE(TypeRegistry::contains_ref(dict_with_ref));
}

TEST_CASE("TypeRegistry::dereference unwraps refs and recurses into containers")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *ref_int  = registry.ref(ts_int);

    REQUIRE(registry.dereference(ref_int) == ts_int);
    REQUIRE(registry.dereference(ts_int) == ts_int);

    const auto *list_of_refs = registry.tsl(ref_int);
    const auto *list_deref   = registry.dereference(list_of_refs);
    REQUIRE(list_deref != list_of_refs);
    REQUIRE(list_deref->kind == TSTypeKind::TSL);
    REQUIRE(list_deref->element_ts() == ts_int);

    const auto *list_of_ts  = registry.tsl(ts_int);
    REQUIRE(registry.dereference(list_of_ts) == list_of_ts);
}

TEST_CASE("TypeRegistry::synthetic_atomic interns by name")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();

    // Trigger ref() to indirectly create the synthetic TimeSeriesReference atomic.
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    (void)registry.ref(ts_int);

    const auto *synthetic = registry.value_type("TimeSeriesReference");
    REQUIRE(synthetic != nullptr);
    REQUIRE(synthetic->kind == ValueTypeKind::Atomic);
    REQUIRE(synthetic->is_hashable());
    REQUIRE(synthetic->is_equatable());
}

TEST_CASE("TypeRegistry: value_type and time_series_type return null for unknown names")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    REQUIRE(registry.value_type("nonexistent_value_type_xyzzy") == nullptr);
    REQUIRE(registry.time_series_type("nonexistent_ts_type_xyzzy") == nullptr);
}
