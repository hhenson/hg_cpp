#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series_reference.h>
#include <hgraph/types/value/value.h>
#include <hgraph/util/date_time.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

namespace
{
    template <typename Range>
    std::size_t range_count(const Range &range)
    {
        std::size_t count = 0;
        for (const auto _ : range)
        {
            (void)_;
            ++count;
        }
        return count;
    }

    struct RecordingNotifiable : hgraph::Notifiable
    {
        std::vector<hgraph::DateTime> notified{};

        void notify(hgraph::DateTime modified_time) override
        {
            notified.push_back(modified_time);
        }
    };

    struct SelfUnsubscribingNotifiable : RecordingNotifiable
    {
        hgraph::TSDataView observed{};

        void notify(hgraph::DateTime modified_time) override
        {
            RecordingNotifiable::notify(modified_time);
            observed.unsubscribe(this);
        }
    };

    struct RemovingNotifiable : RecordingNotifiable
    {
        hgraph::TSDataView observed{};
        hgraph::Notifiable *target{nullptr};

        void notify(hgraph::DateTime modified_time) override
        {
            RecordingNotifiable::notify(modified_time);
            observed.unsubscribe(target);
        }
    };

    struct AddingNotifiable : RecordingNotifiable
    {
        hgraph::TSDataView observed{};
        hgraph::Notifiable *target{nullptr};
        bool added{false};

        void notify(hgraph::DateTime modified_time) override
        {
            RecordingNotifiable::notify(modified_time);
            if (!added)
            {
                observed.subscribe(target);
                added = true;
            }
        }
    };

    struct ReplacingNotifiable : RecordingNotifiable
    {
        hgraph::TSDataView observed{};
        hgraph::Notifiable *removed{nullptr};
        hgraph::Notifiable *replacement{nullptr};

        void notify(hgraph::DateTime modified_time) override
        {
            RecordingNotifiable::notify(modified_time);
            observed.unsubscribe(removed);
            observed.unsubscribe(this);
            observed.subscribe(replacement);
        }
    };
}

TEST_CASE("TSOutput owns root TSData and exposes TS validity")
{
    using namespace hgraph;

    static_assert(!std::is_copy_constructible_v<TSDataView>);
    static_assert(!std::is_copy_assignable_v<TSDataView>);
    static_assert(std::is_move_constructible_v<TSDataView>);
    static_assert(!std::is_copy_constructible_v<TSDataMutationView>);
    static_assert(!std::is_copy_constructible_v<TSSDataView>);
    static_assert(!std::is_copy_constructible_v<TSDDataView>);
    static_assert(!std::is_copy_constructible_v<TSBDataView>);
    static_assert(!std::is_copy_constructible_v<TSLDataView>);
    static_assert(!std::is_copy_constructible_v<TSWDataView>);
    static_assert(!std::is_copy_constructible_v<TSOutputView>);
    static_assert(!std::is_copy_assignable_v<TSOutputView>);
    static_assert(!std::is_copy_constructible_v<TSSOutputView>);
    static_assert(!std::is_copy_constructible_v<TSDOutputView>);
    static_assert(!std::is_copy_constructible_v<TSBOutputView>);
    static_assert(!std::is_copy_constructible_v<TSLOutputView>);
    static_assert(!std::is_copy_constructible_v<TSWOutputView>);
    static_assert(std::is_copy_constructible_v<TSOutputHandle>);

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    TSOutput output{*ts_int};
    REQUIRE(output.has_value());
    REQUIRE(output.schema() == ts_int);

    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};

    auto initial = output.view(t1);
    REQUIRE(initial.binding() != nullptr);
    REQUIRE(initial.bound());
    REQUIRE(initial.evaluation_time() == t1);
    REQUIRE_FALSE(initial.valid());
    REQUIRE_FALSE(initial.all_valid());
    REQUIRE_FALSE(initial.modified());
    REQUIRE(initial.last_modified_time() == MIN_DT);
    REQUIRE(initial.value().checked_as<std::int32_t>() == 0);
    REQUIRE_FALSE(initial.delta_value().has_value());

    Value forty_two{42};
    {
        auto mutation = output.begin_mutation(t1);
        REQUIRE(mutation.current_mutation_time() == t1);
        REQUIRE(mutation.copy_value_from(forty_two.view()));
        REQUIRE(mutation.modified());
    }

    auto modified = output.view(t1);
    REQUIRE(modified.valid());
    REQUIRE(modified.all_valid());
    REQUIRE(modified.modified());
    REQUIRE_FALSE(output.view(t2).modified());
    REQUIRE(modified.last_modified_time() == t1);
    REQUIRE(modified.value().checked_as<std::int32_t>() == 42);
    REQUIRE(modified.delta_value().checked_as<std::int32_t>() == 42);
    REQUIRE_FALSE(output.view(t2).delta_value().has_value());
}

TEST_CASE("TSOutputHandle stores output identity without evaluation time")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    TSOutput output{*ts_int};
    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};

    Value value{7};
    {
        auto mutation = output.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(value.view()));
    }

    const auto view = output.view(t1);
    auto       handle = view.handle();
    REQUIRE(handle.bound());
    REQUIRE(handle.output() == &output);
    REQUIRE(handle.binding() == view.binding());
    REQUIRE(handle.schema() == view.schema());

    const TSOutputHandle from_view{view};
    REQUIRE(handle.same_as(from_view));

    const auto replay_at_t1 = handle.view(t1);
    const auto replay_at_t2 = handle.view(t2);
    REQUIRE(replay_at_t1.output() == &output);
    REQUIRE(replay_at_t1.data_view().data() == view.data_view().data());
    REQUIRE(replay_at_t1.evaluation_time() == t1);
    REQUIRE(replay_at_t1.modified());
    REQUIRE(replay_at_t2.evaluation_time() == t2);
    REQUIRE_FALSE(replay_at_t2.modified());

    handle.reset();
    REQUIRE_FALSE(handle.bound());
}

TEST_CASE("TSOutput REF stores TimeSeriesReference as value and delta")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *ref_int  = registry.ref(ts_int);

    TSOutput target{*ts_int};
    TSOutput ref_output{*ref_int};

    const auto t1 = MIN_ST + TimeDelta{1};
    const TimeSeriesReference reference{target.view(t1)};
    Value                     wrapped{reference};

    {
        auto mutation = ref_output.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(wrapped.view()));
    }

    auto view = ref_output.view(t1);
    REQUIRE(view.valid());
    REQUIRE(view.modified());

    const auto &stored = view.value().checked_as<TimeSeriesReference>();
    REQUIRE(stored.has_output());
    REQUIRE(stored.target_schema() == ts_int);
    REQUIRE(stored == reference);

    const auto &delta = view.delta_value().checked_as<TimeSeriesReference>();
    REQUIRE(delta.has_output());
    REQUIRE(delta == reference);
}

TEST_CASE("TSOutput slot deltas are reclaimed lazily on the next mutation")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *tss      = registry.tss(int_meta);

    TSOutput output{*tss};
    auto     root = output.data_view();
    auto     set = root.as_set();

    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    Value      one{1};

    // There is no eager cleanup: after adding at t1 the add delta stays physically
    // present (the data-layer accessor is raw) until the next mutation reclaims it.
    {
        auto mutation = set.begin_mutation(t1);
        REQUIRE(mutation.add(one.view()));
    }
    REQUIRE(range_count(set.added()) == 1);
    REQUIRE(set.contains(one.view()));

    // The next mutation (at t2) reclaims the t1 add delta via prepare_delta, then
    // records its own removal — no explicit cleanup call required.
    {
        auto mutation = set.begin_mutation(t2);
        REQUIRE(mutation.remove(one.view()));
    }
    REQUIRE(range_count(set.added()) == 0);
    REQUIRE(range_count(set.removed()) == 1);
    REQUIRE_FALSE(set.contains(one.view()));

    // Same-cycle add+remove cancels structurally; the next mutation reclaims it.
    const auto t3 = t2 + TimeDelta{1};
    {
        auto mutation = set.begin_mutation(t3);
        REQUIRE(mutation.add(one.view()));
        REQUIRE(mutation.remove(one.view()));
    }
    REQUIRE(output.view(t3).modified());
    REQUIRE(output.view(t3).valid());
    REQUIRE(range_count(set.removed()) == 0);
    REQUIRE_THROWS_AS(output.begin_mutation(MIN_DT), std::invalid_argument);
}

TEST_CASE("TSOutput root parent is reattached after copy and move")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    const auto t3 = t2 + TimeDelta{1};

    Value one{1};
    Value two{2};
    Value three{3};

    TSOutput source{*ts_int};
    {
        auto mutation = source.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(one.view()));
    }

    TSOutput copied{source};
    REQUIRE(copied.data_view().has_parent());
    {
        auto mutation = copied.begin_mutation(t2);
        REQUIRE(mutation.copy_value_from(two.view()));
    }
    REQUIRE(copied.view(t2).modified());

    TSOutput moved{std::move(copied)};
    REQUIRE(moved.data_view().has_parent());
    {
        auto mutation = moved.begin_mutation(t3);
        REQUIRE(mutation.copy_value_from(three.view()));
    }
    REQUIRE(moved.view(t3).modified());
}

TEST_CASE("TSOutputView all_valid recurses through fixed bundle children")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsb      = registry.tsb("TSOutputAllValidBundle", {{"a", ts_int}, {"b", ts_int}});

    TSOutput output{*tsb};

    const auto t1 = MIN_ST;
    Value      one{1};
    Value      two{2};

    auto root = output.data_view();
    auto bundle = root.as_bundle();
    auto a = bundle.field("a");
    auto b = bundle.field("b");

    {
        auto mutation = a.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(one.view()));
    }

    auto partially_valid = output.view(t1);
    REQUIRE(partially_valid.valid());
    REQUIRE_FALSE(partially_valid.all_valid());

    {
        auto mutation = b.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(two.view()));
    }

    auto fully_valid = output.view(t1);
    REQUIRE(fully_valid.valid());
    REQUIRE(fully_valid.all_valid());

    auto fully_valid_bundle = fully_valid.as_bundle();
    REQUIRE(fully_valid_bundle.field("a").value().checked_as<std::int32_t>() == 1);
    REQUIRE(fully_valid_bundle.field("b").value().checked_as<std::int32_t>() == 2);
}

TEST_CASE("TSData observers notify at the modified level and bubble to parents")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsb      = registry.tsb("TSOutputObserverBundle", {{"a", ts_int}, {"b", ts_int}});

    TSOutput output{*tsb};
    auto     root = output.data_view();
    auto     bundle = root.as_bundle();
    auto     a = bundle.field("a");
    auto     b = bundle.field("b");

    RecordingNotifiable root_observer;
    RecordingNotifiable a_observer;
    RecordingNotifiable a_second_observer;
    RecordingNotifiable b_observer;

    output.subscribe(&root_observer);
    a.subscribe(&a_observer);
    a.subscribe(&a_second_observer);
    b.subscribe(&b_observer);

    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    const auto t3 = t2 + TimeDelta{1};

    Value one{1};
    Value two{2};
    Value three{3};

    {
        auto mutation = a.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(one.view()));
    }

    CHECK(root_observer.notified == std::vector<DateTime>{t1});
    CHECK(a_observer.notified == std::vector<DateTime>{t1});
    CHECK(a_second_observer.notified == std::vector<DateTime>{t1});
    CHECK(b_observer.notified.empty());

    {
        auto mutation = a.begin_mutation(t1);
        REQUIRE_FALSE(mutation.copy_value_from(two.view()));
    }

    CHECK(root_observer.notified == std::vector<DateTime>{t1});
    CHECK(a_observer.notified == std::vector<DateTime>{t1});
    CHECK(a_second_observer.notified == std::vector<DateTime>{t1});

    {
        auto mutation = b.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(three.view()));
    }

    CHECK(root_observer.notified == std::vector<DateTime>{t1});
    CHECK(b_observer.notified == std::vector<DateTime>{t1});

    a.unsubscribe(&a_observer);
    {
        auto mutation = a.begin_mutation(t2);
        REQUIRE(mutation.copy_value_from(two.view()));
    }

    CHECK(root_observer.notified == std::vector<DateTime>{t1, t2});
    CHECK(a_observer.notified == std::vector<DateTime>{t1});
    CHECK(a_second_observer.notified == std::vector<DateTime>{t1, t2});

    a.unsubscribe(&a_second_observer);
    {
        auto mutation = a.begin_mutation(t3);
        REQUIRE(mutation.copy_value_from(three.view()));
    }

    CHECK(root_observer.notified == std::vector<DateTime>{t1, t2, t3});
    CHECK(a_second_observer.notified == std::vector<DateTime>{t1, t2});

    output.unsubscribe(&root_observer);
    b.unsubscribe(&b_observer);
}

TEST_CASE("TSData observers support reentrant subscribe and unsubscribe")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    TSOutput output{*ts_int};
    auto     observed = output.data_view();

    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    const auto t3 = t2 + TimeDelta{1};
    Value      one{1};
    Value      two{2};
    Value      three{3};

    SelfUnsubscribingNotifiable self_unsubscribing;
    RecordingNotifiable         survivor;
    self_unsubscribing.observed = observed.borrowed_ref();
    observed.subscribe(&self_unsubscribing);
    observed.subscribe(&survivor);
    REQUIRE(observed.observer_count() == 2);

    {
        auto mutation = output.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(one.view()));
    }

    CHECK(self_unsubscribing.notified == std::vector<DateTime>{t1});
    CHECK(survivor.notified == std::vector<DateTime>{t1});
    CHECK(observed.observer_count() == 1);

    {
        auto mutation = output.begin_mutation(t2);
        REQUIRE(mutation.copy_value_from(two.view()));
    }

    CHECK(self_unsubscribing.notified == std::vector<DateTime>{t1});
    CHECK(survivor.notified == std::vector<DateTime>{t1, t2});
    observed.unsubscribe(&survivor);
    REQUIRE_FALSE(observed.has_observers());

    RemovingNotifiable remover;
    RecordingNotifiable removed_before_notify;
    RecordingNotifiable after_removed;
    remover.observed = observed.borrowed_ref();
    remover.target   = &removed_before_notify;
    observed.subscribe(&remover);
    observed.subscribe(&removed_before_notify);
    observed.subscribe(&after_removed);

    {
        auto mutation = output.begin_mutation(t3);
        REQUIRE(mutation.copy_value_from(three.view()));
    }

    CHECK(remover.notified == std::vector<DateTime>{t3});
    CHECK(removed_before_notify.notified.empty());
    CHECK(after_removed.notified == std::vector<DateTime>{t3});
    CHECK(observed.observer_count() == 2);

    observed.unsubscribe(&remover);
    observed.unsubscribe(&after_removed);
    REQUIRE_FALSE(observed.has_observers());

    const auto t4 = t3 + TimeDelta{1};
    const auto t5 = t4 + TimeDelta{1};
    Value      four{4};
    Value      five{5};
    AddingNotifiable adding;
    RecordingNotifiable added_later;
    adding.observed = observed.borrowed_ref();
    adding.target   = &added_later;
    observed.subscribe(&adding);

    {
        auto mutation = output.begin_mutation(t4);
        REQUIRE(mutation.copy_value_from(four.view()));
    }

    CHECK(adding.notified == std::vector<DateTime>{t4});
    CHECK(added_later.notified.empty());
    CHECK(observed.observer_count() == 2);

    {
        auto mutation = output.begin_mutation(t5);
        REQUIRE(mutation.copy_value_from(five.view()));
    }

    CHECK(adding.notified == std::vector<DateTime>{t4, t5});
    CHECK(added_later.notified == std::vector<DateTime>{t5});

    observed.unsubscribe(&adding);
    observed.unsubscribe(&added_later);

    const auto t6 = t5 + TimeDelta{1};
    const auto t7 = t6 + TimeDelta{1};
    Value      six{6};
    Value      seven{7};
    ReplacingNotifiable replacing;
    RecordingNotifiable replaced;
    RecordingNotifiable replacement;
    replacing.observed    = observed.borrowed_ref();
    replacing.removed     = &replaced;
    replacing.replacement = &replacement;
    observed.subscribe(&replacing);
    observed.subscribe(&replaced);

    {
        auto mutation = output.begin_mutation(t6);
        REQUIRE(mutation.copy_value_from(six.view()));
    }

    CHECK(replacing.notified == std::vector<DateTime>{t6});
    CHECK(replaced.notified.empty());
    CHECK(replacement.notified.empty());
    CHECK(observed.observer_count() == 1);

    {
        auto mutation = output.begin_mutation(t7);
        REQUIRE(mutation.copy_value_from(seven.view()));
    }

    CHECK(replacement.notified == std::vector<DateTime>{t7});
    observed.unsubscribe(&replacement);
}

TEST_CASE("TSOutputView delegates validity through slot TSData ops")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *tss      = registry.tss(int_meta);
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsd      = registry.tsd(int_meta, ts_int);

    const auto t1 = MIN_ST;
    Value      one{1};

    TSOutput set_output{*tss};
    REQUIRE(set_output.view(t1).binding() == set_output.binding());
    REQUIRE_FALSE(set_output.view(t1).valid());
    REQUIRE_FALSE(set_output.view(t1).all_valid());

    auto set_root = set_output.data_view();
    auto set      = set_root.as_set();
    {
        auto mutation = set.begin_mutation(t1);
        REQUIRE(mutation.add(one.view()));
    }

    REQUIRE(set_output.view(t1).valid());
    REQUIRE(set_output.view(t1).all_valid());

    TSOutput dict_output{*tsd};
    auto     dict_root = dict_output.data_view();
    auto     dict      = dict_root.as_dict();
    Value    key{7};
    Value    value{42};

    {
        auto mutation = dict.begin_mutation(t1);
        static_cast<void>(mutation.at(key.view()));
    }

    REQUIRE(dict_output.view(t1).valid());
    REQUIRE_FALSE(dict_output.view(t1).all_valid());

    {
        auto child = dict.at(key.view());
        auto mutation = child.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(value.view()));
    }

    REQUIRE(dict_output.view(t1).all_valid());
}

TEST_CASE("TSOutput shape casts return endpoint views for slot collections")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *tss      = registry.tss(int_meta);
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsd      = registry.tsd(int_meta, ts_int);

    const auto t1 = MIN_ST;
    Value      one{1};
    Value      key{7};
    Value      value{42};

    TSOutput set_output{*tss};
    auto     set_view = set_output.view(t1);
    auto     set = set_view.as_set();
    REQUIRE(set.base().binding() == set_output.binding());
    {
        auto mutation = set.begin_mutation(t1);
        REQUIRE(mutation.add(one.view()));
    }
    auto current_set_view = set_output.view(t1);
    auto current_set = current_set_view.as_set();
    REQUIRE(current_set.contains(one.view()));
    REQUIRE(range_count(current_set.data_view().values()) == 1);

    TSOutput dict_output{*tsd};
    auto     dict_view = dict_output.view(t1);
    auto     dict = dict_view.as_dict();
    REQUIRE(dict.base().binding() == dict_output.binding());
    {
        auto mutation = dict.begin_mutation(t1);
        auto child = mutation.at(key.view());
        auto child_mutation = child.begin_mutation(t1);
        REQUIRE(child_mutation.copy_value_from(value.view()));
    }

    auto current_dict_view = dict_output.view(t1);
    auto current_dict = current_dict_view.as_dict();
    REQUIRE(current_dict.contains(key.view()));
    auto child = current_dict.at(key.view());
    REQUIRE(child.valid());
    REQUIRE(child.value().checked_as<std::int32_t>() == 42);
    REQUIRE(range_count(current_dict.values()) == 1);
    REQUIRE(range_count(current_dict.items()) == 1);
}

TEST_CASE("TSOutputView delegates window all_valid through TSData ops")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *tsw      = registry.tsw(int_meta, 2, 2);

    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    Value      one{1};
    Value      two{2};

    TSOutput output{*tsw};
    auto     root = output.data_view();
    auto     window = root.as_window();

    {
        auto mutation = window.begin_mutation(t1);
        mutation.push(one.view());
    }

    REQUIRE(output.view(t1).valid());
    REQUIRE_FALSE(output.view(t1).all_valid());

    {
        auto mutation = window.begin_mutation(t2);
        mutation.push(two.view());
    }

    REQUIRE(output.view(t2).valid());
    REQUIRE(output.view(t2).all_valid());
}
