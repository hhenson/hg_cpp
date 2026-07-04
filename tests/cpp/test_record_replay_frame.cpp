#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/types/value/table_codec.h>

#include <catch2/catch_test_macros.hpp>

// Step 4 of the record/replay/table design record: the Arrow data-frame
// record/replay backend (model record_replay::DATA_FRAME) over the registered
// frame store (P6), plus the replay_const read. Recording happens in one
// graph run and replays in a SECOND run - the store outlives the graph.

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    struct RecordGraph
    {
        [[maybe_unused]] static constexpr auto name = "record_frame_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            wire<stdlib::record>(w, ts, Str{"prices"}, arg<"recordable_id">(Str{"book"}));
            return ts;   // eval_node needs an output; the recording is the side effect
        }
    };

    struct ReplayGraph
    {
        [[maybe_unused]] static constexpr auto name = "replay_frame_graph";

        static Port<TS<Int>> compose(Wiring &w)
        {
            return wire<stdlib::replay, TS<Int>>(w, Str{"prices"}, arg<"recordable_id">(Str{"book"}))
                .as<TS<Int>>();
        }
    };

    struct TraitRecordGraph
    {
        [[maybe_unused]] static constexpr auto name = "trait_record_frame_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            // No explicit recordable_id: the graph trait provides it at runtime
            // through the TraitsView injectable.
            w.set_trait(std::string{record_replay::RECORDABLE_ID_TRAIT}, Value{Str{"desk.fx"}});
            wire<stdlib::record>(w, ts, Str{"orders"});
            return ts;
        }
    };
}  // namespace

TEST_CASE("frame backend: record writes a bitemporal frame to the store; replay re-emits it")
{
    stdlib::register_standard_operators();
    record_replay::set_config(record_replay::Config{.model = std::string{record_replay::DATA_FRAME}});

    // Run 1: record (values at cycles 0, 2 and 3 - gaps preserved).
    (void)eval_node<RecordGraph>(values<Int>(10, none, 30, 40));

    REQUIRE(record_replay::store_contains("book.prices"));
    const Frame recorded = record_replay::store_read("book.prices");
    CHECK(frame_rows(recorded) == 3);

    // Run 2: replay - values re-emitted at the RECORDED times (cycle-aligned).
    CHECK_OUTPUT(eval_node<ReplayGraph>(), values<Int>(10, none, 30, 40));
}

TEST_CASE("frame backend: the recordable id resolves through graph traits at runtime")
{
    stdlib::register_standard_operators();
    record_replay::set_config(record_replay::Config{.model = std::string{record_replay::DATA_FRAME}});

    (void)eval_node<TraitRecordGraph>(values<Int>(7));
    CHECK(record_replay::store_contains("desk.fx.orders"));
}

TEST_CASE("frame backend: replay_const_value reads the last row at or before tm")
{
    stdlib::register_standard_operators();
    record_replay::set_config(record_replay::Config{.model = std::string{record_replay::DATA_FRAME}});

    (void)eval_node<RecordGraph>(values<Int>(10, none, 30, 40));

    const auto *int_meta = scalar_descriptor<Int>::value_meta();
    // Everything <= MAX_DT: the last recorded value.
    CHECK(record_replay::replay_const_value("book.prices", int_meta).view().checked_as<Int>() == Int{40});
    // Cut at the second recorded tick (cycle 2 = MIN_ST + 2).
    CHECK(record_replay::replay_const_value("book.prices", int_meta, MIN_ST + TimeDelta{2})
              .view()
              .checked_as<Int>() == Int{30});
    // Before the first tick: nothing qualifies.
    CHECK_FALSE(record_replay::replay_const_value("book.prices", int_meta, MIN_ST - TimeDelta{1}).has_value());
    // Unknown key: empty.
    CHECK_FALSE(record_replay::replay_const_value("missing.key", int_meta).has_value());
}

TEST_CASE("frame backend: the in-memory model still resolves record/replay by default")
{
    stdlib::register_standard_operators();
    // Default config = IN_MEMORY: the frame backend must NOT be selected and
    // the testing (GlobalState) backend continues to serve the names.
    CHECK(record_replay::model_is(record_replay::IN_MEMORY));
    CHECK_OUTPUT(eval_node<stdlib::to_json>(values<Int>(1)), values<Str>(Str{"1"}));
}
