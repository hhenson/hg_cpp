// Tests for the eval_node harness: feed a node a per-cycle input sequence and
// read back its per-cycle outputs (wiring replay -> node -> record internally).

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

#include <optional>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;  // `none`

    struct AddOne
    {
        static constexpr auto name = "add_one";
        static void           eval(In<"in", TS<Int>> in, Out<TS<Int>> out) { out.set(in.value() + 1); }
    };

    // A stateful node: a running sum, to show state persists across cycles.
    struct RunningSum
    {
        static constexpr auto name = "running_sum";
        static void           eval(In<"in", TS<Int>> in, State<Int> total, Out<TS<Int>> out)
        {
            total.set(total.get() + in.value());
            out.set(total.get());
        }
    };

    // In + Scalar config: shifts each input by a fixed delta.
    struct Shift
    {
        static constexpr auto name = "eval_node_shift";
        static void           eval(In<"in", TS<Int>> in, Scalar<"delta", Int> delta, Out<TS<Int>> out)
        {
            out.set(in.value() + delta.value());
        }
    };

    // Two time-series inputs: sums them (uses the persisted value when one input
    // does not tick on a given cycle).
    struct Sum
    {
        static constexpr auto name = "eval_node_sum";
        static void           eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs, Out<TS<Int>> out)
        {
            out.set(lhs.value() + rhs.value());
        }
    };

    // Emits each input, then schedules one extra "echo" of value+100 a cycle later
    // — so it produces MORE output ticks than there are inputs.
    struct EchoOnce
    {
        static constexpr auto name = "echo_once";
        static void           eval(In<"in", TS<Int>> in, NodeScheduler sched, State<Int> echo, Out<TS<Int>> out)
        {
            if (in.modified())
            {
                out.set(in.value());
                echo.set(in.value() + 100);
                sched.schedule(MIN_TD);   // emit the echo on the next cycle
            }
            else
            {
                out.set(echo.get());      // the scheduled echo (no input this cycle)
            }
        }
    };

    // TS<Int> in -> TSS<Int> out: each ticked value is added to the set (the
    // per-cycle delta is therefore {added: {value}}). Exercises a TSS *output*.
    struct AccumulateSet
    {
        static constexpr auto name = "eval_accumulate_set";
        static void           eval(In<"in", TS<Int>> in, Out<TSS<Int>> out) { out.add(in.value()); }
    };

    // TSS<Int> in -> TS<Int> out: emits the cumulative set size. Exercises a TSS
    // *input* (the first, braced, parameter).
    struct SetSizeNode
    {
        static constexpr auto name = "eval_set_size";
        static void           eval(In<"s", TSS<Int>> s, Out<TS<Int>> out) { out.set(static_cast<Int>(s.size())); }
    };

    // TSS<Int> in -> TSS<Int> out: re-applies this cycle's delta (remove then add),
    // so the output delta mirrors the input. Exercises TSS on both ends.
    struct MirrorSet
    {
        static constexpr auto name = "eval_mirror_set";
        static void           eval(In<"s", TSS<Int>> s, Out<TSS<Int>> out)
        {
            for (Int r : s.removed()) { out.remove(r); }
            for (Int a : s.added()) { out.add(a); }
        }
    };
}  // namespace

TEST_CASE("eval_node: maps each input tick through a compute node")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    // A skipped input cycle (none) stays skipped in the output.
    CHECK_OUTPUT(testing::eval_node<AddOne>({1, none, 3}), {2, none, 4});
}

TEST_CASE("eval_node: node state persists across cycles")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    CHECK_OUTPUT(testing::eval_node<RunningSum>({1, 2, 3, 4}), {1, 3, 6, 10});
}

TEST_CASE("eval_node: an all-empty input produces no output ticks")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    CHECK_OUTPUT(testing::eval_node<AddOne>({none, none}), {none, none});
}

TEST_CASE("eval_node: output longer than the input is not truncated")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    // One input (5) produces two output cycles: 5, then the echo 105.
    CHECK_OUTPUT(testing::eval_node<EchoOnce>({5}), {5, 105});
}

TEST_CASE("eval_node: passes scalar inputs to the node-under-test")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    // The time-series input is braced; the scalar arg (delta = 5) follows.
    CHECK_OUTPUT(testing::eval_node<Shift>({1, 2, 3}, 5), {6, 7, 8});
}

TEST_CASE("eval_node: drives a node with multiple time-series inputs")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    // First input braced; later inputs as named vectors. rhs ticks every cycle, so
    // at cycle 1 lhs's persisted value (1) is summed with rhs (20) -> 21.
    const std::vector<std::optional<Int>> rhs{10, 20, 30};
    CHECK_OUTPUT(testing::eval_node<Sum>({1, none, 3}, rhs), {11, 21, 33});
}

TEST_CASE("eval_node: a TSS output is read back as a per-cycle SetDelta")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    // Each cycle adds one element, so each delta is {added: {value}}.
    CHECK_OUTPUT(testing::eval_node<AccumulateSet>({1, 2, 3}),
                 {set_delta<Int>({1}, {}), set_delta<Int>({2}, {}), set_delta<Int>({3}, {})});
}

TEST_CASE("eval_node: a TSS input is fed from a per-cycle SetDelta sequence")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    // {1,2} -> +3 -1 -> {2,3} -> -2 -3 -> {}; cumulative sizes 2, 2, 0.
    CHECK_OUTPUT(testing::eval_node<SetSizeNode>(
                     {set_delta<Int>({1, 2}, {}), set_delta<Int>({3}, {1}), set_delta<Int>({}, {2, 3})}),
                 {2, 2, 0});
}

TEST_CASE("eval_node: TSS on both input and output round-trips the delta")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    const std::vector<std::optional<Value>> deltas{
        set_delta<Int>({1, 2}, {}), set_delta<Int>({3}, {1}), set_delta<Int>({}, {2, 3})};
    CHECK_OUTPUT(testing::eval_node<MirrorSet>(deltas), deltas);
}
