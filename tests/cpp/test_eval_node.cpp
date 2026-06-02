// Tests for the eval_node harness: feed a node a per-cycle input sequence and
// read back its per-cycle outputs (wiring replay -> node -> record internally).

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>

#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;  // `none`

    struct AddOne
    {
        static constexpr auto name = "add_one";
        static void           eval(In<"in", TS<int>> in, Out<TS<int>> out) { out.set(in.value() + 1); }
    };

    // A stateful node: a running sum, to show state persists across cycles.
    struct RunningSum
    {
        static constexpr auto name = "running_sum";
        static void           eval(In<"in", TS<int>> in, State<int> total, Out<TS<int>> out)
        {
            total.set(total.get() + in.value());
            out.set(total.get());
        }
    };

    // In + Scalar config: shifts each input by a fixed delta.
    struct Shift
    {
        static constexpr auto name = "eval_node_shift";
        static void           eval(In<"in", TS<int>> in, Scalar<"delta", int> delta, Out<TS<int>> out)
        {
            out.set(in.value() + delta.value());
        }
    };

    // Two time-series inputs: sums them (uses the persisted value when one input
    // does not tick on a given cycle).
    struct Sum
    {
        static constexpr auto name = "eval_node_sum";
        static void           eval(In<"lhs", TS<int>> lhs, In<"rhs", TS<int>> rhs, Out<TS<int>> out)
        {
            out.set(lhs.value() + rhs.value());
        }
    };

    // Emits each input, then schedules one extra "echo" of value+100 a cycle later
    // — so it produces MORE output ticks than there are inputs.
    struct EchoOnce
    {
        static constexpr auto name = "echo_once";
        static void           eval(In<"in", TS<int>> in, NodeScheduler sched, State<int> echo, Out<TS<int>> out)
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
}  // namespace

TEST_CASE("eval_node: maps each input tick through a compute node")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<int>("int");

    // A skipped input cycle (none) stays skipped in the output.
    CHECK_OUTPUT(testing::eval_node<AddOne>({1, none, 3}), {2, none, 4});
}

TEST_CASE("eval_node: node state persists across cycles")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<int>("int");

    CHECK_OUTPUT(testing::eval_node<RunningSum>({1, 2, 3, 4}), {1, 3, 6, 10});
}

TEST_CASE("eval_node: an all-empty input produces no output ticks")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<int>("int");

    CHECK_OUTPUT(testing::eval_node<AddOne>({none, none}), {none, none});
}

TEST_CASE("eval_node: output longer than the input is not truncated")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<int>("int");

    // One input (5) produces two output cycles: 5, then the echo 105.
    CHECK_OUTPUT(testing::eval_node<EchoOnce>({5}), {5, 105});
}

TEST_CASE("eval_node: passes scalar inputs to the node-under-test")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<int>("int");

    // The time-series input is braced; the scalar arg (delta = 5) follows.
    CHECK_OUTPUT(testing::eval_node<Shift>({1, 2, 3}, 5), {6, 7, 8});
}

TEST_CASE("eval_node: drives a node with multiple time-series inputs")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<int>("int");

    // First input braced; later inputs as named vectors. rhs ticks every cycle, so
    // at cycle 1 lhs's persisted value (1) is summed with rhs (20) -> 21.
    const std::vector<std::optional<int>> rhs{10, 20, 30};
    CHECK_OUTPUT(testing::eval_node<Sum>({1, none, 3}, rhs), {11, 21, 33});
}
