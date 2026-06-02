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
