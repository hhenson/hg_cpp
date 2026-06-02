// Tests for the eval_node harness: feed a node a per-cycle input sequence and
// read back its per-cycle outputs (wiring replay -> node -> record internally).

#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>

#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <vector>

namespace
{
    using namespace hgraph;

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
}  // namespace

TEST_CASE("eval_node: maps each input tick through a compute node")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<int>("int");

    const auto out = testing::eval_node<AddOne>({1, std::nullopt, 3});
    REQUIRE(out.size() == 3);
    CHECK(out[0] == std::optional<int>{2});
    CHECK(out[1] == std::nullopt);   // no input tick -> no output tick
    CHECK(out[2] == std::optional<int>{4});
}

TEST_CASE("eval_node: node state persists across cycles")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<int>("int");

    const auto out = testing::eval_node<RunningSum>({1, 2, 3, 4});
    REQUIRE(out.size() == 4);
    CHECK(out[0] == std::optional<int>{1});
    CHECK(out[1] == std::optional<int>{3});
    CHECK(out[2] == std::optional<int>{6});
    CHECK(out[3] == std::optional<int>{10});
}

TEST_CASE("eval_node: an all-empty input produces no output ticks")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<int>("int");

    const auto out = testing::eval_node<AddOne>({std::nullopt, std::nullopt});
    for (const auto &v : out) { CHECK(v == std::nullopt); }
}
