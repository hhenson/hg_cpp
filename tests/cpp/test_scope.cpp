#include <hgraph/util/scope.h>

#include <catch2/catch_test_macros.hpp>

#include <stdexcept>

TEST_CASE("fallback_on_exception returns the callable result on success")
{
    const auto result = hgraph::fallback_on_exception(7, [] { return 42; });
    REQUIRE(result == 42);
}

TEST_CASE("fallback_on_exception returns the fallback on failure")
{
    const auto result = hgraph::fallback_on_exception(7, []() -> int {
        throw std::runtime_error("boom");
    });

    REQUIRE(result == 7);
}
