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

TEST_CASE("scope_exit propagates cleanup exceptions by default")
{
    REQUIRE_THROWS_AS(
        [] {
            auto cleanup = hgraph::make_scope_exit([] { throw std::runtime_error("cleanup failed"); });
        }(),
        std::runtime_error);
}

TEST_CASE("scope_exit can suppress cleanup exceptions")
{
    REQUIRE_NOTHROW([] {
        auto cleanup = hgraph::make_scope_exit<true>([] { throw std::runtime_error("cleanup failed"); });
    }());
}
