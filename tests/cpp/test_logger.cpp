#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/runtime/logger.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>

#include <spdlog/sinks/ringbuffer_sink.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <memory>
#include <string>

// The LOGGER injectable (ruling 2026-07-04: spdlog behind LoggerView) and
// the log_ operator over it.

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    struct CapturedLog
    {
        std::shared_ptr<spdlog::sinks::ringbuffer_sink_mt> sink;

        CapturedLog() : sink(std::make_shared<spdlog::sinks::ringbuffer_sink_mt>(16))
        {
            log::set_logger(std::make_shared<spdlog::logger>("hgraph-test", sink));
        }

        ~CapturedLog() { log::set_logger(nullptr); }

        [[nodiscard]] std::string joined() const
        {
            std::string all;
            for (const auto &line : sink->last_formatted()) { all += line; }
            return all;
        }
    };

    struct LoggingNode
    {
        static constexpr auto name = "logging_node";

        static void start(LoggerView log) { log.info("logging node started"); }

        static void eval(In<"ts", TS<Int>> ts, LoggerView log, Out<TS<Int>> out)
        {
            log.warn("tick value {}", ts.value());
            out.set(ts.value());
        }
    };

    struct LogOperatorGraph
    {
        [[maybe_unused]] static constexpr auto name = "log_operator_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            auto fmt_source = wire<stdlib::const_>(w, Str{"observed {}"});
            wire<stdlib::log_>(w, fmt_source.as<TS<Str>>(), ts, arg<"level">(Int{4}));   // error level
            return ts;
        }
    };
}  // namespace

TEST_CASE("logger: the LoggerView injectable logs from start and eval hooks")
{
    stdlib::register_standard_operators();
    CapturedLog captured;

    CHECK_OUTPUT(eval_node<LoggingNode>(values<Int>(7, none, 9)), values<Int>(7, none, 9));

    const std::string all = captured.joined();
    CHECK_THAT(all, Catch::Matchers::ContainsSubstring("logging node started"));
    CHECK_THAT(all, Catch::Matchers::ContainsSubstring("tick value 7"));
    CHECK_THAT(all, Catch::Matchers::ContainsSubstring("tick value 9"));
}

TEST_CASE("logger: the log_ operator formats and logs through the injectable")
{
    stdlib::register_standard_operators();
    CapturedLog captured;

    CHECK_OUTPUT(eval_node<LogOperatorGraph>(values<Int>(42)), values<Int>(42));
    CHECK_THAT(captured.joined(), Catch::Matchers::ContainsSubstring("observed 42"));
}
