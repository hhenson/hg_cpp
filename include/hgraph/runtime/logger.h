#ifndef HGRAPH_RUNTIME_LOGGER_H
#define HGRAPH_RUNTIME_LOGGER_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/static_node.h>

#include <spdlog/spdlog.h>

#include <memory>
#include <string_view>

namespace hgraph
{
    /**
     * The LOGGER injectable (ruling 2026-07-04: LOGGER is a C++-native
     * logger — spdlog, built against the project fmt). Declare a
     * ``LoggerView`` parameter on any node hook to log:
     *
     * .. code-block:: cpp
     *
     *    static void eval(In<"ts", TS<Int>> ts, LoggerView log)
     *    {
     *        log.info("tick: {}", ts.value());
     *    }
     *
     * A transparent, stateless injectable (the ``SingleShotScheduler``
     * pattern): no signature footprint, no per-node slot. The view BORROWS
     * the process logger (``log::logger()``) — no reference counting on the
     * per-tick path; per-tick logging is allocation-light (spdlog formats
     * into its own buffers). Configure the destination once with
     * ``log::set_logger`` (configuration-time; a ``shared_ptr`` there is
     * sanctioned — the hot path only ever sees the borrowed pointer).
     */
    class HGRAPH_EXPORT LoggerView
    {
      public:
        LoggerView() noexcept = default;
        explicit LoggerView(spdlog::logger *logger) noexcept : logger_(logger) {}

        template <typename... Args>
        void trace(fmt::format_string<Args...> format, Args &&...args) const
        {
            if (logger_ != nullptr) { logger_->trace(format, std::forward<Args>(args)...); }
        }

        template <typename... Args>
        void debug(fmt::format_string<Args...> format, Args &&...args) const
        {
            if (logger_ != nullptr) { logger_->debug(format, std::forward<Args>(args)...); }
        }

        template <typename... Args>
        void info(fmt::format_string<Args...> format, Args &&...args) const
        {
            if (logger_ != nullptr) { logger_->info(format, std::forward<Args>(args)...); }
        }

        template <typename... Args>
        void warn(fmt::format_string<Args...> format, Args &&...args) const
        {
            if (logger_ != nullptr) { logger_->warn(format, std::forward<Args>(args)...); }
        }

        template <typename... Args>
        void error(fmt::format_string<Args...> format, Args &&...args) const
        {
            if (logger_ != nullptr) { logger_->error(format, std::forward<Args>(args)...); }
        }

        /**
         * Erased-level entry (the ``log_`` operator's path): ``level`` uses
         * the spdlog scale — 0 trace, 1 debug, 2 info, 3 warn, 4 error,
         * 5 critical (values are clamped).
         */
        void log(int level, std::string_view message) const
        {
            if (logger_ == nullptr) { return; }
            const auto clamped = level < 0 ? 0 : (level > 5 ? 5 : level);
            logger_->log(static_cast<spdlog::level::level_enum>(clamped), "{}", message);
        }

        [[nodiscard]] spdlog::logger *raw() const noexcept { return logger_; }

      private:
        spdlog::logger *logger_{nullptr};   // borrowed from log::logger()
    };

    namespace log
    {
        /** The process hgraph logger (created on demand: a colour stdout logger named "hgraph"). */
        [[nodiscard]] HGRAPH_EXPORT spdlog::logger &logger();

        /**
         * Replace the process logger (configuration-time; the injectable
         * borrows whatever is set here). Passing ``nullptr`` restores the
         * default.
         */
        HGRAPH_EXPORT void set_logger(std::shared_ptr<spdlog::logger> logger);
    }  // namespace log

    namespace static_node_detail
    {
        // Transparent, stateless injectable (see LoggerView above).
        template <>
        struct arg_provider<LoggerView>
        {
            static LoggerView get(const NodeView &, DateTime) { return LoggerView{&log::logger()}; }
        };
    }  // namespace static_node_detail
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_LOGGER_H
