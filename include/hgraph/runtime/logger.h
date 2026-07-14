#ifndef HGRAPH_RUNTIME_LOGGER_H
#define HGRAPH_RUNTIME_LOGGER_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/static_node.h>

#include <spdlog/fmt/fmt.h>
#include <spdlog/spdlog.h>

#include <memory>
#include <string_view>
#include <utility>

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
     * per-tick path. Configure the destination once with
     * ``log::set_logger`` (configuration-time; a ``shared_ptr`` there is
     * sanctioned — the hot path only ever sees the borrowed pointer).
     * Formatting is skipped when the target level is disabled.
     */
    class HGRAPH_EXPORT LoggerView
    {
      public:
        LoggerView() noexcept = default;
        explicit LoggerView(spdlog::logger *logger) noexcept : logger_(logger) {}

        template <typename... Args>
        void trace(fmt::format_string<Args...> format, Args &&...args) const
        {
            log_formatted(spdlog::level::trace, format, std::forward<Args>(args)...);
        }

        template <typename... Args>
        void debug(fmt::format_string<Args...> format, Args &&...args) const
        {
            log_formatted(spdlog::level::debug, format, std::forward<Args>(args)...);
        }

        template <typename... Args>
        void info(fmt::format_string<Args...> format, Args &&...args) const
        {
            log_formatted(spdlog::level::info, format, std::forward<Args>(args)...);
        }

        template <typename... Args>
        void warn(fmt::format_string<Args...> format, Args &&...args) const
        {
            log_formatted(spdlog::level::warn, format, std::forward<Args>(args)...);
        }

        template <typename... Args>
        void error(fmt::format_string<Args...> format, Args &&...args) const
        {
            log_formatted(spdlog::level::err, format, std::forward<Args>(args)...);
        }

        /**
         * Whether ``level`` would actually be emitted — check BEFORE doing
         * any formatting/rendering work for the erased path (the typed
         * methods above are already lazy: spdlog checks the level before
         * formatting). ``level`` uses the spdlog scale — 0 trace, 1 debug,
         * 2 info, 3 warn, 4 error, 5 critical (values are clamped).
         */
        [[nodiscard]] bool should_log(int level) const noexcept
        {
            return logger_ != nullptr && logger_->should_log(clamp_level(level));
        }

        /** Erased-level entry (the ``log_`` operator's path); see ``should_log``. */
        void log(int level, std::string_view message) const
        {
            if (logger_ == nullptr) { return; }
            const auto spd_level = clamp_level(level);
            if (!logger_->should_log(spd_level)) { return; }
            logger_->log(spdlog::source_loc{}, spd_level, spdlog::string_view_t{message.data(), message.size()});
        }

        [[nodiscard]] spdlog::logger *raw() const noexcept { return logger_; }

      private:
        [[nodiscard]] static spdlog::level::level_enum clamp_level(int level) noexcept
        {
            return static_cast<spdlog::level::level_enum>(level < 0 ? 0 : (level > 5 ? 5 : level));
        }

        template <typename... Args>
        void log_formatted(spdlog::level::level_enum level, fmt::format_string<Args...> format, Args &&...args) const
        {
            if (logger_ == nullptr || !logger_->should_log(level)) { return; }
            const auto message = fmt::format(format, std::forward<Args>(args)...);
            logger_->log(spdlog::source_loc{}, level, spdlog::string_view_t{message.data(), message.size()});
        }

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

        /**
         * Drop the process logger so the next ``logger()`` call rebuilds it
         * (including its sinks). Needed wherever the log destination's file
         * descriptor is redirected after the logger was first created:
         * spdlog's Windows stdout sinks cache the raw OS handle at
         * construction, so a redirect (e.g. pytest's per-test fd capture)
         * is only honoured by a freshly built sink.
         */
        HGRAPH_EXPORT void reset_logger();
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
