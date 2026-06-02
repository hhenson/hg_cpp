#ifndef HGRAPH_RUNTIME_NODE_SCHEDULER_H
#define HGRAPH_RUNTIME_NODE_SCHEDULER_H

#include <hgraph/hgraph_export.h>
#include <hgraph/runtime/graph.h>
#include <hgraph/util/date_time.h>

#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace hgraph
{
    /**
     * Persistent per-node scheduler **state** — the small footprint stored on a
     * node that declares a ``NodeScheduler``. It holds the set of pending
     * ``(time, tag)`` events (ordered by time then tag) and the ``tag -> time``
     * index used to replace/cancel tagged schedules. A node that never schedules
     * stores nothing (the slot exists only when ``uses_scheduler`` is set).
     *
     * Behaviour lives on the :cpp:class:`NodeScheduler` view, constructed on
     * demand when the scheduler is injected — the value/view split keeps the node
     * memory minimal and the graph/node-index/now context out of storage.
     */
    struct HGRAPH_EXPORT NodeSchedulerState
    {
        std::set<std::pair<engine_time_t, std::string>> events{};
        std::map<std::string, engine_time_t>            tags{};
    };

    /**
     * Borrowing **view** over a node's scheduler — the injectable, built on demand
     * (state reference + node index + graph reference + current time). It mirrors
     * the 2603 ``NodeScheduler`` / Python ``SCHEDULER`` interface: schedule
     * absolute times or deltas (optionally tagged), query/replace/cancel by tag,
     * and re-arm the node. Scheduling pushes the earliest pending time onto the
     * graph; ``advance`` (run after each evaluation) consumes fired events and
     * re-arms the next.
     *
     * Wall-clock alarms (``on_wall_clock = true``) require the real-time
     * evaluation clock, which is not built yet, so that path throws for now.
     */
    class HGRAPH_EXPORT NodeScheduler
    {
      public:
        NodeScheduler() noexcept = default;
        NodeScheduler(NodeSchedulerState &state, GraphValue *graph, std::size_t node_index, engine_time_t now) noexcept
            : state_(&state), graph_(graph), node_index_(node_index), now_(now)
        {
        }

        /** The current evaluation time. */
        [[nodiscard]] engine_time_t now() const noexcept { return now_; }

        /** Earliest pending time, or ``MIN_DT`` when nothing is scheduled. */
        [[nodiscard]] engine_time_t next_scheduled_time() const noexcept
        {
            return (state_ != nullptr && !state_->events.empty()) ? state_->events.begin()->first : MIN_DT;
        }

        /** Whether any events are pending. */
        [[nodiscard]] bool is_scheduled() const noexcept
        {
            return state_ != nullptr && !state_->events.empty();
        }

        /**
         * Whether the node is scheduled for the current engine cycle — i.e. the
         * earliest pending event is *exactly* now. Mirrors the authoritative
         * Python ``is_scheduled_now`` (``events[0][0] == evaluation_time``);
         * fired events never linger below ``now`` because :cpp:func:`advance`
         * consumes them.
         */
        [[nodiscard]] bool is_scheduled_now() const noexcept
        {
            return state_ != nullptr && !state_->events.empty() && state_->events.begin()->first == now_;
        }

        /** Whether a schedule is registered under ``tag``. */
        [[nodiscard]] bool has_tag(std::string_view tag) const
        {
            return state_ != nullptr && state_->tags.contains(std::string{tag});
        }

        /** Time registered under ``tag``, or ``default_time`` when absent. */
        [[nodiscard]] engine_time_t tag_time(std::string_view tag, engine_time_t default_time = MIN_DT) const
        {
            if (state_ == nullptr) { return default_time; }
            const auto it = state_->tags.find(std::string{tag});
            return it != state_->tags.end() ? it->second : default_time;
        }

        /** Whether ``tag``'s schedule is due in the current cycle. */
        [[nodiscard]] bool tag_is_scheduled_now(std::string_view tag) const { return has_tag(tag) && tag_time(tag) == now_; }

        /** Remove ``tag``'s event and return its time, or ``default_time`` when absent. */
        engine_time_t pop_tag(std::string_view tag, engine_time_t default_time = MIN_DT) const
        {
            require_state("pop_tag");
            const auto it = state_->tags.find(std::string{tag});
            if (it == state_->tags.end()) { return default_time; }
            const engine_time_t when = it->second;
            state_->events.erase({when, it->first});
            state_->tags.erase(it);
            return when;
        }

        /**
         * Schedule the node at ``when`` (must be in the future). A ``tag``
         * replaces any prior event under the same tag. ``on_wall_clock`` is not
         * yet supported.
         */
        void schedule(engine_time_t when, std::optional<std::string> tag = std::nullopt,
                      bool on_wall_clock = false) const
        {
            require_state("schedule");
            if (on_wall_clock)
            {
                throw std::logic_error("NodeScheduler: wall-clock alarms are not supported in simulation yet");
            }
            const std::string tag_value = tag.value_or("");
            if (tag.has_value())
            {
                if (const auto it = state_->tags.find(tag_value); it != state_->tags.end())
                {
                    state_->events.erase({it->second, tag_value});  // replace existing tagged event
                }
            }
            if (when <= now_) { return; }  // cannot schedule in the past / current cycle

            const engine_time_t prev_first = state_->events.empty() ? MAX_DT : state_->events.begin()->first;
            if (tag.has_value()) { state_->tags[tag_value] = when; }  // only tagged events are indexed
            state_->events.insert({when, tag_value});
            const engine_time_t next = state_->events.begin()->first;
            if (graph_ != nullptr && next < prev_first) { graph_->schedule_node(node_index_, next); }
        }

        /** Schedule the node ``delta`` after the current evaluation time. */
        void schedule(engine_time_delta_t delta, std::optional<std::string> tag = std::nullopt,
                      bool on_wall_clock = false) const
        {
            schedule(now_ + delta, std::move(tag), on_wall_clock);
        }

        /** Cancel the event registered under ``tag`` (no-op if absent). */
        void un_schedule(const std::string &tag) const
        {
            require_state("un_schedule");
            if (const auto it = state_->tags.find(tag); it != state_->tags.end())
            {
                state_->events.erase({it->second, it->first});
                state_->tags.erase(it);
            }
        }

        /** Cancel the next (earliest) pending event. */
        void un_schedule() const
        {
            require_state("un_schedule");
            if (state_->events.empty()) { return; }
            const auto ev = *state_->events.begin();
            state_->events.erase(state_->events.begin());
            state_->tags.erase(ev.second);
        }

        /** Remove all pending events. */
        void reset() const
        {
            require_state("reset");
            state_->events.clear();
            state_->tags.clear();
        }

        /**
         * Lifecycle hook run when the node fired on a scheduler event: drop the
         * fired events (``time <= now``) and re-arm the node at the next pending
         * time. This mirrors the authoritative Python ``advance`` (``while
         * events and events[0][0] <= until``); the runtime only calls it when the
         * node was ``is_scheduled_now`` for this cycle (see ``node.cpp``). Safe to
         * call when the node has no scheduler state.
         */
        void advance() const
        {
            if (state_ == nullptr) { return; }
            while (!state_->events.empty() && state_->events.begin()->first <= now_)
            {
                const std::string &tag = state_->events.begin()->second;
                if (!tag.empty()) { state_->tags.erase(tag); }  // only tagged events are indexed
                state_->events.erase(state_->events.begin());
            }
            if (graph_ != nullptr && !state_->events.empty())
            {
                graph_->schedule_node(node_index_, state_->events.begin()->first);
            }
        }

      private:
        void require_state(const char *what) const
        {
            if (state_ == nullptr)
            {
                throw std::logic_error(std::string{"NodeScheduler::"} + what + " requires live scheduler state");
            }
        }

        NodeSchedulerState *state_{nullptr};
        GraphValue         *graph_{nullptr};
        std::size_t         node_index_{0};
        engine_time_t       now_{MIN_DT};
    };
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_NODE_SCHEDULER_H
