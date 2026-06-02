#ifndef HGRAPH_LIB_TESTING_RECORD_REPLAY_H
#define HGRAPH_LIB_TESTING_RECORD_REPLAY_H

#include <hgraph/runtime/global_state.h>
#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/value.h>
#include <hgraph/util/date_time.h>

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace hgraph::testing
{
    /**
     * The in-memory testing toolkit: ``replay`` (a source that emits a recorded
     * sequence) and ``record`` (a sink that captures one), plus the helpers that
     * build/read their shared buffer. Both are static nodes layered on the
     * ``GlobalState`` injectable and the ``NodeScheduler``.
     *
     * The buffer is a value-layer **mutable** ``List<Any>`` stored in the
     * ``GlobalState`` under a string key and **cycle-aligned**: index ``i`` is
     * engine time ``MIN_ST + i * MIN_TD``; an empty ``Any`` means "no tick that
     * cycle", a non-empty ``Any`` means "tick with that value". See
     * ``docs/source/user_guide/testing_graphs_cpp.rst``.
     */

    // -----------------------------------------------------------------
    // Buffer helpers (value-layer <-> ordinary C++ vectors)
    // -----------------------------------------------------------------

    /** An empty ``Any`` value (the "no tick this cycle" marker). */
    [[nodiscard]] inline Value empty_any()
    {
        const auto *binding = ValuePlanFactory::instance().binding_for(TypeRegistry::instance().any());
        return Value{*binding};  // unset == empty == None
    }

    /** An ``Any`` boxing a copy of ``inner``. */
    [[nodiscard]] inline Value make_any(const Value &inner)
    {
        const auto *binding = ValuePlanFactory::instance().binding_for(TypeRegistry::instance().any());
        Value       any{*binding};
        any.as_any().begin_mutation().set(inner.view());
        return any;
    }

    /** A fresh, empty cycle-aligned buffer (a mutable ``List<Any>``). */
    [[nodiscard]] inline Value make_buffer()
    {
        auto       &registry = TypeRegistry::instance();
        const auto *schema   = registry.mutable_list(registry.any());
        const auto *binding  = ValuePlanFactory::instance().binding_for(schema);
        return Value{*binding};
    }

    /** The cycle index for ``now`` (offset from ``MIN_ST`` in ``MIN_TD`` steps). */
    [[nodiscard]] inline std::size_t cycle_offset(engine_time_t now) noexcept
    {
        return static_cast<std::size_t>((now - MIN_ST) / MIN_TD);
    }

    /**
     * Seed a replay buffer in the ``GlobalState``: one entry per engine cycle,
     * ``std::nullopt`` meaning "no tick that cycle".
     */
    template <typename T>
    void set_replay_values(const GlobalStateView &gs, std::string_view key,
                           const std::vector<std::optional<T>> &values)
    {
        Value buffer  = make_buffer();
        auto  mutation = buffer.as_list().begin_mutation();
        for (const auto &value : values)
        {
            if (value.has_value()) { mutation.push_back(make_any(Value{*value}).view()); }
            else { mutation.push_back(empty_any().view()); }
        }
        gs.set(key, buffer);
    }

    /** Read a recorded buffer back out of the ``GlobalState`` as a C++ vector. */
    template <typename T>
    [[nodiscard]] std::vector<std::optional<T>> get_recorded_values(const GlobalStateView &gs, std::string_view key)
    {
        std::vector<std::optional<T>> result;
        const ValueView               buffer = gs.get(key);
        if (!buffer.valid()) { return result; }
        const auto list = buffer.as_list();
        result.reserve(list.size());
        for (std::size_t i = 0; i < list.size(); ++i)
        {
            const auto element = list.at(i).as_any();
            if (element.has_value()) { result.push_back(element.get().template checked_as<T>()); }
            else { result.push_back(std::nullopt); }
        }
        return result;
    }

    // -----------------------------------------------------------------
    // replay<T> — a multi-cycle source over a cycle-aligned buffer
    // -----------------------------------------------------------------

    /**
     * Emits a recorded ``TS<T>`` sequence read from the ``GlobalState`` under
     * ``key``: one output tick per cycle whose buffer element has a value,
     * rescheduling itself until the buffer is exhausted.
     */
    template <typename T>
    struct replay
    {
        static constexpr auto name = "replay";

        static void eval(Scalar<"key", std::string> key, GlobalStateView gs, NodeScheduler sched,
                         State<int> index, Out<TS<T>> out)
        {
            const ValueView buffer = gs.get(key.value());
            if (!buffer.valid()) { return; }  // nothing seeded under this key

            const auto      list = buffer.as_list();
            const int       i    = index.get();
            const int       size = static_cast<int>(list.size());
            if (i < size)
            {
                const auto element = list.at(static_cast<std::size_t>(i)).as_any();
                if (element.has_value()) { out.set(element.get().template checked_as<T>()); }
            }
            index.set(i + 1);
            if (i + 1 < size) { sched.schedule(MIN_TD); }  // re-arm for the next cycle
        }
    };

    // -----------------------------------------------------------------
    // record<T> — captures a TS<T> into a cycle-aligned buffer
    // -----------------------------------------------------------------

    /**
     * Captures each tick of its input into a cycle-aligned ``List<Any>`` in the
     * ``GlobalState`` under ``key`` (created on ``start``). Skipped cycles are
     * padded with empty ``Any`` entries so the buffer index matches the engine
     * cycle, mirroring the replay buffer shape.
     */
    template <typename T>
    struct record
    {
        static constexpr auto name = "record";

        static void start(Scalar<"key", std::string> key, GlobalStateView gs)
        {
            gs.set(key.value(), make_buffer());  // fresh, empty cycle-aligned buffer
        }

        static void eval(In<"ts", TS<T>> ts, Scalar<"key", std::string> key, GlobalStateView gs, engine_time_t now)
        {
            const std::size_t offset = cycle_offset(now);

            // The buffer is a mutable List<Any> in the GlobalState, so it is
            // appended in place: the GlobalState hands back a writable view of a
            // mutable value.
            const ValueView buffer   = gs.get(key.value());
            auto            list     = buffer.as_list();
            std::size_t     size     = list.size();
            auto            mutation = list.begin_mutation();
            while (size < offset)  // pad the skipped cycles
            {
                mutation.push_back(empty_any().view());
                ++size;
            }
            mutation.push_back(make_any(Value{ts.value()}).view());
        }
    };
}  // namespace hgraph::testing

#endif  // HGRAPH_LIB_TESTING_RECORD_REPLAY_H
