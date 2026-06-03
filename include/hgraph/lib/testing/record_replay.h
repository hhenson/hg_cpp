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
#include <set>
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

    /** An ``Any`` boxing a copy of the value behind ``inner`` (type-erased). */
    [[nodiscard]] inline Value make_any(const ValueView &inner)
    {
        const auto *binding = ValuePlanFactory::instance().binding_for(TypeRegistry::instance().any());
        Value       any{*binding};
        any.as_any().begin_mutation().set(inner);
        return any;
    }

    /** An ``Any`` boxing a copy of ``inner``. */
    [[nodiscard]] inline Value make_any(const Value &inner) { return make_any(inner.view()); }

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

        // A source initiates itself at the start cycle; the declarative attribute
        // inserts that scheduling (default = not scheduled). Per-cycle rescheduling
        // below uses the full NodeScheduler.
        static constexpr bool schedule_on_start = true;

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
                // Type-erased: copy the stored value straight into the output.
                if (element.has_value()) { out.apply(element.get()); }
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
     * ``GlobalState`` under ``key`` (created on ``start``). It records the input's
     * ``delta_value`` (the per-tick event), not the cumulative ``value`` — they
     * coincide for scalar time-series but differ for compound types. Skipped cycles
     * are padded with empty ``Any`` entries so the buffer index matches the engine
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
            // Record the per-tick delta_value (the event), not the cumulative
            // value — they coincide for scalar TS but differ for compound types.
            mutation.push_back(make_any(ts.view().delta_value()).view());
        }
    };

    // -----------------------------------------------------------------
    // TSS (set time-series): delta-aware replay / record
    //
    // A set time-series ticks a *delta* each cycle — the elements added and
    // removed — represented by the core ``SetDelta<T>`` (a lightweight wrapper
    // over a ``Bundle{added, removed}`` value; see ``static_node.h``). Build
    // deltas with ``set_delta(added, removed)``. The per-cycle buffer is the usual
    // ``List<Any>`` of these (empty Any = no tick).
    // -----------------------------------------------------------------

    /** Seed a TSS replay buffer from a sequence of ``SetDelta``s (nullopt = no tick). */
    template <typename T>
    void set_replay_deltas(const GlobalStateView &gs, std::string_view key,
                           const std::vector<std::optional<SetDelta<T>>> &deltas)
    {
        Value buffer   = make_buffer();
        auto  mutation = buffer.as_list().begin_mutation();
        for (const auto &delta : deltas)
        {
            if (!delta.has_value()) { mutation.push_back(empty_any().view()); continue; }
            mutation.push_back(make_any(make_set_delta_value<T>(delta->added(), delta->removed()).view()).view());
        }
        gs.set(key, buffer);
    }

    /** Read a recorded TSS buffer back as a sequence of ``SetDelta``s (owning copies). */
    template <typename T>
    [[nodiscard]] std::vector<std::optional<SetDelta<T>>> get_recorded_deltas(const GlobalStateView &gs,
                                                                              std::string_view key)
    {
        std::vector<std::optional<SetDelta<T>>> result;
        const ValueView                         buffer = gs.get(key);
        if (!buffer.valid()) { return result; }
        const auto list = buffer.as_list();
        for (std::size_t i = 0; i < list.size(); ++i)
        {
            const auto element = list.at(i).as_any();
            if (!element.has_value()) { result.push_back(std::nullopt); continue; }
            result.push_back(SetDelta<T>{element.get()});
        }
        return result;
    }

    /**
     * Replays a recorded sequence of set deltas onto a ``TSS<T>`` output: each
     * cycle it applies the buffered delta (remove then add). Like ``replay``, it
     * initiates at start and reschedules until the buffer is exhausted.
     */
    template <typename T>
    struct replay_set
    {
        static constexpr auto name              = "replay_set";
        static constexpr bool schedule_on_start = true;

        static void eval(Scalar<"key", std::string> key, GlobalStateView gs, NodeScheduler sched, State<int> index,
                         Out<TSS<T>> out)
        {
            const ValueView buffer = gs.get(key.value());
            if (!buffer.valid()) { return; }
            const auto list = buffer.as_list();
            const int  i    = index.get();
            const int  size = static_cast<int>(list.size());
            if (i < size)
            {
                const auto element = list.at(static_cast<std::size_t>(i)).as_any();
                if (element.has_value())
                {
                    const auto bundle   = element.get().as_bundle();
                    auto       mutation = out.view().as_set().begin_mutation(out.evaluation_time());
                    const auto removed  = bundle.field("removed").as_indexed_view();
                    for (std::size_t k = 0; k < removed.size(); ++k) { (void)mutation.remove(removed.at(k)); }
                    const auto added = bundle.field("added").as_indexed_view();
                    for (std::size_t k = 0; k < added.size(); ++k) { (void)mutation.add(added.at(k)); }
                }
            }
            index.set(i + 1);
            if (i + 1 < size) { sched.schedule(MIN_TD); }
        }
    };

    /** Captures each tick of a ``TSS<T>`` input as a ``SetDelta`` into the buffer. */
    template <typename T>
    struct record_set
    {
        static constexpr auto name = "record_set";

        static void start(Scalar<"key", std::string> key, GlobalStateView gs) { gs.set(key.value(), make_buffer()); }

        static void eval(In<"ts", TSS<T>> ts, Scalar<"key", std::string> key, GlobalStateView gs, engine_time_t now)
        {
            const std::size_t offset   = cycle_offset(now);
            const ValueView   buffer   = gs.get(key.value());
            auto              list     = buffer.as_list();
            std::size_t       size     = list.size();
            auto              mutation = list.begin_mutation();
            while (size < offset)
            {
                mutation.push_back(empty_any().view());
                ++size;
            }
            // Capture this cycle's delta (added/removed) from the typed views.
            mutation.push_back(make_any(make_set_delta_value<T>(ts.added(), ts.removed()).view()).view());
        }
    };
}  // namespace hgraph::testing

#endif  // HGRAPH_LIB_TESTING_RECORD_REPLAY_H
