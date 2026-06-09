#ifndef HGRAPH_LIB_TESTING_RECORD_REPLAY_H
#define HGRAPH_LIB_TESTING_RECORD_REPLAY_H

#include <hgraph/runtime/global_state.h>
#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/value/value.h>
#include <hgraph/util/date_time.h>

#include <cstddef>
#include <cstdint>
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
     * evaluation time ``MIN_ST + i * MIN_TD``; an empty ``Any`` means "no tick that
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
    [[nodiscard]] inline std::size_t cycle_offset(DateTime now) noexcept
    {
        return static_cast<std::size_t>((now - MIN_ST) / MIN_TD);
    }

    // -----------------------------------------------------------------
    // Delta buffer helpers (canonical delta Values <-> cycle-aligned buffer)
    //
    // A cycle's entry is the canonical per-tick delta ``Value`` (``TS<T>`` -> the
    // scalar; ``TSS``/``TSL`` -> the bundle/map; see ``static_node.h``), or empty for
    // "no tick". These are schema-agnostic — they move ``Value``s, not typed wrappers.
    // -----------------------------------------------------------------

    /** Seed a replay buffer from a sequence of canonical delta ``Value``s (nullopt = no tick). */
    inline void set_replay_deltas(const GlobalStateView &gs, std::string_view key,
                                  const std::vector<std::optional<Value>> &deltas)
    {
        Value buffer   = make_buffer();
        auto  mutation = buffer.as_list().begin_mutation();
        for (const auto &delta : deltas)
        {
            if (delta.has_value()) { mutation.push_back(make_any(delta->view()).view()); }
            else { mutation.push_back(empty_any().view()); }
        }
        gs.set(key, buffer);
    }

    /** Read a recorded buffer back as a sequence of canonical delta ``Value``s (owning copies). */
    [[nodiscard]] inline std::vector<std::optional<Value>> get_recorded_deltas(const GlobalStateView &gs,
                                                                               std::string_view key)
    {
        std::vector<std::optional<Value>> result;
        const ValueView                   buffer = gs.get(key);
        if (!buffer.valid()) { return result; }
        const auto list = buffer.as_list();
        result.reserve(list.size());
        for (std::size_t i = 0; i < list.size(); ++i)
        {
            const auto element = list.at(i).as_any();
            if (element.has_value()) { result.emplace_back(Value{element.get()}); }
            else { result.emplace_back(std::nullopt); }
        }
        return result;
    }

    /** Scalar convenience: seed a ``TS<T>`` buffer from plain C++ values (nullopt = no tick). */
    template <typename T>
    void set_replay_values(const GlobalStateView &gs, std::string_view key, const std::vector<std::optional<T>> &values)
    {
        std::vector<std::optional<Value>> deltas;
        deltas.reserve(values.size());
        for (const auto &value : values)
        {
            if (value.has_value()) { deltas.emplace_back(Value{*value}); }
            else { deltas.emplace_back(std::nullopt); }
        }
        set_replay_deltas(gs, key, deltas);
    }

    /** Scalar convenience: read a recorded ``TS<T>`` buffer back as plain C++ values. */
    template <typename T>
    [[nodiscard]] std::vector<std::optional<T>> get_recorded_values(const GlobalStateView &gs, std::string_view key)
    {
        std::vector<std::optional<T>> out;
        for (const auto &delta : get_recorded_deltas(gs, key))
        {
            if (delta.has_value()) { out.push_back(delta->view().template checked_as<T>()); }
            else { out.push_back(std::nullopt); }
        }
        return out;
    }

    // -----------------------------------------------------------------
    // replay / record — a SINGLE erased source / sink (not templated per schema),
    // over a cycle-aligned buffer of canonical delta ``Value``s. Each is authored
    // once over a deferred time-series type (``TsVar``) and resolved at wiring; the
    // behaviour is schema-as-data, driven by the runtime ``capture_delta`` /
    // ``apply_delta``. ``record`` is a sink whose type resolves from its connected
    // input port — ``wire<record>(w, port, key)``; ``replay`` is a source whose type
    // is supplied explicitly — ``wire<replay, TS<Int>>(w, key)``.
    // -----------------------------------------------------------------

    /** Multi-cycle source: emits a recorded delta sequence for its (resolved) output type. */
    struct replay
    {
        static constexpr auto name = "replay";
        // A source initiates itself at the start cycle (default = not scheduled);
        // per-cycle rescheduling below uses the full NodeScheduler.
        static constexpr bool schedule_on_start = true;

        static void eval(Scalar<"key", std::string> key,
                         GlobalStateView gs,
                         NodeScheduler sched,
                         State<Int> index,
                         Out<TsVar<"S">> out)
        {
            const ValueView buffer = gs.get(key.value());
            if (!buffer.valid()) { return; }  // nothing seeded under this key

            const auto list = buffer.as_list();
            const auto i    = index.get();
            const auto size = static_cast<Int>(list.size());
            if (i < size)
            {
                const auto element = list.at(static_cast<std::size_t>(i)).as_any();
                if (element.has_value()) { apply_delta(out, element.get()); }
            }
            index.set(i + 1);
            if (i + 1 < size) { sched.schedule(MIN_TD); }  // re-arm for the next cycle
        }
    };

    /** Multi-cycle sink: captures each tick's canonical delta into the buffer. */
    struct record
    {
        static constexpr auto name = "record";

        static void start(Scalar<"key", std::string> key, GlobalStateView gs)
        {
            gs.set(key.value(), make_buffer());  // fresh, empty cycle-aligned buffer
        }

        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"key", std::string> key, GlobalStateView gs, DateTime now)
        {
            const std::size_t offset   = cycle_offset(now);
            const ValueView   buffer   = gs.get(key.value());
            auto              list     = buffer.as_list();
            std::size_t       size     = list.size();
            auto              mutation = list.begin_mutation();
            while (size < offset)  // pad skipped cycles so the buffer index matches the evaluation cycle
            {
                mutation.push_back(empty_any().view());
                ++size;
            }
            // The canonical per-tick delta, rebuilt as an owned value-layer Value (the
            // runtime's transient delta storage omits copy hooks), then boxed.
            mutation.push_back(make_any(capture_delta(ts.base())).view());
        }
    };
}  // namespace hgraph::testing

#endif  // HGRAPH_LIB_TESTING_RECORD_REPLAY_H
