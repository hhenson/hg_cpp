#ifndef HGRAPH_LIB_TESTING_RECORD_REPLAY_H
#define HGRAPH_LIB_TESTING_RECORD_REPLAY_H

#include <hgraph/types/record_replay.h>
#include <hgraph/runtime/global_state.h>
#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/value/mutable_container_ops.h>
#include <hgraph/types/value/value.h>
#include <hgraph/util/date_time.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <vector>

namespace hgraph::testing
{
    [[nodiscard]] inline ValueTypeRef recording_binding_for(const ValueTypeMetaData *schema)
    {
        if (const auto *snapshot = active_type_realization(); snapshot != nullptr)
        {
            return snapshot->type_for(schema);
        }
        return ValuePlanFactory::instance().type_for(schema);
    }

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
        const auto binding = ValuePlanFactory::instance().type_for(TypeRegistry::instance().any());
        return Value{binding};  // unset == empty == None
    }

    /** An ``Any`` boxing a copy of the value behind ``inner`` (type-erased). */
    [[nodiscard]] inline Value make_any(const ValueView &inner)
    {
        const auto binding = ValuePlanFactory::instance().type_for(TypeRegistry::instance().any());
        Value       any{binding};
        any.as_any().begin_mutation().set(inner);
        return any;
    }

    /** An ``Any`` boxing a copy of ``inner``. */
    [[nodiscard]] inline Value make_any(const Value &inner) { return make_any(inner.view()); }

    /** A fresh, empty cycle-aligned buffer (a mutable ``List<Any>``) — the
        SEEDED replay layout (set_replay does not know one element schema). */
    [[nodiscard]] inline Value make_buffer()
    {
        auto       &registry = TypeRegistry::instance();
        const auto *schema   = registry.mutable_list(registry.any());
        const auto binding  = ValuePlanFactory::instance().type_for(schema);
        return Value{binding};
    }

    /** A fresh, empty TYPED dense recording buffer: ``List<delta_schema>``
        with holes as UNSET elements (element validity). */
    [[nodiscard]] inline Value make_dense_buffer(ValueTypeRef delta_binding)
    {
        return Value{mutable_list_type(delta_binding)};
    }

    [[nodiscard]] inline Value make_dense_buffer(const ValueTypeMetaData *delta_schema)
    {
        return make_dense_buffer(recording_binding_for(delta_schema));
    }

    /** The delta at ``index`` of a dense buffer, either layout: the seeded
        ``List<Any>`` (empty box = no tick) or the typed recorded list
        (UNSET element = no tick). nullopt = no tick. */
    [[nodiscard]] inline std::optional<Value> dense_entry_delta(const ListView &list, std::size_t index)
    {
        const auto element = list.at(index);
        if (!element.has_value()) { return std::nullopt; }   // typed hole
        if (element.schema()->value_kind() == ValueTypeKind::Any)
        {
            const auto boxed = element.as_any();
            if (!boxed.has_value()) { return std::nullopt; }   // legacy empty box
            return Value{boxed.get()};
        }
        return Value{element};
    }

    /** The cycle index for ``now`` (offset from ``MIN_ST`` in ``MIN_TD`` steps). */
    [[nodiscard]] inline std::size_t cycle_offset(DateTime now) noexcept
    {
        return static_cast<std::size_t>((now - MIN_ST) / MIN_TD);
    }

    /** Densification guard: a recording (or read-back) spanning more cycles
        than this must go through the sparse path. */
    inline constexpr std::size_t max_dense_cycles = 1'000'000;

    /** SPARSE recordings are TYPED: the recorded schema is known when the
        node runs, so the buffer is ``List<Tuple<datetime, delta_schema>>``
        - no per-entry ``Any`` boxing. */
    [[nodiscard]] inline const ValueTypeMetaData *sparse_entry_meta(const ValueTypeMetaData *delta_schema)
    {
        auto &registry = TypeRegistry::instance();
        return registry.tuple({registry.value_type("datetime"), delta_schema});
    }

    [[nodiscard]] inline ValueTypeRef sparse_entry_binding(ValueTypeRef delta_binding)
    {
        auto       &factory = ValuePlanFactory::instance();
        const auto *schema  = sparse_entry_meta(delta_binding.schema());
        const auto canonical_delta = factory.type_for(delta_binding.schema());
        if (delta_binding == canonical_delta) { return recording_binding_for(schema); }
        const std::array fields{
            factory.type_for(TypeRegistry::instance().value_type("datetime")),
            delta_binding,
        };
        return factory.realized_composite_type_for(schema, fields);
    }

    /** A fresh, empty sparse buffer for the given delta schema. */
    [[nodiscard]] inline Value make_sparse_buffer(ValueTypeRef delta_binding)
    {
        return Value{mutable_list_type(sparse_entry_binding(delta_binding))};
    }

    [[nodiscard]] inline Value make_sparse_buffer(const ValueTypeMetaData *delta_schema)
    {
        return make_sparse_buffer(recording_binding_for(delta_schema));
    }

    /** Build a (time, delta) sparse-buffer entry. */
    [[nodiscard]] inline Value make_sparse_entry(ValueTypeRef delta_binding, DateTime when, Value delta)
    {
        BundleBuilder entry{sparse_entry_binding(delta_binding)};
        entry.set(0, Value{when});
        entry.set(1, std::move(delta));
        return entry.build();
    }

    [[nodiscard]] inline Value make_sparse_entry(const ValueTypeMetaData *delta_schema, DateTime when, Value delta)
    {
        (void)delta_schema;
        const auto binding = delta.binding();
        return make_sparse_entry(binding, when, std::move(delta));
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

    /** Read a SPARSE recording back as (cycle, delta) pairs. The list is
        written in evaluation order, so it is naturally time-sorted. */
    [[nodiscard]] inline std::vector<std::pair<std::size_t, Value>> get_recorded_sparse(const GlobalStateView &gs,
                                                                                        std::string_view key)
    {
        std::vector<std::pair<std::size_t, Value>> result;
        const ValueView                            buffer = gs.get(key);
        if (!buffer.valid()) { return result; }
        const auto list = buffer.as_list();
        result.reserve(list.size());
        for (std::size_t i = 0; i < list.size(); ++i)
        {
            const auto entry = list.at(i).as_indexed_view();
            result.emplace_back(cycle_offset(entry.at(0).checked_as<DateTime>()), Value{entry.at(1)});
        }
        return result;
    }

    /** Read a DENSE recording back as a sequence of canonical delta
        ``Value``s (owning copies; nullopt = no tick). */
    [[nodiscard]] inline std::vector<std::optional<Value>> get_recorded_deltas(const GlobalStateView &gs,
                                                                               std::string_view key)
    {
        std::vector<std::optional<Value>> result;
        const ValueView                   buffer = gs.get(key);
        if (!buffer.valid()) { return result; }
        const auto list = buffer.as_list();
        if (list.size() > max_dense_cycles)
        {
            throw std::logic_error(
                "get_recorded_deltas: the recording spans too many cycles to read densely - "
                "read it with get_recorded_sparse");
        }
        result.reserve(list.size());
        for (std::size_t i = 0; i < list.size(); ++i)
        {
            result.emplace_back(dense_entry_delta(list, i));
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

        /** The in-memory backend: active only under the default model. */
        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return record_replay::model_is(context.global_state, record_replay::IN_MEMORY);
        }
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
                // Either dense layout: seeded List<Any> or a typed recording
                // (component ReplayOutput replays recorded buffers).
                if (auto delta = dense_entry_delta(list, static_cast<std::size_t>(i)); delta.has_value())
                {
                    apply_delta(out, delta->view());
                }
            }
            index.set(i + 1);
            if (i + 1 < size) { sched.schedule(MIN_TD); }  // re-arm for the next cycle
        }
    };

    /** Multi-cycle sink: captures each tick's canonical delta into the buffer. */
    struct record
    {
        static constexpr auto name = "record";

        /** The in-memory backend: active only under the default model. */
        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return record_replay::model_is(context.global_state, record_replay::IN_MEMORY);
        }

        static auto defaults() { return std::tuple{arg<"sparse">(Bool{false})}; }

        static void start(Scalar<"key", std::string> key, Scalar<"sparse", Bool>, GlobalStateView gs)
        {
            // Both buffer layouts are TYPED by the recorded delta schema,
            // which only eval sees - creation is lazy on the first tick (a
            // never-ticking recording reads back empty either way). A seeded
            // state may contain the prior run's result under this key.
            gs.erase(key.value());
        }

        static void eval(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts, Scalar<"key", std::string> key,
                         Scalar<"sparse", Bool> sparse, GlobalStateView gs, DateTime now)
        {
            // Record every TICK, plus TICK-window TSW pre-validity ticks: a
            // window below its min period is invalid yet its delta stream
            // already flows (hgraph records those). Invalid structural
            // collection unbinds are also recorded when they carry a real
            // removal delta over a previously published key set.
            if (!ts.modified()) { return; }
            if (!ts.valid())
            {
                const auto *schema = ts.base().schema();
                const bool tick_window = schema->kind == TSTypeKind::TSW && !schema->data.tsw.is_duration_based;
                bool structural_removal = false;
                if (schema->kind == TSTypeKind::TSS)
                {
                    const auto input = ts.base().as_set();
                    const auto removed = input.removed();
                    structural_removal = removed.begin() != removed.end();
                }
                else if (schema->kind == TSTypeKind::TSD)
                {
                    const auto input = ts.base().as_dict();
                    const auto removed = input.removed_keys();
                    structural_removal = removed.begin() != removed.end();
                }
                if (!structural_removal && !tick_window) { return; }
            }
            // The canonical per-tick delta, rebuilt as an owned value-layer Value (the
            // runtime's transient delta storage omits copy hooks).
            Value delta = capture_delta(ts.base());
            const auto *schema = ts.base().schema();
            if (schema->kind == TSTypeKind::TSL && schema->fixed_size() != 0 && delta.view().as_map().empty())
            {
                // A fixed structural list can remain historically valid after every
                // routed REF leaf silently unbinds. Its parent may be scheduled, but
                // the empty map is not a tick and must remain a dense-buffer hole.
                return;
            }
            if (sparse.value())
            {
                // SPARSE (the harness's __elide__): TYPED (time, delta)
                // tuple entries in evaluation order - one entry per tick
                // regardless of the gap, no Any boxing.
                const auto delta_binding = recording_binding_for(schema->delta_value_schema);
                ValueView buffer = gs.get(key.value());
                if (!buffer.valid())
                {
                    gs.set(key.value(), make_sparse_buffer(delta_binding));
                    buffer = gs.get(key.value());
                }
                auto mutation = buffer.as_list().begin_mutation();
                mutation.push_back(make_sparse_entry(delta_binding, now, std::move(delta)).view());
                return;
            }
            // DENSE: a TYPED List<delta_schema>; skipped cycles are UNSET
            // elements (element validity) - one default-constructed slot per
            // hole instead of a boxed Any.
            const auto delta_binding = recording_binding_for(schema->delta_value_schema);
            ValueView buffer = gs.get(key.value());
            if (!buffer.valid())
            {
                gs.set(key.value(), make_dense_buffer(delta_binding));
                buffer = gs.get(key.value());
            }
            const std::size_t offset   = cycle_offset(now);
            auto              list     = buffer.as_list();
            auto              mutation = list.begin_mutation();
            std::size_t       size     = list.size();
            if (offset - size > max_dense_cycles)
            {
                throw std::logic_error(
                    "record: the tick gap spans too many cycles to record densely - "
                    "record sparse (python: eval_node __elide__=True)");
            }
            while (size < offset)  // pad skipped cycles so the buffer index matches the evaluation cycle
            {
                mutation.push_back_unset();
                ++size;
            }
            mutation.push_back(delta.view());
        }
    };
}  // namespace hgraph::testing

#endif  // HGRAPH_LIB_TESTING_RECORD_REPLAY_H
