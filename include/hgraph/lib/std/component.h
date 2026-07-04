#ifndef HGRAPH_LIB_STD_COMPONENT_H
#define HGRAPH_LIB_STD_COMPONENT_H

#include <hgraph/lib/std/operators/io.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/record_replay.h>

#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>

namespace hgraph::stdlib
{
    namespace component_detail
    {
        /** The record/replay key for the I-th input: the graph's ``NamedPort`` name, else ``arg_<I>``. */
        template <typename Param, std::size_t I>
        [[nodiscard]] std::string input_key()
        {
            using P = static_node_detail::selector_of<Param>;
            if constexpr (requires { P::field_name; }) { return std::string{P::field_name.sv()}; }
            else { return "arg_" + std::to_string(I); }
        }

        template <typename S>
        [[nodiscard]] Port<S> wrap_input(Wiring &w, Port<S> port, const std::string &key, const std::string &fq,
                                         record_replay::Mode mode)
        {
            using record_replay::has_mode;
            using record_replay::Mode;
            if (has_mode(mode, Mode::Replay) || has_mode(mode, Mode::Compare))
            {
                // The live input is REPLACED by the recorded one (Python parity).
                port = wire<stdlib::replay, S>(w, Str{key}, arg<"recordable_id">(Str{fq})).template as<S>();
            }
            if (has_mode(mode, Mode::Record))
            {
                wire<stdlib::record>(w, port, Str{key}, arg<"recordable_id">(Str{fq}));
            }
            return port;
        }
    }  // namespace component_detail

    /**
     * ``component<G>(w, "id", inputs...)`` — Python's ``@component`` as a
     * wiring function (design record: *Record/replay, tables and const_fn*,
     * step 5). ``G`` is an ordinary graph struct; the component consults the
     * ambient ``record_replay::scope`` and, per its mode, wraps every input
     * and the output with name-resolved ``record``/``replay`` (whatever
     * backend the active model selects):
     *
     * - ``Record``   — each input and the output (``__out__``) is recorded.
     * - ``Replay`` / ``Compare`` — inputs are REPLACED by their recordings.
     * - ``ReplayOutput`` — the output is replaced by its recording.
     * - ``None`` — no wrapping; the component is a plain ``wire<G>``.
     *
     * Input keys are the graph's ``NamedPort`` names (``arg_<I>`` for plain
     * ``Port`` params); the fully-qualified recordable id chains through
     * nested components via the mode scope
     * (``outer.inner``, replacing Python's trait copy-down at this level —
     * runtime graph traits still serve nodes inside compiled sub-graphs).
     * The consulted (mode, id) manifests structurally in the wiring, so
     * intern identity is respected per the P3 ruling.
     *
     * Deferred (recorded): ``Compare``'s comparison sink (needs the compare
     * operator over the P6 store) and ``Recover`` (P7 start-time seeding) —
     * both throw for now rather than silently mis-wiring.
     */
    template <typename G, typename... S>
    [[nodiscard]] auto component(Wiring &w, std::string_view recordable_id, Port<S>... inputs)
    {
        using sig    = StaticGraphSignature<G>;
        using params = typename sig::param_types;
        static_assert(sig::scalar_count() == 0,
                      "component<G>: scalar compose parameters are not supported yet (time-series inputs only)");
        static_assert(sizeof...(S) == sig::input_count(),
                      "component<G>: pass exactly the graph's time-series inputs");
        using record_replay::has_mode;
        using record_replay::Mode;

        const auto &ambient = record_replay::current_scope();
        const Mode  mode    = ambient.mode;

        std::string fq;
        if (ambient.recordable_id.empty()) { fq = std::string{recordable_id}; }
        else if (recordable_id.empty()) { fq = ambient.recordable_id; }
        else { fq = ambient.recordable_id + "." + std::string{recordable_id}; }

        if (mode != Mode::None && fq.empty())
        {
            throw std::invalid_argument(
                "component<G>: a recordable id is required under an active record/replay mode");
        }
        if (has_mode(mode, Mode::Compare) || has_mode(mode, Mode::Recover))
        {
            throw std::logic_error(
                "component<G>: Compare/Recover modes are not implemented yet (see the design record)");
        }

        auto input_tuple = std::make_tuple(std::move(inputs)...);
        auto wrapped     = [&]<std::size_t... I>(std::index_sequence<I...>) {
            return std::make_tuple(component_detail::wrap_input(
                w, std::move(std::get<I>(input_tuple)),
                component_detail::input_key<std::tuple_element_t<I, params>, I>(), fq, mode)...);
        }(std::index_sequence_for<S...>{});

        // Nested components chain their ids through the scope (mode carries).
        record_replay::scope nested{mode, fq};

        auto out = std::apply([&](auto &...ports) { return wire<G>(w, ports...); }, wrapped);

        using OutPort = decltype(out);
        if constexpr (!std::is_void_v<OutPort>)
        {
            using OutSchema = typename OutPort::schema;
            if (has_mode(mode, Mode::ReplayOutput))
            {
                out = wire<stdlib::replay, OutSchema>(w, Str{"__out__"}, arg<"recordable_id">(Str{fq}))
                          .template as<OutSchema>();
            }
            if (has_mode(mode, Mode::Record))
            {
                wire<stdlib::record>(w, out, Str{"__out__"}, arg<"recordable_id">(Str{fq}));
            }
            return out;
        }
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_COMPONENT_H
