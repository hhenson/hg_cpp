#ifndef HGRAPH_TYPES_RECORD_REPLAY_H
#define HGRAPH_TYPES_RECORD_REPLAY_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/util/date_time.h>

#include <optional>
#include <string>
#include <string_view>

namespace hgraph::record_replay
{
    /**
     * Record/replay wiring configuration and mode scope (design record:
     * *Record/replay, tables and const_fn*, P2/P3 — rulings 2026-07-04).
     *
     * Python drives this through mutable GlobalState magic keys read at
     * wiring time; here it is EXPLICIT wiring-time configuration: set the
     * config before wiring, backends select on it through ordinary
     * ``requires_`` predicates, and the mode rides a wiring-scope stack (the
     * mesh/context-scope machinery). Changing the model mid-wiring is
     * deliberately unsupported (the ruling) — the imperative Python setters
     * become bridge shims over this API.
     */

    /** The record/replay modes (a flag set, mirroring Python's ``RecordReplayEnum``). */
    enum class Mode : unsigned
    {
        None         = 0,
        Record       = 1u << 0,
        Replay       = 1u << 1,
        Compare      = 1u << 2,
        ReplayOutput = 1u << 3,
        Reset        = 1u << 4,
        Recover      = 1u << 5,
    };

    [[nodiscard]] constexpr Mode operator|(Mode lhs, Mode rhs) noexcept
    {
        return static_cast<Mode>(static_cast<unsigned>(lhs) | static_cast<unsigned>(rhs));
    }

    [[nodiscard]] constexpr Mode operator&(Mode lhs, Mode rhs) noexcept
    {
        return static_cast<Mode>(static_cast<unsigned>(lhs) & static_cast<unsigned>(rhs));
    }

    /** True when every bit of ``flag`` is set in ``mode``. */
    [[nodiscard]] constexpr bool has_mode(Mode mode, Mode flag) noexcept
    {
        return (static_cast<unsigned>(mode) & static_cast<unsigned>(flag)) == static_cast<unsigned>(flag) &&
               flag != Mode::None;
    }

    /** The default (in-memory GlobalState buffer) record/replay model. */
    inline constexpr std::string_view IN_MEMORY = "InMemory";

    /**
     * Explicit wiring-time configuration. ``model`` selects the backend
     * (overloads guard on it via ``requires_``); the date / as-of keys name
     * the bitemporal table columns; ``as_of`` overrides the as-of time
     * (unset = the evaluation clock).
     */
    struct Config
    {
        std::string             model{IN_MEMORY};
        std::string             date_key{"__date_time__"};
        std::string             as_of_key{"__as_of__"};
        std::optional<DateTime> as_of{};
    };

    /** Set the configuration (before wiring; a registry reset restores the default). */
    HGRAPH_EXPORT void set_config(Config config);

    /** The active configuration. */
    [[nodiscard]] HGRAPH_EXPORT const Config &config();

    /** ``requires_``-friendly backend guard: true when the active model is ``model``. */
    [[nodiscard]] HGRAPH_EXPORT bool model_is(std::string_view model) noexcept;

    /**
     * The mode scope: a wiring-time stack of ``(mode, recordable_id)``
     * (Python's ``RecordReplayContext``). Anything that consults the ambient
     * scope while wiring MUST fold what it consulted into its intern
     * identity (the ``MapCallConfig`` precedent) — identical calls under
     * different modes are distinct instances.
     */
    struct ScopeState
    {
        Mode        mode{Mode::None};
        std::string recordable_id{};
    };

    /** The innermost scope (``Mode::None`` + empty id when no scope is active). */
    [[nodiscard]] HGRAPH_EXPORT const ScopeState &current_scope() noexcept;

    /** RAII mode scope. */
    class HGRAPH_EXPORT scope
    {
      public:
        explicit scope(Mode mode, std::string recordable_id = {});
        scope(const scope &)            = delete;
        scope &operator=(const scope &) = delete;
        scope(scope &&)                 = delete;
        scope &operator=(scope &&)      = delete;
        ~scope();
    };

    /** Reset config + scopes to defaults (wired into ``reset_all_registries``). */
    HGRAPH_EXPORT void reset() noexcept;
}  // namespace hgraph::record_replay

namespace hgraph
{
    class GraphView;

    namespace record_replay
    {
        /** The graph trait carrying a recordable id (a ``Str`` value). */
        inline constexpr std::string_view RECORDABLE_ID_TRAIT = "recordable_id";

        /** True when the graph (or a parent) carries a recordable id trait. */
        [[nodiscard]] HGRAPH_EXPORT bool has_recordable_id(const GraphView &graph) noexcept;

        /**
         * Resolve the fully-qualified recordable id (Python's
         * ``get_fq_recordable_id``): the parent chain's ``recordable_id``
         * trait joined to the local id with ``.``. With no parent trait the
         * local id must be non-empty (throws otherwise); with a parent trait
         * an empty local id resolves to the parent id alone.
         */
        [[nodiscard]] HGRAPH_EXPORT std::string fq_recordable_id(const GraphView &graph,
                                                                 std::string_view recordable_id);
    }  // namespace record_replay
}  // namespace hgraph

#endif  // HGRAPH_TYPES_RECORD_REPLAY_H
