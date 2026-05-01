#ifndef HGRAPH_CPP_ROOT_TIME_SERIES_REFERENCE_H
#define HGRAPH_CPP_ROOT_TIME_SERIES_REFERENCE_H

#include <hgraph/hgraph_export.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace hgraph
{
    struct TSValueTypeMetaData;

    /**
     * Runtime value of a REF time-series.
     *
     * A ``TimeSeriesReference`` carries the binding state needed to point
     * at (or compose references to) another time-series. It is the C++
     * value type that backs the ``TimeSeriesReference`` atomic schema in
     * the type registry, so a value of any ``REF`` schema holds an instance
     * of this struct.
     *
     * Three kinds are supported:
     *
     * - **EMPTY** — unbound; the reference points at nothing yet. Still
     *   carries a target schema where one is known, so a later binding
     *   attempt can be validated.
     * - **PEERED** — directly bound to a single time-series target. The
     *   target binding payload (output handle / linked context) is
     *   populated by the runtime layer when it lands; today the kind and
     *   target schema are tracked for validation.
     * - **NON_PEERED** — composite reference whose target is itself a
     *   composite time-series (``REF<TSL<T>>``, ``REF<TSB<...>>``, etc.).
     *   Holds a vector of sub-references, one per structural slot of the
     *   composite.
     *
     * The ``target_schema`` pointer records the TS schema (the ``T`` in
     * the surrounding ``REF<T>``) the reference is intended to bind to.
     * It is the basis for binding-time validation: a candidate output's
     * schema must match (or be dereference-compatible with)
     * ``target_schema`` for the binding to succeed.
     */
    struct HGRAPH_EXPORT TimeSeriesReference
    {
        /** The reference's kind discriminator. */
        enum class Kind : uint8_t
        {
            EMPTY      = 0,
            PEERED     = 1,
            NON_PEERED = 2,
        };

        /** Default-construct an EMPTY reference with no target schema. */
        TimeSeriesReference() noexcept = default;

        /** Construct an EMPTY reference that records its expected target schema. */
        explicit TimeSeriesReference(const TSValueTypeMetaData *target_schema) noexcept;

        /**
         * Construct a NON_PEERED composite reference. ``target_schema``
         * describes the composite TS kind (typically ``TSL`` or ``TSB``);
         * ``items`` holds one sub-reference per structural slot of the
         * composite.
         */
        TimeSeriesReference(const TSValueTypeMetaData *target_schema, std::vector<TimeSeriesReference> items);

        /**
         * Build a PEERED reference. The target binding payload is filled
         * in by the runtime layer; for now only the kind and target
         * schema are recorded.
         */
        [[nodiscard]] static TimeSeriesReference peered(const TSValueTypeMetaData *target_schema);

        /** Discriminator: EMPTY / PEERED / NON_PEERED. */
        [[nodiscard]] Kind kind() const noexcept { return kind_; }
        /** True when ``kind() == Kind::EMPTY``. */
        [[nodiscard]] bool is_empty() const noexcept { return kind_ == Kind::EMPTY; }
        /** True when ``kind() == Kind::PEERED``. */
        [[nodiscard]] bool is_peered() const noexcept { return kind_ == Kind::PEERED; }
        /** True when ``kind() == Kind::NON_PEERED``. */
        [[nodiscard]] bool is_non_peered() const noexcept { return kind_ == Kind::NON_PEERED; }

        /**
         * Schema describing the TS type this reference is pointed at — the
         * ``T`` in the surrounding ``REF<T>``. May be null when the
         * reference is unconstrained.
         */
        [[nodiscard]] const TSValueTypeMetaData *target_schema() const noexcept { return target_schema_; }

        /** Sub-references for a NON_PEERED reference. Throws otherwise. */
        [[nodiscard]] const std::vector<TimeSeriesReference> &items() const;
        /** Indexed sub-reference. Throws if not NON_PEERED or if ``index`` is out of range. */
        [[nodiscard]] const TimeSeriesReference &operator[](size_t index) const;

        /**
         * Two references compare equal when they share kind, target schema,
         * and (for NON_PEERED) sub-references.
         */
        [[nodiscard]] bool        operator==(const TimeSeriesReference &other) const noexcept;
        /** Hash compatible with ``std::hash<TimeSeriesReference>``. */
        [[nodiscard]] std::size_t hash() const noexcept;
        /** Human-readable representation; primarily for diagnostics. */
        [[nodiscard]] std::string to_string() const;

        /** Singleton empty reference with no target schema. */
        [[nodiscard]] static const TimeSeriesReference &empty_reference() noexcept;

      private:
        Kind                             kind_{Kind::EMPTY};
        const TSValueTypeMetaData       *target_schema_{nullptr};
        std::vector<TimeSeriesReference> items_{};
        // PEERED target binding payload (LinkedTSContext / output handle /
        // observed_time / invalidator) is added when the runtime layer is
        // ported. Today PEERED is a tag carrying only kind + target_schema.
    };
}  // namespace hgraph

namespace std
{
    /** Hash specialization so ``TimeSeriesReference`` works in unordered containers. */
    template <> struct hash<hgraph::TimeSeriesReference>
    {
        std::size_t operator()(const hgraph::TimeSeriesReference &ref) const noexcept { return ref.hash(); }
    };
}  // namespace std

#endif  // HGRAPH_CPP_ROOT_TIME_SERIES_REFERENCE_H
