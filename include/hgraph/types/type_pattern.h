#ifndef HGRAPH_TYPES_TYPE_PATTERN_H
#define HGRAPH_TYPES_TYPE_PATTERN_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/static_schema.h>      // TS / TSS / TSL / TSD / REF / SIGNAL, TsVar / ScalarVar, descriptors
#include <hgraph/types/type_resolution.h>    // ResolutionMap

#include <cstddef>
#include <string>
#include <vector>

namespace hgraph
{
    /**
     * Runtime *type pattern* — the wiring-time form of a (possibly generic) schema
     * used for operator overload **matching** and **ranking**. It is the single
     * representation shared by C++ and (eventually) Python operator candidates: a
     * C++ node's selector schema types are *lowered* into a ``TypePattern`` via
     * ``to_pattern<S>()``; a Python overload builds the same tree directly from its
     * type metadata. One pattern representation, one matcher — see
     * ``docs/source/developer_guide/operators.rst``.
     *
     * A concrete sub-tree collapses to a single ``Concrete`` leaf (the fully-interned
     * registry pointer); only the parts that contain a type variable keep their
     * structure (so e.g. ``TS<Int>`` lowers to ``Concrete`` while
     * ``TS<ScalarVar<"T">>`` lowers to ``TS(ScalarVar("T"))``).
     */

    /** Scalar-layer pattern leaf: a ``ScalarVar`` placeholder or an interned scalar. */
    struct ScalarPattern
    {
        enum class Kind
        {
            Var,
            Concrete
        };

        Kind                     kind{Kind::Concrete};
        std::string              name{};            ///< ``Var``: the variable name.
        const ValueTypeMetaData *meta{nullptr};     ///< ``Concrete``: the interned scalar.

        [[nodiscard]] static ScalarPattern var(std::string name)
        {
            return ScalarPattern{Kind::Var, std::move(name), nullptr};
        }
        [[nodiscard]] static ScalarPattern concrete(const ValueTypeMetaData *meta)
        {
            return ScalarPattern{Kind::Concrete, {}, meta};
        }
    };

    /** Time-series-layer pattern node. */
    struct TypePattern
    {
        enum class Kind
        {
            Var,        ///< a ``TsVar`` (a whole-time-series variable, e.g. ``TIME_SERIES_TYPE``)
            Concrete,   ///< a fully-interned TS leaf
            TS,         ///< ``TS<scalar>``    (scalar held in ``scalar``)
            TSS,        ///< ``TSS<scalar>``   (element held in ``scalar``)
            TSL,        ///< ``TSL<elem, N>``  (element held in ``children[0]``, size in ``fixed_size``)
            TSD,        ///< ``TSD<key, val>`` (key in ``scalar``, value in ``children[0]``)
            REF,        ///< ``REF<target>``   (target in ``children[0]``)
            Signal      ///< ``SIGNAL``
        };

        Kind                       kind{Kind::Signal};
        std::string                name{};          ///< ``Var``: the variable name.
        const TSValueTypeMetaData *meta{nullptr};    ///< ``Concrete``: the interned TS schema.
        ScalarPattern              scalar{};         ///< ``TS`` / ``TSS`` payload, ``TSD`` key.
        std::vector<TypePattern>   children{};       ///< ``TSL`` elem / ``TSD`` value / ``REF`` target.
        std::size_t                fixed_size{0};    ///< ``TSL`` fixed size (0 = dynamic / unconstrained).

        [[nodiscard]] static TypePattern var(std::string name)
        {
            TypePattern p;
            p.kind = Kind::Var;
            p.name = std::move(name);
            return p;
        }
        [[nodiscard]] static TypePattern concrete(const TSValueTypeMetaData *meta)
        {
            TypePattern p;
            p.kind = Kind::Concrete;
            p.meta = meta;
            return p;
        }
        [[nodiscard]] static TypePattern ts(ScalarPattern value)
        {
            TypePattern p;
            p.kind   = Kind::TS;
            p.scalar = std::move(value);
            return p;
        }
        [[nodiscard]] static TypePattern tss(ScalarPattern element)
        {
            TypePattern p;
            p.kind   = Kind::TSS;
            p.scalar = std::move(element);
            return p;
        }
        [[nodiscard]] static TypePattern tsl(TypePattern element, std::size_t fixed_size)
        {
            TypePattern p;
            p.kind       = Kind::TSL;
            p.fixed_size = fixed_size;
            p.children.push_back(std::move(element));
            return p;
        }
        [[nodiscard]] static TypePattern tsd(ScalarPattern key, TypePattern value)
        {
            TypePattern p;
            p.kind   = Kind::TSD;
            p.scalar = std::move(key);
            p.children.push_back(std::move(value));
            return p;
        }
        [[nodiscard]] static TypePattern ref(TypePattern target)
        {
            TypePattern p;
            p.kind = Kind::REF;
            p.children.push_back(std::move(target));
            return p;
        }
        [[nodiscard]] static TypePattern signal()
        {
            TypePattern p;
            p.kind = Kind::Signal;
            return p;
        }
    };

    // -----------------------------------------------------------------
    // The one matcher / ranker / resolver (runtime; shared C++ & Python).
    // -----------------------------------------------------------------

    /**
     * Match ``pattern`` against the concrete interned schema ``concrete``, binding
     * any variable leaves into ``map`` (rejecting an inconsistent re-bind) and
     * verifying concrete leaves. Returns false on any mismatch; ``map`` may be
     * partially populated on failure (callers use a fresh map per candidate).
     */
    [[nodiscard]] HGRAPH_EXPORT bool ts_pattern_match(const TypePattern &pattern, const TSValueTypeMetaData *concrete,
                                                      ResolutionMap &map);

    /** Scalar-layer counterpart of ``ts_pattern_match``. */
    [[nodiscard]] HGRAPH_EXPORT bool scalar_pattern_match(const ScalarPattern &pattern,
                                                          const ValueTypeMetaData *concrete, ResolutionMap &map);

    /** Integer specificity of a pattern (lower = more specific). See *Operators > Ranking*. */
    [[nodiscard]] HGRAPH_EXPORT int ts_pattern_rank(const TypePattern &pattern);

    /** Scalar-layer counterpart of ``ts_pattern_rank``. */
    [[nodiscard]] HGRAPH_EXPORT int scalar_pattern_rank(const ScalarPattern &pattern);

    /** Substitute bound variables and produce the concrete schema, or ``nullptr`` if unbound. */
    [[nodiscard]] HGRAPH_EXPORT const TSValueTypeMetaData *ts_pattern_resolve(const TypePattern &pattern,
                                                                             const ResolutionMap &map);

    /** Scalar-layer counterpart of ``ts_pattern_resolve``. */
    [[nodiscard]] HGRAPH_EXPORT const ValueTypeMetaData *scalar_pattern_resolve(const ScalarPattern &pattern,
                                                                               const ResolutionMap &map);

    /** Human-readable rendering, for error messages. */
    [[nodiscard]] HGRAPH_EXPORT std::string ts_pattern_to_string(const TypePattern &pattern);
    [[nodiscard]] HGRAPH_EXPORT std::string scalar_pattern_to_string(const ScalarPattern &pattern);

    // -----------------------------------------------------------------
    // Compile-time lowering of a static schema type into a runtime pattern.
    // A concrete schema collapses to a single Concrete leaf; otherwise the
    // structure is decomposed, emitting Var nodes for the type variables.
    // -----------------------------------------------------------------

    template <typename S>
    struct ts_pattern_lower;  // specialised per generic schema kind below

    template <typename T>
    struct scalar_pattern_lower;  // specialised for ScalarVar below

    template <typename S>
    [[nodiscard]] inline TypePattern to_pattern()
    {
        if constexpr (schema_descriptor<S>::is_concrete()) { return TypePattern::concrete(schema_descriptor<S>::ts_meta()); }
        else { return ts_pattern_lower<S>::lower(); }
    }

    template <typename T>
    [[nodiscard]] inline ScalarPattern to_scalar_pattern()
    {
        if constexpr (scalar_descriptor<T>::is_concrete()) { return ScalarPattern::concrete(scalar_descriptor<T>::value_meta()); }
        else { return scalar_pattern_lower<T>::lower(); }
    }

    template <fixed_string Name, typename... C>
    struct ts_pattern_lower<TsVar<Name, C...>>
    {
        [[nodiscard]] static TypePattern lower() { return TypePattern::var(std::string{Name.sv()}); }
    };

    template <typename V>
    struct ts_pattern_lower<TS<V>>
    {
        [[nodiscard]] static TypePattern lower() { return TypePattern::ts(to_scalar_pattern<V>()); }
    };

    template <typename V>
    struct ts_pattern_lower<TSS<V>>
    {
        [[nodiscard]] static TypePattern lower() { return TypePattern::tss(to_scalar_pattern<V>()); }
    };

    template <typename C, std::size_t N>
    struct ts_pattern_lower<TSL<C, N>>
    {
        [[nodiscard]] static TypePattern lower() { return TypePattern::tsl(to_pattern<C>(), N); }
    };

    template <typename K, typename V>
    struct ts_pattern_lower<TSD<K, V>>
    {
        [[nodiscard]] static TypePattern lower() { return TypePattern::tsd(to_scalar_pattern<K>(), to_pattern<V>()); }
    };

    template <typename S>
    struct ts_pattern_lower<REF<S>>
    {
        [[nodiscard]] static TypePattern lower() { return TypePattern::ref(to_pattern<S>()); }
    };

    template <fixed_string Name, typename... C>
    struct scalar_pattern_lower<ScalarVar<Name, C...>>
    {
        [[nodiscard]] static ScalarPattern lower() { return ScalarPattern::var(std::string{Name.sv()}); }
    };
}  // namespace hgraph

#endif  // HGRAPH_TYPES_TYPE_PATTERN_H
