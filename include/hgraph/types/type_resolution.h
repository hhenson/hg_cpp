#ifndef HGRAPH_TYPES_TYPE_RESOLUTION_H
#define HGRAPH_TYPES_TYPE_RESOLUTION_H

#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/static_schema.h>

#include <fmt/format.h>

#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace hgraph
{
    /**
     * Wiring-time resolution of type variables (``TsVar`` / ``ScalarVar``) to concrete
     * registry metadata. A generic node is authored once over deferred schemas; at the
     * wiring site each variable is bound — from a connected input port (``unify``), an
     * inferred scalar value, or an explicit ``ts_type<...>()`` — and the node's concrete
     * input / output / scalar schemas are then computed by substituting the bindings
     * into the (otherwise compile-time) schema descriptors (``resolve``).
     *
     * This is the C++ counterpart of the resolution 2603 performs in its Python wiring
     * layer; keeping it in the wiring core lets the eventual Python bridge reuse it and
     * is the seed for generic ``map_`` / ``reduce`` / ``switch_``.
     */
    struct ResolutionMap
    {
        std::unordered_map<std::string, const TSValueTypeMetaData *> ts_vars;
        std::unordered_map<std::string, const ValueTypeMetaData *>   scalar_vars;

        /** Bind time-series variable ``name``; re-binding to a different meta is an error. */
        void bind_ts(std::string_view name, const TSValueTypeMetaData *meta)
        {
            if (meta == nullptr) { throw std::logic_error(fmt::format("type variable '{}' resolved to null", name)); }
            auto [it, inserted] = ts_vars.try_emplace(std::string{name}, meta);
            if (!inserted && it->second != meta)
            {
                throw std::logic_error(fmt::format("type variable '{}' resolved inconsistently", name));
            }
        }

        /** Bind scalar variable ``name``; re-binding to a different meta is an error. */
        void bind_scalar(std::string_view name, const ValueTypeMetaData *meta)
        {
            if (meta == nullptr) { throw std::logic_error(fmt::format("scalar variable '{}' resolved to null", name)); }
            auto [it, inserted] = scalar_vars.try_emplace(std::string{name}, meta);
            if (!inserted && it->second != meta)
            {
                throw std::logic_error(fmt::format("scalar variable '{}' resolved inconsistently", name));
            }
        }

        [[nodiscard]] const TSValueTypeMetaData *ts(std::string_view name) const
        {
            auto it = ts_vars.find(std::string{name});
            if (it == ts_vars.end()) { throw std::logic_error(fmt::format("unresolved type variable '{}'", name)); }
            return it->second;
        }

        [[nodiscard]] const TSValueTypeMetaData *find_ts(std::string_view name) const
        {
            auto it = ts_vars.find(std::string{name});
            return it != ts_vars.end() ? it->second : nullptr;
        }

        [[nodiscard]] const ValueTypeMetaData *scalar(std::string_view name) const
        {
            auto it = scalar_vars.find(std::string{name});
            if (it == scalar_vars.end()) { throw std::logic_error(fmt::format("unresolved scalar variable '{}'", name)); }
            return it->second;
        }

        [[nodiscard]] const ValueTypeMetaData *find_scalar(std::string_view name) const
        {
            auto it = scalar_vars.find(std::string{name});
            return it != scalar_vars.end() ? it->second : nullptr;
        }
    };

    // -----------------------------------------------------------------
    // scalar_resolver<T> — resolve a scalar schema type to its value meta,
    // substituting a ``ScalarVar`` leaf from the map.
    // -----------------------------------------------------------------
    template <typename T>
    struct scalar_resolver
    {
        [[nodiscard]] static const ValueTypeMetaData *resolve(const ResolutionMap &) { return scalar_descriptor<T>::value_meta(); }
    };

    template <fixed_string Name, typename... C>
    struct scalar_resolver<ScalarVar<Name, C...>>
    {
        [[nodiscard]] static const ValueTypeMetaData *resolve(const ResolutionMap &m) { return m.scalar(Name.sv()); }
    };

    // -----------------------------------------------------------------
    // ts_resolver<S> — resolve a time-series schema type to its TS meta,
    // substituting any ``TsVar`` / ``ScalarVar`` leaves. The primary template
    // is the concrete fall-back (used for SIGNAL / TSB / anything without a
    // var); the composite kinds are specialised so they recurse.
    // -----------------------------------------------------------------
    template <typename S>
    struct ts_resolver
    {
        [[nodiscard]] static const TSValueTypeMetaData *resolve(const ResolutionMap &) { return schema_descriptor<S>::ts_meta(); }
    };

    template <fixed_string Name, typename... C>
    struct ts_resolver<TsVar<Name, C...>>
    {
        [[nodiscard]] static const TSValueTypeMetaData *resolve(const ResolutionMap &m) { return m.ts(Name.sv()); }
    };

    template <typename V>
    struct ts_resolver<TS<V>>
    {
        [[nodiscard]] static const TSValueTypeMetaData *resolve(const ResolutionMap &m)
        {
            return TypeRegistry::instance().ts(scalar_resolver<V>::resolve(m));
        }
    };

    template <typename V>
    struct ts_resolver<TSS<V>>
    {
        [[nodiscard]] static const TSValueTypeMetaData *resolve(const ResolutionMap &m)
        {
            return TypeRegistry::instance().tss(scalar_resolver<V>::resolve(m));
        }
    };

    template <typename C, std::size_t N>
    struct ts_resolver<TSL<C, N>>
    {
        [[nodiscard]] static const TSValueTypeMetaData *resolve(const ResolutionMap &m)
        {
            return TypeRegistry::instance().tsl(ts_resolver<C>::resolve(m), N);
        }
    };

    template <typename V, std::size_t Period, std::size_t MinPeriod>
    struct ts_resolver<TSW<V, Period, MinPeriod>>
    {
        [[nodiscard]] static const TSValueTypeMetaData *resolve(const ResolutionMap &m)
        {
            return TypeRegistry::instance().tsw(scalar_resolver<V>::resolve(m), Period, MinPeriod);
        }
    };

    template <typename K, typename V>
    struct ts_resolver<TSD<K, V>>
    {
        [[nodiscard]] static const TSValueTypeMetaData *resolve(const ResolutionMap &m)
        {
            return TypeRegistry::instance().tsd(scalar_resolver<K>::resolve(m), ts_resolver<V>::resolve(m));
        }
    };

    template <typename... Fields>
    struct ts_resolver<UnNamedTSB<Fields...>>
    {
        [[nodiscard]] static const TSValueTypeMetaData *resolve(const ResolutionMap &m)
        {
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
            fields.reserve(sizeof...(Fields));
            (fields.emplace_back(ts_field_descriptor<Fields>::field_name(),
                                 ts_resolver<typename ts_field_descriptor<Fields>::schema>::resolve(m)),
             ...);
            return TypeRegistry::instance().un_named_tsb(fields);
        }
    };

    template <fixed_string Name, typename... Fields>
    struct ts_resolver<TSB<Name, Fields...>>
    {
        [[nodiscard]] static const TSValueTypeMetaData *resolve(const ResolutionMap &m)
        {
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
            fields.reserve(sizeof...(Fields));
            (fields.emplace_back(ts_field_descriptor<Fields>::field_name(),
                                 ts_resolver<typename ts_field_descriptor<Fields>::schema>::resolve(m)),
             ...);
            return TypeRegistry::instance().tsb(Name.sv(), fields);
        }
    };

    template <typename S>
    struct ts_resolver<REF<S>>
    {
        [[nodiscard]] static const TSValueTypeMetaData *resolve(const ResolutionMap &m)
        {
            return TypeRegistry::instance().ref(ts_resolver<S>::resolve(m));
        }
    };

    // -----------------------------------------------------------------
    // scalar_unifier<T> / ts_unifier<S> — bind variables by matching a
    // pattern schema type against a concrete runtime meta (e.g. a connected
    // input port's schema). A concrete leaf is a no-op (structural match is
    // already enforced at compile time on the concrete wiring path).
    // -----------------------------------------------------------------
    template <typename T>
    struct scalar_unifier
    {
        static void unify(const ValueTypeMetaData *, ResolutionMap &) noexcept {}
    };

    template <fixed_string Name, typename... C>
    struct scalar_unifier<ScalarVar<Name, C...>>
    {
        static void unify(const ValueTypeMetaData *concrete, ResolutionMap &m) { m.bind_scalar(Name.sv(), concrete); }
    };

    template <typename S>
    struct ts_unifier
    {
        static void unify(const TSValueTypeMetaData *, ResolutionMap &) noexcept {}
    };

    template <fixed_string Name, typename... C>
    struct ts_unifier<TsVar<Name, C...>>
    {
        static void unify(const TSValueTypeMetaData *concrete, ResolutionMap &m) { m.bind_ts(Name.sv(), concrete); }
    };

    template <typename V>
    struct ts_unifier<TS<V>>
    {
        static void unify(const TSValueTypeMetaData *c, ResolutionMap &m)
        {
            scalar_unifier<V>::unify(c != nullptr ? c->value_schema : nullptr, m);
        }
    };

    template <typename V>
    struct ts_unifier<TSS<V>>
    {
        static void unify(const TSValueTypeMetaData *c, ResolutionMap &m)
        {
            const ValueTypeMetaData *elem = (c != nullptr && c->value_schema != nullptr) ? c->value_schema->element_type : nullptr;
            scalar_unifier<V>::unify(elem, m);
        }
    };

    template <typename C, std::size_t N>
    struct ts_unifier<TSL<C, N>>
    {
        static void unify(const TSValueTypeMetaData *c, ResolutionMap &m)
        {
            ts_unifier<C>::unify(c != nullptr ? c->element_ts() : nullptr, m);
        }
    };

    template <typename K, typename V>
    struct ts_unifier<TSD<K, V>>
    {
        static void unify(const TSValueTypeMetaData *c, ResolutionMap &m)
        {
            scalar_unifier<K>::unify(c != nullptr ? c->key_type() : nullptr, m);
            ts_unifier<V>::unify(c != nullptr ? c->element_ts() : nullptr, m);
        }
    };

    template <typename S>
    struct ts_unifier<REF<S>>
    {
        static void unify(const TSValueTypeMetaData *c, ResolutionMap &m)
        {
            if (c != nullptr && c->kind == TSTypeKind::REF) { c = c->referenced_ts(); }
            ts_unifier<S>::unify(c, m);
        }
    };

    template <typename V, std::size_t Period, std::size_t MinPeriod>
    struct ts_unifier<TSW<V, Period, MinPeriod>>
    {
        static void unify(const TSValueTypeMetaData *c, ResolutionMap &m)
        {
            scalar_unifier<V>::unify(c != nullptr && c->kind == TSTypeKind::TSW ? c->value_type : nullptr, m);
        }
    };

    namespace type_resolution_detail
    {
        template <typename Field>
        void unify_tsb_field(const TSValueTypeMetaData *c, ResolutionMap &m)
        {
            if (c == nullptr || c->kind != TSTypeKind::TSB) { return; }
            const std::string expected_name = ts_field_descriptor<Field>::field_name();
            for (std::size_t i = 0; i < c->field_count(); ++i)
            {
                const TSFieldMetaData &field = c->fields()[i];
                if (field.name != nullptr && expected_name == field.name)
                {
                    ts_unifier<typename ts_field_descriptor<Field>::schema>::unify(field.type, m);
                    return;
                }
            }
        }
    }  // namespace type_resolution_detail

    template <typename... Fields>
    struct ts_unifier<UnNamedTSB<Fields...>>
    {
        static void unify(const TSValueTypeMetaData *c, ResolutionMap &m)
        {
            (type_resolution_detail::unify_tsb_field<Fields>(c, m), ...);
        }
    };

    template <fixed_string Name, typename... Fields>
    struct ts_unifier<TSB<Name, Fields...>>
    {
        static void unify(const TSValueTypeMetaData *c, ResolutionMap &m)
        {
            (type_resolution_detail::unify_tsb_field<Fields>(c, m), ...);
        }
    };

    // -----------------------------------------------------------------
    // Explicit type-argument helpers (source-side resolution): supply a
    // concrete schema for a variable that cannot be inferred from inputs
    // (e.g. ``replay``'s output). ``wire<replay, TS<Int>>(w, key)``.
    // -----------------------------------------------------------------
    template <typename S>
    [[nodiscard]] inline const TSValueTypeMetaData *ts_type()
    {
        return schema_descriptor<S>::ts_meta();
    }

    template <typename T>
    [[nodiscard]] inline const ValueTypeMetaData *scalar_type()
    {
        return scalar_descriptor<T>::value_meta();
    }
}  // namespace hgraph

#endif  // HGRAPH_TYPES_TYPE_RESOLUTION_H
