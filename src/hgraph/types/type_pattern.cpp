#include <hgraph/types/type_pattern.h>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/endpoint_schema.h>  // time_series_schema_equivalent

#include <fmt/format.h>

namespace hgraph
{
    namespace
    {
        // A bare time-series variable (``TIME_SERIES_TYPE``) is always the least
        // specific match: strictly larger than any var nested inside a collection of
        // realistic depth. This reproduces the 2603 rule that a top-level generic is
        // less specific than a generic *inside* a structure (recursively).
        constexpr int LARGE_RANK = 10000;

        // Nested scalar genericness (inside TS / TSS / TSD): a scalar variable counts
        // far less than a bare time-series variable, but more than a concrete leaf.
        constexpr int SCALAR_VAR_RANK = 100;
    }  // namespace

    bool scalar_pattern_match(const ScalarPattern &pattern, const ValueTypeMetaData *concrete, ResolutionMap &map)
    {
        if (concrete == nullptr) { return false; }
        switch (pattern.kind)
        {
            case ScalarPattern::Kind::Var:
            {
                if (const ValueTypeMetaData *bound = map.find_scalar(pattern.name)) { return bound == concrete; }
                map.bind_scalar(pattern.name, concrete);
                return true;
            }
            case ScalarPattern::Kind::Concrete: return pattern.meta == concrete;  // interned: pointer identity
        }
        return false;
    }

    bool ts_pattern_match(const TypePattern &pattern, const TSValueTypeMetaData *concrete, ResolutionMap &map)
    {
        if (concrete == nullptr) { return false; }
        switch (pattern.kind)
        {
            case TypePattern::Kind::Var:
            {
                if (const TSValueTypeMetaData *bound = map.find_ts(pattern.name)) { return bound == concrete; }
                map.bind_ts(pattern.name, concrete);
                return true;
            }
            case TypePattern::Kind::Concrete: return time_series_schema_equivalent(pattern.meta, concrete);
            case TypePattern::Kind::TS:
                return concrete->kind == TSTypeKind::TS && scalar_pattern_match(pattern.scalar, concrete->value_schema, map);
            case TypePattern::Kind::TSS:
                return concrete->kind == TSTypeKind::TSS && concrete->value_schema != nullptr &&
                       scalar_pattern_match(pattern.scalar, concrete->value_schema->element_type, map);
            case TypePattern::Kind::TSL:
                if (concrete->kind != TSTypeKind::TSL) { return false; }
                if (pattern.fixed_size != 0 && pattern.fixed_size != concrete->fixed_size()) { return false; }
                return ts_pattern_match(pattern.children[0], concrete->element_ts(), map);
            case TypePattern::Kind::TSD:
                return concrete->kind == TSTypeKind::TSD && scalar_pattern_match(pattern.scalar, concrete->key_type(), map) &&
                       ts_pattern_match(pattern.children[0], concrete->element_ts(), map);
            case TypePattern::Kind::REF:
                return concrete->kind == TSTypeKind::REF && ts_pattern_match(pattern.children[0], concrete->referenced_ts(), map);
            case TypePattern::Kind::Signal: return concrete->kind == TSTypeKind::SIGNAL;
        }
        return false;
    }

    int scalar_pattern_rank(const ScalarPattern &pattern)
    {
        return pattern.kind == ScalarPattern::Kind::Var ? SCALAR_VAR_RANK : 0;
    }

    int ts_pattern_rank(const TypePattern &pattern)
    {
        switch (pattern.kind)
        {
            case TypePattern::Kind::Var: return LARGE_RANK;
            case TypePattern::Kind::Concrete: return 0;
            case TypePattern::Kind::TS: return 1 + scalar_pattern_rank(pattern.scalar);
            case TypePattern::Kind::TSS: return 1 + scalar_pattern_rank(pattern.scalar);
            case TypePattern::Kind::TSL: return 1 + ts_pattern_rank(pattern.children[0]);
            case TypePattern::Kind::TSD: return 1 + scalar_pattern_rank(pattern.scalar) + ts_pattern_rank(pattern.children[0]);
            case TypePattern::Kind::REF: return ts_pattern_rank(pattern.children[0]);
            case TypePattern::Kind::Signal: return 0;
        }
        return 0;
    }

    const ValueTypeMetaData *scalar_pattern_resolve(const ScalarPattern &pattern, const ResolutionMap &map)
    {
        switch (pattern.kind)
        {
            case ScalarPattern::Kind::Var: return map.find_scalar(pattern.name);
            case ScalarPattern::Kind::Concrete: return pattern.meta;
        }
        return nullptr;
    }

    const TSValueTypeMetaData *ts_pattern_resolve(const TypePattern &pattern, const ResolutionMap &map)
    {
        TypeRegistry &registry = TypeRegistry::instance();
        switch (pattern.kind)
        {
            case TypePattern::Kind::Var: return map.find_ts(pattern.name);
            case TypePattern::Kind::Concrete: return pattern.meta;
            case TypePattern::Kind::TS:
            {
                const ValueTypeMetaData *value = scalar_pattern_resolve(pattern.scalar, map);
                return value != nullptr ? registry.ts(value) : nullptr;
            }
            case TypePattern::Kind::TSS:
            {
                const ValueTypeMetaData *element = scalar_pattern_resolve(pattern.scalar, map);
                return element != nullptr ? registry.tss(element) : nullptr;
            }
            case TypePattern::Kind::TSL:
            {
                const TSValueTypeMetaData *element = ts_pattern_resolve(pattern.children[0], map);
                return element != nullptr ? registry.tsl(element, pattern.fixed_size) : nullptr;
            }
            case TypePattern::Kind::TSD:
            {
                const ValueTypeMetaData   *key   = scalar_pattern_resolve(pattern.scalar, map);
                const TSValueTypeMetaData *value = ts_pattern_resolve(pattern.children[0], map);
                return (key != nullptr && value != nullptr) ? registry.tsd(key, value) : nullptr;
            }
            case TypePattern::Kind::REF:
            {
                const TSValueTypeMetaData *target = ts_pattern_resolve(pattern.children[0], map);
                return target != nullptr ? registry.ref(target) : nullptr;
            }
            case TypePattern::Kind::Signal: return registry.signal();
        }
        return nullptr;
    }

    std::string scalar_pattern_to_string(const ScalarPattern &pattern)
    {
        switch (pattern.kind)
        {
            case ScalarPattern::Kind::Var: return "~" + pattern.name;
            case ScalarPattern::Kind::Concrete:
                return (pattern.meta != nullptr && pattern.meta->display_name != nullptr)
                           ? std::string{pattern.meta->display_name}
                           : std::string{"scalar"};
        }
        return "?";
    }

    std::string ts_pattern_to_string(const TypePattern &pattern)
    {
        switch (pattern.kind)
        {
            case TypePattern::Kind::Var: return "~" + pattern.name;
            case TypePattern::Kind::Concrete:
                return (pattern.meta != nullptr && pattern.meta->display_name != nullptr)
                           ? std::string{pattern.meta->display_name}
                           : std::string{"TS"};
            case TypePattern::Kind::TS: return fmt::format("TS[{}]", scalar_pattern_to_string(pattern.scalar));
            case TypePattern::Kind::TSS: return fmt::format("TSS[{}]", scalar_pattern_to_string(pattern.scalar));
            case TypePattern::Kind::TSL:
                return fmt::format("TSL[{}, {}]", ts_pattern_to_string(pattern.children[0]), pattern.fixed_size);
            case TypePattern::Kind::TSD:
                return fmt::format("TSD[{}, {}]", scalar_pattern_to_string(pattern.scalar),
                                   ts_pattern_to_string(pattern.children[0]));
            case TypePattern::Kind::REF: return fmt::format("REF[{}]", ts_pattern_to_string(pattern.children[0]));
            case TypePattern::Kind::Signal: return "SIGNAL";
        }
        return "?";
    }
}  // namespace hgraph
