#include <hgraph/types/type_pattern.h>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/endpoint_schema.h>  // time_series_schema_equivalent

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <optional>

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

        [[nodiscard]] bool scalar_allowed_by_constraints(const ScalarPattern &pattern,
                                                         const ValueTypeMetaData *concrete)
        {
            if (pattern.constraints.empty()) { return true; }
            return std::ranges::any_of(pattern.constraints, [concrete](const ValueTypeMetaData *constraint) {
                return constraint != nullptr && constraint == concrete;
            });
        }

        [[nodiscard]] bool ts_allowed_by_constraints(const TypePattern &pattern,
                                                     const TSValueTypeMetaData *concrete)
        {
            if (pattern.constraints.empty()) { return true; }
            return std::ranges::any_of(pattern.constraints, [concrete](const TSValueTypeMetaData *constraint) {
                return constraint != nullptr && time_series_schema_equivalent(constraint, concrete);
            });
        }

        [[nodiscard]] bool size_allowed_by_constraints(const TypePattern &pattern, std::size_t concrete)
        {
            if (pattern.size_constraints.empty()) { return true; }
            return std::ranges::any_of(pattern.size_constraints, [concrete](std::size_t constraint) {
                return constraint == concrete;
            });
        }
    }  // namespace

    bool scalar_pattern_match(const ScalarPattern &pattern, const ValueTypeMetaData *concrete, ResolutionMap &map)
    {
        if (concrete == nullptr) { return false; }
        switch (pattern.kind)
        {
            case ScalarPattern::Kind::Var:
            {
                if (const ValueTypeMetaData *bound = map.find_scalar(pattern.name))
                {
                    return bound == concrete && scalar_allowed_by_constraints(pattern, concrete);
                }
                if (!scalar_allowed_by_constraints(pattern, concrete)) { return false; }
                map.bind_scalar(pattern.name, concrete);
                return true;
            }
            case ScalarPattern::Kind::Concrete: return pattern.meta == concrete;  // interned: pointer identity
        }
        return false;
    }

    bool size_pattern_match(const TypePattern &pattern, std::size_t concrete_size, ResolutionMap &map)
    {
        if (!pattern.size_var)
        {
            return pattern.fixed_size == 0 || pattern.fixed_size == concrete_size;
        }

        if (const std::optional<std::size_t> bound = map.find_size(pattern.size_name))
        {
            return *bound == concrete_size && size_allowed_by_constraints(pattern, concrete_size);
        }
        if (!size_allowed_by_constraints(pattern, concrete_size)) { return false; }
        map.bind_size(pattern.size_name, concrete_size);
        return true;
    }

    bool output_ts_pattern_match(const TypePattern &pattern,
                                 const TSValueTypeMetaData *concrete,
                                 ResolutionMap &map)
    {
        if (pattern.kind == TypePattern::Kind::Var && concrete != nullptr && concrete->kind == TSTypeKind::REF)
        {
            // OUTPUT direction: no REF transparency for a top-level variable -
            // the caller asked for a reference, so the produced port must BE
            // one. (Input-side transparency stands: consumers of value ports
            // adapt at input binding.)
            if (const TSValueTypeMetaData *bound = map.find_ts(pattern.name))
            {
                auto &registry = TypeRegistry::instance();
                return time_series_schema_equivalent(registry.dereference(bound), registry.dereference(concrete));
            }
            if (!ts_allowed_by_constraints(pattern, concrete)) { return false; }
            map.bind_ts(pattern.name, concrete);
            return true;
        }
        return ts_pattern_match(pattern, concrete, map);
    }

    bool ts_pattern_match(const TypePattern &pattern, const TSValueTypeMetaData *concrete, ResolutionMap &map)
    {
        if (concrete == nullptr) { return false; }
        // REF is transparent to matching (Python parity: ``REF[X]`` is
        // type-compatible with ``X``; consumers bind through the reference at
        // runtime — a type variable binds the *dereferenced* schema). A
        // reference schema only matches *as* a reference when the pattern asks
        // for one explicitly.
        if (pattern.kind != TypePattern::Kind::REF && concrete->kind == TSTypeKind::REF)
        {
            return ts_pattern_match(pattern, concrete->referenced_ts(), map);
        }
        switch (pattern.kind)
        {
            case TypePattern::Kind::Var:
            {
                if (const TSValueTypeMetaData *bound = map.find_ts(pattern.name))
                {
                    return bound == concrete && ts_allowed_by_constraints(pattern, concrete);
                }
                if (!ts_allowed_by_constraints(pattern, concrete)) { return false; }
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
                if (!size_pattern_match(pattern, concrete->fixed_size(), map)) { return false; }
                return ts_pattern_match(pattern.children[0], concrete->element_ts(), map);
            case TypePattern::Kind::TSD:
                return concrete->kind == TSTypeKind::TSD && scalar_pattern_match(pattern.scalar, concrete->key_type(), map) &&
                       ts_pattern_match(pattern.children[0], concrete->element_ts(), map);
            case TypePattern::Kind::TSW:
                return concrete->kind == TSTypeKind::TSW && !concrete->is_duration_based() &&
                       pattern.fixed_size == concrete->period() && pattern.min_size == concrete->min_period() &&
                       scalar_pattern_match(pattern.scalar, concrete->value_type, map);
            case TypePattern::Kind::TSB:
                if (concrete->kind != TSTypeKind::TSB) { return false; }
                if (pattern.named_bundle)
                {
                    if (!concrete->is_named_tsb() || concrete->bundle_name() == nullptr ||
                        pattern.bundle_name != concrete->bundle_name())
                    {
                        return false;
                    }
                }
                if (concrete->field_count() != pattern.children.size()) { return false; }
                for (std::size_t i = 0; i < pattern.children.size(); ++i)
                {
                    const TSFieldMetaData &field = concrete->fields()[i];
                    if (field.name == nullptr || pattern.field_names[i] != field.name) { return false; }
                    if (!ts_pattern_match(pattern.children[i], field.type, map)) { return false; }
                }
                return true;
            case TypePattern::Kind::REF:
                return concrete->kind == TSTypeKind::REF && ts_pattern_match(pattern.children[0], concrete->referenced_ts(), map);
            case TypePattern::Kind::Signal: return concrete->kind == TSTypeKind::SIGNAL;
        }
        return false;
    }

    int scalar_pattern_rank(const ScalarPattern &pattern)
    {
        if (pattern.kind != ScalarPattern::Kind::Var) { return 0; }
        return pattern.constraints.empty() ? SCALAR_VAR_RANK : SCALAR_VAR_RANK / 2;
    }

    int ts_pattern_rank(const TypePattern &pattern)
    {
        switch (pattern.kind)
        {
            case TypePattern::Kind::Var: return pattern.constraints.empty() ? LARGE_RANK : LARGE_RANK / 2;
            case TypePattern::Kind::Concrete: return 0;
            case TypePattern::Kind::TS: return 1 + scalar_pattern_rank(pattern.scalar);
            case TypePattern::Kind::TSS: return 1 + scalar_pattern_rank(pattern.scalar);
            case TypePattern::Kind::TSL:
                return 1 + ts_pattern_rank(pattern.children[0]) +
                       (pattern.size_var ? 5 : pattern.fixed_size == 0 ? 10 : 0);
            case TypePattern::Kind::TSD: return 1 + scalar_pattern_rank(pattern.scalar) + ts_pattern_rank(pattern.children[0]);
            case TypePattern::Kind::TSW: return 1 + scalar_pattern_rank(pattern.scalar);
            case TypePattern::Kind::TSB:
            {
                int rank = 1;
                for (const TypePattern &child : pattern.children) { rank += ts_pattern_rank(child); }
                return rank;
            }
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
                if (element == nullptr) { return nullptr; }
                std::size_t size = pattern.fixed_size;
                if (pattern.size_var)
                {
                    const std::optional<std::size_t> bound = map.find_size(pattern.size_name);
                    if (!bound.has_value()) { return nullptr; }
                    size = *bound;
                }
                return registry.tsl(element, size);
            }
            case TypePattern::Kind::TSD:
            {
                const ValueTypeMetaData   *key   = scalar_pattern_resolve(pattern.scalar, map);
                const TSValueTypeMetaData *value = ts_pattern_resolve(pattern.children[0], map);
                return (key != nullptr && value != nullptr) ? registry.tsd(key, value) : nullptr;
            }
            case TypePattern::Kind::TSW:
            {
                const ValueTypeMetaData *element = scalar_pattern_resolve(pattern.scalar, map);
                return element != nullptr ? registry.tsw(element, pattern.fixed_size, pattern.min_size) : nullptr;
            }
            case TypePattern::Kind::TSB:
            {
                std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
                fields.reserve(pattern.children.size());
                for (std::size_t i = 0; i < pattern.children.size(); ++i)
                {
                    const TSValueTypeMetaData *child = ts_pattern_resolve(pattern.children[i], map);
                    if (child == nullptr) { return nullptr; }
                    fields.emplace_back(pattern.field_names[i], child);
                }
                return pattern.named_bundle ? registry.tsb(pattern.bundle_name, fields) : registry.un_named_tsb(fields);
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
                return fmt::format("TSL[{}, {}]",
                                   ts_pattern_to_string(pattern.children[0]),
                                   pattern.size_var ? "~" + pattern.size_name : std::to_string(pattern.fixed_size));
            case TypePattern::Kind::TSD:
                return fmt::format("TSD[{}, {}]", scalar_pattern_to_string(pattern.scalar),
                                   ts_pattern_to_string(pattern.children[0]));
            case TypePattern::Kind::TSW:
                return fmt::format("TSW[{}, {}, {}]", scalar_pattern_to_string(pattern.scalar),
                                   pattern.fixed_size, pattern.min_size);
            case TypePattern::Kind::TSB:
            {
                std::vector<std::string> fields;
                fields.reserve(pattern.children.size());
                for (std::size_t i = 0; i < pattern.children.size(); ++i)
                {
                    fields.push_back(fmt::format("{}: {}", pattern.field_names[i], ts_pattern_to_string(pattern.children[i])));
                }
                return pattern.named_bundle ? fmt::format("TSB<{}>[{}]", pattern.bundle_name, fmt::join(fields, ", "))
                                            : fmt::format("TSB[{}]", fmt::join(fields, ", "));
            }
            case TypePattern::Kind::REF: return fmt::format("REF[{}]", ts_pattern_to_string(pattern.children[0]));
            case TypePattern::Kind::Signal: return "SIGNAL";
        }
        return "?";
    }
}  // namespace hgraph
