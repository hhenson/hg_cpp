#include <hgraph/types/operator_dispatch.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <exception>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] bool value_schema_matches_ts_pattern(const TypePattern &pattern,
                                                           const ValueTypeMetaData *value_schema,
                                                           ResolutionMap &map)
        {
            if (value_schema == nullptr) { return false; }
            TypeRegistry &registry = TypeRegistry::instance();
            switch (pattern.kind)
            {
                case TypePattern::Kind::Var:
                    if (const TSValueTypeMetaData *bound = map.find_ts(pattern.name))
                    {
                        return bound->value_schema == value_schema;
                    }
                    return ts_pattern_match(pattern, registry.ts(value_schema), map);
                case TypePattern::Kind::Concrete:
                    return pattern.meta != nullptr && pattern.meta->value_schema == value_schema;
                case TypePattern::Kind::TS:
                    return scalar_pattern_match(pattern.scalar, value_schema, map);
                case TypePattern::Kind::TSS:
                    return value_schema->kind == ValueTypeKind::Set &&
                           scalar_pattern_match(pattern.scalar, value_schema->element_type, map);
                case TypePattern::Kind::TSL:
                    return value_schema->kind == ValueTypeKind::List &&
                           (pattern.fixed_size == 0 || value_schema->fixed_size == 0 ||
                            pattern.fixed_size == value_schema->fixed_size) &&
                           value_schema_matches_ts_pattern(pattern.children[0], value_schema->element_type, map);
                case TypePattern::Kind::TSD:
                    return value_schema->kind == ValueTypeKind::Map &&
                           scalar_pattern_match(pattern.scalar, value_schema->key_type, map) &&
                           value_schema_matches_ts_pattern(pattern.children[0], value_schema->element_type, map);
                case TypePattern::Kind::TSW:
                    return value_schema->kind == ValueTypeKind::List &&
                           pattern.fixed_size == value_schema->fixed_size &&
                           scalar_pattern_match(pattern.scalar, value_schema->element_type, map);
                case TypePattern::Kind::TSB:
                    if (value_schema->kind != ValueTypeKind::Bundle ||
                        value_schema->field_count != pattern.children.size())
                    {
                        return false;
                    }
                    for (std::size_t i = 0; i < pattern.children.size(); ++i)
                    {
                        const ValueFieldMetaData &field = value_schema->fields[i];
                        if (field.name == nullptr || pattern.field_names[i] != field.name) { return false; }
                        if (!value_schema_matches_ts_pattern(pattern.children[i], field.type, map)) { return false; }
                    }
                    return true;
                case TypePattern::Kind::REF:
                    return false;
                case TypePattern::Kind::Signal:
                    return value_schema == scalar_type<Bool>();
            }
            return false;
        }

        [[nodiscard]] bool scalar_value_matches_ts_pattern(const TypePattern &pattern,
                                                           const Value &value,
                                                           ResolutionMap &map,
                                                           int &rank_adjustment)
        {
            if (pattern.kind == TypePattern::Kind::Var)
            {
                if (const TSValueTypeMetaData *bound = map.find_ts(pattern.name))
                {
                    if (const auto *schema = value.schema();
                        schema != nullptr && current_value_schema_compatible(*bound, *schema))
                    {
                        return true;
                    }
                    const bool coerced =
                        operator_dispatch_detail::coerce_scalar_value_to_meta(value, bound->value_schema).has_value();
                    if (coerced) { ++rank_adjustment; }
                    return coerced;
                }
            }
            if (pattern.kind == TypePattern::Kind::Concrete)
            {
                if (pattern.meta == nullptr) { return false; }
                if (const auto *schema = value.schema();
                    schema != nullptr && current_value_schema_compatible(*pattern.meta, *schema))
                {
                    return true;
                }
                const bool coerced =
                    operator_dispatch_detail::coerce_scalar_value_to_meta(value, pattern.meta->value_schema).has_value();
                if (coerced) { ++rank_adjustment; }
                return coerced;
            }
            if (pattern.kind == TypePattern::Kind::TS &&
                pattern.scalar.kind == ScalarPattern::Kind::Concrete)
            {
                if (value.schema() == pattern.scalar.meta) { return true; }
                const bool coerced =
                    operator_dispatch_detail::coerce_scalar_value_to_meta(value, pattern.scalar.meta).has_value();
                if (coerced) { ++rank_adjustment; }
                return coerced;
            }
            return value_schema_matches_ts_pattern(pattern, value.schema(), map);
        }

        // Match one candidate against the supplied arguments, binding into a fresh
        // ``map``. On failure ``why`` records a human-readable reason and ``map`` is
        // discarded by the caller. This is the runtime matcher shared by every
        // candidate (C++ today, Python later).
        bool try_match(const OperatorImpl &impl,
                       std::span<const WiringArg> args,
                       std::optional<bool> output_required,
                       const TSValueTypeMetaData *expected_output,
                       ResolutionMap &map,
                       int &rank_adjustment,
                       std::string &why)
        {
            if (output_required.has_value() && impl.has_output != *output_required)
            {
                why = *output_required ? "candidate has no output" : "candidate unexpectedly has an output";
                return false;
            }
            const std::size_t fixed_params =
                impl.variadic && !impl.params.empty() ? impl.params.size() - 1 : impl.params.size();
            if (impl.variadic ? args.size() < fixed_params : impl.params.size() != args.size())
            {
                why = impl.variadic
                          ? fmt::format("expects at least {} argument(s), got {}", fixed_params, args.size())
                          : fmt::format("expects {} argument(s), got {}", impl.params.size(), args.size());
                return false;
            }
            // Variadic tails are matched independently per supplied argument, so
            // they must also be ranked per supplied argument. The base rank for a
            // variadic impl excludes its tail pattern; add it back for each
            // consumed tail argument plus a small fixed penalty so exact fixed
            // arity wins at equal specificity.
            if (impl.variadic)
            {
                const int tail_rank = operator_dispatch_detail::param_pattern_rank(impl.params.back());
                rank_adjustment +=
                    tail_rank * static_cast<int>(args.size() - fixed_params) + 1;
            }

            if (expected_output != nullptr && impl.has_output)
            {
                if (!ts_pattern_match(impl.output, expected_output, map))
                {
                    why = fmt::format("output does not match requested {}", expected_output->display_name != nullptr
                                                                               ? expected_output->display_name
                                                                               : "time-series");
                    return false;
                }
            }

            for (std::size_t i = 0; i < args.size(); ++i)
            {
                const bool          tail  = impl.variadic && i >= fixed_params;
                const ParamPattern &param = impl.params[std::min(i, impl.params.size() - 1)];
                const WiringArg    &arg   = args[i];

                if (tail)
                {
                    // Variadic tail: time-series only, matched against the
                    // declared pattern INDEPENDENTLY per argument (a throwaway
                    // binding scope — fixed-prefix bindings constrain the
                    // match, but heterogeneous tail args never bind vars).
                    if (arg.kind != WiringArg::Kind::TimeSeries)
                    {
                        why = fmt::format("variadic argument {} must be a time-series", i);
                        return false;
                    }
                    ResolutionMap tail_scope = map;
                    if (!ts_pattern_match(param.ts, arg.port.schema, tail_scope))
                    {
                        why = fmt::format("variadic argument {} (a {}) does not match {}", i,
                                          arg.port.schema != nullptr && arg.port.schema->display_name != nullptr
                                              ? arg.port.schema->display_name
                                              : "time-series",
                                          ts_pattern_to_string(param.ts));
                        return false;
                    }
                    continue;
                }

                if (param.kind == ParamPattern::Kind::Input)
                {
                    if (arg.kind == WiringArg::Kind::TimeSeries)
                    {
                        if (!ts_pattern_match(param.ts, arg.port.schema, map))
                        {
                            why = fmt::format("argument {} (a {}) does not match {}", i,
                                              arg.port.schema != nullptr && arg.port.schema->display_name != nullptr
                                                  ? arg.port.schema->display_name
                                                  : "time-series",
                                              ts_pattern_to_string(param.ts));
                            return false;
                        }
                    }
                    else if (!scalar_value_matches_ts_pattern(param.ts, arg.scalar_value, map, rank_adjustment))
                    {
                        why = fmt::format("scalar argument {} cannot be promoted to {}", i, ts_pattern_to_string(param.ts));
                        return false;
                    }
                }
                else
                {
                    if (arg.kind != WiringArg::Kind::Scalar)
                    {
                        why = fmt::format("argument {} should be a scalar", i);
                        return false;
                    }
                    bool scalar_matches = false;
                    if (param.scalar.kind == ScalarPattern::Kind::Concrete)
                    {
                        scalar_matches = arg.scalar_meta == param.scalar.meta;
                        if (!scalar_matches)
                        {
                            scalar_matches =
                                operator_dispatch_detail::coerce_scalar_value_to_meta(arg.scalar_value, param.scalar.meta)
                                    .has_value();
                            if (scalar_matches) { ++rank_adjustment; }
                        }
                    }
                    else
                    {
                        scalar_matches = scalar_pattern_match(param.scalar, arg.scalar_meta, map);
                    }
                    if (!scalar_matches)
                    {
                        why = fmt::format("scalar argument {} does not match {}", i,
                                          scalar_pattern_to_string(param.scalar));
                        return false;
                    }
                }
            }

            OperatorCallContext context{args, impl.params};
            if (impl.default_resolver)
            {
                try
                {
                    impl.default_resolver(map, context);
                }
                catch (const std::exception &e)
                {
                    why = fmt::format("default type resolution failed: {}", e.what());
                    return false;
                }
            }

            if (impl.has_output && ts_pattern_resolve(impl.output, map) == nullptr)
            {
                why = "output type could not be resolved";
                return false;
            }

            if (impl.requires_predicate)
            {
                bool accepted = false;
                try
                {
                    accepted = impl.requires_predicate(map, context);
                }
                catch (const std::exception &e)
                {
                    why = fmt::format("requires predicate threw: {}", e.what());
                    return false;
                }
                if (!accepted)
                {
                    why = "rejected by requires predicate";
                    return false;
                }
            }
            return true;
        }
    }  // namespace

    OperatorRegistry &OperatorRegistry::instance() noexcept
    {
        static OperatorRegistry registry;
        return registry;
    }

    void OperatorRegistry::register_overload(OperatorImpl impl)
    {
        overloads_[impl.name].push_back(std::move(impl));
    }

    void OperatorRegistry::reset() noexcept { overloads_.clear(); }

    std::pair<const OperatorImpl *, ResolutionMap> OperatorRegistry::resolve(
        std::string_view name,
        std::span<const WiringArg> args,
        std::optional<bool> output_required,
        const TSValueTypeMetaData *expected_output) const
    {
        auto it = overloads_.find(std::string{name});
        if (it == overloads_.end() || it->second.empty())
        {
            throw OperatorResolutionError(fmt::format("no operator '{}' is registered", name));
        }

        struct Survivor
        {
            const OperatorImpl *impl;
            ResolutionMap       map;
            int                 rank{0};
        };

        std::vector<Survivor>    survivors;
        std::vector<std::string> rejected;
        for (const OperatorImpl &impl : it->second)
        {
            ResolutionMap map;
            int           rank_adjustment = 0;
            std::string   why;
            if (try_match(impl, args, output_required, expected_output, map, rank_adjustment, why))
            {
                survivors.push_back({&impl, std::move(map), impl.rank + rank_adjustment});
            }
            else { rejected.push_back(fmt::format("  {} [rank {}]: {}", impl.label, impl.rank, why)); }
        }

        if (survivors.empty())
        {
            throw OperatorResolutionError(
                fmt::format("no matching overload for operator '{}' with {} argument(s)\nrejected candidates:\n{}", name,
                            args.size(), fmt::join(rejected, "\n")));
        }

        // Lowest rank wins; a stable sort preserves registration order on ties.
        std::stable_sort(survivors.begin(), survivors.end(),
                         [](const Survivor &a, const Survivor &b) { return a.rank < b.rank; });

        if (survivors.size() > 1 && survivors[0].rank == survivors[1].rank)
        {
            std::vector<std::string> tied;
            for (const Survivor &s : survivors)
            {
                if (s.rank == survivors[0].rank)
                {
                    tied.push_back(fmt::format("  {} [rank {}]", s.impl->label, s.rank));
                }
            }
            throw OperatorResolutionError(
                fmt::format("ambiguous overloads for operator '{}':\n{}", name, fmt::join(tied, "\n")));
        }

        return {survivors[0].impl, std::move(survivors[0].map)};
    }
}  // namespace hgraph
