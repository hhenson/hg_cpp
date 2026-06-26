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
                           size_pattern_match(pattern, value_schema->fixed_size, map) &&
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

        [[nodiscard]] bool input_ts_pattern_match(const TypePattern &pattern,
                                                  const TSValueTypeMetaData *concrete,
                                                  ResolutionMap &map)
        {
            if (concrete == nullptr) { return false; }
            if (pattern.kind == TypePattern::Kind::Signal) { return true; }
            if (pattern.kind != TypePattern::Kind::REF && concrete->kind == TSTypeKind::REF)
            {
                return input_ts_pattern_match(pattern, concrete->referenced_ts(), map);
            }

            switch (pattern.kind)
            {
                case TypePattern::Kind::Concrete:
                    return graph_wiring_detail::input_accepts_output_schema(pattern.meta, concrete);
                case TypePattern::Kind::TSL:
                    if (concrete->kind != TSTypeKind::TSL) { return false; }
                    if (!size_pattern_match(pattern, concrete->fixed_size(), map)) { return false; }
                    return input_ts_pattern_match(pattern.children[0], concrete->element_ts(), map);
                case TypePattern::Kind::TSD:
                    return concrete->kind == TSTypeKind::TSD &&
                           scalar_pattern_match(pattern.scalar, concrete->key_type(), map) &&
                           input_ts_pattern_match(pattern.children[0], concrete->element_ts(), map);
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
                        if (!input_ts_pattern_match(pattern.children[i], field.type, map)) { return false; }
                    }
                    return true;
                case TypePattern::Kind::REF:
                    return input_ts_pattern_match(pattern.children[0],
                                                  concrete->kind == TSTypeKind::REF ? concrete->referenced_ts() : concrete,
                                                  map);
                case TypePattern::Kind::Var:
                case TypePattern::Kind::TS:
                case TypePattern::Kind::TSS:
                case TypePattern::Kind::TSW:
                case TypePattern::Kind::Signal:
                    return ts_pattern_match(pattern, concrete, map);
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
        struct NormalizedCall
        {
            std::vector<WiringArg>                              args{};
            std::vector<std::pair<std::string, WiringPortRef>>  kwargs{};
            int                                                 defaults_used{0};
        };

        constexpr int variadic_pack_fixed_input_penalty = 100'000'000;

        [[nodiscard]] bool append_tail_arg(const WiringArg &arg,
                                           std::vector<WiringArg> &tail,
                                           std::string &why)
        {
            if (!arg.from_variadic_tail)
            {
                tail.push_back(arg);
                return true;
            }
            if (arg.kind != WiringArg::Kind::TimeSeries || !arg.port.is_structural_source())
            {
                why = "packed variadic tail is not a structural time-series source";
                return false;
            }

            const auto *schema = TypeRegistry::instance().dereference(arg.port.schema);
            if (schema == nullptr || schema->kind != TSTypeKind::TSL)
            {
                why = "packed variadic tail is not a TSL";
                return false;
            }

            for (const WiringPortRef &child : arg.port.structural_children())
            {
                WiringArg child_arg;
                child_arg.kind = WiringArg::Kind::TimeSeries;
                child_arg.port = child;
                tail.push_back(std::move(child_arg));
            }
            return true;
        }

        /**
         * Map the call onto the candidate's declared parameters using the
         * Python calling rules: positional arguments fill parameters in
         * order (overflow goes to the variadic tail), named arguments target
         * named parameters (after all positional ones), omitted parameters
         * take their declared defaults, and named arguments matching no
         * parameter collect into ``**kwargs`` when the candidate has one.
         * The output is positional: arguments in declared parameter order,
         * defaults materialised, tail appended.
         */
        bool normalize_call(const OperatorImpl &impl,
                            std::span<const WiringArg> args,
                            NormalizedCall &out,
                            std::string &why)
        {
            const std::size_t fixed =
                impl.variadic && !impl.params.empty() ? impl.params.size() - 1 : impl.params.size();
            // Params beyond ``positional_params`` are keyword-only (Python's
            // params after *args): positional arguments never fill them.
            const std::size_t positional_limit = std::min(impl.positional_params, fixed);

            std::size_t positional = 0;
            while (positional < args.size() && args[positional].name.empty()) { ++positional; }
            for (std::size_t i = positional; i < args.size(); ++i)
            {
                if (args[i].name.empty())
                {
                    why = "positional argument follows a named argument";
                    return false;
                }
            }

            if (!impl.variadic && positional > positional_limit)
            {
                why = fmt::format("expects at most {} positional argument(s), got {}", positional_limit,
                                  positional);
                return false;
            }

            std::vector<std::optional<WiringArg>> filled(fixed);
            std::vector<WiringArg>                tail;
            for (std::size_t i = 0; i < positional; ++i)
            {
                if (i < positional_limit) { filled[i] = args[i]; }
                else if (!append_tail_arg(args[i], tail, why)) { return false; }
            }

            for (std::size_t i = positional; i < args.size(); ++i)
            {
                const WiringArg &named = args[i];
                std::size_t      index = fixed;
                for (std::size_t p = 0; p < fixed; ++p)
                {
                    if (!impl.params[p].name.empty() && impl.params[p].name == named.name)
                    {
                        index = p;
                        break;
                    }
                }
                if (index < fixed)
                {
                    if (filled[index].has_value())
                    {
                        why = fmt::format("got multiple values for argument '{}'", named.name);
                        return false;
                    }
                    filled[index] = named;
                }
                else if (impl.has_kwargs)
                {
                    if (named.kind != WiringArg::Kind::TimeSeries)
                    {
                        why = fmt::format("keyword argument '{}' must be a time-series", named.name);
                        return false;
                    }
                    if (std::find_if(out.kwargs.begin(), out.kwargs.end(),
                                     [&](const auto &kw) { return kw.first == named.name; }) !=
                        out.kwargs.end())
                    {
                        why = fmt::format("got multiple values for argument '{}'", named.name);
                        return false;
                    }
                    out.kwargs.emplace_back(named.name, named.port);
                }
                else
                {
                    why = fmt::format("got an unexpected keyword argument '{}'", named.name);
                    return false;
                }
            }

            for (std::size_t p = 0; p < fixed; ++p)
            {
                if (filled[p].has_value()) { continue; }
                if (impl.params[p].default_value.has_value())
                {
                    WiringArg synthesised;
                    if (impl.params[p].kind == ParamPattern::Kind::Input &&
                        !impl.params[p].default_value->has_value())
                    {
                        // None default on a time-series parameter: a null
                        // source — the input is left unwired.
                        synthesised.kind = WiringArg::Kind::TimeSeries;
                    }
                    else
                    {
                        synthesised.kind         = WiringArg::Kind::Scalar;
                        synthesised.scalar_value = *impl.params[p].default_value;
                        synthesised.scalar_meta  = synthesised.scalar_value.schema();
                    }
                    filled[p] = std::move(synthesised);
                    ++out.defaults_used;
                    continue;
                }
                why = impl.params[p].name.empty()
                          ? fmt::format("missing required argument {}", p)
                          : fmt::format("missing required argument '{}'", impl.params[p].name);
                return false;
            }

            out.args.reserve(fixed + tail.size());
            for (auto &slot : filled) { out.args.push_back(std::move(*slot)); }
            for (auto &arg : tail) { out.args.push_back(std::move(arg)); }
            return true;
        }

        bool try_match(const OperatorImpl &impl,
                       std::span<const WiringArg> args,
                       std::span<const std::pair<std::string, WiringPortRef>> kwargs,
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
            // A kwargs collector is less specific than an exact signature.
            if (impl.has_kwargs) { ++rank_adjustment; }

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

                if (!tail && arg.from_variadic_tail)
                {
                    rank_adjustment += variadic_pack_fixed_input_penalty;
                }

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
                    if (!input_ts_pattern_match(param.ts, arg.port.schema, tail_scope))
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
                    if (arg.kind == WiringArg::Kind::TimeSeries && arg.port.schema == nullptr)
                    {
                        // A null source (None default / unwired input): matches
                        // any input pattern, binds no type variables.
                        continue;
                    }
                    if (arg.kind == WiringArg::Kind::TimeSeries)
                    {
                        if (!input_ts_pattern_match(param.ts, arg.port.schema, map))
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

            OperatorCallContext context{args, impl.params, kwargs};
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

    void OperatorRegistry::reset() noexcept
    {
        overloads_.clear();
        mesh_scopes_.clear();
    }

    void OperatorRegistry::push_mesh_scope(const TSValueTypeMetaData *element_schema,
                                           const ValueTypeMetaData *key_type,
                                           std::string name)
    {
        mesh_scopes_.push_back(MeshScope{
            .element_schema = element_schema,
            .key_type       = key_type,
            .name           = std::move(name),
        });
    }

    void OperatorRegistry::pop_mesh_scope() noexcept
    {
        if (!mesh_scopes_.empty()) { mesh_scopes_.pop_back(); }
    }

    const TSValueTypeMetaData *OperatorRegistry::resolve_mesh_scope(std::string_view name) const noexcept
    {
        for (auto it = mesh_scopes_.rbegin(); it != mesh_scopes_.rend(); ++it)
        {
            if (name.empty() || it->name == name) { return it->element_schema; }
        }
        return nullptr;
    }

    const ValueTypeMetaData *OperatorRegistry::resolve_mesh_key_scope(std::string_view name) const noexcept
    {
        for (auto it = mesh_scopes_.rbegin(); it != mesh_scopes_.rend(); ++it)
        {
            if (name.empty() || it->name == name) { return it->key_type; }
        }
        return nullptr;
    }

    ResolvedOperatorCall OperatorRegistry::resolve(
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
            NormalizedCall      call;
            int                 rank{0};
        };

        std::vector<Survivor>    survivors;
        std::vector<std::string> rejected;
        for (const OperatorImpl &impl : it->second)
        {
            NormalizedCall call;
            std::string    why;
            if (!normalize_call(impl, args, call, why))
            {
                rejected.push_back(fmt::format("  {} [rank {}]: {}", impl.label, impl.rank, why));
                continue;
            }

            ResolutionMap map;
            // Each default an overload falls back on makes it a little less
            // specific than one whose parameters were all supplied.
            int rank_adjustment = call.defaults_used;
            if (try_match(impl, call.args, call.kwargs, output_required, expected_output, map, rank_adjustment, why))
            {
                survivors.push_back({&impl, std::move(map), std::move(call), impl.rank + rank_adjustment});
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

        return ResolvedOperatorCall{survivors[0].impl, std::move(survivors[0].map),
                                    std::move(survivors[0].call.args), std::move(survivors[0].call.kwargs)};
    }
}  // namespace hgraph
