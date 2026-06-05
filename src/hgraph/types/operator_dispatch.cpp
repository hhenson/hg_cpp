#include <hgraph/types/operator_dispatch.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <exception>

namespace hgraph
{
    namespace
    {
        // Match one candidate against the supplied arguments, binding into a fresh
        // ``map``. On failure ``why`` records a human-readable reason and ``map`` is
        // discarded by the caller. This is the runtime matcher shared by every
        // candidate (C++ today, Python later).
        bool try_match(const OperatorImpl &impl, std::span<const WiringArg> args, ResolutionMap &map, std::string &why)
        {
            if (impl.params.size() != args.size())
            {
                why = fmt::format("expects {} argument(s), got {}", impl.params.size(), args.size());
                return false;
            }

            for (std::size_t i = 0; i < args.size(); ++i)
            {
                const ParamPattern &param = impl.params[i];
                const WiringArg    &arg   = args[i];
                if (param.kind == ParamPattern::Kind::Input)
                {
                    if (arg.kind != WiringArg::Kind::TimeSeries)
                    {
                        why = fmt::format("argument {} should be a time-series port", i);
                        return false;
                    }
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
                else
                {
                    if (arg.kind != WiringArg::Kind::Scalar)
                    {
                        why = fmt::format("argument {} should be a scalar", i);
                        return false;
                    }
                    if (!scalar_pattern_match(param.scalar, arg.scalar_meta, map))
                    {
                        why = fmt::format("scalar argument {} does not match {}", i,
                                          scalar_pattern_to_string(param.scalar));
                        return false;
                    }
                }
            }

            if (impl.default_resolver)
            {
                try
                {
                    impl.default_resolver(map);
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
                    accepted = impl.requires_predicate(map);
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

    std::pair<const OperatorImpl *, ResolutionMap> OperatorRegistry::resolve(std::string_view name,
                                                                            std::span<const WiringArg> args) const
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
        };

        std::vector<Survivor>    survivors;
        std::vector<std::string> rejected;
        for (const OperatorImpl &impl : it->second)
        {
            ResolutionMap map;
            std::string   why;
            if (try_match(impl, args, map, why)) { survivors.push_back({&impl, std::move(map)}); }
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
                         [](const Survivor &a, const Survivor &b) { return a.impl->rank < b.impl->rank; });

        if (survivors.size() > 1 && survivors[0].impl->rank == survivors[1].impl->rank)
        {
            std::vector<std::string> tied;
            for (const Survivor &s : survivors)
            {
                if (s.impl->rank == survivors[0].impl->rank)
                {
                    tied.push_back(fmt::format("  {} [rank {}]", s.impl->label, s.impl->rank));
                }
            }
            throw OperatorResolutionError(
                fmt::format("ambiguous overloads for operator '{}':\n{}", name, fmt::join(tied, "\n")));
        }

        return {survivors[0].impl, std::move(survivors[0].map)};
    }
}  // namespace hgraph
