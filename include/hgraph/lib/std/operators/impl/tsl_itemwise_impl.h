#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_TSL_ITEMWISE_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_TSL_ITEMWISE_IMPL_H

#include <hgraph/lib/std/operators/higher_order.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/util/scope.h>

#include <span>
#include <utility>
#include <vector>

namespace hgraph::stdlib::tsl_itemwise_impl_detail
{
    [[nodiscard]] inline bool is_tsl_time_series_arg(const WiringArg &arg)
    {
        if (arg.kind != WiringArg::Kind::TimeSeries || arg.port.schema == nullptr) { return false; }
        const auto *schema = TypeRegistry::instance().dereference(arg.port.schema);
        return schema != nullptr && schema->kind == TSTypeKind::TSL;
    }

    [[nodiscard]] inline bool is_fixed_tsl_time_series_arg(const WiringArg &arg)
    {
        if (arg.kind != WiringArg::Kind::TimeSeries || arg.port.schema == nullptr) { return false; }
        const auto *schema = TypeRegistry::instance().dereference(arg.port.schema);
        return schema != nullptr && schema->kind == TSTypeKind::TSL && schema->fixed_size() > 0;
    }

    [[nodiscard]] inline bool same_fixed_tsl_size(OperatorCallContext context)
    {
        if (context.args.size() != 2 ||
            !is_fixed_tsl_time_series_arg(context.args[0]) ||
            !is_fixed_tsl_time_series_arg(context.args[1]))
        {
            return false;
        }
        const auto *lhs = TypeRegistry::instance().dereference(context.args[0].port.schema);
        const auto *rhs = TypeRegistry::instance().dereference(context.args[1].port.schema);
        return lhs != nullptr && rhs != nullptr && lhs->fixed_size() == rhs->fixed_size();
    }

    inline void resolve_map_output_with_fn(ResolutionMap &resolution, OperatorCallContext context, WiredFn wired_fn)
    {
        if (resolution.find_ts("__out__") != nullptr) { return; }

        std::vector<WiringArg> args;
        args.reserve(context.args.size() + 1);

        WiringArg func;
        func.kind         = WiringArg::Kind::Scalar;
        func.scalar_value = Value{std::move(wired_fn)};
        func.scalar_meta  = func.scalar_value.schema();
        args.push_back(std::move(func));

        for (const WiringArg &arg : context.args) { args.push_back(arg); }

        const TSValueTypeMetaData *output = fallback_on_exception<const TSValueTypeMetaData *>(nullptr, [&] {
            ResolvedOperatorCall resolved =
                OperatorRegistry::instance().resolve(map_::name,
                                                     std::span<const WiringArg>{args.data(), args.size()},
                                                     true);
            return ts_pattern_resolve(resolved.impl->output, resolved.map);
        });
        if (output != nullptr) { resolution.bind_ts("__out__", output); }
    }

    template <typename Op>
    inline void resolve_map_output(ResolutionMap &resolution, OperatorCallContext context)
    {
        resolve_map_output_with_fn(resolution, context, fn<Op>());
    }

    template <typename Op>
    struct tsl_unary_map
    {
        static constexpr auto name = "tsl_unary_map";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            resolve_map_output<Op>(resolution, context);
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return context.args.size() == 1 && is_fixed_tsl_time_series_arg(context.args[0]);
        }

        static auto compose(Wiring &w, NamedPort<"ts", TSL<TsVar<"S">>> ts)
        {
            return wire<map_>(w, fn<Op>(), ts);
        }
    };

    template <typename Op>
    struct tsl_binary_map
    {
        static constexpr auto name = "tsl_binary_map";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            resolve_map_output<Op>(resolution, context);
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return same_fixed_tsl_size(context);
        }

        static auto compose(Wiring &w,
                            NamedPort<"lhs", TSL<TsVar<"L">>> lhs,
                            NamedPort<"rhs", TSL<TsVar<"R">>> rhs)
        {
            return wire<map_>(w, fn<Op>(), lhs, rhs);
        }
    };

    template <typename Lifted>
    struct tsl_binary_lifted_map
    {
        static constexpr auto name = "tsl_binary_lifted_map";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            resolve_map_output_with_fn(resolution, context, Lifted::fn());
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return same_fixed_tsl_size(context);
        }

        static auto compose(Wiring &w,
                            NamedPort<"lhs", TSL<TsVar<"L">>> lhs,
                            NamedPort<"rhs", TSL<TsVar<"R">>> rhs)
        {
            return wire<map_>(w, Lifted::fn(), lhs, rhs);
        }
    };

    template <typename Op>
    struct tsl_rhs_broadcast_map
    {
        static constexpr auto name = "tsl_rhs_broadcast_map";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            resolve_map_output<Op>(resolution, context);
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return context.args.size() == 2 &&
                   is_fixed_tsl_time_series_arg(context.args[0]) &&
                   !is_tsl_time_series_arg(context.args[1]);
        }

        static auto compose(Wiring &w, NamedPort<"lhs", TSL<TsVar<"L">>> lhs, NamedPort<"rhs", TsVar<"R">> rhs)
        {
            return wire<map_>(w, fn<Op>(), lhs, rhs);
        }
    };

    template <typename Op>
    struct tsl_lhs_broadcast_map
    {
        static constexpr auto name = "tsl_lhs_broadcast_map";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            resolve_map_output<Op>(resolution, context);
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return context.args.size() == 2 &&
                   !is_tsl_time_series_arg(context.args[0]) &&
                   is_fixed_tsl_time_series_arg(context.args[1]);
        }

        static auto compose(Wiring &w, NamedPort<"lhs", TsVar<"L">> lhs, NamedPort<"rhs", TSL<TsVar<"R">>> rhs)
        {
            return wire<map_>(w, fn<Op>(), lhs, rhs);
        }
    };
}  // namespace hgraph::stdlib::tsl_itemwise_impl_detail

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_TSL_ITEMWISE_IMPL_H
