#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_CONTROL_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_CONTROL_IMPL_H

#include <hgraph/lib/std/operators/control.h>
#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/operators/higher_order.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

#include <stdexcept>
#include <string_view>
#include <vector>

namespace hgraph::stdlib
{
    namespace control_impl_detail
    {
        struct merge_binary
        {
            static constexpr auto name = "merge_binary";

            static void start(State<Int> selected) { selected.set(Int{-1}); }

            static void eval(In<"lhs", TsVar<"S">, InputValidity::Unchecked> lhs,
                             In<"rhs", TsVar<"S">, InputValidity::Unchecked> rhs,
                             State<Int> selected,
                             Out<TsVar<"S">> out)
            {
                const Int current = selected.get();
                if (lhs.modified() && lhs.valid())
                {
                    selected.set(Int{0});
                    out.apply(lhs.value());
                    return;
                }
                if (rhs.modified() && rhs.valid())
                {
                    selected.set(Int{1});
                    out.apply(rhs.value());
                    return;
                }

                if (current == 0 && !lhs.valid())
                {
                    if (rhs.valid())
                    {
                        selected.set(Int{1});
                        out.apply(rhs.value());
                    }
                    else { selected.set(Int{-1}); }
                    return;
                }
                if (current == 1 && !rhs.valid())
                {
                    if (lhs.valid())
                    {
                        selected.set(Int{0});
                        out.apply(lhs.value());
                    }
                    else { selected.set(Int{-1}); }
                    return;
                }

                if (current < 0)
                {
                    if (lhs.valid())
                    {
                        selected.set(Int{0});
                        out.apply(lhs.value());
                    }
                    else if (rhs.valid())
                    {
                        selected.set(Int{1});
                        out.apply(rhs.value());
                    }
                }
            }
        };

        struct all_bool_binary
        {
            static constexpr auto name              = "all_bool_binary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"lhs", TS<Bool>, InputValidity::Unchecked> lhs,
                             In<"rhs", TS<Bool>, InputValidity::Unchecked> rhs,
                             Out<TS<Bool>> out)
            {
                out.set(lhs.valid() && lhs.value() && rhs.valid() && rhs.value());
            }
        };

        struct any_bool_binary
        {
            static constexpr auto name              = "any_bool_binary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"lhs", TS<Bool>, InputValidity::Unchecked> lhs,
                             In<"rhs", TS<Bool>, InputValidity::Unchecked> rhs,
                             Out<TS<Bool>> out)
            {
                out.set((lhs.valid() && lhs.value()) || (rhs.valid() && rhs.value()));
            }
        };

        [[nodiscard]] inline Port<TS<Bool>> bool_const(Wiring &w, Bool value)
        {
            return wire<const_, TS<Bool>>(w, value);
        }

    }  // namespace control_impl_detail

    struct merge_graph_impl
    {
        static constexpr auto name = "merge";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (context.args.empty() || context.args[0].kind != WiringArg::Kind::TimeSeries) { return; }
            const TSValueTypeMetaData *schema = TypeRegistry::instance().dereference(context.args[0].port.schema);
            resolution.bind_ts("S", schema);
            resolution.bind_ts("__graph_output", schema);
        }

        static auto compose(Wiring &w, VarIn<"ts", TsVar<"S">> ts)
        {
            if (ts.empty()) { throw std::invalid_argument("merge requires at least one input"); }
            return wire<reduce_>(w, fn<control_impl_detail::merge_binary>(), ts);
        }
    };

    struct all_graph_impl
    {
        static constexpr auto name = "all";

        static Port<TS<Bool>> compose(Wiring &w, VarIn<"args", TS<Bool>> args)
        {
            if (args.empty()) { return control_impl_detail::bool_const(w, true); }
            return wire<reduce_, TS<Bool>>(w, fn<control_impl_detail::all_bool_binary>(),
                                           args, false);
        }
    };

    struct any_graph_impl
    {
        static constexpr auto name = "any";

        static Port<TS<Bool>> compose(Wiring &w, VarIn<"args", TS<Bool>> args)
        {
            if (args.empty()) { return control_impl_detail::bool_const(w, false); }
            return wire<reduce_, TS<Bool>>(w, fn<control_impl_detail::any_bool_binary>(),
                                           args, false);
        }
    };

    struct all_tsd_impl
    {
        static constexpr bool schedule_on_start = true;

        static void eval(In<"arg", TSD<ScalarVar<"K">, TS<Bool>>, InputValidity::Unchecked> arg,
                         Out<TS<Bool>> out)
        {
            Bool result = true;
            for (auto child : arg.values())
            {
                if (!child.valid() || !child.value())
                {
                    result = false;
                    break;
                }
            }
            out.set(result);
        }
    };

    struct any_tsd_impl
    {
        static constexpr bool schedule_on_start = true;

        static void eval(In<"arg", TSD<ScalarVar<"K">, TS<Bool>>, InputValidity::Unchecked> arg,
                         Out<TS<Bool>> out)
        {
            Bool result = false;
            for (auto child : arg.values())
            {
                if (child.valid() && child.value())
                {
                    result = true;
                    break;
                }
            }
            out.set(result);
        }
    };

    struct if_true_impl
    {
        static void eval(In<"condition", TS<Bool>> condition,
                         Scalar<"tick_once_only", Bool> tick_once_only,
                         State<Bool> ticked,
                         Out<TS<Bool>> out)
        {
            if (!condition.value()) { return; }
            if (!tick_once_only.value() || !ticked.get())
            {
                out.set(true);
                ticked.set(true);
            }
        }

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"tick_once_only", Value{Bool{false}}}};
        }
    };

    struct if_then_else_impl
    {
        static void eval(In<"condition", TS<Bool>> condition,
                         In<"true_value", TsVar<"S">, InputValidity::Unchecked> true_value,
                         In<"false_value", TsVar<"S">, InputValidity::Unchecked> false_value,
                         Out<TsVar<"S">> out)
        {
            const TSInputView &selected = condition.value() ? true_value.base() : false_value.base();
            if (selected.valid() && (condition.modified() || selected.modified())) { out.apply(selected.value()); }
        }
    };

    struct if_cmp_impl
    {
        static void eval(In<"cmp", TS<CmpResult>> cmp,
                         In<"lt", TsVar<"O">, InputValidity::Unchecked> lt,
                         In<"eq", TsVar<"O">, InputValidity::Unchecked> eq,
                         In<"gt", TsVar<"O">, InputValidity::Unchecked> gt,
                         Out<TsVar<"O">> out)
        {
            const TSInputView &selected = cmp.value() == CmpResult::LT ? lt.base()
                                      : cmp.value() == CmpResult::EQ ? eq.base()
                                                                     : gt.base();
            if (selected.valid() && (cmp.modified() || selected.modified())) { out.apply(selected.value()); }
        }
    };

    inline void register_control_operators()
    {
        register_graph_overload<merge, merge_graph_impl>();
        register_graph_overload<all_, all_graph_impl>();
        register_graph_overload<any_, any_graph_impl>();
        register_overload<all_, all_tsd_impl>();
        register_overload<any_, any_tsd_impl>();
        register_overload<if_true, if_true_impl>();
        register_overload<if_then_else, if_then_else_impl>();
        register_overload<if_cmp, if_cmp_impl>();
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_CONTROL_IMPL_H
