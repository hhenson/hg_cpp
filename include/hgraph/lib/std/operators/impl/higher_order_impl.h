#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_HIGHER_ORDER_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_HIGHER_ORDER_IMPL_H

#include <hgraph/lib/std/operators/higher_order.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/subgraph_wiring.h>
#include <hgraph/types/wired_fn.h>

#include <array>
#include <cstddef>
#include <optional>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

namespace hgraph::stdlib
{
    /**
     * Default overloads for the higher-order operators — ordinary registered
     * candidates (a graph overload per kind, like any other operator family).
     * User specialisations register alongside and are selected by the standard
     * pattern ranking + ``requires_`` gating, e.g. a concrete overload gated on
     * the wired function's identity.
     */

    namespace higher_order_impl_detail
    {
        // The static reduction layout, mirroring Python _reduce_tsl: a linear
        // chain for small sizes, otherwise balanced binary pairing with an
        // odd-element carry (over_run). Combination is one WiredFn application
        // per reduction node.
        [[nodiscard]] inline WiringPortRef reduce_layout(Wiring &w, const WiredFn &func,
                                                         std::vector<WiringPortRef> elements)
        {
            auto combine = [&](const WiringPortRef &lhs, const WiringPortRef &rhs) {
                const std::array<WiringPortRef, 2> args{lhs, rhs};
                return func.wire(w, std::span<const WiringPortRef>{args.data(), args.size()});
            };

            if (elements.size() < 4)
            {
                WiringPortRef out = elements.front();
                for (std::size_t i = 1; i < elements.size(); ++i) { out = combine(out, elements[i]); }
                return out;
            }

            std::vector<WiringPortRef> outs;
            outs.reserve(elements.size() / 2);
            for (std::size_t i = 0; i + 1 < elements.size(); i += 2)
            {
                outs.push_back(combine(elements[i], elements[i + 1]));
            }
            std::optional<WiringPortRef> over_run;
            if (elements.size() % 2 == 1) { over_run = elements.back(); }

            while (outs.size() > 1)
            {
                std::size_t count = outs.size();
                if (count % 2 == 1)
                {
                    if (over_run.has_value())
                    {
                        outs.push_back(*over_run);
                        over_run.reset();
                        ++count;
                    }
                    else
                    {
                        over_run = outs.back();
                        outs.pop_back();
                        --count;
                    }
                }
                std::vector<WiringPortRef> next;
                next.reserve(count / 2);
                for (std::size_t i = 0; i + 1 < count; i += 2) { next.push_back(combine(outs[i], outs[i + 1])); }
                outs = std::move(next);
            }
            return over_run.has_value() ? combine(outs.front(), *over_run) : outs.front();
        }

        // A fixed-size TSL second argument (shared requires_ of the TSL overloads;
        // the dynamic-TSL/TSD reductions are separate, future overloads).
        [[nodiscard]] inline bool reduce_ts_is_fixed_tsl(OperatorCallContext context, std::size_t arity)
        {
            if (context.args.size() != arity || context.args[1].kind != WiringArg::Kind::TimeSeries)
            {
                return false;
            }
            const auto *schema = context.args[1].port.schema;
            return schema != nullptr && schema->fixed_size() > 0;
        }

        /**
         * The shared fixed-TSL reduction wiring, mirroring Python ``_reduce_tsl``:
         * every leaf is ``default(ts[i], zero)`` — an element that has not ticked
         * yet counts as ``zero`` — then the linear/tree layout combines the
         * leaves. ``default`` is dispatched through the registry at the resolved
         * element schema.
         */
        [[nodiscard]] inline WiringPortRef reduce_tsl_wire(Wiring &w, const WiredFn &combiner,
                                                           const WiringPortRef &ts, const WiringPortRef &zero)
        {
            if (!combiner.valid())
            {
                throw std::invalid_argument("reduce: 'func' must be a wirable function (fn<X>())");
            }
            if (combiner.arity != 2 || !combiner.has_output)
            {
                throw std::invalid_argument(
                    "reduce: the combiner must take (lhs, rhs) time-series inputs and produce an output");
            }

            const TSValueTypeMetaData *schema  = ts.schema;
            const TSValueTypeMetaData *element = schema->element_ts();
            const std::size_t          size    = schema->fixed_size();

            const auto ts_arg = [](WiringPortRef ref) {
                WiringArg arg;
                arg.kind = WiringArg::Kind::TimeSeries;
                arg.port = std::move(ref);
                return arg;
            };

            std::vector<WiringPortRef> elements;
            elements.reserve(size);
            for (std::size_t i = 0; i < size; ++i)
            {
                const std::array<WiringArg, 2> leaf_args{
                    ts_arg(subgraph_wiring_detail::tsl_element_ref(ts, i, element)),
                    ts_arg(zero),
                };
                elements.push_back(
                    wire_operator(w, "default", {leaf_args.data(), leaf_args.size()}, true).output.erased());
            }
            return reduce_layout(w, combiner, std::move(elements));
        }

        /**
         * ``reduce(func, ts: TSL[V, SIZE]) -> V`` — the zero is derived from the
         * operation: ``zero(item_tp, func)`` (op-aware: ``add_`` -> 0, ``mul_``
         * -> 1, ...). A combiner with no registered zero (e.g. a custom
         * sub-graph) is a wiring-time error, like Python's ``KeyError`` — use
         * the explicit-zero arity instead.
         */
        struct reduce_tsl
        {
            static constexpr auto name = "reduce_tsl";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return reduce_ts_is_fixed_tsl(context, 2);
            }

            static Port<TsVar<"V">> compose(Wiring &w, Scalar<"func", WiredFn> func, Port<TSL<TsVar<"V">>> ts)
            {
                const TSValueTypeMetaData *element = ts.erased().schema->element_ts();

                const std::array<WiringArg, 1> zero_args{operator_dispatch_detail::make_wiring_arg(func)};
                const WiringPortRef            zero =
                    wire_operator(w, "zero", {zero_args.data(), zero_args.size()}, true, element).output.erased();

                return Port<TsVar<"V">>{w, reduce_tsl_wire(w, func.value(), ts.erased(), zero)};
            }
        };

        /**
         * ``reduce(func, ts: TSL[V, SIZE], zero) -> V`` — the explicit-zero
         * arity (Python's third parameter): ``zero`` is a plain value wired as
         * ``const(zero)`` at the element schema. Required when the combiner has
         * no registered ``zero`` overload (e.g. a custom sub-graph).
         */
        struct reduce_tsl_zero
        {
            static constexpr auto name = "reduce_tsl_zero";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return reduce_ts_is_fixed_tsl(context, 3);
            }

            static Port<TsVar<"V">> compose(Wiring &w, Scalar<"func", WiredFn> func, Port<TSL<TsVar<"V">>> ts,
                                            Scalar<"zero", ScalarVar<"Z">> zero_value)
            {
                const TSValueTypeMetaData *element = ts.erased().schema->element_ts();

                WiringArg zero_arg;
                zero_arg.kind         = WiringArg::Kind::Scalar;
                zero_arg.scalar_value = Value{zero_value.value()};
                zero_arg.scalar_meta  = zero_arg.scalar_value.schema();
                const WiringPortRef zero =
                    wire_operator(w, "const", {&zero_arg, 1}, true, element).output.erased();

                return Port<TsVar<"V">>{w, reduce_tsl_wire(w, func.value(), ts.erased(), zero)};
            }
        };
    }  // namespace higher_order_impl_detail

    inline void register_higher_order_operators()
    {
        register_graph_overload<reduce_, higher_order_impl_detail::reduce_tsl>();
        register_graph_overload<reduce_, higher_order_impl_detail::reduce_tsl_zero>();
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_HIGHER_ORDER_IMPL_H
