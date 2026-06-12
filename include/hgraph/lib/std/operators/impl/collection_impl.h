#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_COLLECTION_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_COLLECTION_IMPL_H

/**
 * Collection-operator implementations (catalogue: ``operators/collection.h``).
 * Implemented so far: ``keys_`` (TSD -> TSS of its keys) and ``union``
 * (n-ary TSS union, folded over a binary node at wiring time). These are the
 * operators ``map_`` composes to derive its ``__keys__`` lifecycle set,
 * mirroring Python (``__keys__ = union(*key_sets)``).
 */

#include <hgraph/lib/std/operators/collection.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/subgraph_wiring.h>
#include <hgraph/types/static_node.h>

#include <stdexcept>
#include <utility>
#include <vector>

namespace hgraph::stdlib
{
    namespace collection_impl_detail
    {
        /**
         * ``keys_(tsd) -> TSS[K]`` — the dictionary's key set as a ZERO-COPY
         * projection over the same output (``TSDOutputView::key_set``): no
         * node is wired; the returned port addresses the dict's key-set view
         * via the ``ts_key_set_path_component`` path sentinel. This is the
         * C++ analogue of Python's ``keys_tsd_as_tss`` REF.
         */
        struct keys_tsd
        {
            static constexpr auto name = "keys_tsd";

            static Port<TSS<ScalarVar<"K">>> compose(Wiring &w,
                                                     NamedPort<"ts", TSD<ScalarVar<"K">, TsVar<"V">>> ts)
            {
                return Port<TSS<ScalarVar<"K">>>{w, subgraph_wiring_detail::tsd_key_set_ref(ts.erased())};
            }
        };

        /**
         * Binary TSS union — the fold step. Removal semantics mirror Python's
         * ``union_multiple_tss``: an element leaves the union only when no
         * input still holds it.
         */
        struct union_tss_binary
        {
            static constexpr auto name = "union_tss";

            static void eval(In<"lhs", TSS<ScalarVar<"K">>, InputValidity::Unchecked> lhs,
                             In<"rhs", TSS<ScalarVar<"K">>, InputValidity::Unchecked> rhs,
                             Out<TSS<ScalarVar<"K">>> out)
            {
                auto mutation = out.begin_mutation(out.evaluation_time());

                const bool fresh = !out.valid();
                auto       add_side = [&](const TSInputView &side) {
                    if (!side.valid()) { return; }
                    auto set = side.as_set();
                    if (fresh)
                    {
                        for (const ValueView &key : set.values()) { (void)mutation.add(key); }
                        return;
                    }
                    if (!side.modified()) { return; }
                    for (const ValueView &key : set.added()) { (void)mutation.add(key); }
                };
                add_side(lhs.base());
                add_side(rhs.base());

                // Removals reconcile against the FULL current membership: an
                // element leaves when no valid input still holds it — this
                // also covers a whole input going invalid (e.g. a switched
                // source), which delta-only handling would miss.
                if (!fresh)
                {
                    auto held = [&](const TSInputView &side, const ValueView &key) {
                        return side.valid() && side.as_set().contains(key);
                    };
                    std::vector<Value> stale;
                    for (const ValueView &member : out.values())
                    {
                        if (!held(lhs.base(), member) && !held(rhs.base(), member))
                        {
                            stale.push_back(Value{member});
                        }
                    }
                    for (const Value &member : stale) { (void)mutation.remove(member.view()); }
                }
            }
        };

        /** ``union(*ts)`` — n-ary TSS union, folded pairwise at wiring time. */
        struct union_tss_fold
        {
            static constexpr auto name = "union_impl";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                if (context.args.empty()) { return false; }
                for (const WiringArg &argument : context.args)
                {
                    if (argument.kind != WiringArg::Kind::TimeSeries) { return false; }
                    const auto *schema = TypeRegistry::instance().dereference(argument.port.schema);
                    if (schema == nullptr || schema->kind != TSTypeKind::TSS) { return false; }
                }
                return true;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (resolution.find_ts("O") != nullptr) { return; }
                if (context.args.empty() || context.args[0].kind != WiringArg::Kind::TimeSeries) { return; }
                resolution.bind_ts("O", TypeRegistry::instance().dereference(context.args[0].port.schema));
            }

            static Port<TsVar<"O">> compose(Wiring &w, VarIn<"ts", TsVar<"S">> ts)
            {
                if (ts.empty()) { throw std::invalid_argument("union: requires at least one input"); }
                Port<void> acc{w, ts[0]};
                for (std::size_t i = 1; i < ts.size(); ++i)
                {
                    acc = wire<collection_impl_detail::union_tss_binary>(w, acc, Port<void>{w, ts[i]});
                }
                return Port<TsVar<"O">>{w, acc.erased()};
            }
        };
    }  // namespace collection_impl_detail

    inline void register_collection_operators()
    {
        register_graph_overload<keys_, collection_impl_detail::keys_tsd>();
        register_graph_overload<union_, collection_impl_detail::union_tss_fold>();
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_COLLECTION_IMPL_H
