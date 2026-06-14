#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_COLLECTION_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_COLLECTION_IMPL_H

/**
 * Collection-operator implementations (catalogue: ``operators/collection.h``).
 * Implemented so far: ``keys_`` (TSD -> TSS of its keys) and TSS set algebra
 * (``union`` / ``intersection`` / ``difference`` / ``symmetric_difference``),
 * including the Python-compatible TSS/TSD operator aliases
 * (``|`` / ``&`` / ``-`` / ``^``).
 * ``union`` is also what ``map_`` composes to derive its inferred ``__keys__``
 * lifecycle set, mirroring Python (``__keys__ = union(*key_sets)``).
 */

#include <hgraph/lib/std/operators/arithmetic.h>
#include <hgraph/lib/std/operators/collection.h>
#include <hgraph/lib/std/operators/logical.h>
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
        inline void apply_tss_delta(TSSOutputView &out, const std::vector<Value> &removed,
                                    const std::vector<Value> &added)
        {
            if (removed.empty() && added.empty()) { return; }

            auto mutation = out.begin_mutation(out.evaluation_time());
            for (const Value &member : removed) { (void)mutation.remove(member.view()); }
            for (const Value &member : added) { (void)mutation.add(member.view()); }
        }

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
                auto lhs_set = lhs.data_view();
                auto rhs_set = rhs.data_view();

                std::vector<Value> removed;
                for (const ValueView &key : out.values())
                {
                    if (!lhs_set.contains(key) && !rhs_set.contains(key)) { removed.emplace_back(key); }
                }

                std::vector<Value> added;
                for (const ValueView &key : lhs_set.values())
                {
                    if (!out.contains(key)) { added.emplace_back(key); }
                }
                for (const ValueView &key : rhs_set.values())
                {
                    if (!lhs_set.contains(key) && !out.contains(key)) { added.emplace_back(key); }
                }

                apply_tss_delta(out, removed, added);
            }
        };

        struct intersection_tss_binary
        {
            static constexpr auto name = "intersection_tss";

            static void eval(In<"lhs", TSS<ScalarVar<"K">>, InputValidity::Unchecked> lhs,
                             In<"rhs", TSS<ScalarVar<"K">>, InputValidity::Unchecked> rhs,
                             Out<TSS<ScalarVar<"K">>> out)
            {
                auto lhs_set = lhs.data_view();
                auto rhs_set = rhs.data_view();

                std::vector<Value> removed;
                for (const ValueView &key : out.values())
                {
                    if (!lhs_set.contains(key) || !rhs_set.contains(key)) { removed.emplace_back(key); }
                }

                std::vector<Value> added;
                for (const ValueView &key : lhs_set.values())
                {
                    if (rhs_set.contains(key) && !out.contains(key)) { added.emplace_back(key); }
                }

                apply_tss_delta(out, removed, added);
            }
        };

        struct difference_tss_binary
        {
            static constexpr auto name = "difference_tss";

            static void eval(In<"lhs", TSS<ScalarVar<"K">>, InputValidity::Unchecked> lhs,
                             In<"rhs", TSS<ScalarVar<"K">>, InputValidity::Unchecked> rhs,
                             Out<TSS<ScalarVar<"K">>> out)
            {
                auto lhs_set = lhs.data_view();
                auto rhs_set = rhs.data_view();

                std::vector<Value> removed;
                for (const ValueView &key : out.values())
                {
                    if (!lhs_set.contains(key) || rhs_set.contains(key)) { removed.emplace_back(key); }
                }

                std::vector<Value> added;
                for (const ValueView &key : lhs_set.values())
                {
                    if (!rhs_set.contains(key) && !out.contains(key)) { added.emplace_back(key); }
                }

                apply_tss_delta(out, removed, added);
            }
        };

        struct symmetric_difference_tss_binary
        {
            static constexpr auto name = "symmetric_difference_tss";

            static void eval(In<"lhs", TSS<ScalarVar<"K">>, InputValidity::Unchecked> lhs,
                             In<"rhs", TSS<ScalarVar<"K">>, InputValidity::Unchecked> rhs,
                             Out<TSS<ScalarVar<"K">>> out)
            {
                auto lhs_set = lhs.data_view();
                auto rhs_set = rhs.data_view();

                std::vector<Value> removed;
                for (const ValueView &key : out.values())
                {
                    if (lhs_set.contains(key) == rhs_set.contains(key)) { removed.emplace_back(key); }
                }

                std::vector<Value> added;
                for (const ValueView &key : lhs_set.values())
                {
                    if (!rhs_set.contains(key) && !out.contains(key)) { added.emplace_back(key); }
                }
                for (const ValueView &key : rhs_set.values())
                {
                    if (!lhs_set.contains(key) && !out.contains(key)) { added.emplace_back(key); }
                }

                apply_tss_delta(out, removed, added);
            }
        };

        template <typename Keep>
        inline void erase_tsd_keys_not_matching(TSDDataMutationView &mutation, const TSDOutputView &out, Keep keep)
        {
            std::vector<Value> removed;
            for (const ValueView &key : out.keys())
            {
                if (!keep(key)) { removed.emplace_back(key); }
            }
            for (const Value &key : removed) { (void)mutation.erase(key.view()); }
        }

        inline void copy_tsd_child_if_changed(TSDDataMutationView &mutation, const TSDOutputView &out,
                                              const ValueView &key, const TSInputView &source)
        {
            if (!source.valid()) { return; }

            TSOutputView current = out.at(key);
            if (current.valid() && current.value().equals(source.value())) { return; }
            mutation.set(key, source.value());
        }

        inline bool tsd_key_has_modified_valid_child(const TSDInputView &tsd, const ValueView &key)
        {
            const std::size_t slot = tsd.find_slot(key);
            return slot != TS_DATA_NO_CHILD_ID && tsd.slot_modified(slot) && tsd.at_slot(slot).valid();
        }

        struct difference_tsd_binary
        {
            static constexpr auto name = "difference_tsd";

            static void eval(In<"lhs", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> lhs,
                             In<"rhs", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> rhs,
                             Out<TSD<ScalarVar<"K">, TsVar<"V">>> out)
            {
                const TSDInputView  &lhs_dict = lhs;
                const TSDInputView  &rhs_dict = rhs;
                const TSDOutputView &out_dict = out;

                auto mutation = out_dict.begin_mutation(out_dict.evaluation_time());
                erase_tsd_keys_not_matching(mutation, out_dict, [&](const ValueView &key) {
                    return lhs_dict.contains(key) && !rhs_dict.contains(key);
                });

                for (const auto [key, child] : lhs.modified_items())
                {
                    if (!rhs_dict.contains(key)) { copy_tsd_child_if_changed(mutation, out_dict, key, child); }
                }
                for (const ValueView &key : rhs.removed_keys())
                {
                    if (lhs_dict.contains(key))
                    {
                        copy_tsd_child_if_changed(mutation, out_dict, key, lhs_dict.at(key));
                    }
                }
            }
        };

        struct intersection_tsd_binary
        {
            static constexpr auto name = "intersection_tsd";

            static void eval(In<"lhs", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> lhs,
                             In<"rhs", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> rhs,
                             Out<TSD<ScalarVar<"K">, TsVar<"V">>> out)
            {
                const TSDInputView  &lhs_dict = lhs;
                const TSDInputView  &rhs_dict = rhs;
                const TSDOutputView &out_dict = out;

                auto mutation = out_dict.begin_mutation(out_dict.evaluation_time());
                erase_tsd_keys_not_matching(mutation, out_dict, [&](const ValueView &key) {
                    return lhs_dict.contains(key) && rhs_dict.contains(key);
                });

                for (const auto [key, child] : lhs.modified_items())
                {
                    if (rhs_dict.contains(key)) { copy_tsd_child_if_changed(mutation, out_dict, key, child); }
                }
                for (const ValueView &key : rhs.added_keys())
                {
                    if (lhs_dict.contains(key))
                    {
                        copy_tsd_child_if_changed(mutation, out_dict, key, lhs_dict.at(key));
                    }
                }
            }
        };

        struct union_tsd_binary
        {
            static constexpr auto name = "union_tsd";

            static void eval(In<"lhs", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> lhs,
                             In<"rhs", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> rhs,
                             Out<TSD<ScalarVar<"K">, TsVar<"V">>> out)
            {
                const TSDInputView  &lhs_dict = lhs;
                const TSDInputView  &rhs_dict = rhs;
                const TSDOutputView &out_dict = out;

                auto mutation = out_dict.begin_mutation(out_dict.evaluation_time());
                erase_tsd_keys_not_matching(mutation, out_dict, [&](const ValueView &key) {
                    return lhs_dict.contains(key) || rhs_dict.contains(key);
                });

                for (const auto [key, child] : lhs.modified_items())
                {
                    copy_tsd_child_if_changed(mutation, out_dict, key, child);
                }
                for (const auto [key, child] : rhs.modified_items())
                {
                    if (!tsd_key_has_modified_valid_child(lhs_dict, key))
                    {
                        copy_tsd_child_if_changed(mutation, out_dict, key, child);
                    }
                }
                for (const ValueView &key : lhs.removed_keys())
                {
                    if (rhs_dict.contains(key))
                    {
                        copy_tsd_child_if_changed(mutation, out_dict, key, rhs_dict.at(key));
                    }
                }
                for (const ValueView &key : rhs.removed_keys())
                {
                    if (lhs_dict.contains(key))
                    {
                        copy_tsd_child_if_changed(mutation, out_dict, key, lhs_dict.at(key));
                    }
                }
            }
        };

        struct symmetric_difference_tsd_binary
        {
            static constexpr auto name = "symmetric_difference_tsd";

            static void eval(In<"lhs", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> lhs,
                             In<"rhs", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> rhs,
                             Out<TSD<ScalarVar<"K">, TsVar<"V">>> out)
            {
                const TSDInputView  &lhs_dict = lhs;
                const TSDInputView  &rhs_dict = rhs;
                const TSDOutputView &out_dict = out;

                auto mutation = out_dict.begin_mutation(out_dict.evaluation_time());
                erase_tsd_keys_not_matching(mutation, out_dict, [&](const ValueView &key) {
                    return lhs_dict.contains(key) != rhs_dict.contains(key);
                });

                for (const auto [key, child] : lhs.modified_items())
                {
                    if (!rhs_dict.contains(key)) { copy_tsd_child_if_changed(mutation, out_dict, key, child); }
                }
                for (const auto [key, child] : rhs.modified_items())
                {
                    if (!lhs_dict.contains(key)) { copy_tsd_child_if_changed(mutation, out_dict, key, child); }
                }
                for (const ValueView &key : lhs.removed_keys())
                {
                    if (rhs_dict.contains(key))
                    {
                        copy_tsd_child_if_changed(mutation, out_dict, key, rhs_dict.at(key));
                    }
                }
                for (const ValueView &key : rhs.removed_keys())
                {
                    if (lhs_dict.contains(key))
                    {
                        copy_tsd_child_if_changed(mutation, out_dict, key, lhs_dict.at(key));
                    }
                }
            }
        };

        [[nodiscard]] inline bool all_args_are_tss(OperatorCallContext context)
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

        inline void resolve_output_to_first_arg(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (resolution.find_ts("O") != nullptr) { return; }
            if (context.args.empty() || context.args[0].kind != WiringArg::Kind::TimeSeries) { return; }
            resolution.bind_ts("O", TypeRegistry::instance().dereference(context.args[0].port.schema));
        }

        /** ``union(*ts)`` — n-ary TSS union, folded pairwise at wiring time. */
        struct union_tss_fold
        {
            static constexpr auto name = "union_impl";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return all_args_are_tss(context);
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_output_to_first_arg(resolution, context);
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

        /** ``intersection(*ts)`` — n-ary TSS intersection, folded pairwise at wiring time. */
        struct intersection_tss_fold
        {
            static constexpr auto name = "intersection_impl";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return all_args_are_tss(context);
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_output_to_first_arg(resolution, context);
            }

            static Port<TsVar<"O">> compose(Wiring &w, VarIn<"ts", TsVar<"S">> ts)
            {
                if (ts.empty()) { throw std::invalid_argument("intersection: requires at least one input"); }
                Port<void> acc{w, ts[0]};
                for (std::size_t i = 1; i < ts.size(); ++i)
                {
                    acc = wire<collection_impl_detail::intersection_tss_binary>(w, acc, Port<void>{w, ts[i]});
                }
                return Port<TsVar<"O">>{w, acc.erased()};
            }
        };

        /** ``difference(lhs, rhs)`` — binary TSS set difference. */
        struct difference_tss_fold
        {
            static constexpr auto name = "difference_impl";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return all_args_are_tss(context);
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_output_to_first_arg(resolution, context);
            }

            static Port<TsVar<"O">> compose(Wiring &w, VarIn<"ts", TsVar<"S">> ts)
            {
                if (ts.empty()) { throw std::invalid_argument("difference: requires at least one input"); }
                if (ts.size() == 1) { return Port<TsVar<"O">>{w, ts[0]}; }
                if (ts.size() > 2) { throw std::invalid_argument("difference: more than two inputs is not supported"); }
                Port<void> out = wire<collection_impl_detail::difference_tss_binary>(
                    w, Port<void>{w, ts[0]}, Port<void>{w, ts[1]});
                return Port<TsVar<"O">>{w, out.erased()};
            }
        };

        /** ``symmetric_difference(*ts)`` — n-ary TSS symmetric difference, folded pairwise. */
        struct symmetric_difference_tss_fold
        {
            static constexpr auto name = "symmetric_difference_impl";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return all_args_are_tss(context);
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_output_to_first_arg(resolution, context);
            }

            static Port<TsVar<"O">> compose(Wiring &w, VarIn<"ts", TsVar<"S">> ts)
            {
                if (ts.empty())
                {
                    throw std::invalid_argument("symmetric_difference: requires at least one input");
                }
                Port<void> acc{w, ts[0]};
                for (std::size_t i = 1; i < ts.size(); ++i)
                {
                    acc = wire<collection_impl_detail::symmetric_difference_tss_binary>(w, acc,
                                                                                        Port<void>{w, ts[i]});
                }
                return Port<TsVar<"O">>{w, acc.erased()};
            }
        };
    }  // namespace collection_impl_detail

    inline void register_collection_operators()
    {
        register_graph_overload<keys_, collection_impl_detail::keys_tsd>();
        register_graph_overload<union_, collection_impl_detail::union_tss_fold>();
        register_graph_overload<intersection_, collection_impl_detail::intersection_tss_fold>();
        register_graph_overload<difference_, collection_impl_detail::difference_tss_fold>();
        register_graph_overload<symmetric_difference_, collection_impl_detail::symmetric_difference_tss_fold>();

        register_graph_overload<bit_or, collection_impl_detail::union_tss_fold>();
        register_graph_overload<bit_and, collection_impl_detail::intersection_tss_fold>();
        register_graph_overload<sub_, collection_impl_detail::difference_tss_fold>();
        register_graph_overload<bit_xor, collection_impl_detail::symmetric_difference_tss_fold>();

        register_overload<bit_or, collection_impl_detail::union_tsd_binary>();
        register_overload<bit_and, collection_impl_detail::intersection_tsd_binary>();
        register_overload<sub_, collection_impl_detail::difference_tsd_binary>();
        register_overload<bit_xor, collection_impl_detail::symmetric_difference_tsd_binary>();
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_COLLECTION_IMPL_H
