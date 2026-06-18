#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_COLLECTION_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_COLLECTION_IMPL_H

/**
 * Collection-operator implementations (catalogue: ``operators/collection.h``).
 * Implemented so far: ``keys_`` (TSD -> TSS of its keys), ``rekey`` /
 * default-unique ``flip`` for TSDs, and TSS set algebra
 * (``union`` / ``intersection`` / ``difference`` / ``symmetric_difference``),
 * including the Python-compatible TSS/TSD operator aliases
 * (``|`` / ``&`` / ``-`` / ``^``), plus basic TSS/TSD truth/equality
 * overloads.
 * ``union`` is also what ``map_`` composes to derive its inferred ``__keys__``
 * lifecycle set, mirroring Python (``__keys__ = union(*key_sets)``).
 */

#include <hgraph/lib/std/lifted_kernels.h>
#include <hgraph/lib/std/operators/arithmetic.h>
#include <hgraph/lib/std/operators/collection.h>
#include <hgraph/lib/std/operators/comparison.h>
#include <hgraph/lib/std/operators/impl/tsb_itemwise_impl.h>
#include <hgraph/lib/std/operators/impl/tsl_itemwise_impl.h>
#include <hgraph/lib/std/operators/logical.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/lift.h>
#include <hgraph/types/subgraph_wiring.h>
#include <hgraph/types/static_node.h>

#include <ankerl/unordered_dense.h>

#include <compare>
#include <cmath>
#include <limits>
#include <optional>
#include <stdexcept>
#include <tuple>
#include <utility>
#include <vector>

namespace hgraph::stdlib
{
    namespace collection_impl_detail
    {
        struct ValueKeyHash
        {
            [[nodiscard]] std::size_t operator()(const Value &value) const
            {
                return value.has_value() ? value.hash() : 0;
            }
        };

        struct ValueKeyEqual
        {
            [[nodiscard]] bool operator()(const Value &lhs, const Value &rhs) const
            {
                if (lhs.has_value() != rhs.has_value()) { return false; }
                return !lhs.has_value() || lhs.equals(rhs);
            }
        };

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

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (resolution.find_ts("__out__") != nullptr || context.args.size() != 1) { return; }
                if (context.args[0].kind != WiringArg::Kind::TimeSeries) { return; }
                const auto *schema = TypeRegistry::instance().dereference(context.args[0].port.schema);
                if (schema == nullptr || schema->kind != TSTypeKind::TSD) { return; }
                resolution.bind_ts("__out__", TypeRegistry::instance().tss(schema->key_type()));
            }

            static WiringPortRef compose(Wiring &, NamedPort<"ts", TSD<ScalarVar<"K">, TsVar<"V">>> ts)
            {
                return subgraph_wiring_detail::tsd_key_set_ref(ts.erased());
            }
        };

        [[nodiscard]] inline bool tss_equal(const TSSInputView &lhs, const TSSInputView &rhs)
        {
            if (lhs.size() != rhs.size()) { return false; }
            for (const ValueView &key : lhs.values())
            {
                if (!rhs.contains(key)) { return false; }
            }
            return true;
        }

        [[nodiscard]] inline bool tsd_equal(const TSDInputView &lhs, const TSDInputView &rhs)
        {
            if (lhs.size() != rhs.size()) { return false; }
            for (const auto [key, lhs_child] : lhs.items())
            {
                TSInputView rhs_child = rhs.at(key);
                if (!lhs_child.valid() || !rhs_child.valid() || !lhs_child.value().equals(rhs_child.value()))
                {
                    return false;
                }
            }
            return true;
        }

        struct not_tss
        {
            static constexpr auto name = "not_tss";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSS<ScalarVar<"K">>, InputValidity::Unchecked> ts, Out<TS<Bool>> out)
            {
                out.set(ts.empty());
            }
        };

        struct not_tsd
        {
            static constexpr auto name = "not_tsd";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> ts,
                             Out<TS<Bool>> out)
            {
                out.set(ts.empty());
            }
        };

        struct and_tss
        {
            static constexpr auto name = "and_tss";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"lhs", TSS<ScalarVar<"K">>, InputValidity::Unchecked> lhs,
                             In<"rhs", TSS<ScalarVar<"K">>, InputValidity::Unchecked> rhs,
                             Out<TS<Bool>> out)
            {
                out.set(!lhs.empty() && !rhs.empty());
            }
        };

        struct or_tss
        {
            static constexpr auto name = "or_tss";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"lhs", TSS<ScalarVar<"K">>, InputValidity::Unchecked> lhs,
                             In<"rhs", TSS<ScalarVar<"K">>, InputValidity::Unchecked> rhs,
                             Out<TS<Bool>> out)
            {
                out.set(!lhs.empty() || !rhs.empty());
            }
        };

        struct eq_tss
        {
            static constexpr auto name = "eq_tss";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"lhs", TSS<ScalarVar<"K">>, InputValidity::Unchecked> lhs,
                             In<"rhs", TSS<ScalarVar<"K">>, InputValidity::Unchecked> rhs,
                             Out<TS<Bool>> out)
            {
                out.set(tss_equal(lhs, rhs));
            }
        };

        struct eq_tsd
        {
            static constexpr auto name = "eq_tsd";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"lhs", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> lhs,
                             In<"rhs", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> rhs,
                             Out<TS<Bool>> out)
            {
                out.set(tsd_equal(lhs, rhs));
            }
        };

        struct min_tss_unary
        {
            static constexpr auto name = "min_tss_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSS<ScalarVar<"K">>, InputValidity::Unchecked> ts,
                             Out<TS<ScalarVar<"K">>> out)
            {
                const TSSInputView &set = ts;
                std::optional<Value> best;
                for (const ValueView &key : set.values())
                {
                    if (!best.has_value() || key.compare(best->view()) == std::partial_ordering::less)
                    {
                        best.emplace(key);
                    }
                }
                if (best.has_value()) { out.apply(best->view()); }
            }
        };

        struct max_tss_unary
        {
            static constexpr auto name = "max_tss_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSS<ScalarVar<"K">>, InputValidity::Unchecked> ts,
                             Out<TS<ScalarVar<"K">>> out)
            {
                const TSSInputView &set = ts;
                std::optional<Value> best;
                for (const ValueView &key : set.values())
                {
                    if (!best.has_value() || key.compare(best->view()) == std::partial_ordering::greater)
                    {
                        best.emplace(key);
                    }
                }
                if (best.has_value()) { out.apply(best->view()); }
            }
        };

        struct min_tsd_unary
        {
            static constexpr auto name = "min_tsd_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSD<ScalarVar<"K">, TS<ScalarVar<"V">>>, InputValidity::Unchecked> ts,
                             Out<TS<ScalarVar<"V">>> out)
            {
                const TSDInputView &dict = ts;
                std::optional<Value> best;
                for (const TSInputView &child : dict.valid_values())
                {
                    const ValueView value = child.value();
                    if (!best.has_value() || value.compare(best->view()) == std::partial_ordering::less)
                    {
                        best.emplace(value);
                    }
                }
                if (best.has_value()) { out.apply(best->view()); }
            }
        };

        struct max_tsd_unary
        {
            static constexpr auto name = "max_tsd_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSD<ScalarVar<"K">, TS<ScalarVar<"V">>>, InputValidity::Unchecked> ts,
                             Out<TS<ScalarVar<"V">>> out)
            {
                const TSDInputView &dict = ts;
                std::optional<Value> best;
                for (const TSInputView &child : dict.valid_values())
                {
                    const ValueView value = child.value();
                    if (!best.has_value() || value.compare(best->view()) == std::partial_ordering::greater)
                    {
                        best.emplace(value);
                    }
                }
                if (best.has_value()) { out.apply(best->view()); }
            }
        };

        struct min_tsl_unary
        {
            static constexpr auto name = "min_tsl_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSL<TS<ScalarVar<"V">>, SIZE<"N">>, InputValidity::Unchecked> ts,
                             Out<TS<ScalarVar<"V">>> out)
            {
                std::optional<Value> best;
                for (std::size_t i = 0; i < ts.size(); ++i)
                {
                    auto child = ts[i];
                    if (!child.valid()) { continue; }
                    const ValueView value = child.base().value();
                    if (!best.has_value() || value.compare(best->view()) == std::partial_ordering::less)
                    {
                        best.emplace(value);
                    }
                }
                if (best.has_value()) { out.apply(best->view()); }
            }
        };

        struct max_tsl_unary
        {
            static constexpr auto name = "max_tsl_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSL<TS<ScalarVar<"V">>, SIZE<"N">>, InputValidity::Unchecked> ts,
                             Out<TS<ScalarVar<"V">>> out)
            {
                std::optional<Value> best;
                for (std::size_t i = 0; i < ts.size(); ++i)
                {
                    auto child = ts[i];
                    if (!child.valid()) { continue; }
                    const ValueView value = child.base().value();
                    if (!best.has_value() || value.compare(best->view()) == std::partial_ordering::greater)
                    {
                        best.emplace(value);
                    }
                }
                if (best.has_value()) { out.apply(best->view()); }
            }
        };

        template <typename T>
        struct sum_tss_unary
        {
            static constexpr auto name = "sum_tss_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSS<T>, InputValidity::Unchecked> ts, Out<TS<T>> out)
            {
                T total{};
                for (const T &key : ts.values()) { total += key; }
                out.set(total);
            }
        };

        template <typename T, auto N>
        [[nodiscard]] inline std::tuple<Float, std::size_t, std::size_t> tsl_sum_valid_and_size(
            const In<"ts", TSL<TS<T>, N>, InputValidity::Unchecked> &ts)
        {
            Float       total       = 0.0;
            std::size_t valid_count = 0;
            const auto  size        = ts.size();
            for (std::size_t i = 0; i < size; ++i)
            {
                auto child = ts[i];
                if (!child.valid()) { continue; }
                total += static_cast<Float>(child.value());
                ++valid_count;
            }
            return {total, valid_count, size};
        }

        template <typename T>
        struct sum_tsl_unary
        {
            static constexpr auto name = "sum_tsl_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSL<TS<T>, SIZE<"N">>, InputValidity::Unchecked> ts, Out<TS<T>> out)
            {
                T total{};
                for (std::size_t i = 0; i < ts.size(); ++i)
                {
                    auto child = ts[i];
                    if (child.valid()) { total += child.value(); }
                }
                out.set(total);
            }
        };

        template <typename T>
        struct mean_tsl_unary
        {
            static constexpr auto name = "mean_tsl_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSL<TS<T>, SIZE<"N">>, InputValidity::Unchecked> ts, Out<TS<Float>> out)
            {
                const auto [total, _, size] = tsl_sum_valid_and_size(ts);
                static_cast<void>(_);
                out.set(size == 0 ? std::numeric_limits<Float>::quiet_NaN()
                                  : total / static_cast<Float>(size));
            }
        };

        template <typename T>
        struct var_tsl_unary
        {
            static constexpr auto name = "var_tsl_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSL<TS<T>, SIZE<"N">>, InputValidity::Unchecked> ts, Out<TS<Float>> out)
            {
                const auto [total, valid_count, _] = tsl_sum_valid_and_size(ts);
                static_cast<void>(_);
                if (valid_count <= 1)
                {
                    out.set(0.0);
                    return;
                }

                const Float mean_value = total / static_cast<Float>(valid_count);
                Float       sum_sq     = 0.0;
                for (std::size_t i = 0; i < ts.size(); ++i)
                {
                    auto child = ts[i];
                    if (!child.valid()) { continue; }
                    const Float delta = static_cast<Float>(child.value()) - mean_value;
                    sum_sq += delta * delta;
                }
                out.set(sum_sq / static_cast<Float>(valid_count - 1));
            }
        };

        template <typename T>
        struct std_tsl_unary
        {
            static constexpr auto name = "std_tsl_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSL<TS<T>, SIZE<"N">>, InputValidity::Unchecked> ts, Out<TS<Float>> out)
            {
                const auto [total, valid_count, _] = tsl_sum_valid_and_size(ts);
                static_cast<void>(_);
                if (valid_count <= 1)
                {
                    out.set(0.0);
                    return;
                }

                const Float mean_value = total / static_cast<Float>(valid_count);
                Float       sum_sq     = 0.0;
                for (std::size_t i = 0; i < ts.size(); ++i)
                {
                    auto child = ts[i];
                    if (!child.valid()) { continue; }
                    const Float delta = static_cast<Float>(child.value()) - mean_value;
                    sum_sq += delta * delta;
                }
                out.set(std::sqrt(sum_sq / static_cast<Float>(valid_count - 1)));
            }
        };

        template <typename T>
        [[nodiscard]] inline std::pair<Float, std::size_t> tss_sum_and_count(
            const In<"ts", TSS<T>, InputValidity::Unchecked> &ts)
        {
            Float       total = 0.0;
            std::size_t count = 0;
            for (const T &key : ts.values())
            {
                total += static_cast<Float>(key);
                ++count;
            }
            return {total, count};
        }

        template <typename T>
        struct mean_tss_unary
        {
            static constexpr auto name = "mean_tss_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSS<T>, InputValidity::Unchecked> ts, Out<TS<Float>> out)
            {
                const auto [total, count] = tss_sum_and_count(ts);
                out.set(count == 0 ? std::numeric_limits<Float>::quiet_NaN() : total / static_cast<Float>(count));
            }
        };

        template <typename T>
        struct var_tss_unary
        {
            static constexpr auto name = "var_tss_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSS<T>, InputValidity::Unchecked> ts, Out<TS<Float>> out)
            {
                const auto [total, count] = tss_sum_and_count(ts);
                if (count <= 1)
                {
                    out.set(0.0);
                    return;
                }

                const Float mean_value = total / static_cast<Float>(count);
                Float       sum_sq     = 0.0;
                for (const T &key : ts.values())
                {
                    const Float delta = static_cast<Float>(key) - mean_value;
                    sum_sq += delta * delta;
                }
                out.set(sum_sq / static_cast<Float>(count - 1));
            }
        };

        template <typename T>
        struct std_tss_unary
        {
            static constexpr auto name = "std_tss_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSS<T>, InputValidity::Unchecked> ts, Out<TS<Float>> out)
            {
                const auto [total, count] = tss_sum_and_count(ts);
                if (count <= 1)
                {
                    out.set(0.0);
                    return;
                }

                const Float mean_value = total / static_cast<Float>(count);
                Float       sum_sq     = 0.0;
                for (const T &key : ts.values())
                {
                    const Float delta = static_cast<Float>(key) - mean_value;
                    sum_sq += delta * delta;
                }
                out.set(std::sqrt(sum_sq / static_cast<Float>(count - 1)));
            }
        };

        template <typename T>
        struct sum_tsd_unary
        {
            static constexpr auto name = "sum_tsd_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSD<ScalarVar<"K">, TS<T>>, InputValidity::Unchecked> ts, Out<TS<T>> out)
            {
                T total{};
                for (const auto child : ts.valid_values()) { total += child.value(); }
                out.set(total);
            }
        };

        template <typename T>
        [[nodiscard]] inline std::pair<Float, std::size_t> tsd_sum_and_count(
            const In<"ts", TSD<ScalarVar<"K">, TS<T>>, InputValidity::Unchecked> &ts)
        {
            Float       total = 0.0;
            std::size_t count = 0;
            for (const auto child : ts.valid_values())
            {
                total += static_cast<Float>(child.value());
                ++count;
            }
            return {total, count};
        }

        template <typename T>
        struct mean_tsd_unary
        {
            static constexpr auto name = "mean_tsd_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSD<ScalarVar<"K">, TS<T>>, InputValidity::Unchecked> ts,
                             Out<TS<Float>> out)
            {
                const auto [total, count] = tsd_sum_and_count(ts);
                out.set(count == 0 ? std::numeric_limits<Float>::quiet_NaN() : total / static_cast<Float>(count));
            }
        };

        template <typename T>
        struct var_tsd_unary
        {
            static constexpr auto name = "var_tsd_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSD<ScalarVar<"K">, TS<T>>, InputValidity::Unchecked> ts,
                             Out<TS<Float>> out)
            {
                const auto [total, count] = tsd_sum_and_count(ts);
                if (count <= 1)
                {
                    out.set(0.0);
                    return;
                }

                const Float mean_value = total / static_cast<Float>(count);
                Float       sum_sq     = 0.0;
                for (const auto child : ts.valid_values())
                {
                    const Float delta = static_cast<Float>(child.value()) - mean_value;
                    sum_sq += delta * delta;
                }
                out.set(sum_sq / static_cast<Float>(count - 1));
            }
        };

        template <typename T>
        struct std_tsd_unary
        {
            static constexpr auto name = "std_tsd_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSD<ScalarVar<"K">, TS<T>>, InputValidity::Unchecked> ts,
                             Out<TS<Float>> out)
            {
                const auto [total, count] = tsd_sum_and_count(ts);
                if (count <= 1)
                {
                    out.set(0.0);
                    return;
                }

                const Float mean_value = total / static_cast<Float>(count);
                Float       sum_sq     = 0.0;
                for (const auto child : ts.valid_values())
                {
                    const Float delta = static_cast<Float>(child.value()) - mean_value;
                    sum_sq += delta * delta;
                }
                out.set(std::sqrt(sum_sq / static_cast<Float>(count - 1)));
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

        inline void copy_value_if_changed(TSDDataMutationView &mutation, const TSDOutputView &out,
                                          const ValueView &key, const ValueView &value)
        {
            TSOutputView current = out.at(key);
            if (current.valid() && current.value().equals(value)) { return; }
            mutation.set(key, value);
        }

        inline void set_nested_tsd_child(TSDDataMutationView &outer_mutation, DateTime evaluation_time,
                                         const ValueView &outer_key, const ValueView &inner_key,
                                         const TSInputView &source)
        {
            if (!source.valid()) { return; }

            TSDataView       inner = outer_mutation.at(outer_key);
            TSDDataView      inner_dict = inner.as_dict();
            auto             inner_mutation = inner_dict.begin_mutation(evaluation_time);
            TSDataView       current = inner_dict.at(inner_key);
            if (current.valid() && current.value().equals(source.value())) { return; }
            inner_mutation.set(inner_key, source.value());
        }

        inline void erase_nested_tsd_child(TSDDataMutationView &outer_mutation, DateTime evaluation_time,
                                           const ValueView &outer_key, const ValueView &inner_key)
        {
            TSDataView       inner = outer_mutation.at(outer_key);
            TSDDataView      inner_dict = inner.as_dict();
            auto             inner_mutation = inner_dict.begin_mutation(evaluation_time);
            (void)inner_mutation.erase(inner_key);
        }

        inline bool tsd_key_has_modified_valid_child(const TSDInputView &tsd, const ValueView &key)
        {
            const std::size_t slot = tsd.find_slot(key);
            return slot != TS_DATA_NO_CHILD_ID && tsd.slot_modified(slot) && tsd.at_slot(slot).valid();
        }

        using FlippedPreviousIndex = ankerl::unordered_dense::map<Value, Value, ValueKeyHash, ValueKeyEqual>;

        inline FlippedPreviousIndex build_flipped_previous_index(const TSDOutputView &out)
        {
            FlippedPreviousIndex previous;
            previous.reserve(out.size());
            for (const auto [flipped_key, source_key] : out.valid_items())
            {
                previous.insert_or_assign(Value{source_key.value()}, Value{flipped_key});
            }
            return previous;
        }

        struct rekey_tsd_scalar
        {
            static constexpr auto name = "rekey_tsd_scalar";

            static void eval(In<"ts", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> ts,
                             In<"new_keys", TSD<ScalarVar<"K">, TS<ScalarVar<"K1">>>, InputValidity::Unchecked> new_keys,
                             RecordableState<TSD<ScalarVar<"K">, TS<ScalarVar<"K1">>>> previous,
                             Out<TSD<ScalarVar<"K1">, TsVar<"V">>> out)
            {
                TSDOutputView          &out_dict = out;
                TSDOutputView          &prev     = previous;
                auto                    out_mutation = out_dict.begin_mutation(out_dict.evaluation_time());
                auto                    prev_mutation = prev.begin_mutation(prev.evaluation_time());

                for (const ValueView &source_key : ts.removed_keys())
                {
                    TSOutputView mapped_key = prev.at(source_key);
                    if (mapped_key.valid()) { (void)out_mutation.erase(mapped_key.value()); }
                }

                for (const ValueView &source_key : new_keys.removed_keys())
                {
                    TSOutputView mapped_key = prev.at(source_key);
                    if (mapped_key.valid()) { (void)out_mutation.erase(mapped_key.value()); }
                    (void)prev_mutation.erase(source_key);
                }

                for (const auto [source_key, new_key] : new_keys.modified_items())
                {
                    if (!new_key.valid())
                    {
                        TSOutputView mapped_key = prev.at(source_key);
                        if (mapped_key.valid()) { (void)out_mutation.erase(mapped_key.value()); }
                        (void)prev_mutation.erase(source_key);
                        continue;
                    }

                    const TSInputView &new_key_base = new_key.base();
                    TSOutputView mapped_key = prev.at(source_key);
                    if (mapped_key.valid() && !mapped_key.value().equals(new_key_base.value()))
                    {
                        (void)out_mutation.erase(mapped_key.value());
                    }

                    prev_mutation.set(source_key, new_key_base.value());
                    TSInputView source = static_cast<const TSDInputView &>(ts).at(source_key);
                    copy_tsd_child_if_changed(out_mutation, out_dict, new_key_base.value(), source);
                }

                for (const auto [source_key, source] : ts.modified_items())
                {
                    TSOutputView mapped_key = prev.at(source_key);
                    if (mapped_key.valid())
                    {
                        copy_tsd_child_if_changed(out_mutation, out_dict, mapped_key.value(), source);
                    }
                }
            }
        };

        struct flip_tsd_unique
        {
            static constexpr auto name = "flip_tsd_unique";

            static void eval(In<"ts", TSD<ScalarVar<"K">, TS<ScalarVar<"K1">>>, InputValidity::Unchecked> ts,
                             Out<TSD<ScalarVar<"K1">, TS<ScalarVar<"K">>>> out)
            {
                TSDOutputView &out_dict = out;
                auto           previous = build_flipped_previous_index(out_dict);
                auto           out_mutation = out_dict.begin_mutation(out_dict.evaluation_time());

                for (const ValueView &source_key : ts.removed_keys())
                {
                    auto old_value = previous.find(Value{source_key});
                    if (old_value != previous.end()) { (void)out_mutation.erase(old_value->second.view()); }
                }

                for (const auto [source_key, value] : ts.modified_items())
                {
                    if (!value.valid())
                    {
                        auto old_value = previous.find(Value{source_key});
                        if (old_value != previous.end()) { (void)out_mutation.erase(old_value->second.view()); }
                        continue;
                    }

                    const TSInputView &value_base = value.base();
                    auto old_value = previous.find(Value{source_key});
                    if (old_value != previous.end() && !old_value->second.equals(value_base.value()))
                    {
                        (void)out_mutation.erase(old_value->second.view());
                    }

                    copy_value_if_changed(out_mutation, out_dict, value_base.value(), source_key);
                }
            }
        };

        struct partition_tsd_scalar
        {
            static constexpr auto name = "partition_tsd_scalar";

            static void eval(In<"ts", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> ts,
                             In<"partitions", TSD<ScalarVar<"K">, TS<ScalarVar<"K1">>>,
                                InputValidity::Unchecked> partitions,
                             RecordableState<TSD<ScalarVar<"K">, TS<ScalarVar<"K1">>>> previous,
                             Out<TSD<ScalarVar<"K1">, TSD<ScalarVar<"K">, TsVar<"V">>>> out)
            {
                TSDOutputView &out_dict = out;
                TSDOutputView &prev     = previous;
                const auto     evaluation_time = out_dict.evaluation_time();
                auto           out_mutation = out_dict.begin_mutation(evaluation_time);
                auto           prev_mutation = prev.begin_mutation(prev.evaluation_time());

                for (const ValueView &source_key : ts.removed_keys())
                {
                    TSOutputView old_partition = prev.at(source_key);
                    if (old_partition.valid())
                    {
                        erase_nested_tsd_child(out_mutation, evaluation_time, old_partition.value(), source_key);
                    }
                }

                for (const ValueView &source_key : partitions.removed_keys())
                {
                    TSOutputView old_partition = prev.at(source_key);
                    if (old_partition.valid())
                    {
                        erase_nested_tsd_child(out_mutation, evaluation_time, old_partition.value(), source_key);
                    }
                    (void)prev_mutation.erase(source_key);
                }

                for (const auto [source_key, partition] : partitions.modified_items())
                {
                    if (!partition.valid())
                    {
                        TSOutputView old_partition = prev.at(source_key);
                        if (old_partition.valid())
                        {
                            erase_nested_tsd_child(out_mutation, evaluation_time, old_partition.value(), source_key);
                        }
                        (void)prev_mutation.erase(source_key);
                        continue;
                    }

                    const TSInputView &partition_base = partition.base();
                    TSOutputView       old_partition = prev.at(source_key);
                    const bool         partition_changed =
                        !old_partition.valid() || !old_partition.value().equals(partition_base.value());

                    if (old_partition.valid() && partition_changed)
                    {
                        erase_nested_tsd_child(out_mutation, evaluation_time, old_partition.value(), source_key);
                    }

                    prev_mutation.set(source_key, partition_base.value());
                    if (partition_changed)
                    {
                        TSInputView source = static_cast<const TSDInputView &>(ts).at(source_key);
                        set_nested_tsd_child(out_mutation, evaluation_time, partition_base.value(), source_key, source);
                    }
                }

                for (const auto [source_key, source] : ts.modified_items())
                {
                    TSOutputView partition = prev.at(source_key);
                    if (partition.valid())
                    {
                        set_nested_tsd_child(out_mutation, evaluation_time, partition.value(), source_key, source);
                    }
                }
            }
        };

        struct unpartition_tsd
        {
            static constexpr auto name = "unpartition_tsd";

            static void eval(In<"ts", TSD<ScalarVar<"K1">, TSD<ScalarVar<"K">, TsVar<"V">>>,
                                InputValidity::Unchecked> ts,
                             Out<TSD<ScalarVar<"K">, TsVar<"V">>> out)
            {
                TSDOutputView &out_dict = out;
                auto           out_mutation = out_dict.begin_mutation(out_dict.evaluation_time());

                for (const auto [outer_key, inner] : ts.modified_items())
                {
                    static_cast<void>(outer_key);
                    const TSDInputView &inner_dict = inner;
                    for (const ValueView &inner_key : inner_dict.removed_keys())
                    {
                        (void)out_mutation.erase(inner_key);
                    }
                    for (const auto [inner_key, child] : inner_dict.modified_items())
                    {
                        copy_tsd_child_if_changed(out_mutation, out_dict, inner_key, child);
                    }
                }
            }
        };

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
            if (resolution.find_ts("__out__") != nullptr) { return; }
            if (context.args.empty() || context.args[0].kind != WiringArg::Kind::TimeSeries) { return; }
            const TSValueTypeMetaData *output = TypeRegistry::instance().dereference(context.args[0].port.schema);
            if (output == nullptr) { return; }
            if (resolution.find_ts("O") == nullptr) { resolution.bind_ts("O", output); }
            resolution.bind_ts("__out__", output);
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

            static WiringPortRef compose(Wiring &w, VarIn<"ts", TsVar<"S">> ts)
            {
                if (ts.empty()) { throw std::invalid_argument("union: requires at least one input"); }
                Port<void> acc{w, ts[0]};
                for (std::size_t i = 1; i < ts.size(); ++i)
                {
                    acc = wire<collection_impl_detail::union_tss_binary>(w, acc, Port<void>{w, ts[i]});
                }
                return acc.erased();
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

            static WiringPortRef compose(Wiring &w, VarIn<"ts", TsVar<"S">> ts)
            {
                if (ts.empty()) { throw std::invalid_argument("intersection: requires at least one input"); }
                Port<void> acc{w, ts[0]};
                for (std::size_t i = 1; i < ts.size(); ++i)
                {
                    acc = wire<collection_impl_detail::intersection_tss_binary>(w, acc, Port<void>{w, ts[i]});
                }
                return acc.erased();
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

            static WiringPortRef compose(Wiring &w, VarIn<"ts", TsVar<"S">> ts)
            {
                if (ts.empty()) { throw std::invalid_argument("difference: requires at least one input"); }
                if (ts.size() == 1) { return ts[0]; }
                if (ts.size() > 2) { throw std::invalid_argument("difference: more than two inputs is not supported"); }
                Port<void> out = wire<collection_impl_detail::difference_tss_binary>(
                    w, Port<void>{w, ts[0]}, Port<void>{w, ts[1]});
                return out.erased();
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

            static WiringPortRef compose(Wiring &w, VarIn<"ts", TsVar<"S">> ts)
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
                return acc.erased();
            }
        };
    }  // namespace collection_impl_detail

    template <typename Operator, template <typename...> class Kernel>
    inline void register_numeric_binary_collection_overloads()
    {
        register_overload<Operator, lift<Kernel<Int, Int>>>();
        register_overload<Operator, lift<Kernel<Float, Float>>>();
        register_overload<Operator, lift<Kernel<Int, Float>>>();
        register_overload<Operator, lift<Kernel<Float, Int>>>();
    }

    template <typename Operator, template <typename...> class Kernel>
    inline void register_numeric_binary_tsl_lifted_maps()
    {
        using tsl_itemwise_impl_detail::tsl_binary_lifted_map;
        register_graph_overload<Operator, tsl_binary_lifted_map<lift<Kernel<Int, Int>>>>();
        register_graph_overload<Operator, tsl_binary_lifted_map<lift<Kernel<Float, Float>>>>();
        register_graph_overload<Operator, tsl_binary_lifted_map<lift<Kernel<Int, Float>>>>();
        register_graph_overload<Operator, tsl_binary_lifted_map<lift<Kernel<Float, Int>>>>();
    }

    inline void register_collection_operators()
    {
        using tsl_itemwise_impl_detail::tsl_binary_map;
        using tsl_itemwise_impl_detail::tsl_lhs_broadcast_map;
        using tsl_itemwise_impl_detail::tsl_rhs_broadcast_map;
        using tsb_itemwise_impl_detail::tsb_binary_map;

        register_graph_overload<keys_, collection_impl_detail::keys_tsd>();
        register_overload<rekey, collection_impl_detail::rekey_tsd_scalar>();
        register_overload<flip, collection_impl_detail::flip_tsd_unique>();
        register_overload<partition, collection_impl_detail::partition_tsd_scalar>();
        register_overload<unpartition, collection_impl_detail::unpartition_tsd>();
        register_overload<not_, collection_impl_detail::not_tss>();
        register_overload<not_, collection_impl_detail::not_tsd>();
        register_overload<and_, collection_impl_detail::and_tss>();
        register_overload<or_, collection_impl_detail::or_tss>();
        register_overload<eq_, collection_impl_detail::eq_tss>();
        register_overload<eq_, collection_impl_detail::eq_tsd>();
        register_overload<min_, collection_impl_detail::min_tss_unary>();
        register_overload<max_, collection_impl_detail::max_tss_unary>();
        register_overload<min_, collection_impl_detail::min_tsd_unary>();
        register_overload<max_, collection_impl_detail::max_tsd_unary>();
        register_overload<min_, collection_impl_detail::min_tsl_unary>();
        register_overload<max_, collection_impl_detail::max_tsl_unary>();
        register_overload<sum_, collection_impl_detail::sum_tss_unary<Int>>();
        register_overload<sum_, collection_impl_detail::sum_tss_unary<Float>>();
        register_overload<sum_, collection_impl_detail::sum_tsd_unary<Int>>();
        register_overload<sum_, collection_impl_detail::sum_tsd_unary<Float>>();
        register_overload<sum_, collection_impl_detail::sum_tsl_unary<Int>>();
        register_overload<sum_, collection_impl_detail::sum_tsl_unary<Float>>();
        register_overload<mean, collection_impl_detail::mean_tss_unary<Int>>();
        register_overload<mean, collection_impl_detail::mean_tss_unary<Float>>();
        register_overload<std_, collection_impl_detail::std_tss_unary<Int>>();
        register_overload<std_, collection_impl_detail::std_tss_unary<Float>>();
        register_overload<var_, collection_impl_detail::var_tss_unary<Int>>();
        register_overload<var_, collection_impl_detail::var_tss_unary<Float>>();
        register_overload<mean, collection_impl_detail::mean_tsd_unary<Int>>();
        register_overload<mean, collection_impl_detail::mean_tsd_unary<Float>>();
        register_overload<std_, collection_impl_detail::std_tsd_unary<Int>>();
        register_overload<std_, collection_impl_detail::std_tsd_unary<Float>>();
        register_overload<var_, collection_impl_detail::var_tsd_unary<Int>>();
        register_overload<var_, collection_impl_detail::var_tsd_unary<Float>>();
        register_overload<mean, collection_impl_detail::mean_tsl_unary<Int>>();
        register_overload<mean, collection_impl_detail::mean_tsl_unary<Float>>();
        register_overload<std_, collection_impl_detail::std_tsl_unary<Int>>();
        register_overload<std_, collection_impl_detail::std_tsl_unary<Float>>();
        register_overload<var_, collection_impl_detail::var_tsl_unary<Int>>();
        register_overload<var_, collection_impl_detail::var_tsl_unary<Float>>();
        register_graph_overload<sum_, tsl_binary_map<add_>>();
        register_graph_overload<sum_, tsl_rhs_broadcast_map<add_>>();
        register_graph_overload<sum_, tsl_lhs_broadcast_map<add_>>();
        register_graph_overload<sum_, tsb_binary_map<add_>>();

        register_numeric_binary_collection_overloads<mean, scalar_mean>();
        register_numeric_binary_collection_overloads<std_, scalar_std>();
        register_numeric_binary_collection_overloads<var_, scalar_var>();
        register_numeric_binary_tsl_lifted_maps<mean, scalar_mean>();
        register_numeric_binary_tsl_lifted_maps<std_, scalar_std>();
        register_numeric_binary_tsl_lifted_maps<var_, scalar_var>();
        register_graph_overload<mean, tsb_binary_map<mean>>();
        register_graph_overload<std_, tsb_binary_map<std_>>();
        register_graph_overload<var_, tsb_binary_map<var_>>();

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
