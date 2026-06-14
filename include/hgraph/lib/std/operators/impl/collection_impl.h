#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_COLLECTION_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_COLLECTION_IMPL_H

/**
 * Collection-operator implementations (catalogue: ``operators/collection.h``).
 * Implemented so far: ``keys_`` (TSD -> TSS of its keys) and TSS set algebra
 * (``union`` / ``intersection`` / ``difference`` / ``symmetric_difference``),
 * including the Python-compatible TSS/TSD operator aliases
 * (``|`` / ``&`` / ``-`` / ``^``), plus basic TSS/TSD truth/equality
 * overloads.
 * ``union`` is also what ``map_`` composes to derive its inferred ``__keys__``
 * lifecycle set, mirroring Python (``__keys__ = union(*key_sets)``).
 */

#include <hgraph/lib/std/operators/arithmetic.h>
#include <hgraph/lib/std/operators/collection.h>
#include <hgraph/lib/std/operators/comparison.h>
#include <hgraph/lib/std/operators/logical.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/subgraph_wiring.h>
#include <hgraph/types/static_node.h>

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
