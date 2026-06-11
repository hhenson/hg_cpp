#ifndef HGRAPH_LIB_STD_OPERATORS_HIGHER_ORDER_H
#define HGRAPH_LIB_STD_OPERATORS_HIGHER_ORDER_H

#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/wired_fn.h>

#include <cstddef>
#include <functional>
#include <initializer_list>
#include <optional>
#include <utility>
#include <vector>

namespace hgraph::stdlib
{
    /**
     * Higher-order operator **definitions** (markers only), mirroring the
     * ``ext/main`` Python direction where higher-order constructs are
     * ``@operator``s whose default implementations are ordinary
     * registered overloads. The wirable-function argument is the ``WiredFn``
     * scalar (``fn<X>()``), so overload selection — including user
     * specialisations gated by ``requires_`` on the function's identity — runs
     * through the standard registry best-match machinery. There is nothing
     * special about these operators now that sub-graph compilation is
     * standardised (``compile_subgraph`` / ``nested_``); their default overloads
     * live in ``impl/higher_order_impl.h``.
     */

    /**
     * ``reduce`` — reduce a time-series **collection** into a single
     * time-series with the (associative) combiner ``func``. Mirrors Python
     * ``reduce(func, ts, zero=ZERO)``:
     *
     * - ``ts`` — any multiplexed collection (``TSL`` / ``TSD``; ``TSS`` once
     *   the Python reference grows one). Each collection kind is its own
     *   registered overload of this name, selected by pattern rank —
     *   implemented today: fixed-size ``TSL`` (static wiring-time layout);
     *   the dynamic ``TSD`` kernel follows.
     * - ``zero`` — **optional**, modelled as arity overloads (like ``const``):
     *   when omitted, the zero is derived from the operation via the ``zero``
     *   operator (``zero(item_tp, func)``); when supplied, the value is wired
     *   as ``const(zero)`` at the element schema. Elements that have not
     *   ticked yet count as the zero (``default(ts[i], zero)`` leaves).
     */
    struct reduce_ : Operator<"reduce",
                              Scalar<"func", WiredFn>,
                              In<"ts", TsVar<"C">>,           // the collection (TSL / TSD / TSS ...)
                              Scalar<"zero", ScalarVar<"Z">>, // optional (arity): defaults to zero(item_tp, func)
                              Out<TsVar<"V">>>
    {
    };

    /**
     * The ``switch_`` case table: key value -> wirable branch, with an optional
     * default branch. An ordinary registered scalar, so it participates in
     * interning and overload selection like any other configuration value.
     *
     * A branch may consume the switch key as its **first** argument: its arity
     * is either the number of time-series arguments, or that plus one (key
     * first). Branches may also be pure sources (arity zero with no ts args).
     */
    struct SwitchCase
    {
        Value   key{};
        WiredFn branch{};

        [[nodiscard]] bool operator==(const SwitchCase &other) const
        {
            if (!(branch == other.branch)) { return false; }
            if (key.has_value() != other.key.has_value()) { return false; }
            return !key.has_value() || key.equals(other.key);
        }
    };

    struct SwitchCases
    {
        std::vector<SwitchCase> cases{};
        std::optional<WiredFn>  default_branch{};
        /** Rebuild the active branch on EVERY key tick, not just a key change (Python ``reload_on_ticked``). */
        bool reload_on_ticked{false};

        [[nodiscard]] SwitchCases &&reload(bool value = true) &&
        {
            reload_on_ticked = value;
            return std::move(*this);
        }

        [[nodiscard]] bool operator==(const SwitchCases &other) const
        {
            return cases == other.cases && default_branch == other.default_branch &&
                   reload_on_ticked == other.reload_on_ticked;
        }
    };

    [[nodiscard]] inline SwitchCases switch_cases(std::initializer_list<SwitchCase> entries)
    {
        return SwitchCases{.cases = std::vector<SwitchCase>{entries}};
    }

    [[nodiscard]] inline SwitchCases switch_cases(std::initializer_list<SwitchCase> entries, WiredFn default_branch)
    {
        return SwitchCases{.cases = std::vector<SwitchCase>{entries}, .default_branch = default_branch};
    }

    /**
     * ``switch_`` — route through **one** child graph at a time, selected by
     * ``key``. Mirrors Python ``switch_(key, cases, *ts)``:
     *
     * - on a key change the active branch is stopped and destroyed, the branch
     *   for the new key (else the default, else a runtime error) is built,
     *   bound and started, and the output **re-points** — sampling the new
     *   branch at the switch time (the sampled-runtime contract; a deliberate
     *   divergence from Python's ``value = None`` reset);
     * - a branch may take the key as its first argument (by arity);
     * - the time-series arguments are fixed-arity overloads today (none / one);
     *   variadic arguments arrive with variadic operator support.
     */
    struct switch_ : Operator<"switch_",
                              In<"key", TS<ScalarVar<"K">>>,
                              Scalar<"cases", SwitchCases>,
                              In<"ts", TsVar<"TS">>,          // optional (arity) time-series argument(s)
                              Out<TsVar<"O">>>
    {
    };

    /**
     * ``map_`` — apply ``func`` element-wise over a multiplexed collection,
     * one child graph instance **per key**. This is the current C++ subset of
     * Python ``map_(func, *args)``:
     *
     * - the multiplexed input is a ``TSD`` (keyed runtime children) or a
     *   fixed-size ``TSL`` (wiring-time expansion: one inline application of
     *   ``func`` per index, key = the ``Int`` index — Python's
     *   ``_map_no_index``, no runtime node); TSD key lifecycle is reconciled against
     *   the current key set when the mapped source modifies or re-points — a
     *   new key builds/binds/starts a fresh child, a missing key destroys it
     *   and removes the output entry;
     * - ``func`` may take the key as its first argument (by arity): with one
     *   multiplexed input, arity 1 is ``(element)`` and arity 2 is
     *   ``(key, element)``; further (broadcast) time-series arguments are
     *   passed whole and extend the arity accordingly;
     * - the output is an owned ``TSD<K, OUT>`` with a real element
     *   instantiated per key; the child's terminal output is a forwarding
     *   endpoint bound onto that element, so the child **writes the parent's
     *   storage directly** (no copy);
     * - fixed-arity overloads today (no broadcast / one broadcast argument);
     *   variadic arguments, ``__keys__`` / ``pass_through`` / ``no_key``
     *   wrappers, and sink maps arrive later.
     */
    struct map_ : Operator<"map_",
                           Scalar<"func", WiredFn>,
                           In<"ts", TsVar<"TS">>,             // the multiplexed collection (+ broadcast args)
                           Out<TsVar<"O">>>
    {
    };
}  // namespace hgraph::stdlib

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<hgraph::stdlib::SwitchCases>
    {
        static constexpr std::string_view value{"switch_cases"};
    };
}  // namespace hgraph::static_schema_detail

template <>
struct std::hash<hgraph::stdlib::SwitchCases>
{
    [[nodiscard]] std::size_t operator()(const hgraph::stdlib::SwitchCases &cases) const noexcept
    {
        std::size_t h = cases.cases.size();
        const auto  combine = [&h](std::size_t v) { h ^= v + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2); };
        for (const auto &entry : cases.cases)
        {
            combine(entry.key.has_value() ? entry.key.hash() : 0);
            combine(std::hash<hgraph::WiredFn>{}(entry.branch));
        }
        if (cases.default_branch.has_value()) { combine(std::hash<hgraph::WiredFn>{}(*cases.default_branch)); }
        combine(cases.reload_on_ticked ? 1 : 0);
        return h;
    }
};

#endif  // HGRAPH_LIB_STD_OPERATORS_HIGHER_ORDER_H
