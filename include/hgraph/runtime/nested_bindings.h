#ifndef HGRAPH_RUNTIME_NESTED_BINDINGS_H
#define HGRAPH_RUNTIME_NESTED_BINDINGS_H

#include <hgraph/runtime/graph.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_input/detail.h>
#include <hgraph/types/time_series/ts_output.h>

#include <cstddef>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

namespace hgraph
{
    /**
     * Shared boundary-binding helpers for nested-graph node implementations
     * (``single_nested_graph_node`` today; ``switch_`` / ``map_`` build on the
     * same model — no bespoke bind/unbind logic per operator). See the
     * developer guide *Nested Graphs*.
     */

    /**
     * Walk an indexed time-series view (TSB field index or TSL index) along a
     * path. Inputs and outputs traverse identically, so one template serves
     * both; the ``ts_key_set_path_component`` sentinel (a TSD's key-set
     * projection) is output-side only — use ``walk_source_to_output`` when a
     * binding path may carry it.
     */
    template <typename View>
    [[nodiscard]] View walk_ts_path(View view, const std::vector<std::size_t> &path)
    {
        for (const std::size_t component : path)
        {
            const auto *schema = view.schema();
            if (schema == nullptr)
            {
                throw std::logic_error("Nested graph path requires a typed time-series view");
            }
            switch (schema->kind)
            {
                case TSTypeKind::TSB:
                {
                    auto bundle = view.as_bundle();
                    view = bundle[component];
                    break;
                }
                case TSTypeKind::TSL:
                {
                    auto list = view.as_list();
                    view = list[component];
                    break;
                }
                case TSTypeKind::TSD:
                {
                    if constexpr (std::is_same_v<View, TSOutputView>)
                    {
                        if (component == ts_key_set_path_component)
                        {
                            view = view.as_dict().key_set();
                            break;
                        }
                    }
                    throw std::invalid_argument(
                        "Nested graph path through a TSD addresses only its key set (output side)");
                }
                default:
                    throw std::invalid_argument(
                        "Nested graph path can only traverse indexed time-series structures");
            }
        }
        return view;
    }

    /**
     * Walk an output endpoint path intended to be a forwarding target. Unlike
     * normal output projection, peered children are returned as the raw
     * TargetLink storage so callers can bind or clear the forwarding target.
     */
    [[nodiscard]] inline TSOutputView walk_forwarding_target_path(
        TSOutputView view,
        const std::vector<std::size_t> &path)
    {
        for (const std::size_t component : path)
        {
            const auto *schema = view.schema();
            if (schema == nullptr)
            {
                throw std::logic_error("Nested graph forwarding target path requires a typed output view");
            }
            switch (schema->kind)
            {
                case TSTypeKind::TSB:
                case TSTypeKind::TSL:
                {
                    auto projection = detail::input_child_projection(view.data_view(), component);
                    TSDataView child = projection.target_link.valid() ? std::move(projection.target_link)
                                                                      : std::move(projection.visible);
                    if (!child.valid())
                    {
                        throw std::logic_error("Nested graph forwarding target path projection failed");
                    }
                    view = TSOutputView{view.output(), child, view.evaluation_time()};
                    break;
                }
                case TSTypeKind::TSD:
                {
                    if (component == ts_key_set_path_component)
                    {
                        view = view.as_dict().key_set();
                        break;
                    }
                    throw std::invalid_argument(
                        "Nested graph forwarding target path through a TSD addresses only its key set");
                }
                default:
                    throw std::invalid_argument(
                        "Nested graph forwarding target path can only traverse indexed time-series structures");
            }
        }
        return view;
    }

    /**
     * Bind a peered child input endpoint to the *same upstream output* the
     * outer boundary is bound to (re-resolved each cycle; a no-op when the
     * endpoint already references the same output, and unbinds while the
     * upstream is unbound).
     */
    inline void bind_input_to_source(TSInputView target, const TSOutputView &source)
    {
        if (!target.is_bindable())
        {
            throw std::logic_error("Nested graph input binding target must be a peered child input endpoint");
        }

        if (!source.bound())
        {
            if (target.bound()) { target.unbind_output(); }
            return;
        }

        auto current = target.bound_output();
        if (!current.handle().same_as(source.handle())) { target.bind_output(source); }
    }

    /** Re-point a forwarding output endpoint at ``source`` (no-op when already there). */
    inline void bind_forwarding_output_to_source(const TSOutputView &target, const TSOutputView &source)
    {
        if (!target.forwarding())
        {
            throw std::logic_error("Nested graph output binding target must be a forwarding output endpoint");
        }

        const auto current = target.forwarding_target();
        if (!current.same_as(source.handle())) { target.bind_forwarding_target(source); }
    }
    /**
     * Resolve a binding source path from an outer INPUT root to the bound
     * OUTPUT it addresses. A trailing ``ts_key_set_path_component`` applies on
     * the output side after the bound-output hop (the input layer has no
     * key-set surface).
     */
    [[nodiscard]] inline TSOutputView walk_source_to_output(TSInputView root,
                                                            const std::vector<std::size_t> &path)
    {
        if (!path.empty() && path.back() == ts_key_set_path_component)
        {
            const std::vector<std::size_t> prefix{path.begin(), path.end() - 1};
            auto bound = walk_ts_path(std::move(root), prefix).bound_output();
            if (!bound.bound()) { return {}; }
            return bound.as_dict().key_set();
        }
        return walk_ts_path(std::move(root), path).bound_output();
    }
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_NESTED_BINDINGS_H
