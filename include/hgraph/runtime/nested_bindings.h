#ifndef HGRAPH_RUNTIME_NESTED_BINDINGS_H
#define HGRAPH_RUNTIME_NESTED_BINDINGS_H

#include <hgraph/runtime/nested_graph_node.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_input/detail.h>
#include <hgraph/types/time_series/ts_output.h>

#include <cstddef>
#include <span>
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
    [[nodiscard]] View walk_ts_path(View view, std::span<const std::size_t> path)
    {
        for (const std::size_t component : path)
        {
            if (view.schema() == nullptr)
            {
                throw std::logic_error("Nested graph path requires a typed time-series view");
            }
            if constexpr (std::is_same_v<View, TSOutputView>)
            {
                if (component == ts_key_set_path_component)
                {
                    view = view.as_dict().key_set();
                    continue;
                }
            }
            else if (component == ts_key_set_path_component)
            {
                throw std::invalid_argument(
                    "Nested graph path through a TSD addresses only its key set (output side)");
            }
            view = view.indexed_child_at(component);
        }
        return view;
    }

    template <typename View>
    [[nodiscard]] View walk_ts_path(View view, const std::vector<std::size_t> &path)
    {
        return walk_ts_path(std::move(view), std::span<const std::size_t>{path});
    }

    /**
     * Walk an output endpoint path intended to be a forwarding target. Unlike
     * normal output projection, peered children are returned as the raw
     * TargetLink storage so callers can bind or clear the forwarding target.
     */
    [[nodiscard]] inline TSOutputView walk_forwarding_target_path(
        TSOutputView view,
        std::span<const std::size_t> path)
    {
        for (const std::size_t component : path)
        {
            if (view.schema() == nullptr)
            {
                throw std::logic_error("Nested graph forwarding target path requires a typed output view");
            }
            if (component == ts_key_set_path_component)
            {
                view = view.as_dict().key_set();
                continue;
            }

            auto projection = detail::input_child_projection(view.data_view(), component);
            TSDataView child = projection.target_link.valid() ? std::move(projection.target_link)
                                                              : std::move(projection.visible);
            if (!child.valid())
            {
                throw std::logic_error("Nested graph forwarding target path projection failed");
            }
            view = TSOutputView{view.output(), child, view.evaluation_time()};
        }
        return view;
    }

    [[nodiscard]] inline TSOutputView walk_forwarding_target_path(
        TSOutputView view,
        const std::vector<std::size_t> &path)
    {
        return walk_forwarding_target_path(std::move(view), std::span<const std::size_t>{path});
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

    /** Bind a newly constructed child boundary and expose the source's current
        value as a sampled delta for this lifecycle transition. */
    inline void bind_sampled_input_to_source(TSInputView target, const TSOutputView &source,
                                             DateTime evaluation_time)
    {
        if (!target.is_bindable())
        {
            throw std::logic_error(
                "Sampled nested graph input binding target must be a peered child input endpoint");
        }

        if (!source.bound())
        {
            if (target.bound()) { target.unbind_output(); }
            return;
        }

        target.bind_output_sampled(source, evaluation_time);
    }

    /**
     * Resolve a source through any already-bound forwarding-output chain.
     *
     * If a keyed parent has re-homed a nested terminal (for example a switch
     * node that is the terminal of a map_/mesh_ child), binding through the
     * intermediate target link would leave the writer one hop too high. Follow
     * the chain when it is already bound so the child terminal writes the
     * ultimate real storage directly.
     */
    [[nodiscard]] inline TSOutputView resolve_forwarding_source(TSOutputView source)
    {
        const DateTime evaluation_time = source.evaluation_time();
        const auto next_forwarding_target = [evaluation_time](const TSOutputHandle &handle) {
            if (!handle.bound()) { return TSOutputHandle{}; }
            const TSOutputView view = handle.view(evaluation_time);
            if (!view.forwarding()) { return TSOutputHandle{}; }
            const TSOutputHandle target = view.forwarding_target();
            return target.bound() ? target : TSOutputHandle{};
        };

        TSOutputHandle tortoise = source.handle();
        TSOutputHandle hare     = source.handle();
        while (source.bound() && source.forwarding())
        {
            TSOutputHandle target = source.forwarding_target();
            if (!target.bound()) { break; }
            source = target.view(evaluation_time);

            tortoise = next_forwarding_target(tortoise);
            hare     = next_forwarding_target(hare);
            if (hare.bound()) { hare = next_forwarding_target(hare); }
            if (tortoise.bound() && hare.bound() && tortoise.same_as(hare))
            {
                throw std::logic_error("Nested graph forwarding output cycle");
            }
        }
        return source;
    }

    /** Re-point a forwarding output endpoint at ``source`` (no-op when already there). */
    inline void bind_forwarding_output_to_source(const TSOutputView &target, const TSOutputView &source)
    {
        if (!target.forwarding())
        {
            throw std::logic_error("Nested graph output binding target must be a forwarding output endpoint");
        }

        TSOutputView resolved_source = resolve_forwarding_source(source.borrowed_ref());
        if (!resolved_source.bound())
        {
            if (target.forwarding_bound()) { target.clear_forwarding_target(); }
            return;
        }

        const auto current = target.forwarding_target();
        if (!current.same_as(resolved_source.handle())) { target.bind_forwarding_target(resolved_source); }
    }
    /**
     * Resolve a binding source path from an outer INPUT root to the bound
     * OUTPUT it addresses. A trailing ``ts_key_set_path_component`` applies on
     * the output side after the bound-output hop (the input layer has no
     * key-set surface).
     */
    [[nodiscard]] inline TSOutputView walk_source_to_output(TSInputView root,
                                                            std::span<const std::size_t> path)
    {
        if (!path.empty() && path.back() == ts_key_set_path_component)
        {
            auto bound = walk_ts_path(std::move(root), path.first(path.size() - 1)).bound_output();
            if (!bound.bound()) { return {}; }
            return bound.as_dict().key_set();
        }
        return walk_ts_path(std::move(root), path).bound_output();
    }

    [[nodiscard]] inline TSOutputView walk_source_to_output(TSInputView root,
                                                            const std::vector<std::size_t> &path)
    {
        return walk_source_to_output(std::move(root), std::span<const std::size_t>{path});
    }

    [[nodiscard]] inline bool nested_input_binding_has_valid_active_target(
        const NodeView &node,
        DateTime evaluation_time,
        std::span<const std::size_t> target_path)
    {
        const auto *schema = node.schema();
        if (schema == nullptr || schema->input_schema == nullptr) { return false; }

        auto input = node.input(evaluation_time);
        if (!target_path.empty())
        {
            bool active = input.active();
            for (const std::size_t component : target_path)
            {
                input = input.indexed_child_at(component);
                active = active || input.active();
            }
            return active && input.valid();
        }
        if (schema->input_schema->kind != TSTypeKind::TSB)
        {
            return input.active() && input.valid();
        }

        const auto bundle = input.as_bundle();
        for (std::size_t slot = 0; slot < schema->input_schema->field_count(); ++slot)
        {
            const auto child = bundle[slot];
            if (child.active() && child.valid()) { return true; }
        }
        return false;
    }

    /**
     * Schedule the active consumers of already-valid boundary inputs after a
     * newly constructed child graph starts. Startup first establishes the
     * actual endpoint activity, including custom nodes whose activity is more
     * precise than their root schema. Declaratively passive child inputs remain
     * passive and cannot trigger this sample.
     *
     * This is the explicit sampled-initialization step owned by nested graph
     * lifecycle code. Input activation only establishes subscriptions; it does
     * not schedule a node or fabricate modified state.
     */
    inline void schedule_sampled_input_consumers(
        const GraphView &child,
        DateTime evaluation_time,
        std::span<const NestedGraphInputBinding> bindings)
    {
        for (const NestedGraphInputBinding &binding : bindings)
        {
            const auto node = child.node_at(binding.target.node);
            if (nested_input_binding_has_valid_active_target(
                    node,
                    evaluation_time,
                    binding.target.path))
            {
                child.schedule_node(binding.target.node, evaluation_time);
            }
        }
    }

    inline void schedule_sampled_input_consumers(
        const GraphView &child,
        DateTime evaluation_time,
        const std::vector<NestedGraphInputBinding> &bindings)
    {
        schedule_sampled_input_consumers(
            child,
            evaluation_time,
            std::span<const NestedGraphInputBinding>{bindings});
    }
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_NESTED_BINDINGS_H
