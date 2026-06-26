#ifndef HGRAPH_RUNTIME_MAPPED_CHILD_BINDINGS_H
#define HGRAPH_RUNTIME_MAPPED_CHILD_BINDINGS_H

#include <hgraph/runtime/map_node.h>
#include <hgraph/runtime/nested_bindings.h>

#include <cstddef>
#include <optional>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

namespace hgraph::runtime_detail
{
    struct MappedChildSourceOverride
    {
        std::size_t source_index{0};
        TSOutputHandle source{};
    };

    [[nodiscard]] inline std::vector<std::size_t> mapped_child_path_suffix(
        const std::vector<std::size_t> &path)
    {
        return std::vector<std::size_t>{path.begin() + 1, path.end()};
    }

    [[nodiscard]] inline TSOutputView mapped_child_input_source(
        const TSInputView &root_input,
        const MapArgSource &arg,
        const ValueView &key,
        const TSOutputView &key_source,
        const std::vector<std::size_t> &source_path)
    {
        switch (arg.kind)
        {
            case MapArgSourceKind::Key:
                return key_source.borrowed_ref();
            case MapArgSourceKind::Element:
            {
                auto mux_source = walk_ts_path(root_input.borrowed_ref(), std::vector<std::size_t>{arg.outer_index})
                                      .bound_output();
                if (!mux_source.bound()) { return {}; }

                auto dict = mux_source.as_dict();
                if (!dict.contains(key)) { return {}; }
                return walk_ts_path(dict.at(key), mapped_child_path_suffix(source_path));
            }
            case MapArgSourceKind::OuterInput:
            {
                std::vector<std::size_t> outer_path = mapped_child_path_suffix(source_path);
                outer_path.insert(outer_path.begin(), arg.outer_index);
                return walk_source_to_output(root_input.borrowed_ref(), outer_path);
            }
        }
        return {};
    }

    inline void bind_mapped_child_inputs(
        const NodeView &parent,
        const GraphView &child,
        DateTime evaluation_time,
        const SingleNestedGraphNodeSpec &spec,
        std::span<const MapArgSource> args,
        const ValueView &key,
        const TSOutputView &key_source,
        std::optional<MappedChildSourceOverride> source_override = std::nullopt)
    {
        if (spec.input_bindings.empty()) { return; }

        auto root_input = parent.input(evaluation_time);
        for (const NestedGraphInputBinding &binding : spec.input_bindings)
        {
            const std::size_t source_index = binding.source_path[0];
            if (source_index >= args.size())
            {
                throw std::out_of_range("mapped child input binding source ordinal is out of range");
            }
            const MapArgSource &arg = args[source_index];

            TSOutputView source{};
            if (source_override.has_value() && source_index == source_override->source_index)
            {
                source = source_override->source.view(evaluation_time);
            }
            else
            {
                source = mapped_child_input_source(root_input.borrowed_ref(), arg, key, key_source,
                                                   binding.source_path);
            }

            auto target = walk_ts_path(child.node_at(binding.target.node).input(evaluation_time),
                                       binding.target.path);
            bind_input_to_source(std::move(target), source);
        }
    }

    inline void bind_mapped_child_output(
        const NodeView &parent,
        const GraphView &child,
        DateTime evaluation_time,
        const std::optional<NestedGraphOutputBinding> &output_binding,
        std::span<const MapArgSource> args,
        const ValueView &key,
        const TSOutputView &key_source,
        MapOutputBindingMode mode = MapOutputBindingMode::ChildTerminalWritesElement)
    {
        if (!output_binding.has_value()) { return; }

        auto output = parent.output(evaluation_time);
        auto dict   = output.as_dict();
        if (!dict.contains(key)) { return; }
        auto element = dict.at(key);

        if (output_binding->kind == NestedGraphOutputBinding::Kind::ParentInput)
        {
            if (mode != MapOutputBindingMode::OutputElementForwardsToParentSource)
            {
                throw std::logic_error("mapped child parent-input output requires a forwarding map output element");
            }
            if (output_binding->parent_source_path.empty())
            {
                throw std::logic_error("mapped child parent-input output requires a source ordinal");
            }
            const std::size_t source_index = output_binding->parent_source_path[0];
            if (source_index >= args.size())
            {
                throw std::out_of_range("mapped child output binding source ordinal is out of range");
            }

            auto root_input = parent.input(evaluation_time);
            auto source = mapped_child_input_source(root_input.borrowed_ref(), args[source_index], key,
                                                    key_source, output_binding->parent_source_path);
            bind_forwarding_output_to_source(element, source);
            return;
        }

        auto child_terminal = walk_ts_path(
            child.node_at(output_binding->source.node).output(evaluation_time),
            output_binding->source.path);
        switch (mode)
        {
            case MapOutputBindingMode::ChildTerminalWritesElement:
                bind_forwarding_output_to_source(child_terminal, element);
                break;
            case MapOutputBindingMode::OutputElementForwardsToChildTerminal:
                bind_forwarding_output_to_source(element, child_terminal);
                break;
            case MapOutputBindingMode::OutputElementForwardsToParentSource:
                throw std::logic_error("mapped child child-output binding cannot use parent-source mode");
        }
    }

    inline void clear_mapped_output_element_binding(
        const NodeView &parent,
        DateTime evaluation_time,
        const ValueView &key,
        MapOutputBindingMode mode)
    {
        if (mode == MapOutputBindingMode::ChildTerminalWritesElement) { return; }

        auto output = parent.output(evaluation_time);
        auto dict   = output.as_dict();
        if (!dict.contains(key)) { return; }

        auto element = dict.at(key);
        if (element.forwarding() && element.forwarding_bound()) { element.clear_forwarding_target(); }
    }
}  // namespace hgraph::runtime_detail

#endif  // HGRAPH_RUNTIME_MAPPED_CHILD_BINDINGS_H
