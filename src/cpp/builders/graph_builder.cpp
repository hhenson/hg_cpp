#include "hgraph/types/tsd.h"

#include <hgraph/builders/graph_builder.h>
#include <hgraph/builders/node_builder.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/traits.h>
#include <hgraph/types/tsb.h>

namespace hgraph
{
    constexpr int64_t ERROR_PATH = -1;  // The path in the wiring edges representing the error output of the node
    constexpr int64_t STATE_PATH = -2;  // The path in the wiring edges representing the recordable state output of the node
    constexpr int64_t KEY_SET    = -3;  // The path in the wiring edges representing the recordable state output of the node

    time_series_output_ptr _extract_output(node_ptr node, const std::vector<int64_t> &path) {
        if (path.empty()) { throw std::runtime_error("No path to find an output for"); }

        TimeSeriesOutput *output = node->output_ptr();
        for (auto index : path) {
            try {
                if (index == KEY_SET) {
                    output = (&dynamic_cast<TimeSeriesDictOutput *>(output)->key_set());
                } else {
                    output = (*dynamic_cast<IndexedTimeSeriesOutput *>(output))[index].get();
                }
            } catch (const std::exception &) { throw std::runtime_error("Invalid path index"); }
        }
        return output;
    }

    time_series_input_ptr _extract_input(node_ptr node, const std::vector<int64_t> &path) {
        if (path.empty()) { throw std::runtime_error("No path to find an input for"); }

        auto input = dynamic_cast_ref<TimeSeriesInput>(node->input_ptr());
        for (auto index : path) {
            try {
                auto indexed_ts{dynamic_cast<IndexedTimeSeriesInput *>(input.get())};
                input = (*indexed_ts)[index];
            } catch (const std::exception &) { throw std::runtime_error("Invalid path index"); }
        }
        return input;
    }

    GraphBuilder::GraphBuilder(std::vector<node_builder_ptr> node_builders_, std::vector<Edge> edges_)
        : node_builders{std::move(node_builders_)}, edges{std::move(edges_)} {}

    graph_ptr GraphBuilder::make_instance(const std::vector<int64_t> &graph_id, node_ptr parent_node,
                                          const std::string &label) const {
        auto nodes = make_and_connect_nodes(graph_id, 0);
        return nb::ref<Graph>{new Graph{graph_id, nodes, parent_node, label, new Traits()}};
    }

    std::vector<node_ptr> GraphBuilder::make_and_connect_nodes(const std::vector<int64_t> &graph_id, int64_t first_node_ndx) const {
        std::vector<node_ptr> nodes;
        nodes.reserve(node_builders.size());

        for (size_t i = 0; i < node_builders.size(); ++i) {
            nodes.push_back(node_builders[i]->make_instance(graph_id, i + first_node_ndx));
        }

        for (const auto &edge : edges) {
            auto src_node = nodes[edge.src_node];
            auto dst_node = nodes[edge.dst_node];

            time_series_output_ptr output;
            if (edge.output_path.size() == 1 && edge.output_path[0] == ERROR_PATH) {
                output = src_node->error_output_ptr();
            } else if (edge.output_path.size() == 1 && edge.output_path[0] == STATE_PATH) {
                output = dynamic_cast_ref<TimeSeriesOutput>(src_node->recordable_state_ptr());
            } else {
                output = edge.output_path.empty() ? src_node->output_ptr() : _extract_output(src_node, edge.output_path);
            }

            auto input = _extract_input(dst_node, edge.input_path);
            input->bind_output(output);
        }

        return nodes;
    }

    void GraphBuilder::release_instance(graph_ptr item) const {
        auto nodes = item->nodes();
        for (size_t i = 0, l = nodes.size(); i < l; ++i) { node_builders[i]->release_instance(nodes[i]); }
        dispose_component(*item);
    }

    void GraphBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<GraphBuilder, Builder>(m, "GraphBuilder")
            .def(nb::init<std::vector<node_builder_ptr>, std::vector<Edge>>(), "node_builders"_a, "edges"_a)
            .def("make_instance", &GraphBuilder::make_instance, "graph_id"_a, "parent_node"_a = nullptr, "label"_a = "")
            .def("make_and_connect_nodes", &GraphBuilder::make_and_connect_nodes, "graph_id"_a, "first_node_ndx"_a)
            .def("release_instance", &GraphBuilder::release_instance, "item"_a);

        nb::class_<Edge>(m, "Edge")
            .def(nb::init<int64_t, std::vector<int64_t>, int64_t, std::vector<int64_t>>(), "src_node"_a, "output_path"_a,
                 "dst_node"_a, "input_path"_a)
            .def_ro("src_node", &Edge::src_node)
            .def_ro("output_path", &Edge::output_path)
            .def_ro("dst_node", &Edge::dst_node)
            .def_ro("input_path", &Edge::input_path);
    }
}  // namespace hgraph
