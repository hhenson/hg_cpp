//
// Created by Howard Henson on 26/12/2024.
//

#ifndef HGRAPH_FORWARD_DECLARATIONS_H
#define HGRAPH_FORWARD_DECLARATIONS_H

#include <hgraph/hgraph_export.h>
#include <nanobind/intrusive/ref.h>

namespace hgraph
{

    struct NodeSignature;
    using node_signature_ptr = nanobind::ref<NodeSignature>;

    struct Node;
    using node_ptr = nanobind::ref<Node>;

    struct Traits;
    using traits_ptr = nanobind::ref<Traits>;

    struct Graph;
    using graph_ptr = nanobind::ref<Graph>;

    struct SenderReceiverState;
    using sender_receiver_state_ptr = SenderReceiverState *;

    struct GraphBuilder;
    struct NodeBuilder;

    struct EngineEvaluationClock;
    using engine_evalaution_clock_ptr = nanobind::ref<EngineEvaluationClock>;

    struct InputBuilder;
    using input_builder_ptr = nanobind::ref<InputBuilder>;

    struct OutputBuilder;
    using output_builder_ptr = nanobind::ref<OutputBuilder>;

    struct TimeSeriesInput;
    using time_series_input_ptr = nanobind::ref<TimeSeriesInput>;

    struct TimeSeriesBundleInput;
    using time_series_bundle_input_ptr = nanobind::ref<TimeSeriesBundleInput>;

    struct TimeSeriesBundleOutput;
    using time_series_bundle_output_ptr = nanobind::ref<TimeSeriesBundleOutput>;

    struct TimeSeriesOutput;
    using time_series_output_ptr = nanobind::ref<TimeSeriesOutput>;

}  // namespace hgraph

#endif  // HGRAPH_FORWARD_DECLARATIONS_H
