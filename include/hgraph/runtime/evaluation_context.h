//
// Created by Howard Henson on 14/12/2024.
//

#ifndef EVALUATION_CONTEXT_H
#define EVALUATION_CONTEXT_H
#include <hgraph/runtime/evaluation_engine.h>
#include <hgraph/types/graph.h>
#include <hgraph/util/date_time.h>

namespace hgraph {

    struct Graph;
    struct Node;

    struct EvaluationContext
    {
        EvaluationContext(EvaluationClock* evaluation_clock, Graph* graph);
        EvaluationContext(const EvaluationContext& other) = default;
        EvaluationContext(EvaluationContext&& other) noexcept = default;

        // Copy assignment operator
        EvaluationContext& operator=(const EvaluationContext& other) = default;
        // Move assignment operator
        EvaluationContext& operator=(EvaluationContext&& other) noexcept = default;

        [[nodiscard]] EvaluationClock& evaluation_clock() const;
        [[nodiscard]] Graph& graph() const;
        [[nodiscard]] Node& node() const;
    protected:
        void set_node(Node *node);
    private:
        EvaluationClock* _evaluation_clock;
        Graph *_graph;
        Node *_node;
    };

}

#endif //EVALUATION_CONTEXT_H
