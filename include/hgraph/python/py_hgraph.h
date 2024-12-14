//
// Created by Howard Henson on 10/12/2024.
//

#ifndef PY_HGRAPH_H
#define PY_HGRAPH_H

#include "hgraph/util/date_time.h"

#include <hgraph/python/pyb.h>

namespace hgraph {

    namespace detail
    {
        /*
         * Provide a set of utilities to help expose HGraph Elements to C++ especially whilst we are still
         * performing the incremental addition of runtime elements.
         */
        using namespace nanobind;

        inline module_ hgraph_() { return module_::import_("hgraph"); }
        object node_type() { return hgraph_().attr("Node"); }
        object graph_type() { return hgraph_().attr("Graph"); }
        object evaluation_clock_type() { return hgraph_().attr("EvaluationClock"); }

        #define PyNode_Check(op) PyObject_IsInstance(op, node_type().ptr())
        #define PyGraph_Check(op) PyObject_IsInstance(op, graph_type().ptr())
        #define PyEvaluationClock_Check(op) PyObject_IsInstance(op, evaluation_clock_type().ptr())

        template<typename T>
        struct ptr_like_object
        {
            // Dereference operators
            T& operator*() const {
                return *this;
            }

            T* operator->() const {
                return this;
            }
        };

        struct EvaluationClock : object, ptr_like_object<EvaluationClock>
        {
            using ptr = EvaluationClock;
            NB_OBJECT(EvaluationClock, object, "EvaluationClock", PyEvaluationClock_Check)

            engine_time_t evaluation_time() const;
        };

        struct Graph : object, ptr_like_object<Graph>
        {
            using ptr = Graph;
            NB_OBJECT(Graph, object, "Graph", PyGraph_Check)

            [[nodiscard]] EvaluationClock::ptr evaluation_clock() const;
        };

        struct Node : object, ptr_like_object<Node>
        {
            using ptr = Node;  // Since this is a py_object it is ref counted via python.
            NB_OBJECT(Node, object, "Node", PyNode_Check)

            [[nodiscard]] Graph::ptr owning_graph();
            void notify(engine_time_t modified_time);
        };

    }

    using Node = detail::Node;
    using Graph = detail::Graph;

}

#endif //PY_HGRAPH_H
