/*
 * Expose the graph specific elements to python
 */
#include <hgraph/python/pyb_wiring.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/runtime/evaluation_context.h>
#include <hgraph/runtime/evaluation_engine.h>

void export_runtime(nb::module_& m) {
    using namespace hgraph;
    GraphExecutor::register_with_nanobind(m);
    GraphExecutorImpl::register_with_nanobind(m);
    EvaluationClock::register_with_nanobind(m);
    EngineEvaluationClock::register_with_nanobind(m);
    EngineEvaluationClockDelegate::register_with_nanobind(m);
    EvaluationEngineImpl::register_with_nanobind(m);
}