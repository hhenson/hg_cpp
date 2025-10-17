
#include <hgraph/util/lifecycle.h>
#include <hgraph/python/global_keys.h>

void export_utils(nb::module_ &m) {
    using namespace hgraph;

    ComponentLifeCycle::register_with_nanobind(m);
    OutputKeyBuilder::register_with_nanobind(m);
}