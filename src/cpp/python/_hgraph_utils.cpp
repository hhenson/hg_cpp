
#include <hgraph/python/pyb_wiring.h>

#include <hgraph/util/lifecycle.h>

void export_utils(nb::module_ &m) {
    using namespace hgraph;

    ComponentLifeCycle::register_with_nanobind(m);

}