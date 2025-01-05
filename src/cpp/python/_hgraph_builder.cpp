
#include <hgraph/python/pyb_wiring.h>

#include <hgraph/builders/output_builder.h>

void export_builders(nb::module_ &m) {
    using namespace hgraph;
    OutputBuilder::register_with_nanobind(m);

}
