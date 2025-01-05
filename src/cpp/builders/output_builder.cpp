#include <hgraph/builders/output_builder.h>

#include<hgraph/python/pyb_wiring.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/ts.h>
#include <hgraph/types/tsb.h>

namespace hgraph
{

    void OutputBuilder::register_with_nanobind(nb::module_ &m) {

        nb::class_<OutputBuilder>(m, "OutputBuilder", nb::intrusive_ptr<OutputBuilder>([](OutputBuilder *o, PyObject *po) noexcept {
                                      o->set_self_py(po);
                                  }))
        .def("make_instance",[](OutputBuilder::ptr self, nb::object owning_node, nb::object owning_output) -> time_series_output_ptr {
            if (!owning_node.is_none()) {
                return self->make_instance(nb::cast<node_ptr>(owning_node));
            }
            if (!owning_output.is_none()) {
                return self->make_instance(nb::cast<time_series_output_ptr>(owning_output));
            }
            throw std::runtime_error("At least one of owning_node or owning_output must be provided");
        }, "owning_node"_a = nb::none(), "owning_output"_a = nb::none())
        .def("release_instance", &OutputBuilder::release_instance);

        using OutputBuilder_TS_Bool = TimeSeriesValueOutputBuilder<bool>;
        using OutputBuilder_TS_Int = TimeSeriesValueOutputBuilder<int64_t>;
        using OutputBuilder_TS_Float = TimeSeriesValueOutputBuilder<double>;
        using OutputBuilder_TS_Date = TimeSeriesValueOutputBuilder<engine_date_t>;
        using OutputBuilder_TS_DateTime = TimeSeriesValueOutputBuilder<engine_time_t>;
        using OutputBuilder_TS_TimeDelta = TimeSeriesValueOutputBuilder<engine_time_delta_t>;
        using OutputBuilder_TS_Python = TimeSeriesValueOutputBuilder<nb::object>;

        nb::class_<OutputBuilder_TS_Bool, OutputBuilder>(m, "OutputBuilder_TS_Bool");
        nb::class_<OutputBuilder_TS_Int, OutputBuilder>(m, "OutputBuilder_TS_Int");
        nb::class_<OutputBuilder_TS_Float, OutputBuilder>(m, "OutputBuilder_TS_Float");
        nb::class_<OutputBuilder_TS_Date, OutputBuilder>(m, "OutputBuilder_TS_Date");
        nb::class_<OutputBuilder_TS_DateTime, OutputBuilder>(m, "OutputBuilder_TS_DateTime");
        nb::class_<OutputBuilder_TS_TimeDelta, OutputBuilder>(m, "OutputBuilder_TS_TimeDelta");
        nb::class_<OutputBuilder_TS_Python, OutputBuilder>(m, "OutputBuilder_TS_Python");
    }

}  // namespace hgraph
