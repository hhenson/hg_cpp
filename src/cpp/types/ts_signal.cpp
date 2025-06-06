#include <hgraph/types/ts_signal.h>

namespace hgraph
{

    nb::object TimeSeriesSignalInput::py_value() const { return nb::cast(modified()); }

    nb::object TimeSeriesSignalInput::py_delta_value() const { return py_value(); }

    void       TimeSeriesSignalInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesSignalInput, TimeSeriesInput>(m, "TS_Signal");
    }

}  // namespace hgraph