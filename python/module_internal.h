#ifndef HGRAPH_PYTHON_MODULE_INTERNAL_H
#define HGRAPH_PYTHON_MODULE_INTERNAL_H

#include <hgraph/lib/std/operators/comparison.h>
#include <hgraph/lib/std/operators/control.h>
#include <hgraph/python/chrono.h>
#include <hgraph/types/frame.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value_builder.h>

#include <nanobind/nanobind.h>

#include <cstddef>
#include <functional>
#include <string_view>

namespace hgraph::python_bridge
{
    namespace nb = nanobind;

    struct PyObj
    {
        PyObject *object{nullptr};

        PyObj() noexcept = default;
        explicit PyObj(nb::object value) noexcept;
        PyObj(const PyObj &other) noexcept;
        PyObj(PyObj &&other) noexcept;
        PyObj &operator=(const PyObj &other) noexcept;
        PyObj &operator=(PyObj &&other) noexcept;
        ~PyObj();

        [[nodiscard]] nb::object get() const;

        friend bool operator==(const PyObj &lhs, const PyObj &rhs) noexcept;
    };

    struct PyOpaqueRef
    {
        Value value;
    };

    struct PyArrowStream
    {
        Frame frame;

        [[nodiscard]] nb::object capsule() const;
    };

    [[nodiscard]] nb::object &cmp_result_enum_slot();
    [[nodiscard]] nb::object &divide_by_zero_enum_slot();
    [[nodiscard]] nb::object &removed_sentinel_slot();
    [[nodiscard]] nb::dict   &bundle_class_registry();

    [[nodiscard]] Value      py_to_value(nb::handle object);
    [[nodiscard]] nb::object value_to_py(const ValueView &view);
    [[nodiscard]] nb::object frame_to_py(const Frame &frame);
    [[nodiscard]] Value      py_arrow_to_frame(nb::handle object);

    [[nodiscard]] const ValueTypeBinding &delta_binding(const ValueTypeMetaData *meta);
    [[nodiscard]] Value                   py_to_value_as(nb::handle object, const ValueTypeMetaData *meta);
    [[nodiscard]] Value                   py_to_delta(nb::handle object, const TSValueTypeMetaData *ts);
}  // namespace hgraph::python_bridge

template <>
struct std::hash<hgraph::python_bridge::PyObj>
{
    [[nodiscard]] std::size_t operator()(const hgraph::python_bridge::PyObj &value) const noexcept;
};

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<hgraph::python_bridge::PyObj>
    {
        static constexpr std::string_view value{"object"};
    };
}  // namespace hgraph::static_schema_detail

#endif  // HGRAPH_PYTHON_MODULE_INTERNAL_H
