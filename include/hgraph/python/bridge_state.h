#ifndef HGRAPH_PYTHON_BRIDGE_STATE_H
#define HGRAPH_PYTHON_BRIDGE_STATE_H

#if HGRAPH_ENABLE_PYTHON_USER_NODES

#include <nanobind/nanobind.h>

/**
 * Shared python-bridge STATE consulted by type-erased conversion impls
 * (the ops tables' to_python / from_python) and by the module itself.
 * The hgraph python package fills these at import time; ops installed on
 * core bindings read them. All slots are IMMORTAL (new-leaked) - the
 * registries-outlive-everything teardown rule.
 */
namespace hgraph::python_bridge
{
    namespace nb = nanobind;

    [[nodiscard]] inline nb::object &cmp_result_enum_slot()
    {
        static auto *slot = new nb::object{};
        return *slot;
    }

    [[nodiscard]] inline nb::object &divide_by_zero_enum_slot()
    {
        static auto *slot = new nb::object{};
        return *slot;
    }

    [[nodiscard]] inline nb::object &removed_sentinel_slot()
    {
        static auto *slot = new nb::object{};
        return *slot;
    }

    /** The module-installed python->Value INFERENCE hook (schema-free
        conversion is inherently a dispatch on PYTHON types, so it lives in
        the module; core ops that need it - e.g. Any's from_python - call
        through this slot). */
    using PyInferValueFn = void *;   // set as Value (*)(nb::handle) by the module

    [[nodiscard]] inline PyInferValueFn &py_infer_value_slot()
    {
        static PyInferValueFn slot = nullptr;
        return slot;
    }

    /** CompoundScalar bundle name -> python class (read-back reconstruction). */
    [[nodiscard]] inline nb::dict &bundle_class_registry()
    {
        static auto *registry = new nb::dict();
        return *registry;
    }
}  // namespace hgraph::python_bridge

#endif  // HGRAPH_ENABLE_PYTHON_USER_NODES
#endif  // HGRAPH_PYTHON_BRIDGE_STATE_H
