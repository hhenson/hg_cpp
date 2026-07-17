#ifndef HGRAPH_PYTHON_BRIDGE_STATE_H
#define HGRAPH_PYTHON_BRIDGE_STATE_H

#if HGRAPH_ENABLE_PYTHON_USER_NODES

#include <nanobind/nanobind.h>

#include <unordered_map>
#include <vector>

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

    /** hgraph's REMOVE_IF_EXISTS sentinel (lenient TSD key removal; REMOVE
        is strict — absent key raises at delta application). */
    [[nodiscard]] inline nb::object &remove_if_exists_sentinel_slot()
    {
        static auto *slot = new nb::object{};
        return *slot;
    }

    /** hgraph's Removed(item) marker class (TSS delta shaping). */
    [[nodiscard]] inline nb::object &removed_class_slot()
    {
        static auto *slot = new nb::object{};
        return *slot;
    }

    /** hgraph's SetDelta class (a frozenset subclass): TSS delta_value
        results are built as this type so returning them to a TSS output
        applies as a DELTA (a plain frozenset applies as the full value). */
    [[nodiscard]] inline nb::object &set_delta_class_slot()
    {
        static auto *slot = new nb::object{};
        return *slot;
    }

    /** Python callback that maps canonical type-erased delta values onto the
        public compatibility shapes (SetDelta, Removed, and REMOVED). */
    [[nodiscard]] inline nb::object &delta_shaper_slot()
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

    /**
     * CompoundScalar schema address -> python class (read-back
     * reconstruction). Schema identity is nominal; labels are diagnostic and
     * are not a safe registry key once namespaces and specialisations exist.
     */
    [[nodiscard]] inline nb::dict &bundle_class_registry()
    {
        static auto *registry = new nb::dict();
        return *registry;
    }

    struct NB_EXPORT_SHARED PyBundleClassInfo
    {
        using Allocator = PyObject *(*)(PyTypeObject *, Py_ssize_t);

        nb::object           type{};
        Allocator            allocator{nullptr};
        std::vector<nb::str> field_names{};
    };

    /** Schema-addressed companion to ``bundle_class_registry`` for hot value
        conversion. It retains interned field-name objects so conversion does
        not recreate and hash every field name on every node evaluation. */
    [[nodiscard]] inline std::unordered_map<const void *, PyBundleClassInfo> &
    bundle_class_info_registry()
    {
        static auto *registry =
            new std::unordered_map<const void *, PyBundleClassInfo>();
        return *registry;
    }
}  // namespace hgraph::python_bridge

#endif  // HGRAPH_ENABLE_PYTHON_USER_NODES
#endif  // HGRAPH_PYTHON_BRIDGE_STATE_H
