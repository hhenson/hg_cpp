//
// Created by Howard Henson on 01/05/2025.
//

#ifndef NANOBIND_UTILS_H
#define NANOBIND_UTILS_H

#include <string>
#include <functional>
#include <hgraph/hgraph_forward_declarations.h>
#include <nanobind/nanobind.h>

NAMESPACE_BEGIN(NB_NAMESPACE)
NAMESPACE_BEGIN(detail)

template <> struct type_caster<hgraph::c_string_ref> {
    NB_TYPE_CASTER(hgraph::c_string_ref, const_name("str"))

    bool from_python(handle src, uint8_t, cleanup_list *) noexcept {
        return false;
    }

    static handle from_cpp(hgraph::c_string_ref value, rv_policy,
                           cleanup_list *) noexcept {
        auto v{value.get()};
        return PyUnicode_FromStringAndSize(v.data(), v.size());
    }
};

NAMESPACE_END(detail)
NAMESPACE_END(NB_NAMESPACE)

#endif  // NANOBIND_UTILS_H
