#ifndef HGRAPH_PYB_H
#define HGRAPH_PYB_H

#include <nanobind/nanobind.h>
#include <nanobind/intrusive/ref.h>
#include <fmt/format.h>

namespace nb = nanobind;

template <typename T, typename T_> nb::ref<T> dynamic_cast_ref(nb::ref<T_> ptr) {
    auto v = dynamic_cast<T *>(ptr.get());
    if (v != nullptr) {
        return nb::ref<T>(v);
    } else {
        return nb::ref<T>();
    }
}

#endif  // HGRAPH_PYB_H
