#ifndef HGRAPH_PYB_H
#define HGRAPH_PYB_H

#include <fmt/format.h>
#include <nanobind/intrusive/counter.h>
#include <nanobind/intrusive/ref.h>
#include <nanobind/nanobind.h>

namespace nb = nanobind;

template <typename T, typename T_> nb::ref<T> dynamic_cast_ref(nb::ref<T_> ptr) {
    auto v = dynamic_cast<T *>(ptr.get());
    if (v != nullptr) {
        return nb::ref<T>(v);
    } else {
        throw std::runtime_error(fmt::format("Cannot cast from: {} to : {}", typeid(T_).name(), typeid(T).name()));
    }
}

#endif  // HGRAPH_PYB_H
