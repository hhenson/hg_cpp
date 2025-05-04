#ifndef HGRAPH_PYB_H
#define HGRAPH_PYB_H

#include <fmt/format.h>
#include <nanobind/intrusive/ref.h>
#include <nanobind/nanobind.h>
#include <functional>

namespace nb = nanobind;

namespace hgraph
{
    template <typename T, typename T_> nb::ref<T> dynamic_cast_ref(nb::ref<T_> ptr) {
        auto v = dynamic_cast<T *>(ptr.get());
        if (v != nullptr) {
            return nb::ref<T>(v);
        } else {
            return nb::ref<T>();
        }
    }
}  // namespace hgraph

namespace std
{
    template <> struct equal_to<nanobind::object>
    {
        bool operator()(const nanobind::object &a, const nanobind::object &b) const noexcept {
            return a.equal(b);  // nanobind::object::equal handles Python equality
        }
    };

    template <> struct hash<nanobind::object>
    {
        size_t operator()(const nanobind::object &obj) const noexcept {
            // nb::object has .hash() method that returns Py_hash_t
            return static_cast<size_t>(nb::hash(obj));
        }
    };
}  // namespace std

#endif  // HGRAPH_PYB_H
