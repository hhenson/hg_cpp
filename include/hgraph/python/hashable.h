/**
 * Support for hashing and equality comparison of nanobind::object
 */

#ifndef HASHABLE_H
#define HASHABLE_H

#include <nanobind/nanobind.h>

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
            return static_cast<size_t>(nanobind::hash(obj));
        }
    };

    template <typename T, typename U> struct hash<std::tuple<T *, U *>>
    {
        size_t operator()(const std::tuple<T *, U *> &t) const noexcept {
            return std::hash<T *>()(std::get<0>(t)) ^ (std::hash<U *>()(std::get<1>(t)) << 1);
        }
    };

}  // namespace std

#endif //HASHABLE_H
