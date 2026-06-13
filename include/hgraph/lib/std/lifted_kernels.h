#ifndef HGRAPH_LIB_STD_LIFTED_KERNELS_H
#define HGRAPH_LIB_STD_LIFTED_KERNELS_H

#include <hgraph/types/primitive_types.h>

#include <array>
#include <string_view>

namespace hgraph::stdlib
{
    template <typename T>
    struct scalar_add
    {
        static constexpr const char *name = "scalar_add";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};
        static constexpr bool associative = true;
        static constexpr bool commutative = true;

        [[nodiscard]] static T identity() { return T{}; }
        [[nodiscard]] static T apply(const T &lhs, const T &rhs) { return lhs + rhs; }
    };

    template <typename T>
    struct scalar_mul
    {
        static constexpr const char *name = "scalar_mul";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};
        static constexpr bool associative = true;
        static constexpr bool commutative = true;

        [[nodiscard]] static T identity() { return T{1}; }
        [[nodiscard]] static T apply(const T &lhs, const T &rhs) { return lhs * rhs; }
    };
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_LIFTED_KERNELS_H
