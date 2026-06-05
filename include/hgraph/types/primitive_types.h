#ifndef HGRAPH_TYPES_PRIMITIVE_TYPES_H
#define HGRAPH_TYPES_PRIMITIVE_TYPES_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace hgraph
{
    using Bool  = bool;
    using Int   = std::int64_t;
    using Float = double;
    using Str   = std::string;

    [[nodiscard]] constexpr Bool bool_(bool value) noexcept { return value; }

    template <typename T>
    [[nodiscard]] constexpr Int int_(T value) noexcept
    {
        return static_cast<Int>(value);
    }

    template <typename T>
    [[nodiscard]] constexpr Float float_(T value) noexcept
    {
        return static_cast<Float>(value);
    }

    [[nodiscard]] inline Str str_(std::string_view value) { return Str{value}; }
    [[nodiscard]] inline Str str_(const char *value) { return Str{value}; }
    [[nodiscard]] inline Str str_(Str value) { return value; }

    namespace literals
    {
        [[nodiscard]] constexpr Int operator""_i(unsigned long long value) noexcept
        {
            return static_cast<Int>(value);
        }

        [[nodiscard]] constexpr Float operator""_f(unsigned long long value) noexcept
        {
            return static_cast<Float>(value);
        }

        [[nodiscard]] constexpr Float operator""_f(long double value) noexcept
        {
            return static_cast<Float>(value);
        }

        [[nodiscard]] inline Str operator""_str(const char *value, std::size_t size)
        {
            return Str{value, size};
        }
    }  // namespace literals
}  // namespace hgraph

#endif  // HGRAPH_TYPES_PRIMITIVE_TYPES_H
