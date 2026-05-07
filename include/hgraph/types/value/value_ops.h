#ifndef HGRAPH_CPP_ROOT_VALUE_OPS_H
#define HGRAPH_CPP_ROOT_VALUE_OPS_H

#include <hgraph/config.h>
#include <hgraph/types/metadata/type_binding.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/utils/memory_utils.h>

#include <algorithm>
#include <compare>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <typeinfo>

#if HGRAPH_ENABLE_PYTHON_USER_NODES
#include <nanobind/ndarray.h>
#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>

namespace nb = nanobind;
#endif

namespace hgraph
{
    struct ValueOps;
    using ValueTypeBinding = TypeBinding<ValueTypeMetaData, ValueOps>;
#if HGRAPH_ENABLE_PYTHON_USER_NODES
    using ValueArrayElementAt = const void *(*)(const void *owner, std::size_t index);

    /**
     * Contiguous run of value elements in logical order. ``stride`` is
     * measured in bytes so the descriptor works with type-erased storage.
     */
    struct ValueArraySpan
    {
        const void *data{nullptr};
        std::size_t size{0};
        std::size_t stride{0};
    };

    /**
     * Logical sequence of homogeneous value elements used as input when
     * exporting value storage to a Python array. Callers can supply up to
     * two contiguous spans for fast copy paths, plus an indexed fallback
     * for storage that needs per-element conversion or lookup.
     */
    struct ValueArraySource
    {
        const void            *owner{nullptr};
        std::size_t            size{0};
        ValueArrayElementAt    element_at{nullptr};
        ValueArraySpan         first{};
        ValueArraySpan         second{};
    };
#endif

    /**
     * Runtime behaviour vtable for value-layer types.
     *
     * Each entry is a function pointer whose first argument is the ops
     * context and whose remaining arguments are the memory addresses being
     * operated on. ``ValueOps`` is deliberately independent of
     * ``LifecycleOps`` (which lives on the storage plan): lifecycle ops
     * bring the memory into and out of existence, behaviour ops act on
     * already-constructed memory.
     *
     * Slots:
     *
     * - ``hash(memory)`` — content hash; cached entries can drop to a
     *   shallow pointer hash if the type is non-hashable.
     * - ``equals(lhs, rhs)`` — deep equality.
     * - ``compare(lhs, rhs)`` — C++ comparison-category result following
     *   ``operator<=>`` conventions. Non-comparable types may return
     *   ``std::partial_ordering::equivalent``.
     * - ``to_string(memory)`` — diagnostic string. Non-streamable types
     *   may return the type name.
     */
    struct ValueOps
    {
        const void *context{nullptr};
        std::size_t (*hash_impl)(const void *context, const void *memory) noexcept = nullptr;
        bool (*equals_impl)(const void *context, const void *lhs, const void *rhs) noexcept = nullptr;
        std::partial_ordering (*compare_impl)(const void *context, const void *lhs,
                                              const void *rhs) noexcept = nullptr;
        std::string (*to_string_impl)(const void *context, const void *memory) = nullptr;
#if HGRAPH_ENABLE_PYTHON_USER_NODES
        nb::object (*to_python_impl)(const void *context, const void *memory) = nullptr;
        void (*from_python_impl)(const void *context, const ValueTypeBinding &binding, void *memory,
                                 nb::handle source) = nullptr;
        nb::object (*to_python_buffer_impl)(const void *context, const ValueTypeBinding &binding,
                                            const ValueArraySource &source) = nullptr;
#endif

        [[nodiscard]] std::size_t hash(const void *memory) const noexcept
        {
            return hash_impl != nullptr ? hash_impl(context, memory) : 0;
        }

        [[nodiscard]] bool equals(const void *lhs, const void *rhs) const noexcept
        {
            return equals_impl != nullptr ? equals_impl(context, lhs, rhs) : lhs == rhs;
        }

        [[nodiscard]] std::partial_ordering compare(const void *lhs, const void *rhs) const noexcept
        {
            if (compare_impl == nullptr)
            {
                if (lhs == nullptr && rhs == nullptr) { return std::partial_ordering::equivalent; }
                if (lhs == nullptr) { return std::partial_ordering::less; }
                if (rhs == nullptr) { return std::partial_ordering::greater; }
                return lhs == rhs ? std::partial_ordering::equivalent : std::partial_ordering::unordered;
            }
            return compare_impl(context, lhs, rhs);
        }

        [[nodiscard]] std::string to_string(const void *memory) const
        {
            return to_string_impl != nullptr ? to_string_impl(context, memory) : std::string{};
        }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        [[nodiscard]] nb::object to_python(const void *memory) const
        {
            if (to_python_impl == nullptr)
            {
                throw std::logic_error("ValueOps::to_python is not available for this value type");
            }
            return to_python_impl(context, memory);
        }

        void from_python(const ValueTypeBinding &binding, void *memory, nb::handle source) const
        {
            if (from_python_impl == nullptr)
            {
                throw std::logic_error("ValueOps::from_python is not available for this value type");
            }
            from_python_impl(context, binding, memory, source);
        }

        [[nodiscard]] bool can_to_python_buffer(const ValueTypeBinding &binding) const noexcept
        {
            return binding.type_meta != nullptr && binding.type_meta->is_buffer_compatible() &&
                   to_python_buffer_impl != nullptr;
        }

        [[nodiscard]] nb::object to_python_buffer(const ValueTypeBinding  &binding,
                                                  const ValueArraySource &source) const
        {
            if (!can_to_python_buffer(binding))
            {
                throw std::logic_error("ValueOps::to_python_buffer is not available for this value type");
            }
            if (source.element_at == nullptr && source.first.size + source.second.size != source.size)
            {
                throw std::logic_error("ValueOps::to_python_buffer requires an element accessor or complete spans");
            }
            return to_python_buffer_impl(context, binding, source);
        }
#endif
    };

    namespace value_ops_detail
    {
        template <typename T>
        [[nodiscard]] std::optional<std::partial_ordering> null_order(const T *lhs, const T *rhs) noexcept
        {
            if (lhs == nullptr && rhs == nullptr) { return std::partial_ordering::equivalent; }
            if (lhs == nullptr) { return std::partial_ordering::less; }
            if (rhs == nullptr) { return std::partial_ordering::greater; }
            return std::nullopt;
        }

        template <typename T>
        std::size_t hash_thunk(const void *, const void *memory) noexcept
        {
            if constexpr (std::is_same_v<T, bool>)
            {
                return std::hash<bool>{}(*static_cast<const bool *>(memory));
            }
            else if constexpr (requires(const T &v) { std::hash<T>{}(v); })
            {
                return std::hash<T>{}(*static_cast<const T *>(memory));
            }
            else
            {
                // Fallback: byte-wise hash. Better than an unhashable type
                // crashing through the vtable; rarely exercised because the
                // schema flags signal non-hashability up-front.
                std::size_t seed = 0;
                const auto *bytes = static_cast<const unsigned char *>(memory);
                for (std::size_t i = 0; i < sizeof(T); ++i)
                {
                    seed = seed ^ (bytes[i] + 0x9e3779b9U + (seed << 6U) + (seed >> 2U));
                }
                return seed;
            }
        }

        template <typename T>
        bool equals_thunk(const void *, const void *lhs, const void *rhs) noexcept
        {
            if constexpr (requires(const T &a, const T &b) { { a == b } -> std::convertible_to<bool>; })
            {
                return *static_cast<const T *>(lhs) == *static_cast<const T *>(rhs);
            }
            else
            {
                return std::memcmp(lhs, rhs, sizeof(T)) == 0;
            }
        }

        template <typename T>
        std::partial_ordering compare_thunk(const void *, const void *lhs, const void *rhs) noexcept
        {
            if constexpr (requires(const T &a, const T &b) {
                              { a <=> b } -> std::convertible_to<std::partial_ordering>;
                          })
            {
                return *static_cast<const T *>(lhs) <=> *static_cast<const T *>(rhs);
            }
            else if constexpr (requires(const T &a, const T &b) { { a < b } -> std::convertible_to<bool>; })
            {
                const T &a = *static_cast<const T *>(lhs);
                const T &b = *static_cast<const T *>(rhs);
                if (a < b) { return std::partial_ordering::less; }
                if (b < a) { return std::partial_ordering::greater; }
                return std::partial_ordering::equivalent;
            }
            else
            {
                const int cmp = std::memcmp(lhs, rhs, sizeof(T));
                if (cmp < 0) { return std::partial_ordering::less; }
                if (cmp > 0) { return std::partial_ordering::greater; }
                return std::partial_ordering::equivalent;
            }
        }

        template <typename T>
        std::string to_string_thunk(const void *, const void *memory)
        {
            if constexpr (std::is_same_v<T, std::string>)
            {
                return *static_cast<const std::string *>(memory);
            }
            else if constexpr (std::is_same_v<T, bool>)
            {
                return *static_cast<const bool *>(memory) ? "true" : "false";
            }
            else if constexpr (requires(const T &v, std::ostringstream &os) { os << v; })
            {
                std::ostringstream os;
                os << *static_cast<const T *>(memory);
                return os.str();
            }
            else
            {
                return std::string{"<"} + typeid(T).name() + ">";
            }
        }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        template <typename T>
        constexpr bool python_scalar_castable =
            std::is_arithmetic_v<T> || std::is_same_v<T, std::string>;

        template <typename T>
        nb::object to_python_thunk(const void *, const void *memory)
        {
            if constexpr (python_scalar_castable<T>)
            {
                return nb::cast(*static_cast<const T *>(memory));
            }
            else
            {
                throw std::logic_error("ValueOps::to_python is not available for this scalar type");
            }
        }

        template <typename T>
        void from_python_thunk(const void *, const ValueTypeBinding &, void *memory, nb::handle source)
        {
            if constexpr (python_scalar_castable<T>)
            {
                *static_cast<T *>(memory) = nb::cast<T>(source);
            }
            else
            {
                throw std::logic_error("ValueOps::from_python is not available for this scalar type");
            }
        }

        template <typename T>
        struct python_buffer_traits
        {
            using storage_type = T;

            static storage_type convert(const void *memory)
            {
                return *static_cast<const T *>(memory);
            }

            [[nodiscard]] static constexpr const char *numpy_view_dtype() noexcept { return nullptr; }
        };

        template <>
        struct python_buffer_traits<engine_time_t>
        {
            using storage_type = std::int64_t;

            static storage_type convert(const void *memory)
            {
                return static_cast<storage_type>(
                    static_cast<const engine_time_t *>(memory)->time_since_epoch().count());
            }

            [[nodiscard]] static constexpr const char *numpy_view_dtype() noexcept { return "datetime64[us]"; }
        };

        template <>
        struct python_buffer_traits<engine_time_delta_t>
        {
            using storage_type = std::int64_t;

            static storage_type convert(const void *memory)
            {
                return static_cast<storage_type>(static_cast<const engine_time_delta_t *>(memory)->count());
            }

            [[nodiscard]] static constexpr const char *numpy_view_dtype() noexcept { return "timedelta64[us]"; }
        };

        template <>
        struct python_buffer_traits<engine_date_t>
        {
            using storage_type = std::int64_t;

            static storage_type convert(const void *memory)
            {
                const auto days = std::chrono::sys_days{*static_cast<const engine_date_t *>(memory)}
                                      .time_since_epoch()
                                      .count();
                return static_cast<storage_type>(days);
            }

            [[nodiscard]] static constexpr const char *numpy_view_dtype() noexcept { return "datetime64[D]"; }
        };

        template <typename Storage>
        void copy_value_array_span(Storage *&dst, ValueArraySpan span)
        {
            if (span.size == 0) { return; }
            if (span.data == nullptr)
            {
                throw std::logic_error("ValueOps::to_python_buffer span has null data");
            }

            if (span.stride == sizeof(Storage))
            {
                std::memcpy(dst, span.data, span.size * sizeof(Storage));
                dst += span.size;
                return;
            }

            const auto *src = static_cast<const std::byte *>(span.data);
            for (std::size_t index = 0; index < span.size; ++index)
            {
                std::memcpy(dst + index, src + index * span.stride, sizeof(Storage));
            }
            dst += span.size;
        }

        template <typename Storage>
        void delete_python_buffer(void *memory) noexcept
        {
            delete[] static_cast<Storage *>(memory);
        }

        template <typename T>
        nb::object to_python_buffer_thunk(const void *,
                                          const ValueTypeBinding &,
                                          const ValueArraySource &source)
        {
            if constexpr (detail::buffer_compatible_type<T>)
            {
                using traits      = python_buffer_traits<T>;
                using storage_type = typename traits::storage_type;

                auto          owner = std::make_unique<storage_type[]>(std::max<std::size_t>(source.size, 1));
                storage_type *data  = owner.get();

                constexpr bool direct_copy =
                    std::is_same_v<std::remove_cv_t<T>, storage_type> && std::is_trivially_copyable_v<storage_type>;
                if constexpr (direct_copy)
                {
                    if (source.first.size + source.second.size == source.size)
                    {
                        storage_type *dst = data;
                        copy_value_array_span<storage_type>(dst, source.first);
                        copy_value_array_span<storage_type>(dst, source.second);
                    }
                    else
                    {
                        if (source.element_at == nullptr)
                        {
                            throw std::logic_error("ValueOps::to_python_buffer requires an element accessor");
                        }
                        for (std::size_t index = 0; index < source.size; ++index)
                        {
                            data[index] = traits::convert(source.element_at(source.owner, index));
                        }
                    }
                }
                else
                {
                    if (source.element_at == nullptr)
                    {
                        throw std::logic_error("ValueOps::to_python_buffer requires an element accessor");
                    }
                    for (std::size_t index = 0; index < source.size; ++index)
                    {
                        data[index] = traits::convert(source.element_at(source.owner, index));
                    }
                }

                storage_type *owned = owner.release();
                nb::capsule   owner_capsule{owned, &delete_python_buffer<storage_type>};
                nb::ndarray<nb::numpy, const storage_type, nb::ndim<1>> array{owned, {source.size}, owner_capsule};
                nb::object result = array.cast();
                if constexpr (traits::numpy_view_dtype() != nullptr)
                {
                    return result.attr("view")(nb::str{traits::numpy_view_dtype()});
                }
                return result;
            }
            else
            {
                throw std::logic_error("ValueOps::to_python_buffer is not available for this scalar type");
            }
        }
#endif
    }  // namespace value_ops_detail

    /**
     * Synthesise the canonical ``ValueOps`` for a C++ type ``T``.
     *
     * The returned reference is stable for the program lifetime: each
     * instantiation has its own function-local-static, so two callers asking
     * for ``ops_for<int>()`` get the same address. ``TypeRegistry::register_scalar``
     * uses this helper to pair an atomic schema with its ops at registration.
     */
    template <typename T>
    [[nodiscard]] inline const ValueOps &ops_for() noexcept
    {
        static const ValueOps ops{
            nullptr,
            &value_ops_detail::hash_thunk<T>,
            &value_ops_detail::equals_thunk<T>,
            &value_ops_detail::compare_thunk<T>,
            &value_ops_detail::to_string_thunk<T>,
#if HGRAPH_ENABLE_PYTHON_USER_NODES
            &value_ops_detail::to_python_thunk<T>,
            &value_ops_detail::from_python_thunk<T>,
            &value_ops_detail::to_python_buffer_thunk<T>,
#endif
        };
        return ops;
    }
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_VALUE_OPS_H
