#ifndef HGRAPH_CPP_ROOT_VALUE_H
#define HGRAPH_CPP_ROOT_VALUE_H

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/value/value_ops.h>
#include <hgraph/types/value/value_view.h>

#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

namespace hgraph
{
    /**
     * Owning handle for a value-layer instance.
     *
     * ``Value`` is a thin wrapper over ``MemoryUtils::StorageHandle``
     * specialised on the value-layer ``ValueTypeBinding``. The storage
     * handle owns the bytes (inline or heap, deciding via the policy), the
     * binding carries the schema + plan + ops, and ``Value`` adds the
     * typed convenience surface: scalar construction, ``ValueView`` lift,
     * and ``hash`` / ``equals`` / ``compare`` / ``to_string`` dispatch
     * through the bound ``ValueOps``.
     *
     * The first slice covers atomic kinds. Composite construction
     * (tuple, bundle, list, …) lands when the per-kind storage shapes are
     * ported — but the underlying ``StorageHandle`` already knows how to
     * deal with composite plans, so the addition is mostly typed accessors.
     */
    class Value
    {
      public:
        using storage_type = MemoryUtils::StorageHandle<MemoryUtils::InlineStoragePolicy<>, ValueTypeBinding>;

        Value() noexcept = default;

        /**
         * Construct a default-valued ``Value`` for the given binding (the
         * binding's plan default-constructs the payload).
         */
        explicit Value(const ValueTypeBinding &binding)
            : storage_(binding)
        {
        }

        /**
         * Construct an owning value for an atomic ``T`` whose schema has
         * been registered through ``TypeRegistry::register_scalar<T>``.
         * The matching binding is looked up; the storage handle drives
         * allocation and construction.
         */
        template <typename T,
                  typename = std::enable_if_t<!std::is_same_v<std::decay_t<T>, Value> &&
                                              !std::is_same_v<std::decay_t<T>, ValueTypeBinding>>>
        explicit Value(T value)
        {
            const ValueTypeBinding *binding = TypeRegistry::instance().scalar_binding<T>();
            if (binding == nullptr)
            {
                throw std::logic_error(
                    "Value(T): scalar type not registered — call register_scalar<T>(name) first");
            }
            storage_ = storage_type(*binding);
            // The handle has default-constructed the payload; assign the
            // caller-supplied value into the live storage.
            *static_cast<T *>(storage_.data()) = std::move(value);
        }

        // Copy and move are inherited from ``StorageHandle`` — copy
        // performs a deep allocation + copy-construct via the binding's
        // plan; move transfers ownership.
        Value(const Value &)            = default;
        Value &operator=(const Value &) = default;
        Value(Value &&) noexcept        = default;
        Value &operator=(Value &&) noexcept = default;
        ~Value()                            = default;

        /** True when storage is allocated and a value has been constructed in it. */
        [[nodiscard]] bool has_value() const noexcept { return storage_.has_value(); }

        /** The bound schema; ``nullptr`` for a default-constructed ``Value``. */
        [[nodiscard]] const ValueTypeMetaData *schema() const noexcept
        {
            const auto *b = storage_.binding();
            return b != nullptr ? b->type_meta : nullptr;
        }
        [[nodiscard]] const ValueTypeBinding *binding() const noexcept { return storage_.binding(); }

        /** Tear down the constructed value (if any) and release storage. */
        void reset() noexcept { storage_.reset(); }

        // -- view access --
        [[nodiscard]] ValueView view()
        {
            return has_value() ? ValueView{storage_.binding(), storage_.data()} : ValueView{};
        }
        [[nodiscard]] ValueView view() const
        {
            return has_value()
                       ? ValueView{storage_.binding(), const_cast<void *>(storage_.data())}
                       : ValueView{};
        }

        // -- atomic access shortcuts --
        template <typename T>
        [[nodiscard]] const T &as() const
        {
            return view().template checked_as<T>();
        }
        template <typename T>
        [[nodiscard]] T &as()
        {
            return view().template checked_as<T>();
        }
        template <typename T>
        [[nodiscard]] const T *try_as() const noexcept
        {
            return view().template try_as<T>();
        }
        template <typename T>
        [[nodiscard]] T *try_as() noexcept
        {
            return view().template try_as<T>();
        }

        // -- generic ops --
        [[nodiscard]] std::size_t hash() const noexcept { return view().hash(); }
        [[nodiscard]] bool equals(const Value &other) const noexcept
        {
            return view().equals(other.view());
        }
        [[nodiscard]] int compare(const Value &other) const noexcept
        {
            return view().compare(other.view());
        }
        [[nodiscard]] std::string to_string() const { return view().to_string(); }

      private:
        storage_type storage_{};
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_VALUE_H
