#ifndef HGRAPH_CPP_ROOT_VALUE_VIEW_H
#define HGRAPH_CPP_ROOT_VALUE_VIEW_H

#include <hgraph/types/value/value_ops.h>

#include <cassert>
#include <cstddef>
#include <stdexcept>
#include <string>

namespace hgraph
{
    /**
     * Non-owning two-word reference to a value.
     *
     * The view carries a borrowed pointer to a ``ValueTypeBinding`` (which
     * exposes the schema, the storage plan, and the runtime ops) plus a
     * pointer to the underlying memory. Constructing a view does not copy
     * the value; destroying it does not destroy the value. Use ``Value``
     * for owning storage.
     *
     * The first slice exposes the kind-agnostic surface used by every
     * value layer kind: ``hash``, ``equals``, ``compare``, ``to_string``
     * via the bound ops, plus typed scalar access through ``as<T>`` /
     * ``try_as<T>`` / ``checked_as<T>``. Specialised views (TupleView,
     * BundleView, ListView, …) and view casting will land alongside the
     * per-kind storage shapes.
     */
    class ValueView
    {
      public:
        constexpr ValueView() noexcept = default;

        ValueView(const ValueTypeBinding *binding, void *data) noexcept
            : binding_{binding}, data_{data} {}

        /** True when both the binding and the data pointer are non-null. */
        [[nodiscard]] bool valid() const noexcept { return binding_ != nullptr && data_ != nullptr; }

        explicit operator bool() const noexcept { return valid(); }

        [[nodiscard]] const ValueTypeBinding *binding() const noexcept { return binding_; }
        [[nodiscard]] const ValueTypeMetaData *schema() const noexcept
        {
            return binding_ != nullptr ? binding_->type_meta : nullptr;
        }
        [[nodiscard]] void *data() noexcept { return data_; }
        [[nodiscard]] const void *data() const noexcept { return data_; }

        // -- kind queries --
        [[nodiscard]] bool is_atomic() const noexcept
        {
            return valid() && schema()->kind == ValueTypeKind::Atomic;
        }
        [[nodiscard]] bool is_tuple()  const noexcept { return valid() && schema()->kind == ValueTypeKind::Tuple; }
        [[nodiscard]] bool is_bundle() const noexcept { return valid() && schema()->kind == ValueTypeKind::Bundle; }
        [[nodiscard]] bool is_list()   const noexcept { return valid() && schema()->kind == ValueTypeKind::List; }
        [[nodiscard]] bool is_set()    const noexcept { return valid() && schema()->kind == ValueTypeKind::Set; }
        [[nodiscard]] bool is_map()    const noexcept { return valid() && schema()->kind == ValueTypeKind::Map; }

        /**
         * True when the bound ops vtable is the canonical ``ops_for<T>``
         * for some scalar ``T``. Pointer comparison is sufficient because
         * ``ops_for<T>`` returns a stable function-local-static.
         */
        template <typename T>
        [[nodiscard]] bool holds_alternative() const noexcept
        {
            return is_atomic() && binding_->ops == &ops_for<T>();
        }

        // -- typed atomic access (debug-asserted) --
        template <typename T>
        [[nodiscard]] const T &as() const noexcept
        {
            assert(valid() && "as<T>() on invalid ValueView");
            assert(holds_alternative<T>() && "as<T>() type mismatch");
            return *static_cast<const T *>(data_);
        }
        template <typename T>
        [[nodiscard]] T &as() noexcept
        {
            assert(valid() && "as<T>() on invalid ValueView");
            assert(holds_alternative<T>() && "as<T>() type mismatch");
            return *static_cast<T *>(data_);
        }

        /** ``try_as<T>`` returns ``nullptr`` on invalid view, non-atomic kind, or type mismatch. */
        template <typename T>
        [[nodiscard]] const T *try_as() const noexcept
        {
            return holds_alternative<T>() ? static_cast<const T *>(data_) : nullptr;
        }
        template <typename T>
        [[nodiscard]] T *try_as() noexcept
        {
            return holds_alternative<T>() ? static_cast<T *>(data_) : nullptr;
        }

        /** ``checked_as<T>`` throws when the view is invalid, non-atomic, or T doesn't match. */
        template <typename T>
        [[nodiscard]] const T &checked_as() const
        {
            if (!valid()) { throw std::logic_error("checked_as<T> on invalid ValueView"); }
            if (!is_atomic()) { throw std::logic_error("checked_as<T> on non-atomic ValueView"); }
            if (!holds_alternative<T>()) { throw std::logic_error("checked_as<T> type mismatch"); }
            return *static_cast<const T *>(data_);
        }
        template <typename T>
        [[nodiscard]] T &checked_as()
        {
            if (!valid()) { throw std::logic_error("checked_as<T> on invalid ValueView"); }
            if (!is_atomic()) { throw std::logic_error("checked_as<T> on non-atomic ValueView"); }
            if (!holds_alternative<T>()) { throw std::logic_error("checked_as<T> type mismatch"); }
            return *static_cast<T *>(data_);
        }

        // -- generic ops, dispatched via the bound ValueOps --
        [[nodiscard]] std::size_t hash() const noexcept
        {
            return valid() ? binding_->checked_ops().hash(data_) : 0;
        }
        [[nodiscard]] bool equals(const ValueView &other) const noexcept
        {
            if (!valid() || !other.valid()) { return false; }
            if (binding_ != other.binding_) { return false; }
            return binding_->checked_ops().equals(data_, other.data_);
        }
        [[nodiscard]] int compare(const ValueView &other) const noexcept
        {
            if (!valid() || !other.valid() || binding_ != other.binding_) { return 0; }
            return binding_->checked_ops().compare(data_, other.data_);
        }
        [[nodiscard]] std::string to_string() const
        {
            return valid() ? binding_->checked_ops().to_string(data_) : std::string{};
        }

      private:
        const ValueTypeBinding *binding_{nullptr};
        void                   *data_{nullptr};
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_VALUE_VIEW_H
