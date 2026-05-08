#ifndef HGRAPH_CPP_ROOT_VALUE_VIEW_H
#define HGRAPH_CPP_ROOT_VALUE_VIEW_H

#include <hgraph/types/value/value_ops.h>
#include <hgraph/util/tagged_ptr.h>

#include <cassert>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

namespace hgraph
{
    class TupleView;
    class BundleView;
    class ListView;
    class SetView;
    class MapView;
    class CyclicBufferView;
    class QueueView;
    class MutableTupleView;
    class MutableBundleView;
    class MutableListView;
    class MutableCyclicBufferView;
    class MutableQueueView;
    class Value;

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
     * ``try_as<T>`` / ``checked_as<T>``. Kind-specialised casts such as
     * ``as_list()`` and ``as_map()`` are declared here and defined once
     * the specialised view classes are complete.
     *
     * A view created from ``void *`` storage is writable-capable but not
     * mutable. Mutating methods require an explicit ``begin_mutation()``
     * transition, and that transition is allowed only when the bound ops
     * table opts in.
     */
    class ValueView
    {
      public:
        constexpr ValueView() noexcept = default;

        ValueView(const ValueTypeBinding *binding, void *data) noexcept
            : binding_{binding, tag_for(binding, data, BindingTag::Writable)}, data_{data} {}

        ValueView(const ValueTypeBinding *binding, const void *data) noexcept
            : binding_{binding, BindingTag::ReadOnly}, data_{data} {}

        ValueView(const ValueTypeBinding *binding, std::nullptr_t) noexcept
            : binding_{binding, BindingTag::ReadOnly}, data_{nullptr} {}

        /** True when both the binding and the data pointer are non-null. */
        [[nodiscard]] bool valid() const noexcept { return binding() != nullptr && data_ != nullptr; }

        explicit operator bool() const noexcept { return valid(); }

        /** True when the view carries a binding/schema, even if the payload is currently absent. */
        [[nodiscard]] bool bound() const noexcept { return binding() != nullptr; }
        /** True when the view references a live payload. */
        [[nodiscard]] bool has_value() const noexcept { return data_ != nullptr; }
        /** True when the view may write through the payload pointer. */
        [[nodiscard]] bool mutable_payload() const noexcept
        {
            return valid() && binding_.has_enum(BindingTag::Mutable);
        }
        /** True when the view was built from writable storage, even if mutation is not open. */
        [[nodiscard]] bool writable_payload() const noexcept
        {
            return valid() && (binding_.has_enum(BindingTag::Writable) ||
                               binding_.has_enum(BindingTag::Mutable));
        }

        [[nodiscard]] const ValueTypeBinding *binding() const noexcept { return binding_.ptr(); }
        [[nodiscard]] const ValueTypeMetaData *schema() const noexcept
        {
            const auto *bound = binding();
            return bound != nullptr ? bound->type_meta : nullptr;
        }
        [[nodiscard]] const void *data() const noexcept { return data_; }
        [[nodiscard]] void *mutable_data() const
        {
            if (!valid()) { throw std::logic_error("ValueView::mutable_data on invalid view"); }
            if (!binding_.has_enum(BindingTag::Mutable))
            {
                throw std::logic_error("ValueView::mutable_data requires begin_mutation");
            }
            return const_cast<void *>(data_);
        }

        [[nodiscard]] bool can_begin_mutation() const noexcept
        {
            const auto *bound = binding();
            return writable_payload() && bound != nullptr && bound->ops != nullptr &&
                   bound->ops->can_begin_mutation();
        }

        [[nodiscard]] ValueView begin_mutation() const
        {
            if (!valid()) { throw std::logic_error("ValueView::begin_mutation on invalid view"); }
            if (!writable_payload())
            {
                throw std::logic_error("ValueView::begin_mutation requires writable storage");
            }
            const auto *bound = binding();
            if (bound == nullptr || bound->ops == nullptr || !bound->ops->can_begin_mutation())
            {
                throw std::logic_error("ValueView::begin_mutation is not supported by this value ops");
            }
            return ValueView{bound, const_cast<void *>(data_), MutableAccess{}};
        }

        void end_mutation() const noexcept {}

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
        [[nodiscard]] bool is_cyclic_buffer() const noexcept
        {
            return valid() && schema()->kind == ValueTypeKind::CyclicBuffer;
        }
        [[nodiscard]] bool is_queue() const noexcept { return valid() && schema()->kind == ValueTypeKind::Queue; }
        [[nodiscard]] bool is_type(const ValueTypeMetaData *type) const noexcept { return schema() == type; }

        /**
         * True when the bound ops vtable is the canonical ``ops_for<T>``
         * for some scalar ``T``. Pointer comparison is sufficient because
         * ``ops_for<T>`` returns a stable function-local-static.
         */
        template <typename T>
        [[nodiscard]] bool holds_alternative() const noexcept
        {
            const auto *bound = binding();
            return is_atomic() && bound->ops == &ops_for<T>();
        }

        template <typename T>
        [[nodiscard]] bool is_scalar_type() const noexcept
        {
            return holds_alternative<T>();
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
        [[nodiscard]] T &as()
        {
            return checked_mutable_as<T>();
        }

        /** ``try_as<T>`` returns ``nullptr`` on invalid view, non-atomic kind, or type mismatch. */
        template <typename T>
        [[nodiscard]] const T *try_as() const noexcept
        {
            return holds_alternative<T>() ? static_cast<const T *>(data_) : nullptr;
        }

        /** Mutable variant of ``try_as<T>``; returns ``nullptr`` for read-only views. */
        template <typename T>
        [[nodiscard]] T *try_mutable_as() noexcept
        {
            return holds_alternative<T>() && mutable_payload() ? static_cast<T *>(const_cast<void *>(data_))
                                                               : nullptr;
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
        [[nodiscard]] T &checked_mutable_as()
        {
            if (!valid()) { throw std::logic_error("checked_mutable_as<T> on invalid ValueView"); }
            if (!mutable_payload()) { throw std::logic_error("checked_mutable_as<T> requires begin_mutation"); }
            if (!is_atomic()) { throw std::logic_error("checked_mutable_as<T> on non-atomic ValueView"); }
            if (!holds_alternative<T>()) { throw std::logic_error("checked_mutable_as<T> type mismatch"); }
            return *static_cast<T *>(const_cast<void *>(data_));
        }

        template <typename T>
        void set(T &&value)
        {
            set_scalar(std::forward<T>(value));
        }

        template <typename T>
        void set_scalar(T &&value)
        {
            using value_type = std::remove_cvref_t<T>;
            if (!valid()) { throw std::logic_error("set_scalar<T> on invalid ValueView"); }
            if (!mutable_payload()) { throw std::logic_error("set_scalar<T> requires begin_mutation"); }
            if (!is_atomic()) { throw std::logic_error("set_scalar<T> on non-atomic ValueView"); }
            if (!holds_alternative<value_type>()) { throw std::logic_error("set_scalar<T> type mismatch"); }
            *static_cast<value_type *>(const_cast<void *>(data_)) = std::forward<T>(value);
        }

        // -- kind-specialised view casts (definitions in specialised_views.h) --
        [[nodiscard]] TupleView as_tuple() const;
        [[nodiscard]] std::optional<TupleView> try_as_tuple() const;
        [[nodiscard]] BundleView as_bundle() const;
        [[nodiscard]] std::optional<BundleView> try_as_bundle() const;
        [[nodiscard]] ListView as_list() const;
        [[nodiscard]] std::optional<ListView> try_as_list() const;
        [[nodiscard]] SetView as_set() const;
        [[nodiscard]] std::optional<SetView> try_as_set() const;
        [[nodiscard]] MapView as_map() const;
        [[nodiscard]] std::optional<MapView> try_as_map() const;
        [[nodiscard]] CyclicBufferView as_cyclic_buffer() const;
        [[nodiscard]] std::optional<CyclicBufferView> try_as_cyclic_buffer() const;
        [[nodiscard]] QueueView as_queue() const;
        [[nodiscard]] std::optional<QueueView> try_as_queue() const;
        [[nodiscard]] MutableTupleView as_mutable_tuple() const;
        [[nodiscard]] std::optional<MutableTupleView> try_as_mutable_tuple() const;
        [[nodiscard]] MutableBundleView as_mutable_bundle() const;
        [[nodiscard]] std::optional<MutableBundleView> try_as_mutable_bundle() const;
        [[nodiscard]] MutableListView as_mutable_list() const;
        [[nodiscard]] std::optional<MutableListView> try_as_mutable_list() const;
        [[nodiscard]] MutableCyclicBufferView as_mutable_cyclic_buffer() const;
        [[nodiscard]] std::optional<MutableCyclicBufferView> try_as_mutable_cyclic_buffer() const;
        [[nodiscard]] MutableQueueView as_mutable_queue() const;
        [[nodiscard]] std::optional<MutableQueueView> try_as_mutable_queue() const;

        // -- generic ops.
        [[nodiscard]] std::size_t hash() const
        {
            if (!valid()) { throw std::logic_error("ValueView::hash requires a non-empty view"); }
            return binding()->checked_ops().hash(data_);
        }
        [[nodiscard]] bool equals(const ValueView &other) const noexcept;
        [[nodiscard]] std::partial_ordering compare(const ValueView &other) const noexcept;
        [[nodiscard]] bool operator==(const ValueView &other) const noexcept { return equals(other); }
        [[nodiscard]] std::partial_ordering operator<=>(const ValueView &other) const noexcept
        {
            return compare(other);
        }

        [[nodiscard]] Value clone() const;
        void copy_from(const ValueView &other);
        [[nodiscard]] bool try_copy_from(const ValueView &other);

        [[nodiscard]] std::string to_string() const
        {
            if (!valid()) { return std::string{}; }
            return binding()->checked_ops().to_string(data_);
        }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        [[nodiscard]] nb::object to_python() const;
        void from_python(nb::handle source);
#endif

      private:
        enum class BindingTag : std::uintptr_t
        {
            ReadOnly = 0,
            Writable = 1,
            Mutable  = 2,
        };

        using BindingPtr = tagged_ptr<const ValueTypeBinding, 2, BindingTag>;

        struct MutableAccess
        {};

        [[nodiscard]] static constexpr BindingTag tag_for(const ValueTypeBinding *binding,
                                                          const void             *data,
                                                          BindingTag              tag) noexcept
        {
            return binding != nullptr && data != nullptr ? tag : BindingTag::ReadOnly;
        }

        ValueView(const ValueTypeBinding *binding, void *data, MutableAccess) noexcept
            : binding_{binding, tag_for(binding, data, BindingTag::Mutable)}, data_{data} {}

        BindingPtr  binding_{nullptr};
        const void *data_{nullptr};
    };

    static_assert(sizeof(ValueView) == sizeof(void *) * 2, "ValueView must remain a two-word handle");
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_VALUE_VIEW_H
