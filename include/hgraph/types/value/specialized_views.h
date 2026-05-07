#ifndef HGRAPH_CPP_ROOT_VALUE_SPECIALIZED_VIEWS_H
#define HGRAPH_CPP_ROOT_VALUE_SPECIALIZED_VIEWS_H

#include <hgraph/types/value/container_ops.h>
#include <hgraph/types/value/value_range.h>
#include <hgraph/types/value/value_view.h>

#include <cstddef>
#include <iterator>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

namespace hgraph
{
    /**
     * Specialised read-only views over value-layer structured kinds.
     * See *Erased Types > Specialised Views* for the design narrative.
     *
     * Views never cast the data pointer to a concrete C++ storage type.
     * Read access goes through the bound kind-specific ops sub-class
     * (``ListValueOps``, ``SetValueOps``, ``MapValueOps``,
     * ``CyclicBufferValueOps``, ``QueueValueOps``). Tuple, bundle, and
     * fixed-size list views use factory-installed indexed ops backed by
     * structured ``StoragePlan`` offsets. When slot-store-backed
     * time-series variants register their own ops, the same view code
     * works unchanged.
     */

    namespace specialized_view_detail
    {
        inline void require_valid(bool valid, const char *what)
        {
            if (!valid) { throw std::logic_error(std::string{what} + " on invalid view"); }
        }

        template <typename Ops>
        [[nodiscard]] const Ops *kind_ops(const ValueTypeBinding *binding) noexcept
        {
            return binding != nullptr ? static_cast<const Ops *>(binding->ops) : nullptr;
        }

        [[nodiscard]] inline const IndexedValueOps *checked_indexed_ops(const ValueTypeBinding *binding,
                                                                        const char *what)
        {
            const auto *ops = kind_ops<IndexedValueOps>(binding);
            if (ops == nullptr || ops->size == nullptr || ops->element_at == nullptr ||
                ops->element_binding == nullptr)
            {
                throw std::logic_error(std::string{what} + ": binding does not expose indexed ops");
            }
            return ops;
        }

        [[nodiscard]] inline const CyclicBufferValueOps *checked_cyclic_buffer_ops(const ValueTypeBinding *binding,
                                                                                   const char *what)
        {
            (void)checked_indexed_ops(binding, what);
            const auto *ops = kind_ops<CyclicBufferValueOps>(binding);
            if (ops == nullptr || ops->head == nullptr)
            {
                throw std::logic_error(std::string{what} + ": binding does not expose cyclic-buffer ops");
            }
            return ops;
        }

        [[nodiscard]] inline const QueueValueOps *checked_queue_ops(const ValueTypeBinding *binding,
                                                                    const char *what)
        {
            (void)checked_indexed_ops(binding, what);
            const auto *ops = kind_ops<QueueValueOps>(binding);
            if (ops == nullptr || ops->front == nullptr)
            {
                throw std::logic_error(std::string{what} + ": binding does not expose queue ops");
            }
            return ops;
        }

        [[nodiscard]] inline const SetValueOps *checked_set_ops(const ValueTypeBinding *binding, const char *what)
        {
            const auto *indexed = checked_indexed_ops(binding, what);
            const auto *ops     = kind_ops<SetValueOps>(binding);
            if (ops == nullptr || indexed->make_range == nullptr || ops->contains == nullptr)
            {
                throw std::logic_error(std::string{what} + ": binding does not expose set ops");
            }
            return ops;
        }

        [[nodiscard]] inline const MapValueOps *checked_map_ops(const ValueTypeBinding *binding, const char *what)
        {
            const auto *indexed = checked_indexed_ops(binding, what);
            const auto *ops     = kind_ops<MapValueOps>(binding);
            if (ops == nullptr || indexed->make_range == nullptr || ops->contains == nullptr ||
                ops->value_at == nullptr || ops->value_at_index == nullptr || ops->value_binding == nullptr ||
                ops->make_keys_range == nullptr || ops->make_values_range == nullptr ||
                ops->make_kv_range == nullptr || ops->key_set == nullptr)
            {
                throw std::logic_error(std::string{what} + ": binding does not expose map ops");
            }
            return ops;
        }

        [[nodiscard]] inline ValueView require_kind(ValueView base, ValueTypeKind kind, const char *what)
        {
            require_valid(base.valid(), what);
            if (base.schema() == nullptr || base.schema()->kind != kind)
            {
                throw std::logic_error(std::string{what} + " on wrong value kind");
            }
            return base;
        }
    }  // namespace specialized_view_detail

    /**
     * Base for views over positionally-addressed kinds. Tuple, bundle,
     * fixed-size list, and compact dynamic containers all forward
     * through the bound ``IndexedValueOps``.
     */
    class IndexedValueView : public ValueView
    {
      public:
        explicit IndexedValueView(ValueView base)
            : ValueView(base)
            , ops_{specialized_view_detail::checked_indexed_ops(binding(), "IndexedValueView")}
        {
        }

        [[nodiscard]] std::size_t size() const
        {
            return ops_->size(ops_->context, data());
        }

        [[nodiscard]] ValueView at(std::size_t index) const
        {
            const auto n = ops_->size(ops_->context, data());
            if (index >= n) { throw std::out_of_range("IndexedValueView::at: index out of range"); }
            return ValueView{ops_->element_binding(ops_->context, data(), index),
                             const_cast<void *>(ops_->element_at(ops_->context, data(), index))};
        }

        [[nodiscard]] ValueView operator[](std::size_t index) const { return at(index); }

        /**
         * Lazy element range over the bound indexed ops. The range is
         * held by value so callers can both range-for and take its
         * ``begin``/``end`` independently.
         */
        class iterator
        {
          public:
            using iterator_category = std::forward_iterator_tag;
            using difference_type   = std::ptrdiff_t;
            using value_type        = ValueView;
            using reference         = ValueView;
            using pointer           = void;

            iterator() = default;
            iterator(ValueView base, const IndexedValueOps *ops, std::size_t index)
                : base_{base}, ops_{ops}, index_{index}
            {
            }

            [[nodiscard]] ValueView operator*() const { return IndexedValueView{base_, ops_}.at(index_); }

            iterator &operator++() noexcept
            {
                ++index_;
                return *this;
            }
            iterator operator++(int) noexcept
            {
                auto cur = *this;
                ++(*this);
                return cur;
            }

            [[nodiscard]] bool operator==(const iterator &other) const noexcept
            {
                return base_.binding() == other.base_.binding() && base_.data() == other.base_.data() &&
                       index_ == other.index_;
            }

          private:
            ValueView              base_{};
            const IndexedValueOps *ops_{nullptr};
            std::size_t            index_{0};
        };

        struct ElementRange
        {
            ValueView              base{};
            const IndexedValueOps *ops{nullptr};
            std::size_t            limit{0};

            [[nodiscard]] iterator begin() const noexcept { return iterator{base, ops, 0}; }
            [[nodiscard]] iterator end() const noexcept { return iterator{base, ops, limit}; }
        };

        [[nodiscard]] ElementRange elements() const
        {
            return ElementRange{ValueView{binding(), const_cast<void *>(data())}, ops_, size()};
        }

        [[nodiscard]] ElementRange values() const { return elements(); }

        // Convenience: ``begin()`` / ``end()`` go through ``elements()`` so
        // ``for (auto v : view)`` works directly. Each call to ``begin()``
        // builds a fresh range; callers that want a stable iterator
        // pair should grab ``elements()`` first.
        [[nodiscard]] auto begin() const { return elements().begin(); }
        [[nodiscard]] auto end() const { return elements().end(); }

      protected:
        IndexedValueView(ValueView base, const IndexedValueOps *ops) noexcept
            : ValueView(base), ops_{ops}
        {
        }

        const IndexedValueOps *ops_{nullptr};
    };

    /** Read-only view over a positional Tuple. */
    class TupleView : public IndexedValueView
    {
      public:
        explicit TupleView(ValueView base)
            : IndexedValueView(specialized_view_detail::require_kind(base, ValueTypeKind::Tuple, "TupleView"))
        {
        }
    };

    /** Read-only view over a named Bundle. */
    class BundleView : public IndexedValueView
    {
      public:
        using IndexedValueView::at;
        using IndexedValueView::operator[];

        explicit BundleView(ValueView base)
            : IndexedValueView(specialized_view_detail::require_kind(base, ValueTypeKind::Bundle, "BundleView"))
        {
        }

        [[nodiscard]] ValueView at(std::string_view name) const
        {
            for (std::size_t index = 0; index < schema()->field_count; ++index)
            {
                const char *field_name = schema()->fields[index].name;
                if (field_name != nullptr && name == field_name) { return IndexedValueView::at(index); }
            }
            throw std::out_of_range("BundleView::at: field not found");
        }

        [[nodiscard]] bool has_field(std::string_view name) const noexcept
        {
            for (std::size_t index = 0; index < schema()->field_count; ++index)
            {
                const char *field_name = schema()->fields[index].name;
                if (field_name != nullptr && name == field_name) { return true; }
            }
            return false;
        }

        [[nodiscard]] ValueView field(std::string_view name) const { return at(name); }
        [[nodiscard]] ValueView operator[](std::string_view name) const { return at(name); }
    };

    /** Read-only view over a List. Inherits indexed access. */
    class ListView : public IndexedValueView
    {
      public:
        explicit ListView(ValueView base)
            : IndexedValueView(specialized_view_detail::require_kind(base, ValueTypeKind::List, "ListView"))
        {
        }

        [[nodiscard]] bool is_fixed() const noexcept { return schema()->fixed_size != 0; }
        [[nodiscard]] const ValueTypeMetaData *element_schema() const noexcept { return schema()->element_type; }
        [[nodiscard]] bool empty() const { return size() == 0; }
        [[nodiscard]] ValueView front() const
        {
            if (empty()) { throw std::out_of_range("ListView::front on empty list"); }
            return at(0);
        }
        [[nodiscard]] ValueView back() const
        {
            if (empty()) { throw std::out_of_range("ListView::back on empty list"); }
            return at(size() - 1);
        }
    };

    /** Read-only view over a CyclicBuffer. Adds head / empty. */
    class CyclicBufferView : public IndexedValueView
    {
      public:
        explicit CyclicBufferView(ValueView base)
            : IndexedValueView(
                  specialized_view_detail::require_kind(base, ValueTypeKind::CyclicBuffer, "CyclicBufferView"))
        {
            cyclic_buffer_ops_ = specialized_view_detail::checked_cyclic_buffer_ops(binding(), "CyclicBufferView");
        }

        [[nodiscard]] std::size_t head() const
        {
            return cyclic_buffer_ops_->head(data());
        }

        [[nodiscard]] std::size_t capacity() const noexcept { return schema()->fixed_size; }
        [[nodiscard]] const ValueTypeMetaData *element_schema() const noexcept { return schema()->element_type; }
        [[nodiscard]] bool empty() const { return size() == 0; }
        [[nodiscard]] bool full() const { return capacity() != 0 && size() == capacity(); }
        [[nodiscard]] ValueView front() const
        {
            if (empty()) { throw std::out_of_range("CyclicBufferView::front on empty buffer"); }
            return at(0);
        }
        [[nodiscard]] ValueView back() const
        {
            if (empty()) { throw std::out_of_range("CyclicBufferView::back on empty buffer"); }
            return at(size() - 1);
        }

      private:
        const CyclicBufferValueOps *cyclic_buffer_ops_{nullptr};
    };

    /** Read-only view over a Queue. Adds front / empty. */
    class QueueView : public IndexedValueView
    {
      public:
        explicit QueueView(ValueView base)
            : IndexedValueView(specialized_view_detail::require_kind(base, ValueTypeKind::Queue, "QueueView"))
        {
            queue_ops_ = specialized_view_detail::checked_queue_ops(binding(), "QueueView");
        }

        [[nodiscard]] ValueView front() const
        {
            if (empty()) { throw std::out_of_range("QueueView::front on empty queue"); }
            return ValueView{queue_ops_->element_binding(queue_ops_->context, data(), 0),
                             const_cast<void *>(queue_ops_->front(data()))};
        }

        [[nodiscard]] std::size_t max_capacity() const noexcept { return schema()->fixed_size; }
        [[nodiscard]] bool has_max_capacity() const noexcept { return max_capacity() != 0; }
        [[nodiscard]] const ValueTypeMetaData *element_schema() const noexcept { return schema()->element_type; }
        [[nodiscard]] bool empty() const { return size() == 0; }
        [[nodiscard]] bool full() const { return max_capacity() != 0 && size() == max_capacity(); }
        [[nodiscard]] ValueView back() const
        {
            if (empty()) { throw std::out_of_range("QueueView::back on empty queue"); }
            return at(size() - 1);
        }

      private:
        const QueueValueOps *queue_ops_{nullptr};
    };

    /**
     * Read-only view over a Set. Membership query plus a member range
     * built from the bound ``IndexedValueOps`` (the set is unordered
     * so ``at(index)`` is not exposed publicly).
     */
    class SetView : public ValueView
    {
      public:
        explicit SetView(ValueView base)
            : ValueView(specialized_view_detail::require_kind(base, ValueTypeKind::Set, "SetView"))
        {
            ops_ = specialized_view_detail::checked_set_ops(binding(), "SetView");
        }

        [[nodiscard]] std::size_t size() const
        {
            return ops_->size(ops_->context, data());
        }

        [[nodiscard]] bool empty() const { return size() == 0; }
        [[nodiscard]] const ValueTypeMetaData *element_schema() const noexcept { return schema()->element_type; }

        [[nodiscard]] bool contains(const ValueView &key) const
        {
            if (!key.valid()) { return false; }
            return ops_->contains(data(), key.data());
        }

        [[nodiscard]] Range<ValueView> elements() const
        {
            return ops_->make_range(ops_->context, data());
        }

        [[nodiscard]] Range<ValueView> values() const { return elements(); }

        [[nodiscard]] auto begin() const { return elements().begin(); }
        [[nodiscard]] auto end() const { return elements().end(); }

      private:
        const SetValueOps *ops_{nullptr};
    };

    /**
     * Read-only view over a Map. ``contains`` / ``at`` / paired
     * iteration through the bound ops; ``key_set`` returns an adapted
     * ``SetView`` over the same memory.
     */
    class MapView : public ValueView
    {
      public:
        explicit MapView(ValueView base)
            : ValueView(specialized_view_detail::require_kind(base, ValueTypeKind::Map, "MapView"))
        {
            ops_ = specialized_view_detail::checked_map_ops(binding(), "MapView");
        }

        [[nodiscard]] std::size_t size() const
        {
            return ops_->size(ops_->context, data());
        }

        [[nodiscard]] bool empty() const { return size() == 0; }
        [[nodiscard]] const ValueTypeMetaData *key_schema() const noexcept { return schema()->key_type; }
        [[nodiscard]] const ValueTypeMetaData *value_schema() const noexcept { return schema()->element_type; }

        [[nodiscard]] bool contains(const ValueView &key) const
        {
            if (!key.valid()) { return false; }
            return ops_->contains(data(), key.data());
        }

        [[nodiscard]] ValueView at(const ValueView &key) const
        {
            const void *found = key.valid() ? ops_->value_at(data(), key.data()) : nullptr;
            if (found == nullptr) { throw std::out_of_range("MapView::at: key not present"); }
            return ValueView{ops_->value_binding(ops_->context, data()), const_cast<void *>(found)};
        }

        [[nodiscard]] ValueView operator[](const ValueView &key) const { return at(key); }

        /** Returns a read-only ``SetView`` over the live keys. */
        [[nodiscard]] SetView key_set() const
        {
            return ops_->key_set(binding(), data());
        }

        /** Lazy range over the map's keys. */
        [[nodiscard]] Range<ValueView> keys() const
        {
            return ops_->make_keys_range(data());
        }

        /** Lazy range over the map's values. */
        [[nodiscard]] Range<ValueView> values() const
        {
            return ops_->make_values_range(data());
        }

        /** Lazy paired ``(key, value)`` range over live entries. */
        [[nodiscard]] KeyValueRange<ValueView, ValueView> entries() const
        {
            return ops_->make_kv_range(data());
        }

        [[nodiscard]] KeyValueRange<ValueView, ValueView> items() const { return entries(); }

        [[nodiscard]] auto begin() const { return entries().begin(); }
        [[nodiscard]] auto end() const { return entries().end(); }

      private:
        const MapValueOps *ops_{nullptr};
    };

    inline TupleView ValueView::as_tuple() const
    {
        if (!is_tuple()) { throw std::logic_error("ValueView::as_tuple on non-tuple view"); }
        return TupleView{*this};
    }

    inline std::optional<TupleView> ValueView::try_as_tuple() const
    {
        return is_tuple() ? std::optional<TupleView>{TupleView{*this}} : std::nullopt;
    }

    inline BundleView ValueView::as_bundle() const
    {
        if (!is_bundle()) { throw std::logic_error("ValueView::as_bundle on non-bundle view"); }
        return BundleView{*this};
    }

    inline std::optional<BundleView> ValueView::try_as_bundle() const
    {
        return is_bundle() ? std::optional<BundleView>{BundleView{*this}} : std::nullopt;
    }

    inline ListView ValueView::as_list() const
    {
        if (!is_list()) { throw std::logic_error("ValueView::as_list on non-list view"); }
        return ListView{*this};
    }

    inline std::optional<ListView> ValueView::try_as_list() const
    {
        return is_list() ? std::optional<ListView>{ListView{*this}} : std::nullopt;
    }

    inline SetView ValueView::as_set() const
    {
        if (!is_set()) { throw std::logic_error("ValueView::as_set on non-set view"); }
        return SetView{*this};
    }

    inline std::optional<SetView> ValueView::try_as_set() const
    {
        return is_set() ? std::optional<SetView>{SetView{*this}} : std::nullopt;
    }

    inline MapView ValueView::as_map() const
    {
        if (!is_map()) { throw std::logic_error("ValueView::as_map on non-map view"); }
        return MapView{*this};
    }

    inline std::optional<MapView> ValueView::try_as_map() const
    {
        return is_map() ? std::optional<MapView>{MapView{*this}} : std::nullopt;
    }

    inline CyclicBufferView ValueView::as_cyclic_buffer() const
    {
        if (!is_cyclic_buffer())
        {
            throw std::logic_error("ValueView::as_cyclic_buffer on non-cyclic-buffer view");
        }
        return CyclicBufferView{*this};
    }

    inline std::optional<CyclicBufferView> ValueView::try_as_cyclic_buffer() const
    {
        return is_cyclic_buffer() ? std::optional<CyclicBufferView>{CyclicBufferView{*this}} : std::nullopt;
    }

    inline QueueView ValueView::as_queue() const
    {
        if (!is_queue()) { throw std::logic_error("ValueView::as_queue on non-queue view"); }
        return QueueView{*this};
    }

    inline std::optional<QueueView> ValueView::try_as_queue() const
    {
        return is_queue() ? std::optional<QueueView>{QueueView{*this}} : std::nullopt;
    }

}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_VALUE_SPECIALIZED_VIEWS_H
