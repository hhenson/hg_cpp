#ifndef HGRAPH_CPP_ROOT_VALUE_BUILDER_H
#define HGRAPH_CPP_ROOT_VALUE_BUILDER_H

#include <hgraph/types/value/compact_storage.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
#include <utility>

namespace hgraph
{
    /**
     * Per-kind builders for the value-layer compact container storage.
     *
     * The compact value-layer storage shapes (``ListStorage`` /
     * ``SetStorage`` / ``MapStorage`` / ``CyclicBufferStorage`` /
     * ``QueueStorage``) are immutable-after-construction by design:
     * once constructed they cannot grow, shrink, or replace individual
     * elements. Builders are the construction-time mutable shim that
     * lets callers accumulate elements piecemeal and produce the
     * immutable storage on ``build_storage()``.
     *
     * Each builder is single-use. The accumulated elements live in a
     * type-erased contiguous buffer driven by the bound element plan;
     * ``build_storage()`` copies them into a freshly-constructed
     * compact storage of the now-known size and the builder's buffer
     * is reset.
     */

    namespace builder_detail
    {
        /**
         * Type-erased growable contiguous buffer of constructed
         * elements. ``push_back_copy`` copies an element into the
         * trailing slot via the element plan; growth relocates
         * existing elements through the plan's move-construct hook,
         * falling back to ``memcpy`` for trivially-move-constructible
         * types.
         */
        class ElementAccumulator
        {
          public:
            explicit ElementAccumulator(const MemoryUtils::StoragePlan &plan) : plan_{&plan} {}

            ElementAccumulator(const ElementAccumulator &)            = delete;
            ElementAccumulator &operator=(const ElementAccumulator &) = delete;

            ElementAccumulator(ElementAccumulator &&other) noexcept
                : plan_{other.plan_}, bytes_{other.bytes_}, capacity_{other.capacity_}, size_{other.size_}
            {
                other.bytes_    = nullptr;
                other.capacity_ = 0;
                other.size_     = 0;
            }

            ElementAccumulator &operator=(ElementAccumulator &&other) noexcept
            {
                if (this != &other)
                {
                    clear();
                    plan_           = other.plan_;
                    bytes_          = other.bytes_;
                    capacity_       = other.capacity_;
                    size_           = other.size_;
                    other.bytes_    = nullptr;
                    other.capacity_ = 0;
                    other.size_     = 0;
                }
                return *this;
            }

            ~ElementAccumulator() { clear(); }

            void push_back_copy(const void *src)
            {
                if (size_ == capacity_) { grow(std::max<std::size_t>(capacity_ * 2, 8)); }
                plan_->copy_construct(slot_address(size_), src);
                ++size_;
            }

            [[nodiscard]] std::size_t size() const noexcept { return size_; }
            [[nodiscard]] bool        empty() const noexcept { return size_ == 0; }

            [[nodiscard]] const void *at(std::size_t index) const noexcept { return slot_address(index); }
            [[nodiscard]] void       *at(std::size_t index) noexcept { return slot_address(index); }

            [[nodiscard]] ElementSpan as_span() const noexcept
            {
                return ElementSpan{
                    .bytes  = size_ == 0 ? nullptr : bytes_,
                    .size   = size_,
                    .stride = plan_->layout.size,
                    .plan   = plan_,
                };
            }

            void clear() noexcept
            {
                if (bytes_ == nullptr) { return; }
                for (std::size_t i = size_; i > 0; --i) { plan_->destroy(slot_address(i - 1)); }
                ::operator delete(bytes_, std::align_val_t{plan_->layout.alignment});
                bytes_    = nullptr;
                capacity_ = 0;
                size_     = 0;
            }

          private:
            [[nodiscard]] void *slot_address(std::size_t index) noexcept
            {
                return static_cast<std::byte *>(bytes_) + index * plan_->layout.size;
            }
            [[nodiscard]] const void *slot_address(std::size_t index) const noexcept
            {
                return static_cast<const std::byte *>(bytes_) + index * plan_->layout.size;
            }

            void grow(std::size_t new_capacity)
            {
                const std::size_t element_size = plan_->layout.size;
                void *new_bytes =
                    ::operator new(new_capacity * element_size, std::align_val_t{plan_->layout.alignment});

                if (plan_->trivially_move_constructible)
                {
                    std::memcpy(new_bytes, bytes_, size_ * element_size);
                }
                else
                {
                    std::size_t moved = 0;
                    auto        rollback = make_scope_exit([&]() noexcept {
                        for (std::size_t i = moved; i > 0; --i)
                        {
                            plan_->destroy(static_cast<std::byte *>(new_bytes) + (i - 1) * element_size);
                        }
                        ::operator delete(new_bytes, std::align_val_t{plan_->layout.alignment});
                    });
                    for (moved = 0; moved < size_; ++moved)
                    {
                        plan_->move_construct(static_cast<std::byte *>(new_bytes) + moved * element_size,
                                              slot_address(moved));
                    }
                    rollback.release();
                    // Destroy the old slots; their resources have been moved.
                    for (std::size_t i = size_; i > 0; --i) { plan_->destroy(slot_address(i - 1)); }
                }

                if (bytes_ != nullptr)
                {
                    ::operator delete(bytes_, std::align_val_t{plan_->layout.alignment});
                }
                bytes_    = new_bytes;
                capacity_ = new_capacity;
            }

            const MemoryUtils::StoragePlan *plan_{nullptr};
            void                           *bytes_{nullptr};
            std::size_t                     capacity_{0};
            std::size_t                     size_{0};
        };
    }  // namespace builder_detail

    // -----------------------------------------------------------------
    // ListBuilder
    // -----------------------------------------------------------------

    /**
     * Mutable accumulator that produces an immutable ``ListStorage``
     * sized to the number of elements pushed onto it.
     */
    class ListBuilder
    {
      public:
        explicit ListBuilder(const ValueTypeBinding &element_binding)
            : element_binding_{&element_binding}, accumulator_{element_binding.checked_plan()}
        {
        }

        void push_back_copy(const void *src) { accumulator_.push_back_copy(src); }

        template <typename T>
        void push_back(const T &value)
        {
            push_back_copy(static_cast<const void *>(std::addressof(value)));
        }

        [[nodiscard]] std::size_t size() const noexcept { return accumulator_.size(); }
        [[nodiscard]] bool        empty() const noexcept { return accumulator_.empty(); }

        [[nodiscard]] ListStorage build_storage()
        {
            return ListStorage{element_binding_->checked_plan(), accumulator_.as_span()};
        }

      private:
        const ValueTypeBinding              *element_binding_{nullptr};
        builder_detail::ElementAccumulator   accumulator_;
    };

    // -----------------------------------------------------------------
    // CyclicBufferBuilder — fixed declared capacity, oldest evicted on
    // overflow.
    // -----------------------------------------------------------------

    /**
     * Mutable accumulator for ``CyclicBufferStorage``. The declared
     * capacity is fixed at construction; ``push_back`` rotates after
     * the capacity is reached, preserving a sliding window of the most
     * recent ``capacity`` elements. The build's ``head`` matches the
     * logical oldest position.
     */
    class CyclicBufferBuilder
    {
      public:
        CyclicBufferBuilder(const ValueTypeBinding &element_binding, std::size_t capacity)
            : element_binding_{&element_binding}
            , accumulator_{element_binding.checked_plan()}
            , capacity_{capacity}
        {
            if (capacity == 0) { throw std::invalid_argument("CyclicBufferBuilder capacity must be greater than zero"); }
        }

        void push_back_copy(const void *src)
        {
            if (accumulator_.size() < capacity_)
            {
                accumulator_.push_back_copy(src);
            }
            else
            {
                // Replace the slot at `head_` (oldest) with the new value
                // by destroying it in place and copy-constructing into
                // its memory; advance the head.
                const auto &plan = element_binding_->checked_plan();
                plan.destroy(accumulator_.at(head_));
                // If copy_construct throws, the slot is uninitialised;
                // default-construct so the accumulator's destructor
                // doesn't double-free. The rollback runs only on
                // unwind; the success path releases it.
                auto rollback = make_scope_exit(
                    [&]() noexcept { plan.default_construct(accumulator_.at(head_)); });
                plan.copy_construct(accumulator_.at(head_), src);
                rollback.release();
                head_ = (head_ + 1) % capacity_;
            }
        }

        template <typename T>
        void push_back(const T &value)
        {
            push_back_copy(static_cast<const void *>(std::addressof(value)));
        }

        [[nodiscard]] std::size_t size() const noexcept { return accumulator_.size(); }
        [[nodiscard]] std::size_t capacity() const noexcept { return capacity_; }

        [[nodiscard]] CyclicBufferStorage build_storage()
        {
            return CyclicBufferStorage{element_binding_->checked_plan(), accumulator_.as_span(), head_};
        }

      private:
        const ValueTypeBinding              *element_binding_{nullptr};
        builder_detail::ElementAccumulator   accumulator_;
        std::size_t                          capacity_{0};
        std::size_t                          head_{0};
    };

    // -----------------------------------------------------------------
    // QueueBuilder — bounded or unbounded FIFO; rejects on overflow
    // when bounded.
    // -----------------------------------------------------------------

    /**
     * Mutable accumulator for ``QueueStorage``. ``push`` appends; if a
     * non-zero ``max_capacity`` was declared, pushing past it throws.
     * ``max_capacity == 0`` means unbounded.
     */
    class QueueBuilder
    {
      public:
        QueueBuilder(const ValueTypeBinding &element_binding, std::size_t max_capacity = 0)
            : element_binding_{&element_binding}
            , accumulator_{element_binding.checked_plan()}
            , max_capacity_{max_capacity}
        {
        }

        void push_copy(const void *src)
        {
            if (max_capacity_ != 0 && accumulator_.size() >= max_capacity_)
            {
                throw std::overflow_error("QueueBuilder: bounded queue is full");
            }
            accumulator_.push_back_copy(src);
        }

        template <typename T>
        void push(const T &value)
        {
            push_copy(static_cast<const void *>(std::addressof(value)));
        }

        [[nodiscard]] std::size_t size() const noexcept { return accumulator_.size(); }
        [[nodiscard]] std::size_t max_capacity() const noexcept { return max_capacity_; }

        [[nodiscard]] QueueStorage build_storage()
        {
            return QueueStorage{element_binding_->checked_plan(), accumulator_.as_span()};
        }

      private:
        const ValueTypeBinding              *element_binding_{nullptr};
        builder_detail::ElementAccumulator   accumulator_;
        std::size_t                          max_capacity_{0};
    };

    // -----------------------------------------------------------------
    // SetBuilder — content-keyed; ``insert`` is no-op on duplicate.
    // -----------------------------------------------------------------

    /**
     * Mutable accumulator for ``SetStorage``. ``insert`` deduplicates
     * by content using the bound element ops; subsequent inserts of
     * existing keys are no-ops. ``build_storage`` produces a compact
     * set sized to the number of unique keys.
     */
    class SetBuilder
    {
      public:
        explicit SetBuilder(const ValueTypeBinding &element_binding)
            : element_binding_{&element_binding}, accumulator_{element_binding.checked_plan()}
        {
            const auto *meta = element_binding.type_meta;
            if (meta == nullptr || !meta->is_hashable() || !meta->is_equatable())
            {
                throw std::logic_error("SetBuilder requires a hashable and equatable element binding");
            }
        }

        bool insert_copy(const void *src)
        {
            if (contains(src)) { return false; }
            accumulator_.push_back_copy(src);
            return true;
        }

        template <typename T>
        bool insert(const T &value)
        {
            return insert_copy(static_cast<const void *>(std::addressof(value)));
        }

        [[nodiscard]] bool contains(const void *src) const
        {
            const auto &ops = element_binding_->checked_ops();
            for (std::size_t i = 0; i < accumulator_.size(); ++i)
            {
                if (ops.equals(accumulator_.at(i), src)) { return true; }
            }
            return false;
        }

        [[nodiscard]] std::size_t size() const noexcept { return accumulator_.size(); }

        [[nodiscard]] SetStorage build_storage()
        {
            return SetStorage{*element_binding_, accumulator_.as_span()};
        }

      private:
        const ValueTypeBinding              *element_binding_{nullptr};
        builder_detail::ElementAccumulator   accumulator_;
    };

    // -----------------------------------------------------------------
    // MapBuilder — content-keyed; later writes overwrite earlier
    // values for the same key.
    // -----------------------------------------------------------------

    /**
     * Mutable accumulator for ``MapStorage``. ``set_item`` accumulates
     * key/value pairs; if an earlier write for the same key exists,
     * its value is replaced (the map remains content-keyed).
     * ``build_storage`` produces a compact map sized to the number of
     * unique keys.
     */
    class MapBuilder
    {
      public:
        MapBuilder(const ValueTypeBinding &key_binding, const ValueTypeBinding &value_binding)
            : key_binding_{&key_binding}
            , value_binding_{&value_binding}
            , key_acc_{key_binding.checked_plan()}
            , value_acc_{value_binding.checked_plan()}
        {
            const auto *km = key_binding.type_meta;
            if (km == nullptr || !km->is_hashable() || !km->is_equatable())
            {
                throw std::logic_error("MapBuilder requires a hashable and equatable key binding");
            }
        }

        void set_item_copy(const void *key, const void *value)
        {
            if (auto found = find_slot(key); found.has_value())
            {
                const auto &vp = value_binding_->checked_plan();
                vp.destroy(value_acc_.at(*found));
                // If copy_construct throws the slot is uninitialised;
                // default-construct so the accumulator's destructor
                // doesn't double-free.
                auto rollback = make_scope_exit(
                    [&]() noexcept { vp.default_construct(value_acc_.at(*found)); });
                vp.copy_construct(value_acc_.at(*found), value);
                rollback.release();
                return;
            }
            key_acc_.push_back_copy(key);
            // If the value push fails, drop the trailing key so keys and
            // values stay paired. We can't safely undo a contiguous push
            // through the accumulator's API, so we only roll back when
            // the value side actually fails — single-use builder is
            // discarded after that anyway.
            // (Builder remains usable on the success path.)
            value_acc_.push_back_copy(value);
        }

        template <typename K, typename V>
        void set_item(const K &key, const V &value)
        {
            set_item_copy(static_cast<const void *>(std::addressof(key)),
                          static_cast<const void *>(std::addressof(value)));
        }

        [[nodiscard]] bool contains(const void *key) const { return find_slot(key).has_value(); }

        [[nodiscard]] std::size_t size() const noexcept { return key_acc_.size(); }

        [[nodiscard]] MapStorage build_storage()
        {
            return MapStorage{*key_binding_, *value_binding_, key_acc_.as_span(), value_acc_.as_span()};
        }

      private:
        [[nodiscard]] std::optional<std::size_t> find_slot(const void *key) const
        {
            const auto &ops = key_binding_->checked_ops();
            for (std::size_t i = 0; i < key_acc_.size(); ++i)
            {
                if (ops.equals(key_acc_.at(i), key)) { return i; }
            }
            return std::nullopt;
        }

        const ValueTypeBinding              *key_binding_{nullptr};
        const ValueTypeBinding              *value_binding_{nullptr};
        builder_detail::ElementAccumulator   key_acc_;
        builder_detail::ElementAccumulator   value_acc_;
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_VALUE_BUILDER_H
