#ifndef HGRAPH_CPP_ROOT_VALUE_COMPACT_STORAGE_H
#define HGRAPH_CPP_ROOT_VALUE_COMPACT_STORAGE_H

#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/value/value_ops.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <new>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace hgraph
{
    /**
     * Compact, immutable-after-construction storage shapes for the value
     * layer's container kinds. See *Allocation, Plans and Ops > Scalar
     * Plans and Ops > Container Storage Shapes* for the design narrative.
     *
     * Each storage class:
     *
     * - is sized at construction time (no growth, no shrink),
     * - exposes only read-time access (per-element mutation is a TS-layer
     *   concern; the value-layer view is read-only),
     * - is built either through a one-shot constructor (taking a
     *   pre-assembled element span) or through the matching
     *   :class:`ValueBuilder` (see ``value_builder.h``).
     */

    // -----------------------------------------------------------------
    // Element span — a borrowed view of N source elements with the
    // matching element plan. Used as the construction input for every
    // compact storage shape so callers can supply elements either from
    // raw memory, from a builder's accumulated buffer, or from another
    // storage's element memory.
    // -----------------------------------------------------------------

    struct ElementSpan
    {
        const void *bytes{nullptr};
        std::size_t size{0};                // number of elements
        std::size_t stride{0};              // bytes between elements
        const MemoryUtils::StoragePlan *plan{nullptr};

        [[nodiscard]] const void *at(std::size_t index) const noexcept
        {
            return static_cast<const std::byte *>(bytes) + index * stride;
        }
    };

    namespace compact_detail
    {
        [[nodiscard]] inline void *allocate_element_buffer(const MemoryUtils::StoragePlan &plan, std::size_t count)
        {
            if (count == 0) { return nullptr; }
            return ::operator new(count * plan.layout.size, std::align_val_t{plan.layout.alignment});
        }

        inline void deallocate_element_buffer(void *buffer, const MemoryUtils::StoragePlan &plan, std::size_t count) noexcept
        {
            if (buffer == nullptr || count == 0) { return; }
            ::operator delete(buffer, std::align_val_t{plan.layout.alignment});
        }

        inline void destroy_elements_reverse(void *buffer, const MemoryUtils::StoragePlan &plan, std::size_t count) noexcept
        {
            if (buffer == nullptr) { return; }
            const std::size_t stride = plan.layout.size;
            for (std::size_t index = count; index > 0; --index)
            {
                plan.destroy(static_cast<std::byte *>(buffer) + (index - 1) * stride);
            }
        }

        inline void copy_construct_elements(void *dst_buffer, const ElementSpan &src) noexcept(false)
        {
            const std::size_t stride      = src.plan->layout.size;
            std::size_t       constructed = 0;
            auto rollback = make_scope_exit([&]() noexcept {
                destroy_elements_reverse(dst_buffer, *src.plan, constructed);
            });
            for (constructed = 0; constructed < src.size; ++constructed)
            {
                src.plan->copy_construct(static_cast<std::byte *>(dst_buffer) + constructed * stride,
                                         src.at(constructed));
            }
            rollback.release();
        }

        [[nodiscard]] inline std::size_t combine_hash(std::size_t seed, std::size_t value) noexcept
        {
            seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
            return seed;
        }

        // Open-addressing bucket array used by SetStorage / MapStorage.
        // Bucket values are signed indices into the keys buffer; -1 marks
        // an empty bucket. Capacity is rounded to a power of two so the
        // modulo collapses to a mask. Built once at construction time and
        // never mutated thereafter.
        struct OpenAddressTable
        {
            std::vector<std::int32_t> buckets{};

            [[nodiscard]] std::size_t mask() const noexcept { return buckets.size() - 1; }

            void reset(std::size_t entries)
            {
                std::size_t cap = 8;
                while (cap < entries * 2) { cap <<= 1U; }
                buckets.assign(cap, -1);
            }

            template <typename Probe>
            void insert(std::int32_t index, std::size_t hash, Probe &&probe)
            {
                std::size_t pos = hash & mask();
                while (buckets[pos] != -1)
                {
                    (void)probe(buckets[pos], pos);
                    pos = (pos + 1) & mask();
                }
                buckets[pos] = index;
            }

            template <typename Match>
            [[nodiscard]] std::int32_t find(std::size_t hash, Match &&match) const
            {
                if (buckets.empty()) { return -1; }
                std::size_t pos = hash & mask();
                while (buckets[pos] != -1)
                {
                    if (match(buckets[pos])) { return buckets[pos]; }
                    pos = (pos + 1) & mask();
                }
                return -1;
            }
        };
    }  // namespace compact_detail

    // -----------------------------------------------------------------
    // ListStorage — sized contiguous buffer of N elements.
    // -----------------------------------------------------------------

    /**
     * Compact value-layer storage for the ``List`` kind. Holds exactly
     * the number of elements supplied at construction. Cannot be resized
     * and individual elements cannot be replaced; whole-container
     * replacement happens at the ``Value`` level.
     */
    class ListStorage
    {
      public:
        ListStorage() noexcept = default;

        ListStorage(const MemoryUtils::StoragePlan &element_plan, const ElementSpan &source)
            : size_{source.size}, element_plan_{&element_plan}
        {
            bytes_ = compact_detail::allocate_element_buffer(element_plan, size_);
            auto rollback = make_scope_exit([&]() noexcept {
                compact_detail::deallocate_element_buffer(bytes_, element_plan, size_);
            });
            compact_detail::copy_construct_elements(bytes_, source);
            rollback.release();
        }

        ListStorage(const ListStorage &other) { copy_from(other); }

        ListStorage &operator=(const ListStorage &other)
        {
            if (this != &other)
            {
                clear();
                copy_from(other);
            }
            return *this;
        }

        ListStorage(ListStorage &&other) noexcept
            : bytes_{other.bytes_}, size_{other.size_}, element_plan_{other.element_plan_}
        {
            other.bytes_        = nullptr;
            other.size_         = 0;
            other.element_plan_ = nullptr;
        }

        ListStorage &operator=(ListStorage &&other) noexcept
        {
            if (this != &other)
            {
                clear();
                bytes_              = other.bytes_;
                size_               = other.size_;
                element_plan_       = other.element_plan_;
                other.bytes_        = nullptr;
                other.size_         = 0;
                other.element_plan_ = nullptr;
            }
            return *this;
        }

        ~ListStorage() { clear(); }

        [[nodiscard]] std::size_t size() const noexcept { return size_; }
        [[nodiscard]] bool        empty() const noexcept { return size_ == 0; }
        [[nodiscard]] const MemoryUtils::StoragePlan *element_plan() const noexcept { return element_plan_; }

        [[nodiscard]] void *element_at(std::size_t index)
        {
            if (index >= size_) { throw std::out_of_range("ListStorage index out of range"); }
            return static_cast<std::byte *>(bytes_) + index * element_plan_->layout.size;
        }
        [[nodiscard]] const void *element_at(std::size_t index) const
        {
            if (index >= size_) { throw std::out_of_range("ListStorage index out of range"); }
            return static_cast<const std::byte *>(bytes_) + index * element_plan_->layout.size;
        }

      private:
        void clear() noexcept
        {
            if (element_plan_ == nullptr) { return; }
            compact_detail::destroy_elements_reverse(bytes_, *element_plan_, size_);
            compact_detail::deallocate_element_buffer(bytes_, *element_plan_, size_);
            bytes_        = nullptr;
            size_         = 0;
            element_plan_ = nullptr;
        }

        void copy_from(const ListStorage &other)
        {
            element_plan_ = other.element_plan_;
            size_         = other.size_;
            if (element_plan_ == nullptr) { return; }
            bytes_ = compact_detail::allocate_element_buffer(*element_plan_, size_);
            auto rollback = make_scope_exit([&]() noexcept {
                compact_detail::deallocate_element_buffer(bytes_, *element_plan_, size_);
                bytes_        = nullptr;
                size_         = 0;
                element_plan_ = nullptr;
            });
            compact_detail::copy_construct_elements(
                bytes_, ElementSpan{
                            .bytes  = other.bytes_,
                            .size   = other.size_,
                            .stride = element_plan_->layout.size,
                            .plan   = element_plan_,
                        });
            rollback.release();
        }

        void                          *bytes_{nullptr};
        std::size_t                    size_{0};
        const MemoryUtils::StoragePlan *element_plan_{nullptr};
    };

    // -----------------------------------------------------------------
    // CyclicBufferStorage — sized buffer + ring-read interpretation.
    // -----------------------------------------------------------------

    /**
     * Compact value-layer storage for the ``CyclicBuffer`` kind. Holds
     * exactly the elements supplied at construction in arrival order and
     * remembers the logical *head* offset so the read view can walk them
     * in ring order. The buffer is immutable once constructed; the
     * "rolling overwrite" semantics belong to the slot-store-based
     * time-series variant.
     */
    class CyclicBufferStorage
    {
      public:
        CyclicBufferStorage() noexcept = default;

        CyclicBufferStorage(const MemoryUtils::StoragePlan &element_plan, const ElementSpan &source,
                            std::size_t head)
            : storage_{element_plan, source}, head_{source.size == 0 ? 0 : head % source.size}
        {
        }

        [[nodiscard]] std::size_t                     size() const noexcept { return storage_.size(); }
        [[nodiscard]] bool                            empty() const noexcept { return storage_.empty(); }
        [[nodiscard]] std::size_t                     head() const noexcept { return head_; }
        [[nodiscard]] const MemoryUtils::StoragePlan *element_plan() const noexcept { return storage_.element_plan(); }

        [[nodiscard]] std::size_t physical_index(std::size_t logical_index) const
        {
            const std::size_t n = storage_.size();
            if (logical_index >= n) { throw std::out_of_range("CyclicBufferStorage index out of range"); }
            return n == 0 ? 0 : (head_ + logical_index) % n;
        }

        [[nodiscard]] void *element_at(std::size_t logical_index)
        {
            return storage_.element_at(physical_index(logical_index));
        }
        [[nodiscard]] const void *element_at(std::size_t logical_index) const
        {
            return storage_.element_at(physical_index(logical_index));
        }

      private:
        ListStorage storage_{};
        std::size_t head_{0};
    };

    // -----------------------------------------------------------------
    // QueueStorage — sized buffer in arrival order.
    // -----------------------------------------------------------------

    /**
     * Compact value-layer storage for the ``Queue`` kind. Holds the
     * queued elements in arrival order; the read view walks them
     * front-to-back. Immutable after construction.
     */
    class QueueStorage
    {
      public:
        QueueStorage() noexcept = default;

        QueueStorage(const MemoryUtils::StoragePlan &element_plan, const ElementSpan &source)
            : storage_{element_plan, source}
        {
        }

        [[nodiscard]] std::size_t                     size() const noexcept { return storage_.size(); }
        [[nodiscard]] bool                            empty() const noexcept { return storage_.empty(); }
        [[nodiscard]] const MemoryUtils::StoragePlan *element_plan() const noexcept { return storage_.element_plan(); }

        [[nodiscard]] void       *front() { return storage_.element_at(0); }
        [[nodiscard]] const void *front() const { return storage_.element_at(0); }

        [[nodiscard]] void       *element_at(std::size_t index) { return storage_.element_at(index); }
        [[nodiscard]] const void *element_at(std::size_t index) const { return storage_.element_at(index); }

      private:
        ListStorage storage_{};
    };

    // -----------------------------------------------------------------
    // SetStorage — content-keyed open-addressing hash set, populated
    // once at construction.
    // -----------------------------------------------------------------

    /**
     * Compact value-layer storage for the ``Set`` kind. Holds the keys
     * in a contiguous buffer plus an open-addressing bucket array for
     * lookup. Hash and equality are driven by the bound element ops via
     * the borrowed binding.
     *
     * Two sets are *equal* when they contain the same keys regardless of
     * insertion order; the underlying buffer's order is preserved as
     * supplied at construction so iteration is deterministic for that
     * single instance.
     */
    class SetStorage
    {
      public:
        SetStorage() noexcept = default;

        SetStorage(const ValueTypeBinding &element_binding, const ElementSpan &source)
            : element_binding_{&element_binding}, storage_{element_binding.checked_plan(), source}
        {
            ensure_ops();
            build_table();
        }

        SetStorage(const SetStorage &other) { copy_from(other); }
        SetStorage &operator=(const SetStorage &other)
        {
            if (this != &other)
            {
                table_.buckets.clear();
                storage_ = ListStorage{};
                copy_from(other);
            }
            return *this;
        }

        SetStorage(SetStorage &&) noexcept            = default;
        SetStorage &operator=(SetStorage &&) noexcept = default;

        [[nodiscard]] std::size_t              size() const noexcept { return storage_.size(); }
        [[nodiscard]] bool                     empty() const noexcept { return storage_.empty(); }
        [[nodiscard]] const ValueTypeBinding  *element_binding() const noexcept { return element_binding_; }

        [[nodiscard]] const void *element_at(std::size_t index) const { return storage_.element_at(index); }

        [[nodiscard]] bool contains(const void *key) const
        {
            if (storage_.empty() || element_binding_ == nullptr) { return false; }
            const auto &ops = element_binding_->checked_ops();
            const auto  hash = ops.hash(key);
            return table_.find(hash, [&](std::int32_t index) {
                return ops.equals(storage_.element_at(static_cast<std::size_t>(index)), key);
            }) != -1;
        }

      private:
        void ensure_ops() const
        {
            if (element_binding_ == nullptr || element_binding_->type_meta == nullptr ||
                !element_binding_->type_meta->is_hashable() || !element_binding_->type_meta->is_equatable())
            {
                throw std::logic_error("SetStorage requires a hashable and equatable element binding");
            }
        }

        void build_table()
        {
            const auto &ops = element_binding_->checked_ops();
            table_.reset(storage_.size());
            for (std::size_t i = 0; i < storage_.size(); ++i)
            {
                const auto hash = ops.hash(storage_.element_at(i));
                table_.insert(static_cast<std::int32_t>(i), hash, [](std::int32_t, std::size_t) {});
            }
        }

        void copy_from(const SetStorage &other)
        {
            element_binding_ = other.element_binding_;
            if (element_binding_ == nullptr) { return; }
            storage_ = ListStorage{
                element_binding_->checked_plan(),
                ElementSpan{
                    .bytes  = other.storage_.size() == 0 ? nullptr : other.storage_.element_at(0),
                    .size   = other.storage_.size(),
                    .stride = element_binding_->checked_plan().layout.size,
                    .plan   = &element_binding_->checked_plan(),
                },
            };
            build_table();
        }

        const ValueTypeBinding         *element_binding_{nullptr};
        ListStorage                     storage_{};
        compact_detail::OpenAddressTable table_{};
    };

    // -----------------------------------------------------------------
    // MapStorage — content-keyed open-addressing hash map.
    // -----------------------------------------------------------------

    /**
     * Compact value-layer storage for the ``Map`` kind. Pairs a key
     * buffer with a value buffer indexed by the same slot ids. Keys and
     * values are stored as parallel contiguous arrays; lookup goes
     * through an open-addressing bucket array driven by the bound key
     * ops.
     */
    class MapStorage
    {
      public:
        MapStorage() noexcept = default;

        MapStorage(const ValueTypeBinding &key_binding, const ValueTypeBinding &value_binding,
                   const ElementSpan &keys_source, const ElementSpan &values_source)
            : key_binding_{&key_binding}
            , value_binding_{&value_binding}
            , keys_{key_binding.checked_plan(), keys_source}
            , values_{value_binding.checked_plan(), values_source}
        {
            if (keys_source.size != values_source.size)
            {
                throw std::invalid_argument("MapStorage requires keys and values of the same size");
            }
            ensure_key_ops();
            build_table();
        }

        MapStorage(const MapStorage &other) { copy_from(other); }
        MapStorage &operator=(const MapStorage &other)
        {
            if (this != &other)
            {
                table_.buckets.clear();
                keys_   = ListStorage{};
                values_ = ListStorage{};
                copy_from(other);
            }
            return *this;
        }

        MapStorage(MapStorage &&) noexcept            = default;
        MapStorage &operator=(MapStorage &&) noexcept = default;

        [[nodiscard]] std::size_t             size() const noexcept { return keys_.size(); }
        [[nodiscard]] bool                    empty() const noexcept { return keys_.empty(); }
        [[nodiscard]] const ValueTypeBinding *key_binding() const noexcept { return key_binding_; }
        [[nodiscard]] const ValueTypeBinding *value_binding() const noexcept { return value_binding_; }

        [[nodiscard]] const void *key_at(std::size_t slot) const { return keys_.element_at(slot); }
        [[nodiscard]] void       *value_at_index(std::size_t slot) { return values_.element_at(slot); }
        [[nodiscard]] const void *value_at_index(std::size_t slot) const { return values_.element_at(slot); }

        [[nodiscard]] std::int32_t find_slot(const void *key) const
        {
            if (keys_.empty() || key_binding_ == nullptr) { return -1; }
            const auto &ops  = key_binding_->checked_ops();
            const auto  hash = ops.hash(key);
            return table_.find(hash, [&](std::int32_t index) {
                return ops.equals(keys_.element_at(static_cast<std::size_t>(index)), key);
            });
        }

        [[nodiscard]] bool contains(const void *key) const { return find_slot(key) != -1; }

        [[nodiscard]] const void *value_at(const void *key) const
        {
            const auto slot = find_slot(key);
            return slot == -1 ? nullptr : values_.element_at(static_cast<std::size_t>(slot));
        }
        [[nodiscard]] void *value_at(const void *key)
        {
            const auto slot = find_slot(key);
            return slot == -1 ? nullptr : values_.element_at(static_cast<std::size_t>(slot));
        }

      private:
        void ensure_key_ops() const
        {
            if (key_binding_ == nullptr || key_binding_->type_meta == nullptr ||
                !key_binding_->type_meta->is_hashable() || !key_binding_->type_meta->is_equatable())
            {
                throw std::logic_error("MapStorage requires a hashable and equatable key binding");
            }
        }

        void build_table()
        {
            const auto &ops = key_binding_->checked_ops();
            table_.reset(keys_.size());
            for (std::size_t i = 0; i < keys_.size(); ++i)
            {
                const auto hash = ops.hash(keys_.element_at(i));
                table_.insert(static_cast<std::int32_t>(i), hash, [](std::int32_t, std::size_t) {});
            }
        }

        void copy_from(const MapStorage &other)
        {
            key_binding_   = other.key_binding_;
            value_binding_ = other.value_binding_;
            if (key_binding_ == nullptr || value_binding_ == nullptr) { return; }
            const auto &kp = key_binding_->checked_plan();
            const auto &vp = value_binding_->checked_plan();
            keys_   = ListStorage{kp, ElementSpan{.bytes = other.keys_.size() == 0 ? nullptr : other.keys_.element_at(0),
                                                  .size   = other.keys_.size(),
                                                  .stride = kp.layout.size,
                                                  .plan   = &kp}};
            values_ = ListStorage{vp,
                                  ElementSpan{.bytes  = other.values_.size() == 0 ? nullptr : other.values_.element_at(0),
                                              .size   = other.values_.size(),
                                              .stride = vp.layout.size,
                                              .plan   = &vp}};
            build_table();
        }

        const ValueTypeBinding          *key_binding_{nullptr};
        const ValueTypeBinding          *value_binding_{nullptr};
        ListStorage                       keys_{};
        ListStorage                       values_{};
        compact_detail::OpenAddressTable  table_{};
    };

    // -----------------------------------------------------------------
    // Lifecycle context state structs.
    // -----------------------------------------------------------------

    struct ListState
    {
        const ValueTypeBinding *element_binding{nullptr};
    };

    struct SetState
    {
        const ValueTypeBinding *element_binding{nullptr};
    };

    struct MapState
    {
        const ValueTypeBinding *key_binding{nullptr};
        const ValueTypeBinding *value_binding{nullptr};
    };

    struct CyclicBufferState
    {
        const ValueTypeBinding *element_binding{nullptr};
        std::size_t             capacity{0};
    };

    struct QueueState
    {
        const ValueTypeBinding *element_binding{nullptr};
        std::size_t             max_capacity{0};
    };

    namespace compact_detail
    {
        template <typename State>
        [[nodiscard]] const State &checked_state(const void *context, const char *name)
        {
            if (context == nullptr)
            {
                throw std::logic_error(std::string(name) + " requires lifecycle context");
            }
            return *static_cast<const State *>(context);
        }

        // Lifecycle thunks for each storage shape. ``construct`` /
        // ``copy_construct`` / ``move_construct`` etc. — the StoragePlan
        // wires these into LifecycleOps.

        inline void list_construct(void *dst, const void *context)
        {
            const auto &state = checked_state<ListState>(context, "list");
            std::construct_at(static_cast<ListStorage *>(dst));
            (void)state;  // empty list construction needs no element span
        }

        inline void list_destroy(void *memory, const void *) noexcept
        {
            std::destroy_at(static_cast<ListStorage *>(memory));
        }

        inline void list_copy_construct(void *dst, const void *src, const void *)
        {
            std::construct_at(static_cast<ListStorage *>(dst), *static_cast<const ListStorage *>(src));
        }

        inline void list_move_construct(void *dst, void *src, const void *)
        {
            std::construct_at(static_cast<ListStorage *>(dst), std::move(*static_cast<ListStorage *>(src)));
        }

        inline void list_copy_assign(void *dst, const void *src, const void *)
        {
            *static_cast<ListStorage *>(dst) = *static_cast<const ListStorage *>(src);
        }

        inline void list_move_assign(void *dst, void *src, const void *)
        {
            *static_cast<ListStorage *>(dst) = std::move(*static_cast<ListStorage *>(src));
        }

        inline void cyclic_buffer_construct(void *dst, const void *context)
        {
            const auto &state = checked_state<CyclicBufferState>(context, "cyclic buffer");
            std::construct_at(static_cast<CyclicBufferStorage *>(dst));
            (void)state;
        }

        inline void cyclic_buffer_destroy(void *memory, const void *) noexcept
        {
            std::destroy_at(static_cast<CyclicBufferStorage *>(memory));
        }

        inline void cyclic_buffer_copy_construct(void *dst, const void *src, const void *)
        {
            std::construct_at(static_cast<CyclicBufferStorage *>(dst),
                              *static_cast<const CyclicBufferStorage *>(src));
        }

        inline void cyclic_buffer_move_construct(void *dst, void *src, const void *)
        {
            std::construct_at(static_cast<CyclicBufferStorage *>(dst),
                              std::move(*static_cast<CyclicBufferStorage *>(src)));
        }

        inline void cyclic_buffer_copy_assign(void *dst, const void *src, const void *)
        {
            *static_cast<CyclicBufferStorage *>(dst) = *static_cast<const CyclicBufferStorage *>(src);
        }

        inline void cyclic_buffer_move_assign(void *dst, void *src, const void *)
        {
            *static_cast<CyclicBufferStorage *>(dst) = std::move(*static_cast<CyclicBufferStorage *>(src));
        }

        inline void queue_construct(void *dst, const void *context)
        {
            const auto &state = checked_state<QueueState>(context, "queue");
            std::construct_at(static_cast<QueueStorage *>(dst));
            (void)state;
        }

        inline void queue_destroy(void *memory, const void *) noexcept
        {
            std::destroy_at(static_cast<QueueStorage *>(memory));
        }

        inline void queue_copy_construct(void *dst, const void *src, const void *)
        {
            std::construct_at(static_cast<QueueStorage *>(dst), *static_cast<const QueueStorage *>(src));
        }

        inline void queue_move_construct(void *dst, void *src, const void *)
        {
            std::construct_at(static_cast<QueueStorage *>(dst), std::move(*static_cast<QueueStorage *>(src)));
        }

        inline void queue_copy_assign(void *dst, const void *src, const void *)
        {
            *static_cast<QueueStorage *>(dst) = *static_cast<const QueueStorage *>(src);
        }

        inline void queue_move_assign(void *dst, void *src, const void *)
        {
            *static_cast<QueueStorage *>(dst) = std::move(*static_cast<QueueStorage *>(src));
        }

        inline void set_construct(void *dst, const void *context)
        {
            const auto &state = checked_state<SetState>(context, "set");
            std::construct_at(static_cast<SetStorage *>(dst));
            (void)state;
        }

        inline void set_destroy(void *memory, const void *) noexcept
        {
            std::destroy_at(static_cast<SetStorage *>(memory));
        }

        inline void set_copy_construct(void *dst, const void *src, const void *)
        {
            std::construct_at(static_cast<SetStorage *>(dst), *static_cast<const SetStorage *>(src));
        }

        inline void set_move_construct(void *dst, void *src, const void *)
        {
            std::construct_at(static_cast<SetStorage *>(dst), std::move(*static_cast<SetStorage *>(src)));
        }

        inline void set_copy_assign(void *dst, const void *src, const void *)
        {
            *static_cast<SetStorage *>(dst) = *static_cast<const SetStorage *>(src);
        }

        inline void set_move_assign(void *dst, void *src, const void *)
        {
            *static_cast<SetStorage *>(dst) = std::move(*static_cast<SetStorage *>(src));
        }

        inline void map_construct(void *dst, const void *context)
        {
            const auto &state = checked_state<MapState>(context, "map");
            std::construct_at(static_cast<MapStorage *>(dst));
            (void)state;
        }

        inline void map_destroy(void *memory, const void *) noexcept
        {
            std::destroy_at(static_cast<MapStorage *>(memory));
        }

        inline void map_copy_construct(void *dst, const void *src, const void *)
        {
            std::construct_at(static_cast<MapStorage *>(dst), *static_cast<const MapStorage *>(src));
        }

        inline void map_move_construct(void *dst, void *src, const void *)
        {
            std::construct_at(static_cast<MapStorage *>(dst), std::move(*static_cast<MapStorage *>(src)));
        }

        inline void map_copy_assign(void *dst, const void *src, const void *)
        {
            *static_cast<MapStorage *>(dst) = *static_cast<const MapStorage *>(src);
        }

        inline void map_move_assign(void *dst, void *src, const void *)
        {
            *static_cast<MapStorage *>(dst) = std::move(*static_cast<MapStorage *>(src));
        }

        // -------- plan factory: per-instantiation interning -----------

        template <typename Key, typename State, typename KeyHash = std::hash<Key>>
        struct CompactContainerPlanRegistry
        {
            std::mutex                                                              mutex{};
            std::unordered_map<Key, const MemoryUtils::StoragePlan *, KeyHash>      cache{};
            std::vector<std::unique_ptr<State>>                                     states{};
            std::vector<std::unique_ptr<MemoryUtils::StoragePlan>>                  plans{};

            template <typename StateFactory, typename PlanFactory>
            [[nodiscard]] const MemoryUtils::StoragePlan &intern(const Key &key, StateFactory &&state_factory,
                                                                 PlanFactory &&plan_factory)
            {
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    if (auto it = cache.find(key); it != cache.end()) { return *it->second; }
                }
                auto state = state_factory();
                auto plan  = plan_factory(*state);
                std::lock_guard<std::mutex> lock(mutex);
                if (auto it = cache.find(key); it != cache.end()) { return *it->second; }
                const auto *result = plan.get();
                states.push_back(std::move(state));
                plans.push_back(std::move(plan));
                cache.emplace(key, result);
                return *result;
            }

            void clear() noexcept
            {
                std::lock_guard<std::mutex> lock(mutex);
                cache.clear();
                plans.clear();
                states.clear();
            }
        };

        struct UnaryBindingKey
        {
            const ValueTypeBinding *binding{nullptr};
            [[nodiscard]] bool operator==(const UnaryBindingKey &) const noexcept = default;
        };
        struct UnaryBindingKeyHash
        {
            [[nodiscard]] std::size_t operator()(const UnaryBindingKey &k) const noexcept
            {
                return std::hash<const ValueTypeBinding *>{}(k.binding);
            }
        };

        struct BinaryBindingKey
        {
            const ValueTypeBinding *first{nullptr};
            const ValueTypeBinding *second{nullptr};
            [[nodiscard]] bool operator==(const BinaryBindingKey &) const noexcept = default;
        };
        struct BinaryBindingKeyHash
        {
            [[nodiscard]] std::size_t operator()(const BinaryBindingKey &k) const noexcept
            {
                std::size_t seed = std::hash<const ValueTypeBinding *>{}(k.first);
                return combine_hash(seed, std::hash<const ValueTypeBinding *>{}(k.second));
            }
        };

        struct SizedBindingKey
        {
            const ValueTypeBinding *binding{nullptr};
            std::size_t             size{0};
            [[nodiscard]] bool operator==(const SizedBindingKey &) const noexcept = default;
        };
        struct SizedBindingKeyHash
        {
            [[nodiscard]] std::size_t operator()(const SizedBindingKey &k) const noexcept
            {
                std::size_t seed = std::hash<const ValueTypeBinding *>{}(k.binding);
                return combine_hash(seed, std::hash<std::size_t>{}(k.size));
            }
        };

        template <typename Storage, auto Construct, auto Destroy, auto CopyConstruct, auto MoveConstruct,
                  auto CopyAssign, auto MoveAssign, typename State>
        [[nodiscard]] inline MemoryUtils::StoragePlan make_storage_plan(const State &state)
        {
            return MemoryUtils::StoragePlan{
                .layout                       = MemoryUtils::layout_for<Storage>(),
                .lifecycle                    = {.construct      = Construct,
                                                 .destroy        = Destroy,
                                                 .copy_construct = CopyConstruct,
                                                 .move_construct = MoveConstruct,
                                                 .copy_assign    = CopyAssign,
                                                 .move_assign    = MoveAssign},
                .lifecycle_context            = &state,
                .composite_kind_tag           = MemoryUtils::CompositeKind::None,
                .trivially_destructible       = false,
                .trivially_copyable           = false,
                .trivially_move_constructible = false,
            };
        }

        // Per-kind interning registries.
        inline CompactContainerPlanRegistry<UnaryBindingKey, ListState, UnaryBindingKeyHash> &list_registry()
        {
            static CompactContainerPlanRegistry<UnaryBindingKey, ListState, UnaryBindingKeyHash> r;
            return r;
        }
        inline CompactContainerPlanRegistry<UnaryBindingKey, SetState, UnaryBindingKeyHash> &set_registry()
        {
            static CompactContainerPlanRegistry<UnaryBindingKey, SetState, UnaryBindingKeyHash> r;
            return r;
        }
        inline CompactContainerPlanRegistry<BinaryBindingKey, MapState, BinaryBindingKeyHash> &map_registry()
        {
            static CompactContainerPlanRegistry<BinaryBindingKey, MapState, BinaryBindingKeyHash> r;
            return r;
        }
        inline CompactContainerPlanRegistry<SizedBindingKey, CyclicBufferState, SizedBindingKeyHash> &
        cyclic_buffer_registry()
        {
            static CompactContainerPlanRegistry<SizedBindingKey, CyclicBufferState, SizedBindingKeyHash> r;
            return r;
        }
        inline CompactContainerPlanRegistry<SizedBindingKey, QueueState, SizedBindingKeyHash> &queue_registry()
        {
            static CompactContainerPlanRegistry<SizedBindingKey, QueueState, SizedBindingKeyHash> r;
            return r;
        }
    }  // namespace compact_detail

    // -----------------------------------------------------------------
    // Public plan accessors. ``ValuePlanFactory`` calls into these to
    // synthesise the plan for a container schema.
    // -----------------------------------------------------------------

    [[nodiscard]] inline const MemoryUtils::StoragePlan &compact_list_plan(const ValueTypeBinding &element_binding)
    {
        return compact_detail::list_registry().intern(
            compact_detail::UnaryBindingKey{.binding = &element_binding},
            [&] { return std::make_unique<ListState>(ListState{.element_binding = &element_binding}); },
            [&](const ListState &state) {
                return std::make_unique<MemoryUtils::StoragePlan>(
                    compact_detail::make_storage_plan<ListStorage,
                                                       &compact_detail::list_construct,
                                                       &compact_detail::list_destroy,
                                                       &compact_detail::list_copy_construct,
                                                       &compact_detail::list_move_construct,
                                                       &compact_detail::list_copy_assign,
                                                       &compact_detail::list_move_assign>(state));
            });
    }

    [[nodiscard]] inline const MemoryUtils::StoragePlan &compact_set_plan(const ValueTypeBinding &element_binding)
    {
        return compact_detail::set_registry().intern(
            compact_detail::UnaryBindingKey{.binding = &element_binding},
            [&] { return std::make_unique<SetState>(SetState{.element_binding = &element_binding}); },
            [&](const SetState &state) {
                return std::make_unique<MemoryUtils::StoragePlan>(
                    compact_detail::make_storage_plan<SetStorage,
                                                       &compact_detail::set_construct,
                                                       &compact_detail::set_destroy,
                                                       &compact_detail::set_copy_construct,
                                                       &compact_detail::set_move_construct,
                                                       &compact_detail::set_copy_assign,
                                                       &compact_detail::set_move_assign>(state));
            });
    }

    [[nodiscard]] inline const MemoryUtils::StoragePlan &compact_map_plan(const ValueTypeBinding &key_binding,
                                                                          const ValueTypeBinding &value_binding)
    {
        return compact_detail::map_registry().intern(
            compact_detail::BinaryBindingKey{.first = &key_binding, .second = &value_binding},
            [&] {
                return std::make_unique<MapState>(MapState{.key_binding   = &key_binding,
                                                            .value_binding = &value_binding});
            },
            [&](const MapState &state) {
                return std::make_unique<MemoryUtils::StoragePlan>(
                    compact_detail::make_storage_plan<MapStorage,
                                                       &compact_detail::map_construct,
                                                       &compact_detail::map_destroy,
                                                       &compact_detail::map_copy_construct,
                                                       &compact_detail::map_move_construct,
                                                       &compact_detail::map_copy_assign,
                                                       &compact_detail::map_move_assign>(state));
            });
    }

    [[nodiscard]] inline const MemoryUtils::StoragePlan &
    compact_cyclic_buffer_plan(const ValueTypeBinding &element_binding, std::size_t capacity)
    {
        return compact_detail::cyclic_buffer_registry().intern(
            compact_detail::SizedBindingKey{.binding = &element_binding, .size = capacity},
            [&] {
                return std::make_unique<CyclicBufferState>(
                    CyclicBufferState{.element_binding = &element_binding, .capacity = capacity});
            },
            [&](const CyclicBufferState &state) {
                return std::make_unique<MemoryUtils::StoragePlan>(
                    compact_detail::make_storage_plan<CyclicBufferStorage,
                                                       &compact_detail::cyclic_buffer_construct,
                                                       &compact_detail::cyclic_buffer_destroy,
                                                       &compact_detail::cyclic_buffer_copy_construct,
                                                       &compact_detail::cyclic_buffer_move_construct,
                                                       &compact_detail::cyclic_buffer_copy_assign,
                                                       &compact_detail::cyclic_buffer_move_assign>(state));
            });
    }

    [[nodiscard]] inline const MemoryUtils::StoragePlan &compact_queue_plan(const ValueTypeBinding &element_binding,
                                                                            std::size_t             max_capacity)
    {
        return compact_detail::queue_registry().intern(
            compact_detail::SizedBindingKey{.binding = &element_binding, .size = max_capacity},
            [&] {
                return std::make_unique<QueueState>(
                    QueueState{.element_binding = &element_binding, .max_capacity = max_capacity});
            },
            [&](const QueueState &state) {
                return std::make_unique<MemoryUtils::StoragePlan>(
                    compact_detail::make_storage_plan<QueueStorage,
                                                       &compact_detail::queue_construct,
                                                       &compact_detail::queue_destroy,
                                                       &compact_detail::queue_copy_construct,
                                                       &compact_detail::queue_move_construct,
                                                       &compact_detail::queue_copy_assign,
                                                       &compact_detail::queue_move_assign>(state));
            });
    }

    /**
     * Drop every interned compact-container plan and its lifecycle-context
     * state. Test-only helper used by the registry-reset listener;
     * pointers previously returned by ``compact_*_plan`` become invalid.
     */
    inline void clear_compact_container_plans() noexcept
    {
        compact_detail::list_registry().clear();
        compact_detail::set_registry().clear();
        compact_detail::map_registry().clear();
        compact_detail::cyclic_buffer_registry().clear();
        compact_detail::queue_registry().clear();
    }
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_VALUE_COMPACT_STORAGE_H
