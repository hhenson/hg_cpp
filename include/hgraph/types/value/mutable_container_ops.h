#ifndef HGRAPH_CPP_ROOT_VALUE_MUTABLE_CONTAINER_OPS_H
#define HGRAPH_CPP_ROOT_VALUE_MUTABLE_CONTAINER_OPS_H

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/utils/value_slot_store.h>
#include <hgraph/types/value/compact_container_ops.h>
#include <hgraph/types/value/compact_storage.h>
#include <hgraph/types/value/container_ops.h>
#include <hgraph/types/value/value_ops.h>

#include <algorithm>
#include <compare>
#include <cstddef>
#include <memory>
#include <new>
#include <stdexcept>
#include <utility>

namespace hgraph
{
    /**
     * Structurally-mutable value-layer containers, built on the same slot-store
     * substrate the time-series layer uses (here without delta tracking). This
     * header carries the *mutable* implementation of the list kind: a growable
     * storage shape, its lifecycle/read thunks, the canonical
     * ``MutableListValueOps`` table (read surface from ``container_ops.h`` plus
     * the structural-mutation hooks), the per-instantiation storage plan, and
     * the canonical-binding accessor that interns the (schema, plan, ops)
     * triple. The mutable map lives alongside it in a later slice.
     *
     * The element binding is baked into the plan's lifecycle context so an
     * empty mutable container is still element-bound and can accept
     * ``push_back`` immediately (unlike the compact, build-once containers).
     */

    /**
     * Growable contiguous-by-index value storage for a mutable ``List``.
     *
     * Backed by a ``ValueSlotStore`` (the shared slot substrate); logical size
     * tracks the live prefix ``[0, size())``. Elements are addressed by index;
     * ``push_back`` grows capacity geometrically.
     */
    class MutableListStorage
    {
      public:
        MutableListStorage() noexcept = default;

        explicit MutableListStorage(const ValueTypeBinding &element_binding)
            : element_binding_{&element_binding}, slots_{element_binding.checked_plan()}
        {
        }

        MutableListStorage(const MutableListStorage &other) { copy_from(other); }

        MutableListStorage &operator=(const MutableListStorage &other)
        {
            if (this != &other) { copy_from(other); }
            return *this;
        }

        MutableListStorage(MutableListStorage &&other) noexcept
            : element_binding_{other.element_binding_}, slots_{std::move(other.slots_)}, size_{other.size_}
        {
            other.size_ = 0;
        }

        MutableListStorage &operator=(MutableListStorage &&other) noexcept
        {
            if (this != &other)
            {
                slots_           = std::move(other.slots_);
                element_binding_ = other.element_binding_;
                size_            = other.size_;
                other.size_      = 0;
            }
            return *this;
        }

        ~MutableListStorage() = default;  // ValueSlotStore destroys live payloads

        [[nodiscard]] std::size_t              size() const noexcept { return size_; }
        [[nodiscard]] bool                     empty() const noexcept { return size_ == 0; }
        [[nodiscard]] const ValueTypeBinding  *element_binding() const noexcept { return element_binding_; }

        [[nodiscard]] const void *element_at(std::size_t index) const
        {
            require_index(index);
            return slots_.value_memory(index);
        }
        [[nodiscard]] void *element_at(std::size_t index)
        {
            require_index(index);
            return slots_.value_memory(index);
        }

        /** Append a copy of the element at ``src``. */
        void push_back(const void *src)
        {
            require_bound();
            ensure_capacity(size_ + 1);
            slots_.construct_at(size_, src);  // copy-construct from src
            ++size_;
        }

        /** Replace the element at ``index`` with a copy of ``src``. */
        void set_element(std::size_t index, const void *src)
        {
            require_index(index);
            element_binding_->checked_plan().copy_assign(slots_.value_memory(index), src);
        }

        /** Remove the element at ``index``, shifting later elements down one place. */
        void erase(std::size_t index)
        {
            require_index(index);
            const auto &plan = element_binding_->checked_plan();
            for (std::size_t j = index; j + 1 < size_; ++j)
            {
                plan.move_assign(slots_.value_memory(j), slots_.value_memory(j + 1));
            }
            slots_.destroy_at(size_ - 1);  // the tail slot is now a duplicate
            --size_;
        }

        /** Drop the last element. */
        void pop_back()
        {
            if (size_ == 0) { throw std::logic_error("MutableListStorage::pop_back on empty list"); }
            slots_.destroy_at(size_ - 1);
            --size_;
        }

        /** Destroy every element; capacity is retained for reuse. */
        void clear() noexcept
        {
            for (std::size_t index = size_; index > 0; --index) { slots_.destroy_at(index - 1); }
            size_ = 0;
        }

      private:
        const ValueTypeBinding *element_binding_{nullptr};
        ValueSlotStore          slots_{};
        std::size_t             size_{0};

        void require_bound() const
        {
            if (element_binding_ == nullptr) { throw std::logic_error("MutableListStorage is not element-bound"); }
        }
        void require_index(std::size_t index) const
        {
            if (index >= size_) { throw std::out_of_range("MutableListStorage index out of range"); }
        }
        void ensure_capacity(std::size_t needed)
        {
            const std::size_t cap = slots_.slot_capacity();
            if (needed <= cap) { return; }
            slots_.reserve_to(std::max<std::size_t>(needed, cap == 0 ? std::size_t{4} : cap * 2));
        }
        void copy_from(const MutableListStorage &other)
        {
            element_binding_ = other.element_binding_;
            size_            = 0;
            if (element_binding_ == nullptr)
            {
                slots_ = ValueSlotStore{};  // unbound; destroys any prior payloads
                return;
            }
            slots_ = ValueSlotStore{element_binding_->checked_plan()};
            slots_.reserve_to(other.size_);
            for (std::size_t index = 0; index < other.size_; ++index)
            {
                slots_.construct_at(index, other.element_at(index));
                ++size_;
            }
        }
    };

    namespace mutable_container_detail
    {
        struct MutableListState
        {
            const ValueTypeBinding *element_binding{nullptr};
        };

        // -- lifecycle thunks (element binding comes from the plan's state) --
        inline void list_construct(void *dst, const void *context)
        {
            const auto &state = compact_detail::checked_state<MutableListState>(context, "mutable_list");
            if (state.element_binding == nullptr)
            {
                throw std::logic_error("mutable_list construction requires an element binding");
            }
            std::construct_at(static_cast<MutableListStorage *>(dst), *state.element_binding);
        }
        inline void list_destroy(void *memory, const void *) noexcept
        {
            std::destroy_at(static_cast<MutableListStorage *>(memory));
        }
        inline void list_copy_construct(void *dst, const void *src, const void *)
        {
            std::construct_at(static_cast<MutableListStorage *>(dst), *static_cast<const MutableListStorage *>(src));
        }
        inline void list_move_construct(void *dst, void *src, const void *)
        {
            std::construct_at(static_cast<MutableListStorage *>(dst), std::move(*static_cast<MutableListStorage *>(src)));
        }
        inline void list_copy_assign(void *dst, const void *src, const void *)
        {
            *static_cast<MutableListStorage *>(dst) = *static_cast<const MutableListStorage *>(src);
        }
        inline void list_move_assign(void *dst, void *src, const void *)
        {
            *static_cast<MutableListStorage *>(dst) = std::move(*static_cast<MutableListStorage *>(src));
        }

        // -- read-op thunks (same surface as the compact list, over the mutable storage) --
        inline std::size_t list_size(const void *, const void *memory) noexcept
        {
            return static_cast<const MutableListStorage *>(memory)->size();
        }
        inline const void *list_element_at(const void *, const void *memory, std::size_t index)
        {
            return static_cast<const MutableListStorage *>(memory)->element_at(index);
        }
        inline const ValueTypeBinding *list_element_binding(const void *, const void *memory, std::size_t) noexcept
        {
            return static_cast<const MutableListStorage *>(memory)->element_binding();
        }
        inline std::size_t list_hash(const void *, const void *memory)
        {
            const auto *storage = static_cast<const MutableListStorage *>(memory);
            if (storage->element_binding() == nullptr) { throw std::logic_error("mutable list hash requires a binding"); }
            const auto &ops  = storage->element_binding()->checked_ops();
            std::size_t seed = 0;
            for (std::size_t i = 0; i < storage->size(); ++i)
            {
                seed = container_ops_detail::combine_hash(seed, ops.hash(storage->element_at(i)));
            }
            return seed;
        }
        inline bool list_equals(const void *, const void *lhs, const void *rhs) noexcept
        {
            const auto *a = static_cast<const MutableListStorage *>(lhs);
            const auto *b = static_cast<const MutableListStorage *>(rhs);
            if (a->size() != b->size()) { return false; }
            if (a->size() == 0) { return true; }
            if (a->element_binding() == nullptr || b->element_binding() == nullptr) { return false; }
            if (a->element_binding() != b->element_binding()) { return false; }
            const auto &ops = a->element_binding()->checked_ops();
            for (std::size_t i = 0; i < a->size(); ++i)
            {
                if (!ops.equals(a->element_at(i), b->element_at(i))) { return false; }
            }
            return true;
        }
        inline std::partial_ordering list_compare(const void *, const void *lhs, const void *rhs) noexcept
        {
            const auto *a = static_cast<const MutableListStorage *>(lhs);
            const auto *b = static_cast<const MutableListStorage *>(rhs);
            const auto *a_binding = a->element_binding();
            const auto *b_binding = b->element_binding();
            if (const auto order = value_ops_detail::null_order(a_binding, b_binding)) { return *order; }
            if (a_binding != b_binding) { return std::partial_ordering::unordered; }
            const auto &ops = a_binding->checked_ops();
            const auto  n   = std::min(a->size(), b->size());
            for (std::size_t i = 0; i < n; ++i)
            {
                const auto c = ops.compare(a->element_at(i), b->element_at(i));
                if (c != 0) { return c; }
            }
            if (a->size() < b->size()) { return std::partial_ordering::less; }
            if (a->size() > b->size()) { return std::partial_ordering::greater; }
            return std::partial_ordering::equivalent;
        }
        inline std::string list_to_string(const void *, const void *memory)
        {
            const auto *storage = static_cast<const MutableListStorage *>(memory);
            if (storage->element_binding() == nullptr) { return "[]"; }
            const auto &ops = storage->element_binding()->checked_ops();
            return container_ops_detail::format_delimited(
                '[', ']', storage->size(), [&](fmt::memory_buffer &out, std::size_t i) {
                    fmt::format_to(std::back_inserter(out), "{}", ops.to_string(storage->element_at(i)));
                });
        }

        // -- structural-mutation thunks --
        inline void list_push_back(const void *, void *memory, const void *element)
        {
            static_cast<MutableListStorage *>(memory)->push_back(element);
        }
        inline void list_set_element(const void *, void *memory, std::size_t index, const void *element)
        {
            static_cast<MutableListStorage *>(memory)->set_element(index, element);
        }
        inline void list_erase(const void *, void *memory, std::size_t index)
        {
            static_cast<MutableListStorage *>(memory)->erase(index);
        }
        inline void list_pop_back(const void *, void *memory)
        {
            static_cast<MutableListStorage *>(memory)->pop_back();
        }
        inline void list_clear(const void *, void *memory)
        {
            static_cast<MutableListStorage *>(memory)->clear();
        }

        inline compact_detail::CompactContainerPlanRegistry<compact_detail::UnaryBindingKey, MutableListState,
                                                            compact_detail::UnaryBindingKeyHash> &
        list_registry()
        {
            static compact_detail::CompactContainerPlanRegistry<compact_detail::UnaryBindingKey, MutableListState,
                                                                compact_detail::UnaryBindingKeyHash>
                r;
            return r;
        }
    }  // namespace mutable_container_detail

    [[nodiscard]] inline const MemoryUtils::StoragePlan &mutable_list_plan(const ValueTypeBinding &element_binding)
    {
        using namespace mutable_container_detail;
        return list_registry().intern(
            compact_detail::UnaryBindingKey{.binding = &element_binding},
            [&] { return std::make_unique<MutableListState>(MutableListState{.element_binding = &element_binding}); },
            [&](const MutableListState &state) {
                return std::make_unique<MemoryUtils::StoragePlan>(
                    compact_detail::make_storage_plan<MutableListStorage, &list_construct, &list_destroy,
                                                      &list_copy_construct, &list_move_construct, &list_copy_assign,
                                                      &list_move_assign>(state));
            });
    }

    [[nodiscard]] inline const MutableListValueOps &mutable_list_ops() noexcept
    {
        using namespace mutable_container_detail;
        static const MutableListValueOps ops = {
            {{{// ValueOps:
               nullptr,
               true,  // allows_mutation
               &list_hash,
               &list_equals,
               &list_compare,
               &list_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
               ,
               nullptr,
               nullptr
#endif
              },
              // IndexedValueOps:
              &list_size,
              &list_element_at,
              &list_element_binding,
              &container_ops_detail::dense_make_range<&list_size, &list_element_at, &list_element_binding>,
              &container_ops_detail::dense_make_mutable_range<&list_size, &list_element_at, &list_element_binding>}},
            // MutableListValueOps:
            &list_push_back,
            &list_set_element,
            &list_erase,
            &list_pop_back,
            &list_clear,
        };
        return ops;
    }

    [[nodiscard]] inline const ValueTypeBinding &mutable_list_binding(const ValueTypeBinding &element_binding)
    {
        const auto *meta = TypeRegistry::instance().mutable_list(element_binding.type_meta);
        return ValueTypeBinding::intern(*meta, mutable_list_plan(element_binding), mutable_list_ops());
    }

    // =================================================================
    // Mutable Map
    // =================================================================

    namespace mutable_container_detail
    {
        // Adapt a key binding's value-layer ValueOps (hash/equals) to the
        // KeySlotStore hook shape (which passes (key, context) / (lhs, rhs,
        // context)). The context is the key binding's ValueOps.
        inline std::size_t key_hash_adapter(const void *key, const void *context)
        {
            return static_cast<const ValueOps *>(context)->hash(key);
        }
        inline bool key_equal_adapter(const void *lhs, const void *rhs, const void *context)
        {
            return static_cast<const ValueOps *>(context)->equals(lhs, rhs);
        }
        [[nodiscard]] inline KeySlotStoreOps key_ops_for(const ValueTypeBinding &key_binding)
        {
            return KeySlotStoreOps{
                .hash    = &key_hash_adapter,
                .equal   = &key_equal_adapter,
                .context = &key_binding.checked_ops(),
            };
        }
    }  // namespace mutable_container_detail

    /**
     * Structurally-mutable value-layer ``Map`` storage. Keys live in a
     * ``KeySlotStore`` (hashed/compared via the key binding's ``ValueOps``);
     * values live in a parallel ``ValueSlotStore`` co-indexed by slot. Erase is
     * immediate (the key is removed and flushed, the value destroyed at once).
     * Always key/value-bound (the plan's lifecycle context carries the
     * bindings), so it never has an unbound state.
     */
    class MutableMapStorage
    {
      public:
        MutableMapStorage(const ValueTypeBinding &key_binding, const ValueTypeBinding &value_binding)
            : key_binding_{&key_binding}
            , value_binding_{&value_binding}
            , keys_{key_binding.checked_plan(), mutable_container_detail::key_ops_for(key_binding)}
            , values_{value_binding.checked_plan()}
        {
        }

        MutableMapStorage(const MutableMapStorage &other)
            : key_binding_{other.key_binding_}
            , value_binding_{other.value_binding_}
            , keys_{other.key_binding_->checked_plan(), mutable_container_detail::key_ops_for(*other.key_binding_)}
            , values_{other.value_binding_->checked_plan()}
        {
            copy_entries_from(other);
        }

        MutableMapStorage &operator=(const MutableMapStorage &other)
        {
            if (this != &other)
            {
                values_.destroy_all();
                keys_          = KeySlotStore{other.key_binding_->checked_plan(),
                                              mutable_container_detail::key_ops_for(*other.key_binding_)};
                values_        = ValueSlotStore{other.value_binding_->checked_plan()};
                key_binding_   = other.key_binding_;
                value_binding_ = other.value_binding_;
                copy_entries_from(other);
            }
            return *this;
        }

        MutableMapStorage(MutableMapStorage &&) noexcept            = default;
        MutableMapStorage &operator=(MutableMapStorage &&) noexcept = default;
        ~MutableMapStorage()                                       = default;

        [[nodiscard]] std::size_t             size() const noexcept { return keys_.size(); }
        [[nodiscard]] std::size_t             slot_capacity() const noexcept { return keys_.slot_capacity(); }
        [[nodiscard]] bool                    slot_live(std::size_t slot) const noexcept { return keys_.slot_live(slot); }
        [[nodiscard]] const ValueTypeBinding *key_binding() const noexcept { return key_binding_; }
        [[nodiscard]] const ValueTypeBinding *value_binding() const noexcept { return value_binding_; }

        [[nodiscard]] const void *key_at(std::size_t slot) const noexcept { return keys_.key_memory(slot); }
        [[nodiscard]] const void *value_at_slot(std::size_t slot) const noexcept { return values_.value_memory(slot); }

        [[nodiscard]] bool        contains(const void *key) const { return keys_.find_slot(key) != KeySlotStore::npos; }
        [[nodiscard]] const void *value_for(const void *key) const
        {
            const std::size_t slot = keys_.find_slot(key);
            return slot == KeySlotStore::npos ? nullptr : values_.value_memory(slot);
        }

        // Ordinal (nth-live) accessors for the indexed surface; O(n).
        [[nodiscard]] const void *key_at_ordinal(std::size_t ordinal) const { return nth_live_memory(ordinal, true); }
        [[nodiscard]] const void *value_at_ordinal(std::size_t ordinal) const { return nth_live_memory(ordinal, false); }

        /** Insert ``key`` -> ``value`` (copy), or replace the value if the key is present. */
        void insert(const void *key, const void *value)
        {
            const auto result = keys_.insert(key);  // may grow key capacity
            values_.reserve_to(keys_.slot_capacity());
            if (result.inserted) { values_.construct_at(result.slot, value); }
            else { value_binding_->checked_plan().copy_assign(values_.value_memory(result.slot), value); }
        }

        /**
         * Mutable value memory for ``key``, default-constructing an (empty)
         * value entry when the key is absent. Lets callers assign the value in
         * place rather than building and copying a temporary.
         */
        void *value_or_emplace(const void *key)
        {
            const std::size_t slot = keys_.find_slot(key);
            if (slot != KeySlotStore::npos) { return values_.value_memory(slot); }
            const auto result = keys_.insert(key);
            values_.reserve_to(keys_.slot_capacity());
            values_.construct_at(result.slot);  // default-construct the value
            return values_.value_memory(result.slot);
        }

        /** Remove ``key`` (and its value) immediately. Returns true if a key was removed. */
        bool erase(const void *key)
        {
            const std::size_t slot = keys_.find_slot(key);
            if (slot == KeySlotStore::npos) { return false; }
            values_.destroy_at(slot);
            (void)keys_.remove_slot(slot);
            keys_.erase_pending();  // flush this removal immediately
            return true;
        }

        /** Remove every entry. */
        void clear()
        {
            values_.destroy_all();
            keys_.clear();
        }

      private:
        const ValueTypeBinding *key_binding_{nullptr};
        const ValueTypeBinding *value_binding_{nullptr};
        KeySlotStore            keys_;
        ValueSlotStore          values_;

        [[nodiscard]] const void *nth_live_memory(std::size_t ordinal, bool key) const
        {
            std::size_t count = 0;
            for (std::size_t slot = 0; slot < keys_.slot_capacity(); ++slot)
            {
                if (!keys_.slot_live(slot)) { continue; }
                if (count == ordinal) { return key ? keys_.key_memory(slot) : values_.value_memory(slot); }
                ++count;
            }
            throw std::out_of_range("MutableMapStorage ordinal out of range");
        }

        void copy_entries_from(const MutableMapStorage &other)
        {
            for (std::size_t slot = 0; slot < other.keys_.slot_capacity(); ++slot)
            {
                if (other.keys_.slot_live(slot)) { insert(other.keys_.key_memory(slot), other.values_.value_memory(slot)); }
            }
        }
    };

    namespace mutable_container_detail
    {
        struct MutableMapState
        {
            const ValueTypeBinding *key_binding{nullptr};
            const ValueTypeBinding *value_binding{nullptr};
        };

        // -- lifecycle thunks --
        inline void map_construct(void *dst, const void *context)
        {
            const auto &state = compact_detail::checked_state<MutableMapState>(context, "mutable_map");
            if (state.key_binding == nullptr || state.value_binding == nullptr)
            {
                throw std::logic_error("mutable_map construction requires key and value bindings");
            }
            std::construct_at(static_cast<MutableMapStorage *>(dst), *state.key_binding, *state.value_binding);
        }
        inline void map_destroy(void *memory, const void *) noexcept
        {
            std::destroy_at(static_cast<MutableMapStorage *>(memory));
        }
        inline void map_copy_construct(void *dst, const void *src, const void *)
        {
            std::construct_at(static_cast<MutableMapStorage *>(dst), *static_cast<const MutableMapStorage *>(src));
        }
        inline void map_move_construct(void *dst, void *src, const void *)
        {
            std::construct_at(static_cast<MutableMapStorage *>(dst), std::move(*static_cast<MutableMapStorage *>(src)));
        }
        inline void map_copy_assign(void *dst, const void *src, const void *)
        {
            *static_cast<MutableMapStorage *>(dst) = *static_cast<const MutableMapStorage *>(src);
        }
        inline void map_move_assign(void *dst, void *src, const void *)
        {
            *static_cast<MutableMapStorage *>(dst) = std::move(*static_cast<MutableMapStorage *>(src));
        }

        // -- read thunks --
        inline std::size_t map_size(const void *, const void *m) noexcept
        {
            return static_cast<const MutableMapStorage *>(m)->size();
        }
        inline bool map_contains(const void *, const void *m, const void *key)
        {
            return static_cast<const MutableMapStorage *>(m)->contains(key);
        }
        inline const void *map_value_at(const void *, const void *m, const void *key)
        {
            return static_cast<const MutableMapStorage *>(m)->value_for(key);
        }
        inline const void *map_key_at_index(const void *, const void *m, std::size_t i)
        {
            return static_cast<const MutableMapStorage *>(m)->key_at_ordinal(i);
        }
        inline const void *map_value_at_index(const void *, const void *m, std::size_t i)
        {
            return static_cast<const MutableMapStorage *>(m)->value_at_ordinal(i);
        }
        inline const ValueTypeBinding *map_key_binding(const void *, const void *m, std::size_t) noexcept
        {
            return static_cast<const MutableMapStorage *>(m)->key_binding();
        }
        inline const ValueTypeBinding *map_value_binding(const void *, const void *m) noexcept
        {
            return static_cast<const MutableMapStorage *>(m)->value_binding();
        }
        inline const ValueTypeBinding *map_value_binding_indexed(const void *, const void *m, std::size_t) noexcept
        {
            return map_value_binding(nullptr, m);
        }

        // -- predicate-based ranges (skip dead slots) --
        inline bool map_slot_live(const void *, const void *m, std::size_t slot)
        {
            return static_cast<const MutableMapStorage *>(m)->slot_live(slot);
        }
        inline ValueView map_key_projector(const void *, const void *m, std::size_t slot)
        {
            const auto *s = static_cast<const MutableMapStorage *>(m);
            return ValueView{s->key_binding(), s->key_at(slot)};
        }
        inline ValueView map_value_projector(const void *, const void *m, std::size_t slot)
        {
            const auto *s = static_cast<const MutableMapStorage *>(m);
            return ValueView{s->value_binding(), s->value_at_slot(slot)};
        }
        inline std::pair<ValueView, ValueView> map_kv_projector(const void *, const void *m, std::size_t slot)
        {
            const auto *s = static_cast<const MutableMapStorage *>(m);
            return {ValueView{s->key_binding(), s->key_at(slot)}, ValueView{s->value_binding(), s->value_at_slot(slot)}};
        }
        inline Range<ValueView> map_make_keys_range(const void *, const void *m)
        {
            return Range<ValueView>{nullptr, m, static_cast<const MutableMapStorage *>(m)->slot_capacity(),
                                    &map_slot_live, &map_key_projector};
        }
        inline Range<ValueView> map_make_values_range(const void *, const void *m)
        {
            return Range<ValueView>{nullptr, m, static_cast<const MutableMapStorage *>(m)->slot_capacity(),
                                    &map_slot_live, &map_value_projector};
        }
        inline KeyValueRange<ValueView, ValueView> map_make_kv_range(const void *, const void *m)
        {
            return KeyValueRange<ValueView, ValueView>{nullptr, m,
                                                       static_cast<const MutableMapStorage *>(m)->slot_capacity(),
                                                       &map_slot_live, &map_kv_projector};
        }

        // -- whole-map hash/equals/compare/to_string (order-independent) --
        inline std::size_t map_hash(const void *, const void *m)
        {
            const auto *s = static_cast<const MutableMapStorage *>(m);
            const auto &kops = s->key_binding()->checked_ops();
            const auto &vops = s->value_binding()->checked_ops();
            std::size_t acc  = 0;  // xor of per-entry hashes -> order-independent
            for (std::size_t slot = 0; slot < s->slot_capacity(); ++slot)
            {
                if (!s->slot_live(slot)) { continue; }
                acc ^= container_ops_detail::combine_hash(kops.hash(s->key_at(slot)), vops.hash(s->value_at_slot(slot)));
            }
            return acc;
        }
        inline bool map_equals(const void *, const void *lhs, const void *rhs) noexcept
        {
            const auto *a = static_cast<const MutableMapStorage *>(lhs);
            const auto *b = static_cast<const MutableMapStorage *>(rhs);
            if (a->size() != b->size()) { return false; }
            if (a->key_binding() != b->key_binding() || a->value_binding() != b->value_binding()) { return false; }
            const auto &vops = a->value_binding()->checked_ops();
            for (std::size_t slot = 0; slot < a->slot_capacity(); ++slot)
            {
                if (!a->slot_live(slot)) { continue; }
                const void *other_value = b->value_for(a->key_at(slot));
                if (other_value == nullptr || !vops.equals(a->value_at_slot(slot), other_value)) { return false; }
            }
            return true;
        }
        inline std::partial_ordering map_compare(const void *ctx, const void *lhs, const void *rhs) noexcept
        {
            // Maps have no natural order; equal maps are equivalent, otherwise unordered.
            return map_equals(ctx, lhs, rhs) ? std::partial_ordering::equivalent : std::partial_ordering::unordered;
        }
        inline std::string map_to_string(const void *, const void *m)
        {
            const auto *s = static_cast<const MutableMapStorage *>(m);
            const auto &kops = s->key_binding()->checked_ops();
            const auto &vops = s->value_binding()->checked_ops();
            fmt::memory_buffer out;
            fmt::format_to(std::back_inserter(out), "{{");
            bool first = true;
            for (std::size_t slot = 0; slot < s->slot_capacity(); ++slot)
            {
                if (!s->slot_live(slot)) { continue; }
                if (!first) { fmt::format_to(std::back_inserter(out), ", "); }
                first = false;
                fmt::format_to(std::back_inserter(out), "{}: {}", kops.to_string(s->key_at(slot)),
                               vops.to_string(s->value_at_slot(slot)));
            }
            fmt::format_to(std::back_inserter(out), "}}");
            return fmt::to_string(out);
        }

        // -- mutation thunks --
        inline void map_insert(const void *, void *m, const void *key, const void *value)
        {
            static_cast<MutableMapStorage *>(m)->insert(key, value);
        }
        inline void map_erase(const void *, void *m, const void *key)
        {
            (void)static_cast<MutableMapStorage *>(m)->erase(key);
        }
        inline void map_clear(const void *, void *m) { static_cast<MutableMapStorage *>(m)->clear(); }
        inline void *map_value_or_emplace(const void *, void *m, const void *key)
        {
            return static_cast<MutableMapStorage *>(m)->value_or_emplace(key);
        }

        // -- key-set adapter (read-only SetView over the live keys) --
        inline std::size_t map_key_set_hash(const void *, const void *m)
        {
            const auto *s = static_cast<const MutableMapStorage *>(m);
            const auto &kops = s->key_binding()->checked_ops();
            std::size_t acc = 0;
            for (std::size_t slot = 0; slot < s->slot_capacity(); ++slot)
            {
                if (s->slot_live(slot)) { acc ^= kops.hash(s->key_at(slot)); }
            }
            return acc;
        }
        inline bool map_key_set_equals(const void *, const void *lhs, const void *rhs) noexcept
        {
            const auto *a = static_cast<const MutableMapStorage *>(lhs);
            const auto *b = static_cast<const MutableMapStorage *>(rhs);
            if (a->size() != b->size()) { return false; }
            for (std::size_t slot = 0; slot < a->slot_capacity(); ++slot)
            {
                if (a->slot_live(slot) && !b->contains(a->key_at(slot))) { return false; }
            }
            return true;
        }
        inline std::partial_ordering map_key_set_compare(const void *ctx, const void *lhs, const void *rhs) noexcept
        {
            return map_key_set_equals(ctx, lhs, rhs) ? std::partial_ordering::equivalent
                                                     : std::partial_ordering::unordered;
        }
        inline std::string map_key_set_to_string(const void *, const void *m)
        {
            const auto *s = static_cast<const MutableMapStorage *>(m);
            const auto &kops = s->key_binding()->checked_ops();
            fmt::memory_buffer out;
            fmt::format_to(std::back_inserter(out), "{{");
            bool first = true;
            for (std::size_t slot = 0; slot < s->slot_capacity(); ++slot)
            {
                if (!s->slot_live(slot)) { continue; }
                if (!first) { fmt::format_to(std::back_inserter(out), ", "); }
                first = false;
                fmt::format_to(std::back_inserter(out), "{}", kops.to_string(s->key_at(slot)));
            }
            fmt::format_to(std::back_inserter(out), "}}");
            return fmt::to_string(out);
        }

        inline compact_detail::CompactContainerPlanRegistry<compact_detail::BinaryBindingKey, MutableMapState,
                                                            compact_detail::BinaryBindingKeyHash> &
        map_registry()
        {
            static compact_detail::CompactContainerPlanRegistry<compact_detail::BinaryBindingKey, MutableMapState,
                                                                compact_detail::BinaryBindingKeyHash>
                r;
            return r;
        }
    }  // namespace mutable_container_detail

    // Forward-declared: the key-set binding's plan is the map's own plan.
    [[nodiscard]] const MemoryUtils::StoragePlan &mutable_map_plan(const ValueTypeBinding &key_binding,
                                                                   const ValueTypeBinding &value_binding);

    [[nodiscard]] inline const SetValueOps &mutable_map_key_set_ops() noexcept
    {
        using namespace mutable_container_detail;
        static const SetValueOps ops = {
            {{nullptr,
              false,
              &map_key_set_hash,
              &map_key_set_equals,
              &map_key_set_compare,
              &map_key_set_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
              ,
              nullptr,
              nullptr
#endif
             },
             &map_size,
             &map_key_at_index,
             &map_key_binding,
             &map_make_keys_range,
             nullptr},
            &map_contains,
        };
        return ops;
    }

    [[nodiscard]] inline const ValueTypeBinding &mutable_map_key_set_binding(const ValueTypeBinding &key_binding,
                                                                             const ValueTypeBinding &value_binding)
    {
        const auto *set_meta = TypeRegistry::instance().set(key_binding.type_meta);
        return ValueTypeBinding::intern(*set_meta, mutable_map_plan(key_binding, value_binding),
                                        mutable_map_key_set_ops());
    }

    namespace mutable_container_detail
    {
        inline SetView map_key_set_thunk(const void *, const ValueTypeBinding * /*map_binding*/, const void *memory)
        {
            const auto *s = static_cast<const MutableMapStorage *>(memory);
            const ValueTypeBinding &set_binding = mutable_map_key_set_binding(*s->key_binding(), *s->value_binding());
            return SetView{ValueView{&set_binding, memory}};
        }
    }  // namespace mutable_container_detail

    [[nodiscard]] inline const MemoryUtils::StoragePlan &mutable_map_plan(const ValueTypeBinding &key_binding,
                                                                          const ValueTypeBinding &value_binding)
    {
        using namespace mutable_container_detail;
        return map_registry().intern(
            compact_detail::BinaryBindingKey{.first = &key_binding, .second = &value_binding},
            [&] {
                return std::make_unique<MutableMapState>(
                    MutableMapState{.key_binding = &key_binding, .value_binding = &value_binding});
            },
            [&](const MutableMapState &state) {
                return std::make_unique<MemoryUtils::StoragePlan>(
                    compact_detail::make_storage_plan<MutableMapStorage, &map_construct, &map_destroy,
                                                      &map_copy_construct, &map_move_construct, &map_copy_assign,
                                                      &map_move_assign>(state));
            });
    }

    [[nodiscard]] inline const MutableMapValueOps &mutable_map_ops() noexcept
    {
        using namespace mutable_container_detail;
        static const MutableMapValueOps ops = {
            {{{// ValueOps:
               nullptr,
               true,  // allows_mutation
               &map_hash,
               &map_equals,
               &map_compare,
               &map_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
               ,
               nullptr,
               nullptr
#endif
              },
              // IndexedValueOps (key surface):
              &map_size,
              &map_key_at_index,
              &map_key_binding,
              &map_make_keys_range,
              nullptr},
             // MapValueOps:
             &map_contains,
             &map_value_at,
             &map_value_at_index,
             &map_value_binding,
             &map_make_keys_range,
             &map_make_values_range,
             &map_make_kv_range,
             &map_key_set_thunk},
            // MutableMapValueOps:
            &map_insert,
            &map_erase,
            &map_clear,
            &map_value_or_emplace,
        };
        return ops;
    }

    [[nodiscard]] inline const ValueTypeBinding &mutable_map_binding(const ValueTypeBinding &key_binding,
                                                                     const ValueTypeBinding &value_binding)
    {
        const auto *meta = TypeRegistry::instance().mutable_map(key_binding.type_meta, value_binding.type_meta);
        return ValueTypeBinding::intern(*meta, mutable_map_plan(key_binding, value_binding), mutable_map_ops());
    }
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_VALUE_MUTABLE_CONTAINER_OPS_H
