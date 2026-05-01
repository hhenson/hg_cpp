#ifndef HGRAPH_CPP_ROOT_TYPE_BINDING_H
#define HGRAPH_CPP_ROOT_TYPE_BINDING_H

#include <hgraph/types/utils/intern_table.h>
#include <hgraph/types/utils/memory_utils.h>

#include <functional>
#include <stdexcept>

namespace hgraph
{
    /**
     * Interned binding from a schema descriptor to a ``StoragePlan`` and
     * an ops table.
     *
     * ``TypeBinding`` is the canonical handle the value and time-series
     * layers share: it holds three borrowed pointers — schema identity,
     * storage layout (with lifecycle hooks), and the runtime ops vtable —
     * and exposes them through a single stable address. Lightweight views
     * and ``StorageHandle`` instances can recover everything they need
     * from the same binding.
     *
     * Bindings are interned through a per-instantiation ``InternTable``
     * keyed on ``(type_meta, plan, ops)``; equal triples always return the
     * same binding pointer.
     *
     * Type parameters:
     * - ``TypeMeta``  e.g. ``ValueTypeMetaData``, ``TSValueTypeMetaData``.
     * - ``Ops``       the matching ops vtable type (``ValueOps``,
     *                  ``TsValueOps``, etc.).
     */
    template <typename TypeMeta, typename Ops> struct TypeBinding
    {
        /** Schema identity; immutable once interned. */
        const TypeMeta                 *type_meta{nullptr};
        /** Storage plan describing layout and lifecycle hooks. */
        const MemoryUtils::StoragePlan *storage_plan{nullptr};
        /** Runtime ops vtable for the bound type. */
        const Ops                      *ops{nullptr};

        /** True when all three slots are populated. */
        [[nodiscard]] constexpr bool valid() const noexcept {
            return type_meta != nullptr && storage_plan != nullptr && ops != nullptr;
        }

        /** Reference to ``*type_meta`` or throws when missing. */
        [[nodiscard]] const TypeMeta &checked_type() const {
            if (type_meta == nullptr) { throw std::logic_error("TypeBinding is missing type metadata"); }
            return *type_meta;
        }

        /** Pointer to the bound ``StoragePlan`` (may be null). */
        [[nodiscard]] const MemoryUtils::StoragePlan *plan() const noexcept { return storage_plan; }

        /** Reference to ``*storage_plan`` or throws when missing. */
        [[nodiscard]] const MemoryUtils::StoragePlan &checked_plan() const {
            if (const auto *bound_plan = plan(); bound_plan != nullptr) { return *bound_plan; }
            throw std::logic_error("TypeBinding is missing a storage plan");
        }

        /** Lifecycle hooks pulled from the bound plan; throws if no plan is bound. */
        [[nodiscard]] const MemoryUtils::LifecycleOps &checked_lifecycle() const { return checked_plan().lifecycle; }

        /** Reference to ``*ops`` or throws when missing. */
        [[nodiscard]] const Ops &checked_ops() const {
            if (ops == nullptr) { throw std::logic_error("TypeBinding is missing runtime operations"); }
            return *ops;
        }

        /** Pointer to the lifecycle hooks; null when no plan is bound. */
        [[nodiscard]] const MemoryUtils::LifecycleOps *lifecycle() const noexcept {
            return plan() != nullptr ? &plan()->lifecycle : nullptr;
        }

        /** Pointer to the bound plan's ``lifecycle_context``; null when no plan is bound. */
        [[nodiscard]] const void *lifecycle_context() const noexcept {
            return plan() != nullptr ? plan()->lifecycle_context : nullptr;
        }

        /** Convenience: default-construct via the bound plan. */
        void default_construct_at(void *memory) const { checked_plan().default_construct(memory); }

        /** Convenience: destroy via the bound plan. */
        void destroy_at(void *memory) const noexcept { checked_plan().destroy(memory); }

        /** Convenience: copy-construct via the bound plan. */
        void copy_construct_at(void *dst, const void *src) const { checked_plan().copy_construct(dst, src); }

        /** Convenience: move-construct via the bound plan. */
        void move_construct_at(void *dst, void *src) const { checked_plan().move_construct(dst, src); }

        /** Convenience: copy-assign via the bound plan. */
        void copy_assign_at(void *dst, const void *src) const { checked_plan().copy_assign(dst, src); }

        /** Convenience: move-assign via the bound plan. */
        void move_assign_at(void *dst, void *src) const { checked_plan().move_assign(dst, src); }

        /** Structural identity used for interning a binding. */
        struct Key
        {
            const TypeMeta                 *type_meta{nullptr};
            const MemoryUtils::StoragePlan *storage_plan{nullptr};
            const Ops                      *ops{nullptr};

            [[nodiscard]] bool operator==(const Key &) const noexcept = default;
        };

        /** Hash functor over ``Key`` mixing the three pointer hashes. */
        struct KeyHash
        {
            [[nodiscard]] size_t operator()(const Key &key) const noexcept {
                size_t seed = std::hash<const TypeMeta *>{}(key.type_meta);
                combine(seed, std::hash<const MemoryUtils::StoragePlan *>{}(key.storage_plan));
                combine(seed, std::hash<const Ops *>{}(key.ops));
                return seed;
            }

          private:
            static void combine(size_t &seed, size_t value) noexcept {
                seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
            }
        };

        /**
         * Return the canonical binding for the ``(type_meta, plan, ops)``
         * triple, constructing it on first use. Equivalent triples always
         * return the same address.
         */
        [[nodiscard]] static const TypeBinding &intern(const TypeMeta &type_meta, const MemoryUtils::StoragePlan &plan,
                                                       const Ops &ops) {
            return registry().emplace(
                Key{
                    .type_meta    = &type_meta,
                    .storage_plan = &plan,
                    .ops          = &ops,
                },
                &type_meta, &plan, &ops);
        }

        /**
         * Look up an existing binding for the supplied triple; returns
         * ``nullptr`` when the triple has not yet been interned.
         */
        [[nodiscard]] static const TypeBinding *find(const TypeMeta *type_meta, const MemoryUtils::StoragePlan *storage_plan,
                                                     const Ops *ops) noexcept {
            return registry().find(Key{
                .type_meta    = type_meta,
                .storage_plan = storage_plan,
                .ops          = ops,
            });
        }

      private:
        [[nodiscard]] static InternTable<Key, TypeBinding, KeyHash> &registry() noexcept {
            static InternTable<Key, TypeBinding, KeyHash> registry;
            return registry;
        }
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TYPE_BINDING_H
