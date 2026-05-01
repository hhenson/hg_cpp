//
// Created by Howard Henson on 20/04/2026.
//

#ifndef HGRAPH_CPP_ROOT_TYPE_REGISTRY_H
#define HGRAPH_CPP_ROOT_TYPE_REGISTRY_H

#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/utils/intern_table.h>
#include <hgraph/types/utils/memory_utils.h>

#include <memory>
#include <string>
#include <string_view>
#include <typeindex>
#include <unordered_map>
#include <vector>

namespace hgraph
{
    /**
     * Process-wide registry that interns value and time-series schemas.
     *
     * The registry is the source of truth for schema identity. Equivalent
     * descriptions resolve to the same ``ValueTypeMetaData`` /
     * ``TSValueTypeMetaData`` pointer, which makes pointer equality a safe
     * fast path for graph construction and runtime dispatch.
     *
     * The registry owns the storage for descriptors, field arrays, and
     * interned names; pointers it returns remain valid for the lifetime
     * of the process.
     *
     * Use ``TypeRegistry::instance()`` to access the singleton; the
     * constructor is private. The class is non-copyable and non-movable.
     */
    class TypeRegistry
    {
    public:
        /** Singleton accessor; returns a reference to the process-wide registry. */
        static TypeRegistry &instance();

        TypeRegistry(const TypeRegistry &) = delete;
        TypeRegistry &operator=(const TypeRegistry &) = delete;
        TypeRegistry(TypeRegistry &&) = delete;
        TypeRegistry &operator=(TypeRegistry &&) = delete;

        /**
         * Register a scalar (atomic) type and return its canonical
         * ``ValueTypeMetaData``. ``name`` is optional; when empty a
         * synthesised name based on ``typeid(T)`` is used. ``extra_flags``
         * are ORed with the flags inferred by ``compute_scalar_flags``.
         */
        template <typename T>
        const ValueTypeMetaData *register_scalar(std::string_view name = {},
                                                 ValueTypeFlags extra_flags = ValueTypeFlags::None);

        /** Look up a value-layer schema by display name; returns null when unknown. */
        [[nodiscard]] const ValueTypeMetaData *value_type(std::string_view name) const;
        /** Look up a time-series schema by display name; returns null when unknown. */
        [[nodiscard]] const TSValueTypeMetaData *time_series_type(std::string_view name) const;

        /** Intern a positional tuple value-schema with the given element types. */
        const ValueTypeMetaData *tuple(const std::vector<const ValueTypeMetaData *> &element_types);
        /** Intern a named bundle value-schema; ``name`` is optional and used for display only. */
        const ValueTypeMetaData *bundle(const std::vector<std::pair<std::string, const ValueTypeMetaData *>> &fields,
                                        std::string_view name = {});
        /**
         * Intern a list value-schema. Pass ``fixed_size > 0`` for a static
         * list. ``variadic_tuple`` flags the metadata as a variadic-tuple
         * placeholder used during wiring.
         */
        const ValueTypeMetaData *list(const ValueTypeMetaData *element_type,
                                      size_t fixed_size = 0,
                                      bool variadic_tuple = false);
        /** Intern a set value-schema for ``element_type``. */
        const ValueTypeMetaData *set(const ValueTypeMetaData *element_type);
        /** Intern a map value-schema with the given key and value types. */
        const ValueTypeMetaData *map(const ValueTypeMetaData *key_type, const ValueTypeMetaData *value_type);
        /** Intern a fixed-capacity ring-buffer value-schema. */
        const ValueTypeMetaData *cyclic_buffer(const ValueTypeMetaData *element_type, size_t capacity);
        /** Intern a queue value-schema; ``max_capacity == 0`` means unbounded. */
        const ValueTypeMetaData *queue(const ValueTypeMetaData *element_type, size_t max_capacity = 0);

        /** Singleton ``SIGNAL`` time-series schema. */
        const TSValueTypeMetaData *signal();
        /** Intern ``TS[T]`` for the supplied value-schema. */
        const TSValueTypeMetaData *ts(const ValueTypeMetaData *value_type);
        /** Intern ``TSS`` (set-of-T) for the supplied element value-schema. */
        const TSValueTypeMetaData *tss(const ValueTypeMetaData *element_type);
        /** Intern ``TSD`` (dict) with the given key value-schema and per-key TS-schema. */
        const TSValueTypeMetaData *tsd(const ValueTypeMetaData *key_type, const TSValueTypeMetaData *value_ts);
        /** Intern ``TSL`` (list-of-TS); pass ``fixed_size > 0`` for a static list. */
        const TSValueTypeMetaData *tsl(const TSValueTypeMetaData *element_ts, size_t fixed_size = 0);
        /** Intern a tick-count ``TSW`` (sliding window). */
        const TSValueTypeMetaData *tsw(const ValueTypeMetaData *value_type, size_t period, size_t min_period = 0);
        /** Intern a duration-based ``TSW`` (sliding window). */
        const TSValueTypeMetaData *tsw_duration(const ValueTypeMetaData *value_type,
                                                engine_time_delta_t time_range,
                                                engine_time_delta_t min_time_range = engine_time_delta_t{0});
        /** Intern a ``TSB`` (named bundle of TS fields); ``name`` is optional. */
        const TSValueTypeMetaData *tsb(const std::vector<std::pair<std::string, const TSValueTypeMetaData *>> &fields,
                                       std::string_view name = {});
        /** Intern a ``REF`` to the supplied time-series. */
        const TSValueTypeMetaData *ref(const TSValueTypeMetaData *referenced_ts);

        /** True when ``meta`` is a ``REF`` or contains a ``REF`` reachable through its structure. */
        [[nodiscard]] static bool contains_ref(const TSValueTypeMetaData *meta);
        /**
         * Return the ``REF``-stripped version of ``meta``. ``REF<T>`` becomes
         * ``T``; container kinds recurse; non-REF metadata returns the same
         * pointer. Results are cached for repeated lookups.
         */
        const TSValueTypeMetaData *dereference(const TSValueTypeMetaData *meta);

    private:
        TypeRegistry() = default;

        const char *store_name(std::string_view name);
        const char *store_name_interned(std::string_view name);
        ValueFieldMetaData *store_value_fields(std::unique_ptr<ValueFieldMetaData[]> fields);
        TSFieldMetaData *store_ts_fields(std::unique_ptr<TSFieldMetaData[]> fields);

        const ValueTypeMetaData *register_scalar_impl(std::type_index type_key,
                                                      std::string_view name,
                                                      ValueTypeFlags flags,
                                                      const MemoryUtils::StoragePlan *canonical_plan);
        const ValueTypeMetaData *synthetic_atomic(std::string_view name,
                                                  ValueTypeFlags flags);

        void register_value_alias(std::string_view name, const ValueTypeMetaData *meta);
        void register_ts_alias(std::string_view name, const TSValueTypeMetaData *meta);

        /**
         * Populate ``meta.value_schema`` and ``meta.delta_value_schema`` for a
         * partially-constructed time-series schema. Called from each TS
         * factory before the metadata is interned, so the resulting interned
         * schema carries the pre-computed pointers as immutable properties.
         */
        void populate_ts_schemas(TSValueTypeMetaData &meta);

        // ----------------------------------------------------------------
        // Cache keys for the identity caches.
        //
        // Each cache uses a typed structural key that captures exactly the
        // meaningful information for that cache's kind. Equivalent inputs
        // produce equal keys, so the InternTables dedupe correctly without
        // any string-encoding step. Single-pointer caches (set, ts, tss,
        // ref) just key on the pointer directly.
        // ----------------------------------------------------------------

        /** boost::hash_combine-style mix; private to the registry's keys. */
        static constexpr size_t combine(size_t seed, size_t value) noexcept
        {
            return seed ^ (value + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U));
        }

        /** Key for the value-side ``tuple`` cache: ordered list of element schemas. */
        struct TupleKey
        {
            std::vector<const ValueTypeMetaData *> elements;
            bool operator==(const TupleKey &) const noexcept = default;
        };
        struct TupleKeyHash
        {
            size_t operator()(const TupleKey &k) const noexcept
            {
                size_t seed = 0;
                for (const auto *e : k.elements)
                {
                    seed = combine(seed, std::hash<const ValueTypeMetaData *>{}(e));
                }
                return seed;
            }
        };

        /** Key for the value-side ``bundle`` cache: ordered ``(name, type)`` fields. */
        struct BundleKey
        {
            struct Field
            {
                std::string name;
                const ValueTypeMetaData *type{nullptr};
                bool operator==(const Field &) const noexcept = default;
            };
            std::vector<Field> fields;
            bool operator==(const BundleKey &) const noexcept = default;
        };
        struct BundleKeyHash
        {
            size_t operator()(const BundleKey &k) const noexcept
            {
                size_t seed = 0;
                for (const auto &f : k.fields)
                {
                    seed = combine(seed, std::hash<std::string>{}(f.name));
                    seed = combine(seed, std::hash<const ValueTypeMetaData *>{}(f.type));
                }
                return seed;
            }
        };

        /** Key for the value-side ``list`` cache: element type + size + variadic-tuple flag. */
        struct ListKey
        {
            const ValueTypeMetaData *element_type{nullptr};
            size_t fixed_size{0};
            bool variadic_tuple{false};
            bool operator==(const ListKey &) const noexcept = default;
        };
        struct ListKeyHash
        {
            size_t operator()(const ListKey &k) const noexcept
            {
                size_t seed = std::hash<const ValueTypeMetaData *>{}(k.element_type);
                seed = combine(seed, std::hash<size_t>{}(k.fixed_size));
                seed = combine(seed, std::hash<bool>{}(k.variadic_tuple));
                return seed;
            }
        };

        /** Key for the value-side ``map`` cache: ``(key_type, value_type)``. */
        struct MapKey
        {
            const ValueTypeMetaData *key_type{nullptr};
            const ValueTypeMetaData *element_type{nullptr};
            bool operator==(const MapKey &) const noexcept = default;
        };
        struct MapKeyHash
        {
            size_t operator()(const MapKey &k) const noexcept
            {
                size_t seed = std::hash<const ValueTypeMetaData *>{}(k.key_type);
                return combine(seed, std::hash<const ValueTypeMetaData *>{}(k.element_type));
            }
        };

        /** Shared key for ``cyclic_buffer`` and ``queue`` caches: ``(element, size)``. */
        struct SizedKey
        {
            const ValueTypeMetaData *element_type{nullptr};
            size_t size{0};
            bool operator==(const SizedKey &) const noexcept = default;
        };
        struct SizedKeyHash
        {
            size_t operator()(const SizedKey &k) const noexcept
            {
                size_t seed = std::hash<const ValueTypeMetaData *>{}(k.element_type);
                return combine(seed, std::hash<size_t>{}(k.size));
            }
        };

        /** Key for the ``tsd`` cache: ``(value-key-type, ts-value-schema)``. */
        struct TSDictKey
        {
            const ValueTypeMetaData *key_type{nullptr};
            const TSValueTypeMetaData *value_ts{nullptr};
            bool operator==(const TSDictKey &) const noexcept = default;
        };
        struct TSDictKeyHash
        {
            size_t operator()(const TSDictKey &k) const noexcept
            {
                size_t seed = std::hash<const ValueTypeMetaData *>{}(k.key_type);
                return combine(seed, std::hash<const TSValueTypeMetaData *>{}(k.value_ts));
            }
        };

        /** Key for the ``tsl`` cache: ``(element-ts-schema, fixed_size)``. */
        struct TSListKey
        {
            const TSValueTypeMetaData *element_ts{nullptr};
            size_t fixed_size{0};
            bool operator==(const TSListKey &) const noexcept = default;
        };
        struct TSListKeyHash
        {
            size_t operator()(const TSListKey &k) const noexcept
            {
                size_t seed = std::hash<const TSValueTypeMetaData *>{}(k.element_ts);
                return combine(seed, std::hash<size_t>{}(k.fixed_size));
            }
        };

        /**
         * Key for the ``tsw`` cache: ``(value-type, is_duration_based, p1, p2)``.
         *
         * ``p1`` and ``p2`` carry either tick-window ``(period, min_period)`` or
         * duration-window ``(time_range, min_time_range)`` counts depending on
         * ``is_duration_based``.
         */
        struct TSWindowKey
        {
            const ValueTypeMetaData *value_type{nullptr};
            bool is_duration_based{false};
            int64_t param1{0};
            int64_t param2{0};
            bool operator==(const TSWindowKey &) const noexcept = default;
        };
        struct TSWindowKeyHash
        {
            size_t operator()(const TSWindowKey &k) const noexcept
            {
                size_t seed = std::hash<const ValueTypeMetaData *>{}(k.value_type);
                seed = combine(seed, std::hash<bool>{}(k.is_duration_based));
                seed = combine(seed, std::hash<int64_t>{}(k.param1));
                seed = combine(seed, std::hash<int64_t>{}(k.param2));
                return seed;
            }
        };

        /** Key for the ``tsb`` cache: ordered ``(name, ts-schema)`` fields. */
        struct TSBundleKey
        {
            struct Field
            {
                std::string name;
                const TSValueTypeMetaData *type{nullptr};
                bool operator==(const Field &) const noexcept = default;
            };
            std::vector<Field> fields;
            bool operator==(const TSBundleKey &) const noexcept = default;
        };
        struct TSBundleKeyHash
        {
            size_t operator()(const TSBundleKey &k) const noexcept
            {
                size_t seed = 0;
                for (const auto &f : k.fields)
                {
                    seed = combine(seed, std::hash<std::string>{}(f.name));
                    seed = combine(seed, std::hash<const TSValueTypeMetaData *>{}(f.type));
                }
                return seed;
            }
        };

        // Auxiliary memory referenced by metadata (display names, field arrays).
        std::vector<std::unique_ptr<std::string>> name_storage_;
        std::vector<std::unique_ptr<ValueFieldMetaData[]>> value_field_storage_;
        std::vector<std::unique_ptr<TSFieldMetaData[]>> ts_field_storage_;

        // Identity caches: thin wrappers over InternTable that own the
        // metadata. Equivalent keys always return the same canonical pointer.
        InternTable<std::type_index, ValueTypeMetaData> scalar_cache_;
        InternTable<std::string, ValueTypeMetaData> synthetic_scalar_cache_;
        InternTable<TupleKey, ValueTypeMetaData, TupleKeyHash> tuple_cache_;
        InternTable<BundleKey, ValueTypeMetaData, BundleKeyHash> bundle_cache_;
        InternTable<ListKey, ValueTypeMetaData, ListKeyHash> list_cache_;
        InternTable<const ValueTypeMetaData *, ValueTypeMetaData> set_cache_;
        InternTable<MapKey, ValueTypeMetaData, MapKeyHash> map_cache_;
        InternTable<SizedKey, ValueTypeMetaData, SizedKeyHash> cyclic_buffer_cache_;
        InternTable<SizedKey, ValueTypeMetaData, SizedKeyHash> queue_cache_;

        InternTable<const ValueTypeMetaData *, TSValueTypeMetaData> ts_cache_;
        InternTable<const ValueTypeMetaData *, TSValueTypeMetaData> tss_cache_;
        InternTable<TSDictKey, TSValueTypeMetaData, TSDictKeyHash> tsd_cache_;
        InternTable<TSListKey, TSValueTypeMetaData, TSListKeyHash> tsl_cache_;
        InternTable<TSWindowKey, TSValueTypeMetaData, TSWindowKeyHash> tsw_cache_;
        InternTable<TSBundleKey, TSValueTypeMetaData, TSBundleKeyHash> tsb_cache_;
        InternTable<const TSValueTypeMetaData *, TSValueTypeMetaData> ref_cache_;

        // Aliasing maps: borrow pointers to canonical metadata stored in the
        // identity caches above; do not own the metadata.
        std::unordered_map<std::string, const ValueTypeMetaData *> value_name_cache_;
        std::unordered_map<std::string, const TSValueTypeMetaData *> ts_name_cache_;

        // Transformation cache (REF dereferencing): values are borrowed
        // pointers to metadata owned by other caches.
        std::unordered_map<const TSValueTypeMetaData *, const TSValueTypeMetaData *> deref_cache_;

        // Singletons that don't fit any of the keyed caches.
        std::unique_ptr<TSValueTypeMetaData> signal_meta_;
        const ValueTypeMetaData *time_series_reference_meta_{nullptr};
    };

    template <typename T>
    const ValueTypeMetaData *TypeRegistry::register_scalar(std::string_view name, ValueTypeFlags extra_flags)
    {
        return register_scalar_impl(
            std::type_index(typeid(T)),
            name,
            compute_scalar_flags<T>() | extra_flags,
            &MemoryUtils::plan_for<T>());
    }
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TYPE_REGISTRY_H
