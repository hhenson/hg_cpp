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

        // Auxiliary memory referenced by metadata (display names, field arrays).
        std::vector<std::unique_ptr<std::string>> name_storage_;
        std::vector<std::unique_ptr<ValueFieldMetaData[]>> value_field_storage_;
        std::vector<std::unique_ptr<TSFieldMetaData[]>> ts_field_storage_;

        // Identity caches: each one is a thin wrapper over InternTable that
        // also owns the metadata. Equivalent keys always return the same
        // canonical pointer.
        InternTable<std::type_index, ValueTypeMetaData> scalar_cache_;
        InternTable<std::string, ValueTypeMetaData> synthetic_scalar_cache_;
        InternTable<std::string, ValueTypeMetaData> tuple_cache_;
        InternTable<std::string, ValueTypeMetaData> bundle_cache_;
        InternTable<std::string, ValueTypeMetaData> list_cache_;
        InternTable<std::string, ValueTypeMetaData> set_cache_;
        InternTable<std::string, ValueTypeMetaData> map_cache_;
        InternTable<std::string, ValueTypeMetaData> cyclic_buffer_cache_;
        InternTable<std::string, ValueTypeMetaData> queue_cache_;

        InternTable<std::string, TSValueTypeMetaData> ts_cache_;
        InternTable<std::string, TSValueTypeMetaData> tss_cache_;
        InternTable<std::string, TSValueTypeMetaData> tsd_cache_;
        InternTable<std::string, TSValueTypeMetaData> tsl_cache_;
        InternTable<std::string, TSValueTypeMetaData> tsw_cache_;
        InternTable<std::string, TSValueTypeMetaData> tsb_cache_;
        InternTable<std::string, TSValueTypeMetaData> ref_cache_;

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
