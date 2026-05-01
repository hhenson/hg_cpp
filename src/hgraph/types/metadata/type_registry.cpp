#include <hgraph/types/metadata/type_registry.h>

#include <hgraph/types/metadata/value_plan_factory.h>

#include <cstdint>
#include <utility>

namespace hgraph
{
    namespace
    {
        constexpr ValueTypeFlags kCompositeSeedFlags = ValueTypeFlags::TriviallyConstructible |
                                                       ValueTypeFlags::TriviallyDestructible |
                                                       ValueTypeFlags::TriviallyCopyable | ValueTypeFlags::Hashable |
                                                       ValueTypeFlags::Equatable | ValueTypeFlags::Comparable |
                                                       ValueTypeFlags::BufferCompatible;

        [[nodiscard]] ValueTypeFlags intersect_with(ValueTypeFlags flags, const ValueTypeMetaData *meta) noexcept
        {
            if (!meta)
            {
                return flags;
            }

            if (!meta->is_trivially_constructible())
            {
                flags = flags & ~ValueTypeFlags::TriviallyConstructible;
            }
            if (!meta->is_trivially_destructible())
            {
                flags = flags & ~ValueTypeFlags::TriviallyDestructible;
            }
            if (!meta->is_trivially_copyable())
            {
                flags = flags & ~ValueTypeFlags::TriviallyCopyable;
            }
            if (!meta->is_hashable())
            {
                flags = flags & ~ValueTypeFlags::Hashable;
            }
            if (!meta->is_equatable())
            {
                flags = flags & ~ValueTypeFlags::Equatable;
            }
            if (!meta->is_comparable())
            {
                flags = flags & ~ValueTypeFlags::Comparable;
            }
            if (!meta->is_buffer_compatible())
            {
                flags = flags & ~ValueTypeFlags::BufferCompatible;
            }

            return flags;
        }

        [[nodiscard]] ValueTypeFlags compute_composite_flags(const std::vector<const ValueTypeMetaData *> &element_types)
        {
            ValueTypeFlags flags = kCompositeSeedFlags;
            for (const ValueTypeMetaData *meta : element_types)
            {
                flags = intersect_with(flags, meta);
            }
            return flags;
        }

        [[nodiscard]] ValueTypeFlags
        compute_bundle_flags(const std::vector<std::pair<std::string, const ValueTypeMetaData *>> &fields)
        {
            ValueTypeFlags flags = kCompositeSeedFlags;
            for (const auto &[_, type] : fields)
            {
                flags = intersect_with(flags, type);
            }
            return flags;
        }

        [[nodiscard]] ValueTypeFlags list_flags(const ValueTypeMetaData *element_type,
                                                size_t fixed_size,
                                                bool variadic_tuple) noexcept
        {
            ValueTypeFlags flags = variadic_tuple ? ValueTypeFlags::VariadicTuple : ValueTypeFlags::None;
            if (!element_type)
            {
                return flags;
            }

            if (fixed_size > 0)
            {
                if (element_type->is_trivially_constructible())
                {
                    flags |= ValueTypeFlags::TriviallyConstructible;
                }
                if (element_type->is_trivially_destructible())
                {
                    flags |= ValueTypeFlags::TriviallyDestructible;
                }
                if (element_type->is_trivially_copyable())
                {
                    flags |= ValueTypeFlags::TriviallyCopyable;
                }
                if (element_type->is_buffer_compatible())
                {
                    flags |= ValueTypeFlags::BufferCompatible;
                }
            }
            if (element_type->is_hashable())
            {
                flags |= ValueTypeFlags::Hashable;
            }
            if (element_type->is_equatable())
            {
                flags |= ValueTypeFlags::Equatable;
            }
            if (element_type->is_comparable())
            {
                flags |= ValueTypeFlags::Comparable;
            }

            return flags;
        }

        [[nodiscard]] ValueTypeFlags set_flags(const ValueTypeMetaData *element_type) noexcept
        {
            if (!element_type)
            {
                return ValueTypeFlags::None;
            }

            ValueTypeFlags flags = ValueTypeFlags::None;
            if (element_type->is_hashable() && element_type->is_equatable())
            {
                flags |= ValueTypeFlags::Hashable | ValueTypeFlags::Equatable;
            }
            return flags;
        }

        [[nodiscard]] ValueTypeFlags map_flags(const ValueTypeMetaData *key_type, const ValueTypeMetaData *value_type) noexcept
        {
            ValueTypeFlags flags = ValueTypeFlags::None;
            if (key_type && value_type && key_type->is_hashable() && key_type->is_equatable())
            {
                flags |= ValueTypeFlags::Equatable;
                if (value_type->is_hashable() && value_type->is_equatable())
                {
                    flags |= ValueTypeFlags::Hashable;
                }
            }
            return flags;
        }
    }  // namespace

    TypeRegistry &TypeRegistry::instance()
    {
        static TypeRegistry registry;
        return registry;
    }

    const ValueTypeMetaData *TypeRegistry::value_type(std::string_view name) const
    {
        const auto it = value_name_cache_.find(std::string(name));
        return it == value_name_cache_.end() ? nullptr : it->second;
    }

    const TSValueTypeMetaData *TypeRegistry::time_series_type(std::string_view name) const
    {
        const auto it = ts_name_cache_.find(std::string(name));
        return it == ts_name_cache_.end() ? nullptr : it->second;
    }

    const char *TypeRegistry::store_name(std::string_view name)
    {
        auto stored = std::make_unique<std::string>(name);
        const char *ptr = stored->c_str();
        name_storage_.push_back(std::move(stored));
        return ptr;
    }

    const char *TypeRegistry::store_name_interned(std::string_view name)
    {
        if (name.empty())
        {
            return nullptr;
        }

        for (const auto &entry : name_storage_)
        {
            if (*entry == name)
            {
                return entry->c_str();
            }
        }
        return store_name(name);
    }

    ValueFieldMetaData *TypeRegistry::store_value_fields(std::unique_ptr<ValueFieldMetaData[]> fields)
    {
        ValueFieldMetaData *ptr = fields.get();
        value_field_storage_.push_back(std::move(fields));
        return ptr;
    }

    TSFieldMetaData *TypeRegistry::store_ts_fields(std::unique_ptr<TSFieldMetaData[]> fields)
    {
        TSFieldMetaData *ptr = fields.get();
        ts_field_storage_.push_back(std::move(fields));
        return ptr;
    }

    void TypeRegistry::register_value_alias(std::string_view name, const ValueTypeMetaData *meta)
    {
        if (name.empty() || !meta)
        {
            return;
        }

        value_name_cache_[std::string(name)] = meta;
        if (!meta->display_name)
        {
            const_cast<ValueTypeMetaData *>(meta)->display_name = store_name_interned(name);
        }
    }

    void TypeRegistry::register_ts_alias(std::string_view name, const TSValueTypeMetaData *meta)
    {
        if (name.empty() || !meta)
        {
            return;
        }

        ts_name_cache_[std::string(name)] = meta;
        if (!meta->display_name)
        {
            const_cast<TSValueTypeMetaData *>(meta)->display_name = store_name_interned(name);
        }
    }

    const ValueTypeMetaData *TypeRegistry::register_scalar_impl(std::type_index type_key,
                                                                std::string_view name,
                                                                ValueTypeFlags flags,
                                                                const MemoryUtils::StoragePlan *canonical_plan)
    {
        const ValueTypeMetaData &meta = scalar_cache_.intern(type_key, [&]() {
            return ValueTypeMetaData(ValueTypeKind::Atomic, flags, store_name_interned(name));
        });
        register_value_alias(name, &meta);
        ValuePlanFactory::instance().register_atomic(&meta, canonical_plan);
        return &meta;
    }

    const ValueTypeMetaData *TypeRegistry::synthetic_atomic(std::string_view name, ValueTypeFlags flags)
    {
        const ValueTypeMetaData &meta = synthetic_scalar_cache_.intern(std::string(name), [&]() {
            return ValueTypeMetaData(ValueTypeKind::Atomic, flags, store_name_interned(name));
        });
        register_value_alias(name, &meta);
        return &meta;
    }

    const ValueTypeMetaData *TypeRegistry::tuple(const std::vector<const ValueTypeMetaData *> &element_types)
    {
        TupleKey key{element_types};
        const ValueTypeMetaData &meta = tuple_cache_.intern(std::move(key), [&]() {
            std::unique_ptr<ValueFieldMetaData[]> fields =
                element_types.empty() ? nullptr : std::make_unique<ValueFieldMetaData[]>(element_types.size());
            for (size_t index = 0; index < element_types.size(); ++index)
            {
                fields[index].name = nullptr;
                fields[index].index = index;
                fields[index].type = element_types[index];
            }
            ValueFieldMetaData *fields_ptr = fields ? store_value_fields(std::move(fields)) : nullptr;

            ValueTypeMetaData m(ValueTypeKind::Tuple, compute_composite_flags(element_types));
            m.fields = fields_ptr;
            m.field_count = element_types.size();
            return m;
        });
        return &meta;
    }

    const ValueTypeMetaData *
    TypeRegistry::bundle(const std::vector<std::pair<std::string, const ValueTypeMetaData *>> &fields, std::string_view name)
    {
        BundleKey key;
        key.fields.reserve(fields.size());
        for (const auto &[field_name, field_type] : fields)
        {
            key.fields.push_back(BundleKey::Field{field_name, field_type});
        }
        const ValueTypeMetaData &meta = bundle_cache_.intern(std::move(key), [&]() {
            std::unique_ptr<ValueFieldMetaData[]> stored_fields =
                fields.empty() ? nullptr : std::make_unique<ValueFieldMetaData[]>(fields.size());
            for (size_t index = 0; index < fields.size(); ++index)
            {
                const auto &[field_name, field_type] = fields[index];
                stored_fields[index].name = store_name_interned(field_name);
                stored_fields[index].index = index;
                stored_fields[index].type = field_type;
            }
            ValueFieldMetaData *fields_ptr = stored_fields ? store_value_fields(std::move(stored_fields)) : nullptr;

            ValueTypeMetaData m(ValueTypeKind::Bundle, compute_bundle_flags(fields), store_name_interned(name));
            m.fields = fields_ptr;
            m.field_count = fields.size();
            return m;
        });
        register_value_alias(name, &meta);
        return &meta;
    }

    const ValueTypeMetaData *
    TypeRegistry::list(const ValueTypeMetaData *element_type, size_t fixed_size, bool variadic_tuple)
    {
        const ListKey key{element_type, fixed_size, variadic_tuple};
        const ValueTypeMetaData &meta = list_cache_.intern(key, [&]() {
            ValueTypeMetaData m(ValueTypeKind::List, list_flags(element_type, fixed_size, variadic_tuple));
            m.element_type = element_type;
            m.fixed_size = fixed_size;
            return m;
        });
        return &meta;
    }

    const ValueTypeMetaData *TypeRegistry::set(const ValueTypeMetaData *element_type)
    {
        const ValueTypeMetaData &meta = set_cache_.intern(element_type, [&]() {
            ValueTypeMetaData m(ValueTypeKind::Set, set_flags(element_type));
            m.element_type = element_type;
            return m;
        });
        return &meta;
    }

    const ValueTypeMetaData *TypeRegistry::map(const ValueTypeMetaData *key_type, const ValueTypeMetaData *value_type)
    {
        const MapKey key{key_type, value_type};
        const ValueTypeMetaData &meta = map_cache_.intern(key, [&]() {
            ValueTypeMetaData m(ValueTypeKind::Map, map_flags(key_type, value_type));
            m.key_type = key_type;
            m.element_type = value_type;
            return m;
        });
        return &meta;
    }

    const ValueTypeMetaData *TypeRegistry::cyclic_buffer(const ValueTypeMetaData *element_type, size_t capacity)
    {
        const SizedKey key{element_type, capacity};
        const ValueTypeMetaData &meta = cyclic_buffer_cache_.intern(key, [&]() {
            ValueTypeMetaData m(ValueTypeKind::CyclicBuffer, ValueTypeFlags::None);
            m.element_type = element_type;
            m.fixed_size = capacity;
            return m;
        });
        return &meta;
    }

    const ValueTypeMetaData *TypeRegistry::queue(const ValueTypeMetaData *element_type, size_t max_capacity)
    {
        const SizedKey key{element_type, max_capacity};
        const ValueTypeMetaData &meta = queue_cache_.intern(key, [&]() {
            ValueTypeMetaData m(ValueTypeKind::Queue, ValueTypeFlags::None);
            m.element_type = element_type;
            m.fixed_size = max_capacity;
            return m;
        });
        return &meta;
    }

    const TSValueTypeMetaData *TypeRegistry::signal()
    {
        if (!signal_meta_)
        {
            signal_meta_ = std::make_unique<TSValueTypeMetaData>(
                TSTypeKind::SIGNAL, register_scalar<bool>("bool"), store_name_interned("SIGNAL"));
            register_ts_alias("SIGNAL", signal_meta_.get());
        }
        return signal_meta_.get();
    }

    const TSValueTypeMetaData *TypeRegistry::ts(const ValueTypeMetaData *value_type)
    {
        const TSValueTypeMetaData &meta = ts_cache_.intern(value_type, [&]() {
            return TSValueTypeMetaData(TSTypeKind::TS, value_type);
        });
        return &meta;
    }

    const TSValueTypeMetaData *TypeRegistry::tss(const ValueTypeMetaData *element_type)
    {
        const TSValueTypeMetaData &meta = tss_cache_.intern(element_type, [&]() {
            return TSValueTypeMetaData(TSTypeKind::TSS, element_type ? set(element_type) : nullptr);
        });
        return &meta;
    }

    const TSValueTypeMetaData *TypeRegistry::tsd(const ValueTypeMetaData *key_type, const TSValueTypeMetaData *value_ts)
    {
        const TSDictKey key{key_type, value_ts};
        const TSValueTypeMetaData &meta = tsd_cache_.intern(key, [&]() {
            TSValueTypeMetaData m(
                TSTypeKind::TSD, key_type && value_ts ? map(key_type, value_ts->value_type) : nullptr);
            m.set_tsd(key_type, value_ts);
            return m;
        });
        return &meta;
    }

    const TSValueTypeMetaData *TypeRegistry::tsl(const TSValueTypeMetaData *element_ts, size_t fixed_size)
    {
        const TSListKey key{element_ts, fixed_size};
        const TSValueTypeMetaData &meta = tsl_cache_.intern(key, [&]() {
            TSValueTypeMetaData m(
                TSTypeKind::TSL,
                element_ts && element_ts->value_type ? list(element_ts->value_type, fixed_size) : nullptr);
            m.set_tsl(element_ts, fixed_size);
            return m;
        });
        return &meta;
    }

    const TSValueTypeMetaData *TypeRegistry::tsw(const ValueTypeMetaData *value_type, size_t period, size_t min_period)
    {
        const TSWindowKey key{value_type, false, static_cast<int64_t>(period), static_cast<int64_t>(min_period)};
        const TSValueTypeMetaData &meta = tsw_cache_.intern(key, [&]() {
            TSValueTypeMetaData m(TSTypeKind::TSW, value_type);
            m.set_tsw_tick(period, min_period);
            return m;
        });
        return &meta;
    }

    const TSValueTypeMetaData *TypeRegistry::tsw_duration(const ValueTypeMetaData *value_type,
                                                          engine_time_delta_t time_range,
                                                          engine_time_delta_t min_time_range)
    {
        const TSWindowKey key{value_type, true, time_range.count(), min_time_range.count()};
        const TSValueTypeMetaData &meta = tsw_cache_.intern(key, [&]() {
            TSValueTypeMetaData m(TSTypeKind::TSW, value_type);
            m.set_tsw_duration(time_range, min_time_range);
            return m;
        });
        return &meta;
    }

    const TSValueTypeMetaData *
    TypeRegistry::tsb(const std::vector<std::pair<std::string, const TSValueTypeMetaData *>> &fields, std::string_view name)
    {
        TSBundleKey ts_key;
        ts_key.fields.reserve(fields.size());
        for (const auto &[field_name, field_type] : fields)
        {
            ts_key.fields.push_back(TSBundleKey::Field{field_name, field_type});
        }
        const TSValueTypeMetaData &meta = tsb_cache_.intern(std::move(ts_key), [&]() {
            std::vector<std::pair<std::string, const ValueTypeMetaData *>> value_fields;
            value_fields.reserve(fields.size());
            for (const auto &[field_name, ts_type] : fields)
            {
                value_fields.emplace_back(field_name, ts_type ? ts_type->value_type : nullptr);
            }

            std::unique_ptr<TSFieldMetaData[]> stored_fields =
                fields.empty() ? nullptr : std::make_unique<TSFieldMetaData[]>(fields.size());
            for (size_t index = 0; index < fields.size(); ++index)
            {
                stored_fields[index].name = store_name_interned(fields[index].first);
                stored_fields[index].index = index;
                stored_fields[index].type = fields[index].second;
            }
            TSFieldMetaData *fields_ptr = stored_fields ? store_ts_fields(std::move(stored_fields)) : nullptr;

            TSValueTypeMetaData m(
                TSTypeKind::TSB, bundle(value_fields, name), store_name_interned(name));
            m.set_tsb(fields_ptr, fields.size(), store_name_interned(name));
            return m;
        });
        register_ts_alias(name, &meta);
        return &meta;
    }

    const TSValueTypeMetaData *TypeRegistry::ref(const TSValueTypeMetaData *referenced_ts)
    {
        const TSValueTypeMetaData &meta = ref_cache_.intern(referenced_ts, [&]() {
            if (!time_series_reference_meta_)
            {
                time_series_reference_meta_ =
                    synthetic_atomic("TimeSeriesReference", ValueTypeFlags::Hashable | ValueTypeFlags::Equatable);
            }
            TSValueTypeMetaData m(TSTypeKind::REF, time_series_reference_meta_);
            m.set_ref(referenced_ts);
            return m;
        });
        return &meta;
    }

    bool TypeRegistry::contains_ref(const TSValueTypeMetaData *meta)
    {
        if (!meta)
        {
            return false;
        }

        switch (meta->kind)
        {
            case TSTypeKind::REF: return true;
            case TSTypeKind::TSB:
                for (size_t index = 0; index < meta->field_count(); ++index)
                {
                    if (contains_ref(meta->fields()[index].type))
                    {
                        return true;
                    }
                }
                return false;
            case TSTypeKind::TSD:
            case TSTypeKind::TSL: return contains_ref(meta->element_ts());
            default: return false;
        }
    }

    const TSValueTypeMetaData *TypeRegistry::dereference(const TSValueTypeMetaData *meta)
    {
        if (!meta)
        {
            return nullptr;
        }

        if (const auto it = deref_cache_.find(meta); it != deref_cache_.end())
        {
            return it->second;
        }

        const TSValueTypeMetaData *result = meta;
        switch (meta->kind)
        {
            case TSTypeKind::REF:
                result = dereference(meta->referenced_ts());
                break;

            case TSTypeKind::TSB:
            {
                if (!contains_ref(meta))
                {
                    result = meta;
                    break;
                }

                std::vector<std::pair<std::string, const TSValueTypeMetaData *>> deref_fields;
                deref_fields.reserve(meta->field_count());
                for (size_t index = 0; index < meta->field_count(); ++index)
                {
                    deref_fields.emplace_back(meta->fields()[index].name, dereference(meta->fields()[index].type));
                }

                std::string deref_name = meta->bundle_name() ? std::string(meta->bundle_name()) : std::string{};
                if (!deref_name.empty())
                {
                    deref_name += "_deref";
                }
                result = tsb(deref_fields, deref_name);
                break;
            }

            case TSTypeKind::TSL:
            {
                const TSValueTypeMetaData *element = dereference(meta->element_ts());
                result = element == meta->element_ts() ? meta : tsl(element, meta->fixed_size());
                break;
            }

            case TSTypeKind::TSD:
            {
                const TSValueTypeMetaData *value_ts = dereference(meta->element_ts());
                result = value_ts == meta->element_ts() ? meta : tsd(meta->key_type(), value_ts);
                break;
            }

            default:
                result = meta;
                break;
        }

        deref_cache_[meta] = result;
        return result;
    }
}  // namespace hgraph
