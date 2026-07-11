#include <hgraph/types/metadata/type_registry.h>

#include <hgraph/lib/std/standard_types.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/time_series_reference.h>

#include <cstdint>
#include <mutex>
#include <stdexcept>
#include <unordered_map>
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

        [[nodiscard]] ValueTypeFlags ordered_container_flags(const ValueTypeMetaData *element_type) noexcept
        {
            if (!element_type)
            {
                return ValueTypeFlags::None;
            }

            ValueTypeFlags flags = ValueTypeFlags::None;
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
            if (element_type->is_buffer_compatible())
            {
                flags |= ValueTypeFlags::BufferCompatible;
            }
            return flags;
        }

        [[nodiscard]] std::string value_label(const ValueTypeMetaData *meta)
        {
            return meta == nullptr || meta->name().empty() ? std::string{"<unresolved>"} : std::string{meta->name()};
        }

        [[nodiscard]] std::string unary_label(std::string_view family, const ValueTypeMetaData *element_type)
        {
            std::string label{family};
            label.push_back('[');
            label.append(value_label(element_type));
            label.push_back(']');
            return label;
        }

        [[nodiscard]] std::string sized_label(std::string_view family,
                                              const ValueTypeMetaData *element_type,
                                              size_t size)
        {
            std::string label{family};
            label.push_back('[');
            label.append(value_label(element_type));
            if (size != 0)
            {
                label.push_back(',');
                label.append(std::to_string(size));
            }
            label.push_back(']');
            return label;
        }

        [[nodiscard]] std::string map_label(std::string_view family,
                                            const ValueTypeMetaData *key_type,
                                            const ValueTypeMetaData *value_type)
        {
            std::string label{family};
            label.push_back('[');
            label.append(value_label(key_type));
            label.push_back(',');
            label.append(value_label(value_type));
            label.push_back(']');
            return label;
        }
    }  // namespace

    TypeRegistry &TypeRegistry::instance()
    {
        // Immortal (see OperatorRegistry::instance): metas must outlive every
        // consumer, including static-teardown Value destructors.
        static TypeRegistry *registry_ptr = new TypeRegistry();
        static TypeRegistry &registry     = *registry_ptr;
        static const bool    seeded       = [] {
            (void)stdlib::register_standard_types(registry);
            return true;
        }();
        static_cast<void>(seeded);
        return registry;
    }

    const ValueTypeMetaData *TypeRegistry::value_type(std::string_view name) const
    {
        const std::lock_guard lock(mutex_);
        const auto it = value_name_cache_.find(std::string(name));
        return it == value_name_cache_.end() ? nullptr : it->second;
    }

    const TSValueTypeMetaData *TypeRegistry::time_series_type(std::string_view name) const
    {
        const std::lock_guard lock(mutex_);
        const auto it = ts_name_cache_.find(std::string(name));
        return it == ts_name_cache_.end() ? nullptr : it->second;
    }

    void TypeRegistry::register_value_type_alias(std::string_view name, const ValueTypeMetaData *meta)
    {
        const std::lock_guard lock(mutex_);
        register_value_alias(name, meta);
    }

    void TypeRegistry::register_time_series_type_alias(std::string_view name, const TSValueTypeMetaData *meta)
    {
        const std::lock_guard lock(mutex_);
        register_ts_alias(name, meta);
    }

    const ValueTypeMetaData *TypeRegistry::named_bundle(std::string_view name) const
    {
        const std::lock_guard lock(mutex_);
        const ValueTypeMetaData *meta = value_type(name);
        return (meta != nullptr && meta->is_named_bundle()) ? meta : nullptr;
    }

    namespace
    {
        /** Per-enum ValueOps: Int ops with member-aware rendering and the
            python conversion hooks; ``context`` = the enum meta. Cleared on
            registry reset (metas are re-interned, pointers reuse). */
        std::mutex &enum_ops_mutex()
        {
            static std::mutex m;
            return m;
        }

        std::unordered_map<const ValueTypeMetaData *, ValueOps> &enum_ops_store()
        {
            static auto *store = new std::unordered_map<const ValueTypeMetaData *, ValueOps>{};
            return *store;
        }

        std::string enum_to_string(const void *context, const void *memory)
        {
            const auto *meta  = static_cast<const ValueTypeMetaData *>(context);
            const auto  value = *static_cast<const Int *>(memory);
            for (size_t index = 0; index < meta->field_count; ++index)
            {
                if (meta->fields[index].enum_value == value) { return std::string{meta->fields[index].name}; }
            }
            return std::to_string(value);
        }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        nanobind::object enum_to_python(const void *context, const void *memory)
        {
            const auto *meta = static_cast<const ValueTypeMetaData *>(context);
            if (enum_to_python_slot() == nullptr)
            {
                throw std::logic_error("enum python conversion requires the python bridge");
            }
            return enum_to_python_slot()(meta, *static_cast<const Int *>(memory));
        }

        void enum_from_python(const void *context, const ValueTypeRef &, void *memory, nanobind::handle source)
        {
            const auto *meta = static_cast<const ValueTypeMetaData *>(context);
            if (enum_from_python_slot() == nullptr)
            {
                throw std::logic_error("enum python conversion requires the python bridge");
            }
            *static_cast<Int *>(memory) = enum_from_python_slot()(meta, source);
        }
#endif

        const ValueOps &enum_ops_for(const ValueTypeMetaData *meta)
        {
            std::lock_guard<std::mutex> lock(enum_ops_mutex());
            auto [it, fresh] = enum_ops_store().try_emplace(meta);
            if (fresh)
            {
                ValueOps ops       = ops_for<Int>();
                ops.context        = meta;
                ops.to_string_impl = &enum_to_string;
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                ops.to_python_impl        = &enum_to_python;
                ops.from_python_impl      = &enum_from_python;
                ops.to_python_buffer_impl = nullptr;
#endif
                it->second = ops;
            }
            return it->second;
        }

        void clear_enum_ops() noexcept
        {
            std::lock_guard<std::mutex> lock(enum_ops_mutex());
            enum_ops_store().clear();
        }
    }  // namespace

#if HGRAPH_ENABLE_PYTHON_USER_NODES
    EnumToPythonFn &enum_to_python_slot() noexcept
    {
        static EnumToPythonFn slot = nullptr;
        return slot;
    }

    EnumFromPythonFn &enum_from_python_slot() noexcept
    {
        static EnumFromPythonFn slot = nullptr;
        return slot;
    }
#endif

    const ValueTypeMetaData *TypeRegistry::named_enum(std::string_view name) const
    {
        const std::lock_guard lock(mutex_);
        const ValueTypeMetaData *meta = value_type(name);
        return (meta != nullptr && meta->is_enum()) ? meta : nullptr;
    }

    const ValueTypeMetaData *
    TypeRegistry::enum_type(std::string_view name, const std::vector<std::pair<std::string, long long>> &members)
    {
        const std::lock_guard lock(mutex_);
        if (name.empty()) { throw std::invalid_argument("enum_type requires a non-empty name"); }
        if (members.empty()) { throw std::invalid_argument("enum_type requires at least one member"); }

        if (const ValueTypeMetaData *existing = named_enum(name); existing != nullptr)
        {
            bool same = existing->field_count == members.size();
            for (size_t index = 0; same && index < members.size(); ++index)
            {
                same = members[index].first == existing->fields[index].name &&
                       members[index].second == existing->fields[index].enum_value;
            }
            if (!same)
            {
                throw std::invalid_argument(std::string("enum '") + std::string(name) +
                                            "' is already registered with a different member table");
            }
            return existing;
        }

        const ValueTypeMetaData &meta = named_enum_cache_.intern(std::string{name}, [&]() {
            auto stored = std::make_unique<ValueFieldMetaData[]>(members.size());
            for (size_t index = 0; index < members.size(); ++index)
            {
                stored[index].name       = store_name_interned(members[index].first);
                stored[index].index      = index;
                stored[index].enum_value = members[index].second;
                stored[index].type       = nullptr;
            }
            ValueFieldMetaData *fields_ptr = store_value_fields(std::move(stored));

            ValueTypeMetaData m(ValueTypeKind::Atomic,
                                ValueTypeFlags::Enum | ValueTypeFlags::Hashable | ValueTypeFlags::Equatable |
                                    ValueTypeFlags::Comparable | ValueTypeFlags::TriviallyConstructible |
                                    ValueTypeFlags::TriviallyDestructible | ValueTypeFlags::TriviallyCopyable,
                                store_name_interned(name));
            m.fields      = fields_ptr;
            m.field_count = members.size();
            return m;
        });
        register_value_alias(name, &meta);

        // Pair with the Int plan + this enum's ops so Value/TS machinery
        // resolves uniformly (the register_scalar pattern for a nominal type).
        const auto &plan = MemoryUtils::plan_for<Int>();
        ValuePlanFactory::instance().register_atomic(&meta, &plan, &enum_ops_for(&meta));
        return &meta;
    }

    const TSValueTypeMetaData *TypeRegistry::named_tsb(std::string_view name) const
    {
        const std::lock_guard lock(mutex_);
        const TSValueTypeMetaData *meta = time_series_type(name);
        return (meta != nullptr && meta->is_named_tsb()) ? meta : nullptr;
    }

    void TypeRegistry::reset() noexcept
    {
        const std::lock_guard lock(mutex_);
        // Drop every interned identity cache.
        scalar_cache_.clear();
        synthetic_scalar_cache_.clear();
        tuple_cache_.clear();
        bundle_cache_.clear();
        named_bundle_cache_.clear();
        named_enum_cache_.clear();
        clear_enum_ops();
        list_cache_.clear();
        set_cache_.clear();
        mutable_list_cache_.clear();
        nullable_tuple_cache_.clear();
        series_cache_.clear();
        frame_cache_.clear();
        mutable_set_cache_.clear();
        map_cache_.clear();
        mutable_map_cache_.clear();
        cyclic_buffer_cache_.clear();
        queue_cache_.clear();
        any_cache_.clear();
        ts_cache_.clear();
        tss_cache_.clear();
        tsd_cache_.clear();
        tsl_cache_.clear();
        tsw_cache_.clear();
        tsb_cache_.clear();
        named_tsb_cache_.clear();
        ref_cache_.clear();

        // Drop the alias maps and the dereference cache.
        value_name_cache_.clear();
        ts_name_cache_.clear();
        deref_cache_.clear();

        // Drop auxiliary storage referenced by metadata.
        name_storage_.clear();
        value_field_storage_.clear();
        ts_field_storage_.clear();

        // Reset singletons that don't fit any of the keyed caches.
        signal_meta_.reset();
        time_series_reference_meta_ = nullptr;

        // Restore the standard scalar / TS / TSS vocabulary so the registry is always left
        // in its default-seeded state — the documented "automatically seeded" contract, and
        // the same seed applied on first construction (see ``instance()``).
        // ``register_standard_types`` is idempotent; like the construction seed, an
        // allocation failure here is unrecoverable.
        (void)stdlib::register_standard_types(*this);
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

        std::string key{name};
        if (const auto it = value_name_cache_.find(key); it != value_name_cache_.end())
        {
            if (it->second != meta)
            {
                throw std::invalid_argument("value type alias '" + key + "' is already registered for a different schema");
            }
            return;
        }

        value_name_cache_.emplace(std::move(key), meta);
    }

    void TypeRegistry::register_ts_alias(std::string_view name, const TSValueTypeMetaData *meta)
    {
        if (name.empty() || !meta)
        {
            return;
        }

        std::string key{name};
        if (const auto it = ts_name_cache_.find(key); it != ts_name_cache_.end())
        {
            if (it->second != meta)
            {
                throw std::invalid_argument("time-series type alias '" + key +
                                            "' is already registered for a different schema");
            }
            return;
        }

        ts_name_cache_.emplace(std::move(key), meta);
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
        (void)canonical_plan;
        const ValueTypeMetaData &meta = scalar_cache_.intern(type_key, [&]() {
            return ValueTypeMetaData(ValueTypeKind::Atomic, flags, store_name_interned(name));
        });
        register_value_alias(name, &meta);
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
        const std::lock_guard lock(mutex_);
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

            std::string label{"Tuple["};
            for (size_t index = 0; index < element_types.size(); ++index)
            {
                if (index != 0) { label.push_back(','); }
                label.append(value_label(element_types[index]));
            }
            label.push_back(']');
            ValueTypeMetaData m(ValueTypeKind::Tuple,
                                compute_composite_flags(element_types),
                                store_name_interned(label));
            m.fields = fields_ptr;
            m.field_count = element_types.size();
            return m;
        });
        return &meta;
    }

    const ValueTypeMetaData *
    TypeRegistry::un_named_bundle(const std::vector<std::pair<std::string, const ValueTypeMetaData *>> &fields)
    {
        const std::lock_guard lock(mutex_);
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

            std::string label{"Bundle{"};
            for (size_t index = 0; index < fields.size(); ++index)
            {
                if (index != 0) { label.push_back(','); }
                label.append(fields[index].first);
                label.push_back(':');
                label.append(value_label(fields[index].second));
            }
            label.push_back('}');
            ValueTypeMetaData m(ValueTypeKind::Bundle,
                                compute_bundle_flags(fields),
                                store_name_interned(label));
            m.fields = fields_ptr;
            m.field_count = fields.size();
            return m;
        });
        return &meta;
    }

    const ValueTypeMetaData *
    TypeRegistry::bundle(std::string_view name,
                         const std::vector<std::pair<std::string, const ValueTypeMetaData *>> &fields)
    {
        const std::lock_guard lock(mutex_);
        if (name.empty())
        {
            throw std::invalid_argument("bundle requires a non-empty name; use un_named_bundle for the structural form");
        }
        const ValueTypeMetaData *un_named = un_named_bundle(fields);

        // Bundle name namespace must be unique: a name already in use by a
        // named bundle with a different field list is a conflict, not a
        // re-registration. Same name + same fields is idempotent and falls
        // through to the regular intern path below.
        if (const ValueTypeMetaData *existing = named_bundle(name);
            existing != nullptr && existing->wrapped_un_named != un_named)
        {
            throw std::invalid_argument(
                std::string("named bundle '") + std::string(name) +
                "' is already registered with a different schema");
        }

        NamedBundleKey key{std::string{name}, un_named};
        const ValueTypeMetaData &meta = named_bundle_cache_.intern(std::move(key), [&]() {
            // Named bundle wraps the un-named: shares the same field array
            // (no duplication), records its own name, and sets
            // wrapped_un_named so consumers can navigate to the structural
            // twin.
            ValueTypeMetaData m(ValueTypeKind::Bundle, un_named->flags, store_name_interned(name));
            m.fields = un_named->fields;
            m.field_count = un_named->field_count;
            m.wrapped_un_named = un_named;
            return m;
        });
        register_value_alias(name, &meta);
        return &meta;
    }

    const ValueTypeMetaData *
    TypeRegistry::list(const ValueTypeMetaData *element_type, size_t fixed_size, bool variadic_tuple)
    {
        const std::lock_guard lock(mutex_);
        const ListKey key{element_type, fixed_size, variadic_tuple};
        const ValueTypeMetaData &meta = list_cache_.intern(key, [&]() {
            const std::string label = variadic_tuple ? unary_label("VariadicTuple", element_type)
                                                     : sized_label("List", element_type, fixed_size);
            ValueTypeMetaData m(ValueTypeKind::List,
                                list_flags(element_type, fixed_size, variadic_tuple),
                                store_name_interned(label));
            m.element_type = element_type;
            m.fixed_size = fixed_size;
            return m;
        });
        return &meta;
    }

    const ValueTypeMetaData *TypeRegistry::mutable_list(const ValueTypeMetaData *element_type)
    {
        const std::lock_guard lock(mutex_);
        const ValueTypeMetaData &meta = mutable_list_cache_.intern(element_type, [&]() {
            ValueTypeMetaData m(ValueTypeKind::List,
                                list_flags(element_type, /*fixed_size=*/0, /*variadic_tuple=*/false) |
                                    ValueTypeFlags::Mutable,
                                store_name_interned(unary_label("MutableList", element_type)));
            m.element_type = element_type;
            return m;
        });
        return &meta;
    }

    const ValueTypeMetaData *TypeRegistry::series(const ValueTypeMetaData *element_type)
    {
        const std::lock_guard lock(mutex_);
        const ValueTypeMetaData *base = value_type("series");
        if (base == nullptr) { throw std::logic_error("series scalar is not registered"); }
        if (element_type == nullptr) { return base; }

        const ValueTypeMetaData &meta = series_cache_.intern(element_type, [&]() {
            ValueTypeMetaData m(ValueTypeKind::Atomic,
                                base->flags,
                                store_name_interned(unary_label(base->name(), element_type)));
            m.element_type = element_type;
            return m;
        });
        // Share the base series storage plan + ops (arrow conversion is
        // element-agnostic). Re-registered every call so it self-heals across
        // a registry reset (the meta is interned once; the plan/binding caches
        // clear on reset).
        const auto base_type = ValuePlanFactory::instance().type_for(base);
        ValuePlanFactory::instance().register_atomic(&meta, base_type.plan(), base_type.ops());
        return &meta;
    }

    bool TypeRegistry::is_series(const ValueTypeMetaData *meta) const
    {
        const std::lock_guard lock(mutex_);
        if (meta == nullptr) { return false; }
        const auto base = value_name_cache_.find("series");
        if (base != value_name_cache_.end() && meta == base->second) { return true; }
        return meta->element_type != nullptr && series_cache_.find(meta->element_type) == meta;
    }

    const ValueTypeMetaData *TypeRegistry::frame(const ValueTypeMetaData *column_schema)
    {
        const std::lock_guard lock(mutex_);
        const ValueTypeMetaData *base = value_type("frame");
        if (base == nullptr) { throw std::logic_error("frame scalar is not registered"); }
        if (column_schema == nullptr) { return base; }

        const ValueTypeMetaData &meta = frame_cache_.intern(column_schema, [&]() {
            ValueTypeMetaData m(ValueTypeKind::Atomic,
                                base->flags,
                                store_name_interned(unary_label(base->name(), column_schema)));
            m.element_type = column_schema;
            return m;
        });
        // Share the base frame storage plan + ops (arrow conversion is
        // schema-agnostic); re-registered every call so it self-heals across
        // a registry reset (the series() pattern).
        const auto base_type = ValuePlanFactory::instance().type_for(base);
        ValuePlanFactory::instance().register_atomic(&meta, base_type.plan(), base_type.ops());
        return &meta;
    }

    bool TypeRegistry::is_frame(const ValueTypeMetaData *meta) const
    {
        const std::lock_guard lock(mutex_);
        if (meta == nullptr) { return false; }
        const auto base = value_name_cache_.find("frame");
        if (base != value_name_cache_.end() && meta == base->second) { return true; }
        return meta->element_type != nullptr && frame_cache_.find(meta->element_type) == meta;
    }

    const ValueTypeMetaData *TypeRegistry::nullable_tuple(const ValueTypeMetaData *element_type)
    {
        const std::lock_guard lock(mutex_);
        const ValueTypeMetaData &meta = nullable_tuple_cache_.intern(element_type, [&]() {
            ValueTypeMetaData m(ValueTypeKind::List,
                                list_flags(element_type, /*fixed_size=*/0, /*variadic_tuple=*/true) |
                                    ValueTypeFlags::Nullable,
                                store_name_interned(unary_label("NullableTuple", element_type)));
            m.element_type = element_type;
            return m;
        });
        return &meta;
    }

    const ValueTypeMetaData *TypeRegistry::set(const ValueTypeMetaData *element_type)
    {
        const std::lock_guard lock(mutex_);
        const ValueTypeMetaData &meta = set_cache_.intern(element_type, [&]() {
            ValueTypeMetaData m(ValueTypeKind::Set,
                                set_flags(element_type),
                                store_name_interned(unary_label("Set", element_type)));
            m.element_type = element_type;
            return m;
        });
        return &meta;
    }

    const ValueTypeMetaData *TypeRegistry::mutable_set(const ValueTypeMetaData *element_type)
    {
        const std::lock_guard lock(mutex_);
        const ValueTypeMetaData &meta = mutable_set_cache_.intern(element_type, [&]() {
            ValueTypeMetaData m(ValueTypeKind::Set,
                                set_flags(element_type) | ValueTypeFlags::Mutable,
                                store_name_interned(unary_label("MutableSet", element_type)));
            m.element_type = element_type;
            return m;
        });
        return &meta;
    }

    const ValueTypeMetaData *TypeRegistry::map(const ValueTypeMetaData *key_type, const ValueTypeMetaData *value_type)
    {
        const std::lock_guard lock(mutex_);
        const MapKey key{key_type, value_type};
        const ValueTypeMetaData &meta = map_cache_.intern(key, [&]() {
            ValueTypeMetaData m(ValueTypeKind::Map,
                                map_flags(key_type, value_type),
                                store_name_interned(map_label("Map", key_type, value_type)));
            m.key_type = key_type;
            m.element_type = value_type;
            return m;
        });
        return &meta;
    }

    const ValueTypeMetaData *TypeRegistry::json()
    {
        const std::lock_guard lock(mutex_);
        // JSON = a distinct schema identity over Any STORAGE: the boxed value
        // is one of Bool | Int | Float | Str | List<JSON> | Map<Str, JSON>;
        // an EMPTY box is JSON null.
        const ValueTypeMetaData &meta = any_cache_.intern(1, [&]() {
            return ValueTypeMetaData(ValueTypeKind::Any,
                                     ValueTypeFlags::Hashable | ValueTypeFlags::Equatable |
                                         ValueTypeFlags::Comparable,
                                     store_name_interned("JSON"));
        });
        if (value_name_cache_.find("JSON") == value_name_cache_.end())
        {
            const_cast<TypeRegistry *>(this)->register_value_type_alias("JSON", &meta);
        }
        return &meta;
    }

    const ValueTypeMetaData *TypeRegistry::any()
    {
        const std::lock_guard lock(mutex_);
        // Unconstrained, singleton: no element/key/fields. Hashable / equatable /
        // comparable because the Any ops delegate to the embedded value (which may
        // itself throw if the contained value lacks the capability).
        const ValueTypeMetaData &meta = any_cache_.intern(0, [&]() {
            return ValueTypeMetaData(ValueTypeKind::Any,
                                     ValueTypeFlags::Hashable | ValueTypeFlags::Equatable |
                                         ValueTypeFlags::Comparable,
                                     store_name_interned("Any"));
        });
        return &meta;
    }

    const ValueTypeMetaData *TypeRegistry::mutable_map(const ValueTypeMetaData *key_type,
                                                       const ValueTypeMetaData *value_type)
    {
        const std::lock_guard lock(mutex_);
        const MapKey key{key_type, value_type};
        const ValueTypeMetaData &meta = mutable_map_cache_.intern(key, [&]() {
            ValueTypeMetaData m(ValueTypeKind::Map,
                                map_flags(key_type, value_type) | ValueTypeFlags::Mutable,
                                store_name_interned(map_label("MutableMap", key_type, value_type)));
            m.key_type     = key_type;
            m.element_type = value_type;
            return m;
        });
        return &meta;
    }

    const ValueTypeMetaData *TypeRegistry::cyclic_buffer(const ValueTypeMetaData *element_type, size_t capacity)
    {
        const std::lock_guard lock(mutex_);
        const SizedKey key{element_type, capacity};
        const ValueTypeMetaData &meta = cyclic_buffer_cache_.intern(key, [&]() {
            ValueTypeMetaData m(ValueTypeKind::CyclicBuffer,
                                ordered_container_flags(element_type),
                                store_name_interned(sized_label("CyclicBuffer", element_type, capacity)));
            m.element_type = element_type;
            m.fixed_size = capacity;
            return m;
        });
        return &meta;
    }

    const ValueTypeMetaData *TypeRegistry::queue(const ValueTypeMetaData *element_type, size_t max_capacity)
    {
        const std::lock_guard lock(mutex_);
        const SizedKey key{element_type, max_capacity};
        const ValueTypeMetaData &meta = queue_cache_.intern(key, [&]() {
            ValueTypeMetaData m(ValueTypeKind::Queue,
                                ordered_container_flags(element_type),
                                store_name_interned(sized_label("Queue", element_type, max_capacity)));
            m.element_type = element_type;
            m.fixed_size = max_capacity;
            return m;
        });
        return &meta;
    }

    const TSValueTypeMetaData *TypeRegistry::signal()
    {
        const std::lock_guard lock(mutex_);
        if (!signal_meta_)
        {
            signal_meta_ = std::make_unique<TSValueTypeMetaData>(
                TSTypeKind::SIGNAL, register_scalar<bool>("bool"), store_name_interned("SIGNAL"));
            populate_ts_schemas(*signal_meta_);
            register_ts_alias("SIGNAL", signal_meta_.get());
        }
        return signal_meta_.get();
    }

    const TSValueTypeMetaData *TypeRegistry::ts(const ValueTypeMetaData *value_type)
    {
        const std::lock_guard lock(mutex_);
        const TSValueTypeMetaData &meta = ts_cache_.intern(value_type, [&]() {
            TSValueTypeMetaData m(TSTypeKind::TS, value_type);
            populate_ts_schemas(m);
            return m;
        });
        return &meta;
    }

    const TSValueTypeMetaData *TypeRegistry::tss(const ValueTypeMetaData *element_type)
    {
        const std::lock_guard lock(mutex_);
        const TSValueTypeMetaData &meta = tss_cache_.intern(element_type, [&]() {
            TSValueTypeMetaData m(TSTypeKind::TSS, element_type ? set(element_type) : nullptr);
            populate_ts_schemas(m);
            return m;
        });
        return &meta;
    }

    const TSValueTypeMetaData *TypeRegistry::tsd(const ValueTypeMetaData *key_type, const TSValueTypeMetaData *value_ts)
    {
        const std::lock_guard lock(mutex_);
        const TSDictKey key{key_type, value_ts};
        const TSValueTypeMetaData &meta = tsd_cache_.intern(key, [&]() {
            TSValueTypeMetaData m(
                TSTypeKind::TSD, key_type && value_ts ? map(key_type, value_ts->value_schema) : nullptr);
            m.set_tsd(key_type, value_ts);
            populate_ts_schemas(m);
            return m;
        });
        return &meta;
    }

    const TSValueTypeMetaData *TypeRegistry::tsl(const TSValueTypeMetaData *element_ts, size_t fixed_size)
    {
        const std::lock_guard lock(mutex_);
        const TSListKey key{element_ts, fixed_size};
        const TSValueTypeMetaData &meta = tsl_cache_.intern(key, [&]() {
            TSValueTypeMetaData m(
                TSTypeKind::TSL,
                element_ts && element_ts->value_schema ? list(element_ts->value_schema, fixed_size) : nullptr);
            m.set_tsl(element_ts, fixed_size);
            populate_ts_schemas(m);
            return m;
        });
        return &meta;
    }

    const TSValueTypeMetaData *TypeRegistry::tsw(const ValueTypeMetaData *value_type, size_t period, size_t min_period)
    {
        const std::lock_guard lock(mutex_);
        const TSWindowKey key{value_type, false, static_cast<std::int64_t>(period), static_cast<std::int64_t>(min_period)};
        const TSValueTypeMetaData &meta = tsw_cache_.intern(key, [&]() {
            TSValueTypeMetaData m(TSTypeKind::TSW, value_type);
            m.set_tsw_tick(period, min_period);
            populate_ts_schemas(m);
            return m;
        });
        return &meta;
    }

    const TSValueTypeMetaData *TypeRegistry::tsw_duration(const ValueTypeMetaData *value_type,
                                                          TimeDelta time_range,
                                                          TimeDelta min_time_range)
    {
        const std::lock_guard lock(mutex_);
        const TSWindowKey key{value_type, true, time_range.count(), min_time_range.count()};
        const TSValueTypeMetaData &meta = tsw_cache_.intern(key, [&]() {
            TSValueTypeMetaData m(TSTypeKind::TSW, value_type);
            m.set_tsw_duration(time_range, min_time_range);
            populate_ts_schemas(m);
            return m;
        });
        return &meta;
    }

    const TSValueTypeMetaData *
    TypeRegistry::un_named_tsb(const std::vector<std::pair<std::string, const TSValueTypeMetaData *>> &fields)
    {
        const std::lock_guard lock(mutex_);
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
                value_fields.emplace_back(field_name, ts_type ? ts_type->value_schema : nullptr);
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

            // Un-named TSB: value-side bundle is the matching un-named Bundle.
            TSValueTypeMetaData m(TSTypeKind::TSB, un_named_bundle(value_fields), nullptr);
            m.set_tsb(fields_ptr, fields.size(), nullptr, /*wrapped_un_named=*/nullptr);
            populate_ts_schemas(m);
            return m;
        });
        return &meta;
    }

    const TSValueTypeMetaData *
    TypeRegistry::tsb(std::string_view name,
                      const std::vector<std::pair<std::string, const TSValueTypeMetaData *>> &fields)
    {
        const std::lock_guard lock(mutex_);
        if (name.empty())
        {
            throw std::invalid_argument("tsb requires a non-empty name; use un_named_tsb for the structural form");
        }
        const TSValueTypeMetaData *un_named = un_named_tsb(fields);

        // TSB name namespace must be unique: same name with a different
        // field list is a conflict.
        if (const TSValueTypeMetaData *existing = named_tsb(name);
            existing != nullptr && existing->wrapped_un_named_tsb() != un_named)
        {
            throw std::invalid_argument(
                std::string("named tsb '") + std::string(name) +
                "' is already registered with a different schema");
        }

        NamedTSBundleKey key{std::string{name}, un_named};
        const TSValueTypeMetaData &meta = named_tsb_cache_.intern(std::move(key), [&]() {
            // Build the matching named value-side bundle so the named TSB's
            // value pointer carries the same nominal identity.
            std::vector<std::pair<std::string, const ValueTypeMetaData *>> value_fields;
            value_fields.reserve(fields.size());
            for (const auto &[field_name, ts_type] : fields)
            {
                value_fields.emplace_back(field_name, ts_type ? ts_type->value_schema : nullptr);
            }
            const ValueTypeMetaData *named_value_bundle = bundle(name, value_fields);

            const char *interned_name = store_name_interned(name);
            TSValueTypeMetaData m(TSTypeKind::TSB, named_value_bundle, interned_name);
            // Field array is shared with the un-named twin (no duplication).
            m.set_tsb(un_named->fields(), un_named->field_count(), interned_name, un_named);
            populate_ts_schemas(m);
            return m;
        });
        register_ts_alias(name, &meta);
        return &meta;
    }

    const TSValueTypeMetaData *TypeRegistry::ref(const TSValueTypeMetaData *referenced_ts)
    {
        const std::lock_guard lock(mutex_);
        if (referenced_ts == nullptr)
        {
            throw std::invalid_argument("TypeRegistry::ref requires a referenced time-series schema");
        }
        if (referenced_ts->kind == TSTypeKind::REF)
        {
            return referenced_ts;
        }

        const TSValueTypeMetaData &meta = ref_cache_.intern(referenced_ts, [&]() {
            if (!time_series_reference_meta_)
            {
                // Register the real C++ TimeSeriesReference type so the
                // schema is paired with a proper StoragePlan via
                // ValuePlanFactory. Every REF schema's value_schema /
                // delta_value_schema points at this canonical metadata.
                time_series_reference_meta_ = register_scalar<TimeSeriesReference>("TimeSeriesReference");
            }
            TSValueTypeMetaData m(TSTypeKind::REF, time_series_reference_meta_);
            m.set_ref(referenced_ts);
            populate_ts_schemas(m);
            return m;
        });
        return &meta;
    }

    void TypeRegistry::populate_ts_schemas(TSValueTypeMetaData &meta)
    {
        switch (meta.kind)
        {
            case TSTypeKind::TS:
            case TSTypeKind::SIGNAL:
            case TSTypeKind::REF:
                // REF is conceptually TS<TimeSeriesReference>: the reference
                // token itself is the value. ``value_type`` was set during
                // construction (T for TS, ``bool`` for SIGNAL, the synthetic
                // ``TimeSeriesReference`` atomic for REF).
                meta.value_schema       = meta.value_type;
                meta.delta_value_schema = meta.value_type;
                break;

            case TSTypeKind::TSW:
            {
                const size_t length     = meta.is_duration_based() ? 0 : meta.period();
                meta.value_schema       = list(meta.value_type, length);
                meta.delta_value_schema = meta.value_type;
                break;
            }

            case TSTypeKind::TSS:
            {
                meta.value_schema = meta.value_type;
                if (meta.value_type != nullptr)
                {
                    const ValueTypeMetaData *element  = meta.value_type->element_type;
                    const ValueTypeMetaData *set_of_t = set(element);
                    meta.delta_value_schema = un_named_bundle({{"added", set_of_t}, {"removed", set_of_t}});
                }
                break;
            }

            case TSTypeKind::TSD:
            {
                meta.value_schema = meta.value_type;
                const ValueTypeMetaData *key = meta.key_type();
                if (const TSValueTypeMetaData *value_ts = meta.element_ts(); key != nullptr && value_ts != nullptr)
                {
                    const ValueTypeMetaData *value_delta = value_ts->delta_value_schema;
                    const ValueTypeMetaData *delta_map   = map(key, value_delta);
                    const ValueTypeMetaData *removed_set = set(key);
                    meta.delta_value_schema =
                        un_named_bundle({{"removed", removed_set}, {"modified", delta_map}});
                }
                break;
            }

            case TSTypeKind::TSL:
            {
                meta.value_schema = meta.value_type;
                if (const TSValueTypeMetaData *element_ts = meta.element_ts(); element_ts != nullptr)
                {
                    const ValueTypeMetaData *index_type    = register_scalar<Int>("int");
                    const ValueTypeMetaData *element_delta = element_ts->delta_value_schema;
                    meta.delta_value_schema                = map(index_type, element_delta);
                }
                break;
            }

            case TSTypeKind::TSB:
            {
                meta.value_schema = meta.value_type;

                std::vector<std::pair<std::string, const ValueTypeMetaData *>> delta_fields;
                delta_fields.reserve(meta.field_count());
                for (size_t i = 0; i < meta.field_count(); ++i)
                {
                    const TSFieldMetaData &field = meta.fields()[i];
                    delta_fields.emplace_back(field.name ? std::string(field.name) : std::string{},
                                              field.type ? field.type->delta_value_schema : nullptr);
                }
                meta.delta_value_schema = un_named_bundle(delta_fields);
                break;
            }

        }
    }

    bool TypeRegistry::contains_ref(const TSValueTypeMetaData *meta)
    {
        TypeRegistry &registry = instance();
        const std::lock_guard lock(registry.mutex_);
        return contains_ref_unlocked(meta);
    }

    bool TypeRegistry::contains_ref_unlocked(const TSValueTypeMetaData *meta)
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
                    if (contains_ref_unlocked(meta->fields()[index].type))
                    {
                        return true;
                    }
                }
                return false;
            case TSTypeKind::TSD:
            case TSTypeKind::TSL: return contains_ref_unlocked(meta->element_ts());
            default: return false;
        }
    }

    const TSValueTypeMetaData *TypeRegistry::dereference(const TSValueTypeMetaData *meta)
    {
        const std::lock_guard lock(mutex_);
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
                if (!contains_ref_unlocked(meta))
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

                if (meta->is_un_named_tsb())
                {
                    result = un_named_tsb(deref_fields);
                }
                else
                {
                    std::string deref_name = std::string(meta->bundle_name()) + "_deref";
                    result = tsb(deref_name, deref_fields);
                }
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
