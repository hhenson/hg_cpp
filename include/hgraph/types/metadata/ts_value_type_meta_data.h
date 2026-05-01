//
// Created by Howard Henson on 20/04/2026.
//

#ifndef HGRAPH_CPP_ROOT_TS_VALUE_TYPE_META_DATA_H
#define HGRAPH_CPP_ROOT_TS_VALUE_TYPE_META_DATA_H

#include <hgraph/util/date_time.h>
#include <hgraph/types/metadata/type_meta_data.h>
#include <hgraph/types/metadata/value_type_meta_data.h>

#include <cstddef>
#include <cstdint>

namespace hgraph
{
    struct TSValueTypeMetaData;

    /**
     * The seven time-series kinds enumerated in the developer guide.
     *
     * - ``Value``       — ``TS[T]``: scalar time-series of one atomic type.
     * - ``Set``         — ``TSS``: unordered set of one scalar type.
     * - ``Dict``        — ``TSD``: keyed dictionary with TS values.
     * - ``List``        — ``TSL``: ordered list of one TS type.
     * - ``Window``      — ``TSW``: sliding window of one scalar type.
     * - ``Bundle``      — ``TSB``: named bundle of TS fields.
     * - ``Reference``   — ``REF``: reference to another TS target.
     * - ``Signal``      — ``SIGNAL``: zero-payload tick stream.
     */
    enum class TSValueTypeKind : uint8_t
    {
        Value,
        Set,
        Dict,
        List,
        Window,
        Bundle,
        Reference,
        Signal,
    };

    /**
     * Field descriptor for a ``TSB`` bundle.
     *
     * Names live in the registry's interned string storage; ``index`` is
     * the field's position in declaration order.
     */
    struct TSFieldMetaData
    {
        /** Field name; registry-interned. */
        const char *name{nullptr};
        /** Field index in declaration order. */
        size_t index{0};
        /** Schema for this field's time-series type. */
        const TSValueTypeMetaData *type{nullptr};
    };

    /**
     * Schema descriptor for time-series-layer types.
     *
     * The shape of ``data`` depends on ``kind``: dictionaries carry a key
     * type and value-TS schema, lists carry an element-TS schema and
     * fixed size, windows carry tick or duration parameters, bundles
     * carry a field array, references carry the referenced TS schema.
     * Use the ``set_*`` methods to populate ``data`` consistently with
     * ``kind``.
     *
     * Always interned through ``TypeRegistry``: equivalent metadata
     * resolves to the same pointer. ``value_type`` records the underlying
     * value-layer schema where one applies (``Value``, ``Set``, ``Dict``
     * key, ``Window``).
     */
    struct TSValueTypeMetaData final : TypeMetaData
    {
        /** Window-size parameters: tick-based or duration-based. */
        union WindowParams
        {
            /** Tick-count window: ``period`` of ticks, with ``min_period`` warm-up. */
            struct
            {
                size_t period;
                size_t min_period;
            } tick;
            /** Duration window: ``time_range`` engine-time delta with ``min_time_range`` warm-up. */
            struct
            {
                engine_time_delta_t time_range;
                engine_time_delta_t min_time_range;
            } duration;

            constexpr WindowParams()
                : tick{0, 0}
            {
            }
        };

        /** Empty payload used for kinds without extra metadata (``Value``, ``Set``, ``Signal``). */
        struct EmptyData
        {
        };

        /** ``Dict`` payload: key value-schema and per-key TS-schema. */
        struct DictData
        {
            const ValueTypeMetaData *key_type{nullptr};
            const TSValueTypeMetaData *value_ts{nullptr};
        };

        /** ``List`` payload: element TS-schema and fixed size (``0`` for dynamic). */
        struct ListData
        {
            const TSValueTypeMetaData *element_ts{nullptr};
            size_t fixed_size{0};
        };

        /** ``Window`` payload: a flag plus the tick or duration parameters. */
        struct WindowData
        {
            bool is_duration_based{false};
            WindowParams window{};
        };

        /** ``Bundle`` payload: field array, count, and the bundle name. */
        struct BundleData
        {
            const TSFieldMetaData *fields{nullptr};
            size_t field_count{0};
            const char *bundle_name{nullptr};
        };

        /** ``Reference`` payload: schema of the referenced time-series. */
        struct ReferenceData
        {
            const TSValueTypeMetaData *referenced_ts{nullptr};
        };

        /** Tagged union for kind-specific metadata; the active member is determined by ``kind``. */
        union KindData
        {
            EmptyData empty;
            DictData dict;
            ListData list;
            WindowData window;
            BundleData bundle;
            ReferenceData reference;

            constexpr KindData()
                : empty{}
            {
            }
        };

        /** Default construct an empty descriptor (kind defaults to ``Signal``). */
        constexpr TSValueTypeMetaData() noexcept
            : TypeMetaData(MetaCategory::TimeSeries)
        {
        }

        /** Construct with kind, optional underlying value type, and optional name. */
        constexpr TSValueTypeMetaData(TSValueTypeKind kind_,
                                      const ValueTypeMetaData *value_type_ = nullptr,
                                      const char *display_name_ = nullptr) noexcept
            : TypeMetaData(MetaCategory::TimeSeries, display_name_)
            , kind(kind_)
            , value_type(value_type_)
        {
        }

        /** Kind discriminator for ``data``. */
        TSValueTypeKind kind{TSValueTypeKind::Signal};
        /** Underlying value schema for ``Value`` / ``Set`` / ``Window`` kinds. */
        const ValueTypeMetaData *value_type{nullptr};
        /** Kind-dependent payload; populated through the ``set_*`` helpers. */
        KindData data{};

        /** Populate ``data.dict`` for a ``Dict`` (TSD) descriptor. */
        constexpr void set_dict(const ValueTypeMetaData *key_type, const TSValueTypeMetaData *value_ts) noexcept
        {
            data.dict = DictData{key_type, value_ts};
        }

        /** Populate ``data.list`` for a ``List`` (TSL) descriptor. */
        constexpr void set_list(const TSValueTypeMetaData *element_ts, size_t fixed_size) noexcept
        {
            data.list = ListData{element_ts, fixed_size};
        }

        /** Populate ``data.window`` for a tick-count ``Window`` (TSW). */
        constexpr void set_tick_window(size_t period, size_t min_period) noexcept
        {
            data.window = WindowData{false, WindowParams{}};
            data.window.window.tick.period = period;
            data.window.window.tick.min_period = min_period;
        }

        /** Populate ``data.window`` for a duration-based ``Window`` (TSW). */
        constexpr void set_duration_window(engine_time_delta_t time_range,
                                           engine_time_delta_t min_time_range) noexcept
        {
            data.window = WindowData{true, WindowParams{}};
            data.window.window.duration.time_range = time_range;
            data.window.window.duration.min_time_range = min_time_range;
        }

        /** Populate ``data.bundle`` for a ``Bundle`` (TSB) descriptor. */
        constexpr void set_bundle(const TSFieldMetaData *fields, size_t field_count, const char *bundle_name) noexcept
        {
            data.bundle = BundleData{fields, field_count, bundle_name};
        }

        /** Populate ``data.reference`` for a ``Reference`` (REF) descriptor. */
        constexpr void set_reference(const TSValueTypeMetaData *referenced_ts) noexcept
        {
            data.reference = ReferenceData{referenced_ts};
        }

        /** Key value-schema for ``Dict`` (TSD); null for other kinds. */
        [[nodiscard]] constexpr const ValueTypeMetaData *key_type() const noexcept
        {
            return kind == TSValueTypeKind::Dict ? data.dict.key_type : nullptr;
        }

        /** Element TS-schema for ``Dict`` (value-TS), ``List``, or ``Reference`` (target). */
        [[nodiscard]] constexpr const TSValueTypeMetaData *element_ts() const noexcept
        {
            switch (kind)
            {
                case TSValueTypeKind::Dict: return data.dict.value_ts;
                case TSValueTypeKind::List: return data.list.element_ts;
                case TSValueTypeKind::Reference: return data.reference.referenced_ts;
                default: return nullptr;
            }
        }

        /** Fixed size of a static ``List`` (TSL); zero for dynamic or non-list kinds. */
        [[nodiscard]] constexpr size_t fixed_size() const noexcept
        {
            return kind == TSValueTypeKind::List ? data.list.fixed_size : 0;
        }

        /** True when ``Window`` (TSW) is duration-based; false for tick-based or non-window. */
        [[nodiscard]] constexpr bool is_duration_based() const noexcept
        {
            return kind == TSValueTypeKind::Window && data.window.is_duration_based;
        }

        /** Tick-count window period; zero for duration-based or non-window kinds. */
        [[nodiscard]] constexpr size_t period() const noexcept
        {
            return kind == TSValueTypeKind::Window && !data.window.is_duration_based ? data.window.window.tick.period : 0;
        }

        /** Tick-count window warm-up size; zero for duration-based or non-window kinds. */
        [[nodiscard]] constexpr size_t min_period() const noexcept
        {
            return kind == TSValueTypeKind::Window && !data.window.is_duration_based
                       ? data.window.window.tick.min_period
                       : 0;
        }

        /** Duration-window time range; zero for tick-based or non-window kinds. */
        [[nodiscard]] constexpr engine_time_delta_t time_range() const noexcept
        {
            return kind == TSValueTypeKind::Window && data.window.is_duration_based
                       ? data.window.window.duration.time_range
                       : engine_time_delta_t{0};
        }

        /** Duration-window warm-up; zero for tick-based or non-window kinds. */
        [[nodiscard]] constexpr engine_time_delta_t min_time_range() const noexcept
        {
            return kind == TSValueTypeKind::Window && data.window.is_duration_based
                       ? data.window.window.duration.min_time_range
                       : engine_time_delta_t{0};
        }

        /** Field array for ``Bundle`` (TSB); null for other kinds. */
        [[nodiscard]] constexpr const TSFieldMetaData *fields() const noexcept
        {
            return kind == TSValueTypeKind::Bundle ? data.bundle.fields : nullptr;
        }

        /** Field count for ``Bundle`` (TSB); zero for other kinds. */
        [[nodiscard]] constexpr size_t field_count() const noexcept
        {
            return kind == TSValueTypeKind::Bundle ? data.bundle.field_count : 0;
        }

        /** Bundle display name for ``Bundle`` (TSB); null for other kinds. */
        [[nodiscard]] constexpr const char *bundle_name() const noexcept
        {
            return kind == TSValueTypeKind::Bundle ? data.bundle.bundle_name : nullptr;
        }

        /** Schema referenced by a ``Reference`` (REF); null for other kinds. */
        [[nodiscard]] constexpr const TSValueTypeMetaData *referenced_ts() const noexcept
        {
            return kind == TSValueTypeKind::Reference ? data.reference.referenced_ts : nullptr;
        }

        /** True for the keyed/structural kinds (``Set``, ``Dict``, ``List``, ``Bundle``). */
        [[nodiscard]] constexpr bool is_collection() const noexcept
        {
            return kind == TSValueTypeKind::Set || kind == TSValueTypeKind::Dict || kind == TSValueTypeKind::List ||
                   kind == TSValueTypeKind::Bundle;
        }

        /** True for kinds whose payload is a single scalar TS (``Value``, ``Window``, ``Signal``). */
        [[nodiscard]] constexpr bool is_scalar_ts() const noexcept
        {
            return kind == TSValueTypeKind::Value || kind == TSValueTypeKind::Window || kind == TSValueTypeKind::Signal;
        }
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_VALUE_TYPE_META_DATA_H
