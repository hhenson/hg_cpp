#ifndef HGRAPH_LIB_STD_STANDARD_TYPES_H
#define HGRAPH_LIB_STD_STANDARD_TYPES_H

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/util/date_time.h>

#include <cstdint>
#include <initializer_list>
#include <stdexcept>
#include <string>
#include <string_view>

namespace hgraph::stdlib
{
    /**
     * Pointers for the standard scalar schemas and their common ``TS`` / ``TSS``
     * schemas after registration.
     *
     * User-facing names intentionally follow the Python-facing vocabulary:
     * ``int`` is ``std::int64_t``, ``float`` is ``double``, and ``str`` is
     * ``std::string``. Explicit fixed-width aliases such as ``int8`` and
     * ``uint32`` are also registered for callers that need narrower storage.
     */
    struct RegisteredStandardTypes
    {
        const ValueTypeMetaData *bool_type{nullptr};
        const ValueTypeMetaData *int_type{nullptr};
        const ValueTypeMetaData *float_type{nullptr};
        const ValueTypeMetaData *date_type{nullptr};
        const ValueTypeMetaData *datetime_type{nullptr};
        const ValueTypeMetaData *timedelta_type{nullptr};
        const ValueTypeMetaData *str_type{nullptr};

        const ValueTypeMetaData *int8_type{nullptr};
        const ValueTypeMetaData *int16_type{nullptr};
        const ValueTypeMetaData *int32_type{nullptr};
        const ValueTypeMetaData *int64_type{nullptr};
        const ValueTypeMetaData *uint8_type{nullptr};
        const ValueTypeMetaData *uint16_type{nullptr};
        const ValueTypeMetaData *uint32_type{nullptr};
        const ValueTypeMetaData *uint64_type{nullptr};
        const ValueTypeMetaData *float32_type{nullptr};
        const ValueTypeMetaData *float64_type{nullptr};

        const TSValueTypeMetaData *ts_bool{nullptr};
        const TSValueTypeMetaData *ts_int{nullptr};
        const TSValueTypeMetaData *ts_float{nullptr};
        const TSValueTypeMetaData *ts_date{nullptr};
        const TSValueTypeMetaData *ts_datetime{nullptr};
        const TSValueTypeMetaData *ts_timedelta{nullptr};
        const TSValueTypeMetaData *ts_str{nullptr};

        const TSValueTypeMetaData *tss_bool{nullptr};
        const TSValueTypeMetaData *tss_int{nullptr};
        const TSValueTypeMetaData *tss_float{nullptr};
        const TSValueTypeMetaData *tss_date{nullptr};
        const TSValueTypeMetaData *tss_datetime{nullptr};
        const TSValueTypeMetaData *tss_timedelta{nullptr};
        const TSValueTypeMetaData *tss_str{nullptr};
    };

    namespace standard_types_detail
    {
        template <typename T>
        [[nodiscard]] inline const ValueTypeMetaData *
        register_scalar_aliases(TypeRegistry &registry, std::initializer_list<std::string_view> names)
        {
            const ValueTypeMetaData *meta = nullptr;
            for (std::string_view name : names)
            {
                const ValueTypeMetaData *registered = registry.register_scalar<T>(name);
                if (meta == nullptr)
                {
                    meta = registered;
                }
                else if (registered != meta)
                {
                    throw std::logic_error("standard scalar alias resolved to a different schema");
                }
            }
            return meta;
        }

        inline void register_ts_aliases(TypeRegistry                  &registry,
                                        const ValueTypeMetaData       *scalar,
                                        std::initializer_list<std::string_view> names,
                                        const TSValueTypeMetaData    *&ts_out,
                                        const TSValueTypeMetaData    *&tss_out)
        {
            const TSValueTypeMetaData *ts  = registry.ts(scalar);
            const TSValueTypeMetaData *tss = registry.tss(scalar);
            for (std::string_view name : names)
            {
                registry.register_time_series_type_alias(std::string{"TS["} + std::string{name} + "]", ts);
                registry.register_time_series_type_alias(std::string{"TSS["} + std::string{name} + "]", tss);
            }
            ts_out  = ts;
            tss_out = tss;
        }
    }  // namespace standard_types_detail

    /**
     * Register the standard scalar vocabulary and pre-intern the common ``TS`` /
     * ``TSS`` schemas.
     *
     * Primary aliases:
     *
     * - ``bool`` -> ``bool``
     * - ``int`` -> ``std::int64_t``
     * - ``float`` -> ``double``
     * - ``date`` -> ``engine_date_t``
     * - ``datetime`` -> ``engine_time_t``
     * - ``timedelta`` -> ``engine_time_delta_t``
     * - ``str`` -> ``std::string``
     *
     * Explicit aliases include ``int8``/``int16``/``int32``/``int64``,
     * ``uint8``/``uint16``/``uint32``/``uint64``, ``float32`` and
     * ``float64``. ``double`` and ``string`` are accepted compatibility aliases
     * for ``float64`` and ``str``.
     */
    [[nodiscard]] inline RegisteredStandardTypes register_standard_types(
        TypeRegistry &registry = TypeRegistry::instance())
    {
        RegisteredStandardTypes types{};

        types.bool_type      = standard_types_detail::register_scalar_aliases<bool>(registry, {"bool"});
        types.int_type       = standard_types_detail::register_scalar_aliases<std::int64_t>(registry, {"int", "int64"});
        types.float_type     = standard_types_detail::register_scalar_aliases<double>(registry, {"float", "float64", "double"});
        types.date_type      = standard_types_detail::register_scalar_aliases<engine_date_t>(registry, {"date"});
        types.datetime_type  = standard_types_detail::register_scalar_aliases<engine_time_t>(registry, {"datetime"});
        types.timedelta_type = standard_types_detail::register_scalar_aliases<engine_time_delta_t>(registry, {"timedelta"});
        types.str_type       = standard_types_detail::register_scalar_aliases<std::string>(registry, {"str", "string"});

        types.int8_type    = standard_types_detail::register_scalar_aliases<std::int8_t>(registry, {"int8"});
        types.int16_type   = standard_types_detail::register_scalar_aliases<std::int16_t>(registry, {"int16"});
        types.int32_type   = standard_types_detail::register_scalar_aliases<std::int32_t>(registry, {"int32"});
        types.int64_type   = types.int_type;
        types.uint8_type   = standard_types_detail::register_scalar_aliases<std::uint8_t>(registry, {"uint8"});
        types.uint16_type  = standard_types_detail::register_scalar_aliases<std::uint16_t>(registry, {"uint16"});
        types.uint32_type  = standard_types_detail::register_scalar_aliases<std::uint32_t>(registry, {"uint32"});
        types.uint64_type  = standard_types_detail::register_scalar_aliases<std::uint64_t>(registry, {"uint64"});
        types.float32_type = standard_types_detail::register_scalar_aliases<float>(registry, {"float32"});
        types.float64_type = types.float_type;

        standard_types_detail::register_ts_aliases(registry, types.bool_type, {"bool"}, types.ts_bool, types.tss_bool);
        standard_types_detail::register_ts_aliases(registry, types.int_type, {"int", "int64"}, types.ts_int, types.tss_int);
        standard_types_detail::register_ts_aliases(registry, types.float_type, {"float", "float64", "double"},
                                                   types.ts_float, types.tss_float);
        standard_types_detail::register_ts_aliases(registry, types.date_type, {"date"}, types.ts_date, types.tss_date);
        standard_types_detail::register_ts_aliases(registry, types.datetime_type, {"datetime"}, types.ts_datetime,
                                                   types.tss_datetime);
        standard_types_detail::register_ts_aliases(registry, types.timedelta_type, {"timedelta"}, types.ts_timedelta,
                                                   types.tss_timedelta);
        standard_types_detail::register_ts_aliases(registry, types.str_type, {"str", "string"}, types.ts_str,
                                                   types.tss_str);

        const TSValueTypeMetaData *unused_ts  = nullptr;
        const TSValueTypeMetaData *unused_tss = nullptr;
        standard_types_detail::register_ts_aliases(registry, types.int8_type, {"int8"}, unused_ts, unused_tss);
        standard_types_detail::register_ts_aliases(registry, types.int16_type, {"int16"}, unused_ts, unused_tss);
        standard_types_detail::register_ts_aliases(registry, types.int32_type, {"int32"}, unused_ts, unused_tss);
        standard_types_detail::register_ts_aliases(registry, types.uint8_type, {"uint8"}, unused_ts, unused_tss);
        standard_types_detail::register_ts_aliases(registry, types.uint16_type, {"uint16"}, unused_ts, unused_tss);
        standard_types_detail::register_ts_aliases(registry, types.uint32_type, {"uint32"}, unused_ts, unused_tss);
        standard_types_detail::register_ts_aliases(registry, types.uint64_type, {"uint64"}, unused_ts, unused_tss);
        standard_types_detail::register_ts_aliases(registry, types.float32_type, {"float32"}, unused_ts, unused_tss);

        return types;
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_STANDARD_TYPES_H
