#ifndef HGRAPH_LIB_STD_OPERATORS_CONVERSION_H
#define HGRAPH_LIB_STD_OPERATORS_CONVERSION_H

#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/util/date_time.h>

namespace hgraph::stdlib
{
    /**
     * Type / shape conversion operator **definitions** (markers only). Mirrors the Python
     * ``hgraph`` conversion operators (``_time_series_conversion.py``, ``_type_operators.py``)
     * plus the ``str_`` / ``type_`` introspection helpers and the ``zero`` / ``nothing`` /
     * ``default`` graph utilities. The target type of ``convert`` / ``cast_`` / etc. is
     * supplied as an explicit output schema at the wiring site (``wire<convert, TS<Int>>(…)``).
     *
     * .. note::
     *
     *    The data-frame / table / JSON conversion operators (``to_table`` / ``from_table`` /
     *    ``to_json`` / ``from_json`` / ``to_data_frame`` / ``from_data_frame`` / …) are
     *    **deferred** — they need scalar value types (``Frame`` / ``JSON`` / ``TABLE``) the
     *    C++ value layer does not model yet.
     */

    /** ``const_`` — a source that emits a configured ``value`` once at the start cycle, or
        ``delay`` after it. The output type is the registered ``TS`` of the value's type, or an
        explicit output schema at the wiring site (``wire<const_, TSS<Int>>(w, set_value)``).
        Two arities: ``const(value)`` (tick at start) and ``const(value, delay)`` (tick at
        ``start_time + delay``). */
    struct const_
        : Operator<"const", Scalar<"value", ScalarVar<"T">>, Scalar<"delay", TimeDelta>, Out<TsVar<"S">>>
    {
    };

    /** ``convert`` — convert the incoming time-series to the requested output type. */
    struct convert : Operator<"convert", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``combine`` — combine several time-series into one collection time-series (variadic). */
    struct combine : Operator<"combine", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``collect`` — accumulate ``ts`` into a collection time-series (output type via ``OUT``). */
    struct collect : Operator<"collect", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``emit`` — stream a collection's elements out as individual ticks. */
    struct emit : Operator<"emit", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``cast_`` — cast a ``TS`` value to a different scalar type (output type supplied explicitly). */
    struct cast_ : Operator<"cast_", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``downcast_`` — downcast a ``TS`` value to a (checked) derived type. */
    struct downcast_ : Operator<"downcast_", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``downcast_ref`` — downcast a ``REF`` to a derived type (fast, unchecked). */
    struct downcast_ref : Operator<"downcast_ref", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``str_`` — convert the incoming time-series to its ``TS<Str>`` representation. */
    struct str_ : Operator<"str_", In<"ts", TsVar<"S">>, Out<TS<Str>>>
    {
    };

    /** ``type_`` — the (python) type of the time-series value. */
    struct type_ : Operator<"type_", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``zero_`` — the additive (or operation-specific) zero source for a requested output type. */
    struct zero_ : Operator<"zero", Out<TsVar<"S">>>
    {
    };

    /** ``nothing`` — a source that never ticks, of the requested output type. */
    struct nothing : Operator<"nothing", Out<TsVar<"O">>>
    {
    };

    /** ``default_`` — pass ``ts`` through, substituting ``default_value`` while ``ts`` is invalid. */
    struct default_ : Operator<"default", In<"ts", TsVar<"S">>, In<"default_value", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_CONVERSION_H
