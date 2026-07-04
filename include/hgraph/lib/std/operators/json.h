#ifndef HGRAPH_LIB_STD_OPERATORS_JSON_H
#define HGRAPH_LIB_STD_OPERATORS_JSON_H

#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

namespace hgraph::stdlib
{
    /**
     * JSON serialization operators (design record: *Record/replay, tables and
     * const_fn*, step 1). The wire format is the Python one — see
     * ``types/value/json_codec.h``.
     *
     * ``to_json(ts, delta=false)`` serialises the time-series VALUE per tick;
     * with ``delta=true`` it serialises the canonical per-tick delta value
     * (``capture_delta``) instead — the canonical delta *is* the recorded
     * delta wire form.
     *
     * ``from_json`` parses into the resolved output type and applies the
     * parsed value as the tick's delta:
     * ``wire<from_json, TS<MySchema>>(w, ts)``.
     */
    struct to_json : Operator<"to_json", In<"ts", TsVar<"S">>, Scalar<"delta", Bool>, Out<TS<Str>>>
    {
    };

    struct from_json : Operator<"from_json", In<"ts", TS<Str>>, Out<TsVar<"O">>>
    {
    };
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_JSON_H
