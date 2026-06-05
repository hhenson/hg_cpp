#ifndef HGRAPH_LIB_STD_OPERATORS_STRING_H
#define HGRAPH_LIB_STD_OPERATORS_STRING_H

#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

namespace hgraph::stdlib
{
    /**
     * String operator **definitions** (markers only). Mirrors the Python ``hgraph`` string
     * operators (``_string.py``). (``str_`` lives in *conversion*.)
     */

    /** ``match_`` — match a regex ``pattern`` against ``s``; result is a bundle (``is_match`` / ``groups``). */
    struct match_ : Operator<"match", In<"pattern", TS<Str>>, In<"s", TS<Str>>, Out<TsVar<"O">>>
    {
    };

    /** ``replace`` — replace ``pattern`` with ``repl`` in ``s``. */
    struct replace : Operator<"replace", In<"pattern", TS<Str>>, In<"repl", TS<Str>>, In<"s", TS<Str>>, Out<TS<Str>>>
    {
    };

    /** ``substr`` — extract a substring of ``s`` between ``start`` and ``end``. */
    struct substr : Operator<"substr", In<"s", TS<Str>>, In<"start", TS<Int>>, In<"end", TS<Int>>, Out<TS<Str>>>
    {
    };

    /** ``split`` — split ``s`` over ``separator`` into the requested output shape (tuple / list). */
    struct split : Operator<"split", In<"s", TS<Str>>, Scalar<"separator", Str>, Out<TsVar<"O">>>
    {
    };

    /** ``join`` — join several string time-series with ``separator`` (variadic). */
    struct join : Operator<"join", In<"strings", TsVar<"S">>, Scalar<"separator", Str>, Out<TS<Str>>>
    {
    };

    /** ``format_`` — format the supplied time-series values into a string using ``fmt`` (variadic args). */
    struct format_ : Operator<"format", In<"fmt", TS<Str>>, In<"args", TsVar<"A">>, Out<TS<Str>>>
    {
    };
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_STRING_H
