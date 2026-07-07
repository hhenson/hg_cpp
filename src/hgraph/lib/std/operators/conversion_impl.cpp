#include <hgraph/lib/std/operators/impl/conversion_impl.h>

namespace hgraph::stdlib
{
    void register_conversion_operators()
    {
        register_overload<const_, const_source>();    // const(value)         -> tick at start
        register_overload<const_, const_delayed>();   // const(value, delay)  -> tick at start + delay
        register_overload<nothing, nothing_source>(); // nothing              -> never ticks

        register_graph_overload<zero_, zero_int>();
        register_graph_overload<zero_, zero_float>();
        register_graph_overload<zero_, zero_str>();

        register_overload<default_, default_impl>();
        register_overload<str_, str_impl>();
    }
}  // namespace hgraph::stdlib
