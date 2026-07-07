#include <hgraph/lib/std/operators/impl/table_impl.h>

namespace hgraph::stdlib
{
    void register_table_operators()
    {
        register_overload<to_table, to_table_impl>();
        register_overload<from_table, from_table_impl>();
        register_overload<from_table_const, from_table_const_impl>();
    }
}  // namespace hgraph::stdlib
