#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_OPERATORS_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_OPERATORS_IMPL_H

/**
 * The standard operator **implementations** + registration, grouped by family to mirror
 * the operator *definitions* under ``include/hgraph/lib/std/operators/``. Each definition
 * file ``<family>.h`` has a matching implementation file ``impl/<family>_impl.h`` that
 * provides the concrete overloads and a ``register_<family>_operators()`` function; this
 * header aggregates them into ``register_standard_operators``.
 *
 * Only a small set is implemented so far (scalar arithmetic / comparison / logical,
 * string basics, date components, ``const_`` / ``zero_`` / ``debug_print`` / ``null_sink``); further families gain an
 * ``impl/<family>_impl.h`` and a call below as their implementations land. See
 * ``docs/source/developer_guide/operators.rst``.
 */

#include <hgraph/lib/std/operators/impl/arithmetic_impl.h>
#include <hgraph/lib/std/operators/impl/comparison_impl.h>
#include <hgraph/lib/std/operators/impl/container_impl.h>
#include <hgraph/lib/std/operators/impl/collection_impl.h>
#include <hgraph/lib/std/operators/impl/conversion_impl.h>
#include <hgraph/lib/std/operators/impl/higher_order_impl.h>
#include <hgraph/lib/std/operators/impl/io_impl.h>
#include <hgraph/lib/std/operators/impl/logical_impl.h>
#include <hgraph/lib/std/operators/impl/string_impl.h>
#include <hgraph/lib/std/operators/impl/temporal_impl.h>

namespace hgraph::stdlib
{
    /**
     * Register the standard operator overloads. Call once per registry lifetime (e.g. at
     * application / Python-module startup, or at the top of a test that uses these
     * operators). Registration order is fixed and deterministic.
     */
    inline void register_standard_operators()
    {
        register_arithmetic_operators();
        register_comparison_operators();
        register_logical_operators();
        register_container_operators();
        register_conversion_operators();
        register_collection_operators();
        register_higher_order_operators();
        register_io_operators();
        register_string_operators();
        register_temporal_operators();
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_OPERATORS_IMPL_H
