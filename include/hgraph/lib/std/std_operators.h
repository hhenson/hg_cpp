#ifndef HGRAPH_LIB_STD_STD_OPERATORS_H
#define HGRAPH_LIB_STD_STD_OPERATORS_H

/**
 * Convenience umbrella for the standard library operators.
 *
 * - The operator **definitions** (the abstract ``Operator<>`` markers) live under
 *   ``include/hgraph/lib/std/operators/``, grouped by family, and are aggregated by
 *   ``operators/operators.h``.
 * - The operator **implementations** + registration live under
 *   ``include/hgraph/lib/std/operators/impl/`` — each definition file ``<family>.h`` has a
 *   matching ``impl/<family>_impl.h`` — and are aggregated (with
 *   ``register_standard_operators``) by ``operators/impl/operators_impl.h``.
 *
 * Including this header pulls in both, plus the opt-in wiring expression syntax under
 * ``hgraph::stdlib::syntax``. See ``docs/source/developer_guide/operators.rst``.
 */

#include <hgraph/lib/std/lifted_kernels.h>
#include <hgraph/lib/std/operators/impl/operators_impl.h>
#include <hgraph/lib/std/operators/operators.h>
#include <hgraph/lib/std/operators/syntax.h>
#include <hgraph/types/lift.h>

#endif  // HGRAPH_LIB_STD_STD_OPERATORS_H
