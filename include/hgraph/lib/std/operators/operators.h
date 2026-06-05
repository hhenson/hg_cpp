#ifndef HGRAPH_LIB_STD_OPERATORS_OPERATORS_H
#define HGRAPH_LIB_STD_OPERATORS_OPERATORS_H

/**
 * The standard operator **definitions** — the abstract ``Operator<>`` markers for the
 * ``hgraph`` standard library, grouped by family. These declare the operator names and
 * documentary signatures only; the implementations are registered separately (see
 * ``register_standard_operators`` in ``<hgraph/lib/std/std_operators.h>``).
 *
 * The catalogue mirrors the Python ``hgraph`` operators under
 * ``ext/2603/hgraph/_operators/``. A handful are intentionally **not** defined here yet:
 *
 * - ``const_`` / ``debug_print`` / ``null_sink`` already exist as concrete nodes in
 *   ``<hgraph/lib/std/std_nodes.h>`` (node-vs-operator reconciliation is a later step);
 * - the JSON / table / data-frame conversion family needs scalar value types
 *   (``Frame`` / ``JSON`` / ``TABLE``) the value layer does not model yet.
 *
 * See ``docs/source/developer_guide/operators.rst``.
 */

#include <hgraph/lib/std/operators/arithmetic.h>
#include <hgraph/lib/std/operators/collection.h>
#include <hgraph/lib/std/operators/comparison.h>
#include <hgraph/lib/std/operators/container.h>
#include <hgraph/lib/std/operators/control.h>
#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/operators/io.h>
#include <hgraph/lib/std/operators/logical.h>
#include <hgraph/lib/std/operators/stream.h>
#include <hgraph/lib/std/operators/string.h>
#include <hgraph/lib/std/operators/temporal.h>

#endif  // HGRAPH_LIB_STD_OPERATORS_OPERATORS_H
