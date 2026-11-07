# Nested reduce tree

Status: accepted

## Context

Nested reduce arranged reducer graphs in adjacent pairs by TSD iteration order
and rebuilt that graph set after each membership change. For a TSD of
references, such as map output, a rebuild after a sparse change repeated
unchanged work, and retained storage grew with the number of removed keys.

## Decision

Nested reduce uses the Patricia tree of [compact reduce](0040-compact-reduce-tree.md),
keyed by physical TSD item IDs. Each branch owns one nested reducer graph,
which starts when the branch is first evaluated. TSD deltas and reducer graph
scheduling mark the path to the root, and only marked branches evaluate.

Leaves read item references from an added `items` input: a TSD of references
over the source TSD. This is required to keep each item reference valid while a
reducer graph subscribes to it; the TSD of references owns item-reference
retirement. See [time series references](../design/time-series-references.md).

## Consequences

Sparse item changes and membership changes cost work proportional to the
affected paths. Compact and nested reduce share tree maintenance. Removed
branches stop their graphs and release them at the next outer cycle.

See [reduce](../design/reduce.md).
