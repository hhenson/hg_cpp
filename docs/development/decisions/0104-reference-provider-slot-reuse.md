# Reference provider slot reuse

Status: accepted

## Context

A reference provider kept each representation and each subscriber group in a
vector that only grew. Removing an item reference cleared its slots without
reusing them. A graph that replaced per-item representations every cycle grew
provider storage without bound.

## Decision

The provider reuses the lowest released representation index and subscriber
group index through `util::occupancy_index`. A representation receives its
index when it is created, so `resolve_representation` stays correct after
reuse.

The provider tears representations down from the newest to the oldest through
an intrusive list. This is required because a representation's base reference
can point into storage that an older representation owns, for example an item
reference held by a TSD reference representation. Neither index order nor the
base-reference chain gives that order after slot reuse.

## Consequences

Provider storage is proportional to the peak number of live representations
and subscriber groups. See
[time series references](../design/time-series-references.md).
