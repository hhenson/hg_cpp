# hgraph comparative suite results

Each directory holds one run of the
[hgraph comparative suite](../../../bindings/python/benchmarks/hgraph_suite/README.md),
named by the HG source commit. Each platform has a timing report (5 samples per cell)
and a memory report (3 samples per cell), as Markdown and as raw JSON.

## 02e0a682 against hgraph 7e45748c9

| Platform | CPU | Python |
| --- | --- | --- |
| macOS 27 arm64 | Apple M2 Ultra | 3.14.7 |
| Linux 7.0 x86_64 | AMD Ryzen Threadripper 9980X | 3.14.4 |
| Windows 11 AMD64 | AMD Ryzen AI Max+ 395 | 3.14.7 |

Ratios are hgraph/HG medians. A ratio above 1 means HG is faster or grows less.

| Measure | macOS | Linux | Windows |
| --- | ---: | ---: | ---: |
| Timing geometric mean, 70 scenarios | 0.47 | 0.57 | 0.39 |
| Memory growth geometric mean, 47 profiles | 1.63 | 1.05 | 1.22 |

HG imports with about 39 MB RSS; hgraph imports with about 75 MB.

### Coverage

HG runs 70 of the 82 hgraph scenarios. 55 use the same nodes and operator semantics,
12 are adapted, and 3 run a matched workload. The reports list each adaptation.
HG cannot express 12 scenarios:

| Scenario | Missing capability |
| --- | --- |
| `python_global_state_boundary` | `GlobalState` in Python |
| `python_owned_construct_python` | `combine` of a Python object scalar |
| `type_tsw_append_evict_std` | tick-window time series |
| `reduce_fixed_tsl_ordered_std` | `reduce` over TSL or with an ordered reducer |
| `reduce_dynamic_tsl_std` | unbounded TSL |
| `service_subscription_std`, `_py` | subscription keys that are not `final` |
| `adaptor_std`, `_py`, `service_adaptor_std`, `_py` | adaptor API |
| `audit_json_roundtrip_std` | JSON operators |

### Defects found

`reduce` over a `map` output loses updates. When keys in different reduction subtrees
change in the same cycle, only one change reaches the result. A four-key TSD with keys
0 and 2 set to 1 in one cycle gives 1 instead of 2:

```python
@hg.graph
def ident(v: TS[int]) -> TS[int]:
    return v + 0

hg.reduce(op.add, hg.map(ident, values), 0)  # wrong
hg.reduce(op.add, values, 0)                 # correct
op.sum_(hg.map(ident, values))               # correct
```

The same path is slow and retains memory:

| Scenario | hgraph/HG time | Effect |
| --- | ---: | --- |
| `tsd_capacity_growth_std` | 0.003 | 8.9 s against 0.024 s on macOS |
| `tsd_sparse_large_capacity_std` | 0.02–0.04 | crashes with an access violation on Windows |
| `tsd_sparse_std`, `tsd_churn_std`, `tsd_two_input_*`, `tsd_explicit_key_set_std`, `tsd_key_reactivation_std` | 0.02–0.15 | |
| `tsd_clear_repopulate_std` | 0.43–0.66 | growth rises from 4.5 MB to 29.8 MB with duration |
| `tsd_churn_std` | 0.10–0.12 | growth rises from 2.2 MB to 4.6 MB with duration |

In the same scenarios, the map-only stage is 1.8–2.8 times faster than hgraph, and the
reduce-only stage is close to parity. Memory stays flat for both stages.

On Windows, `reduce` over a `map` of 20,000 keys exits with an access violation, at
500 cycles as at 5,000. 6,000 keys complete. `map` alone and `reduce` alone complete with
20,000 keys.

### Behaviour differences

- `switch_` evaluates its key at graph start. An invalid key without a `DEFAULT` case
  raises `key does not match a case`.
- `map` treats any unmatched keyword TSS as the membership set and does not check its
  name.
- `mesh` invalidates the input from a removed predecessor; hgraph keeps its last value.
  `mesh_std` therefore totals 2696 in HG and 3422 in hgraph.

### Timing by area

| Area | hgraph/HG |
| --- | ---: |
| `switch_` with alternating branch sizes | 3.8–4.3 |
| `map` only, sparse and churn | 1.8–2.8 |
| Construction of higher-order and dispatch graphs | 1.3–2.3 |
| Scheduler fan-out, fan-in, conflation | 1.3–1.9 |
| Request-reply services | 1.2–1.9 |
| TSS add and remove | 1.0–1.1 |
| Dense TSD | 0.2–1.1 |
| Per-node scalar cost (`tick_*`, `type_int_std`, `type_str_std`) | 0.2–0.7 |
| Python boundary | 0.4–0.9 |
| Bundle scalars (`type_cs_*`, `python_owned_*_native`) | 0.1–0.4 |
