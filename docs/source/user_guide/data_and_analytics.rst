Data And Analytics
==================

Shaped arrays
-------------

``Array[T, Size[N], ...]`` is a native scalar schema. Each dimension is kept
in the C++ type record and contributes to the planned value layout. For
example, the C++ spelling of ``TS[Array[float, Size[2], Size[3]]]`` is
``TS<ArrayOf<Float, 2, 3>>``. Fixed arrays use inline planned storage with a
logical extent no larger than the declared leading-dimension capacity. This
allows a fixed tick window's value to expose its shorter warm-up prefix without
allocating a dynamic container. C++ writers set that extent with
``MutableListView::resize`` before filling a newly constructed array;
``Array[T]`` and ``Array[T, Size[-1]]`` describe an unbounded one-dimensional
array backed by the compact list representation.

The Python boundary accepts rectangular ``numpy.ndarray`` values and returns
an ndarray. Values larger than a declared dimension are rejected instead of
being flattened or silently reinterpreted. The C++ runtime and operator kernels
do not depend on NumPy.

Scientific operators (``hgraph.numpy_``)
----------------------------------------

The ``hgraph.numpy_`` name retains the familiar vocabulary used by existing
Python code. It is a native scientific-computation surface rather than a
promise to reproduce every NumPy edge-case or historical quirk. The complete
public catalogue delegates to C++ operators:

* ``as_array`` converts a fixed tick window and pads an early-valid window;
* ``get_item`` accepts an integer or integer tuple and resolves slice shape;
* ``cumsum`` flattens when ``axis`` is omitted and preserves shape for an axis;
* ``corrcoef`` accepts one- or two-dimensional numeric arrays and an optional
  second array; and
* ``quantile`` accepts an array or window and a scalar quantile.

The numeric kernels support ``int`` and ``float`` leaves. Quantile and standard
deviation delegate to Arrow Compute, while correlation delegates to Boost.Math;
their documented backend behaviour defines numerical edge cases. Arrow null
results are represented as floating-point NaN because the result time series
has a non-nullable scalar schema. Cumulative sum follows the hgraph array shape
and uses defined two's-complement wrapping for integer overflow.

``quantile`` supports ``linear``, ``lower``, ``higher``, ``midpoint``, and
``nearest`` interpolation; other methods fail explicitly. Its public result
remains a scalar, so ``keepdims`` is accepted for call compatibility but does
not change the result schema. ``as_array`` is limited to fixed tick windows;
duration windows have no fixed output shape.

The ``hgraph.nodes`` compatibility surface also provides native-backed
``np_rolling_window``, ``np_quantile``, ``np_std``, ``pct_change``,
``rolling_window``, and ``rolling_average``. A rolling window whose
``min_window_period`` is smaller than its capacity emits shorter ndarrays while
warming up. Those fields use an unbounded array dimension so the runtime schema
truthfully describes the values; the old Python implementation declared a
fixed dimension while returning shorter arrays.

Dataframes and series
---------------------

``Frame`` and ``Series[T]`` use Arrow-native storage. Sorting, concatenation,
joins, structural filtering, grouping, ungrouping, column replacement and
projection execute in C++. Python expression filters remain a Python-owned
compatibility path because ``pyarrow.compute.Expression`` is itself a Python
scalar. Series-to-tuple conversion is native and represents Arrow nulls as
unset tuple elements.
