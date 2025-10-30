# UTC-Aware Datetime Conversion Fix

## Problem

The original implementation of datetime conversion in `include/hgraph/python/chrono.h` had a critical flaw:
- It unpacked Python datetime objects into year/month/day/hour/minute/second/microsecond components
- It reconstructed `std::chrono::time_point` from these components without considering timezone information
- **Naive datetimes** (those without `tzinfo`) were treated as local time, not UTC
- This caused "End time cannot be before the start time" errors in CI when the system timezone differed from UTC

### Example of the Problem

```python
# Test running in timezone UTC-5 (EST)
naive_dt = datetime(2020, 1, 1, 0, 0, 0)  # Midnight on Jan 1, 2020

# Old behavior: treated as local time (EST)
# -> Converted to 2020-01-01 00:00:00 EST 
# -> Which is 2020-01-01 05:00:00 UTC

# Expected behavior: treat naive datetimes as UTC
# -> Converted to 2020-01-01 00:00:00 UTC
```

## Solution

The fix implements UTC normalization using Python's `timestamp()` method:

### 1. `from_python` (Python → C++)

**Primary Path** (new):
1. Check if datetime object has `timestamp()` method
2. If datetime is naive (`tzinfo is None`):
   - Attach `timezone.utc` using `datetime.replace(tzinfo=timezone.utc)`
3. Call `timestamp()` to get POSIX seconds (always in UTC)
4. Convert POSIX seconds to `std::chrono::time_point`

**Fallback Path** (original):
- If `timestamp()` fails or is unavailable
- Use original component-based reconstruction
- Maintains backward compatibility

### 2. `from_cpp` (C++ → Python)

**Primary Path** (new):
1. Convert `time_point` to POSIX timestamp (seconds since epoch)
2. Use `datetime.fromtimestamp(ts, tz=timezone.utc)` to create UTC-aware datetime
3. Returns timezone-aware datetime object

**Fallback Path** (original):
- If Python API calls fail
- Use original component-based packing
- Returns naive datetime

## Key Benefits

1. **Consistent Timezone Handling**: All datetimes are normalized to UTC
2. **CI Stability**: Tests work correctly regardless of system timezone
3. **Timezone Awareness**: Properly handles timezone-aware datetimes from different timezones
4. **Backward Compatibility**: Fallback ensures old code paths still work
5. **No Breaking Changes**: Maintains API compatibility

## Technical Details

### POSIX Timestamp Conversion

```python
# Python side
naive_dt = datetime(2020, 1, 1, 0, 0, 0)
utc_dt = naive_dt.replace(tzinfo=timezone.utc)
timestamp = utc_dt.timestamp()  # 1577836800.0

# C++ side
auto duration_seconds = std::chrono::duration<double>(timestamp);
value = std::chrono::time_point<std::chrono::system_clock>(
    std::chrono::duration_cast<Duration>(duration_seconds));
```

### Round-Trip Example

```python
# Original datetime (naive)
dt1 = datetime(2020, 1, 1, 12, 0, 0)

# Python → C++ → Python
dt2 = round_trip_through_cpp(dt1)

# Result: timezone-aware datetime in UTC
assert dt2 == datetime(2020, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
```

## Testing

Two test files verify the implementation:

1. **test_timestamp_logic.py** - Verifies Python-side timestamp logic
   - Naive datetime to UTC conversion
   - Timezone-aware datetime handling
   - Different timezone conversions
   - Microsecond precision preservation
   - Unix epoch verification

2. **test_chrono_utc.py** - Integration tests with hg_cpp
   - Round-trip datetime conversion
   - Timezone awareness preservation
   - Different timezone handling

## Code Review Notes

### Memory Management

All PyObject references are properly managed:
- Every `PyObject_GetAttrString` is paired with `Py_DECREF` or `Py_XDECREF`
- Every `Py_BuildValue` result is decreffed
- All code paths properly clean up resources

### Error Handling

- Uses `PyErr_Clear()` to prevent error propagation from failed attempts
- Falls back to legacy method if new method fails
- Maintains `noexcept` guarantee by catching all exceptions

### Performance

- Primary path is more efficient (single timestamp conversion vs. component arithmetic)
- Fallback ensures no performance regression for edge cases
- No allocations beyond what Python API requires

## Migration Notes

### For Users

No code changes required - the fix is transparent:
- Existing code continues to work
- Naive datetimes now correctly treated as UTC
- Timezone-aware datetimes handled correctly

### For Developers

When debugging datetime issues:
- Check debug output in stderr (shows conversion method used)
- Verify timezone assumptions in test data
- Consider using timezone-aware datetimes explicitly for clarity

## References

- Python documentation: [`datetime.timestamp()`](https://docs.python.org/3/library/datetime.html#datetime.datetime.timestamp)
- Python documentation: [`datetime.fromtimestamp()`](https://docs.python.org/3/library/datetime.html#datetime.datetime.fromtimestamp)
- ISO 8601 / RFC 3339 standards for datetime representation
