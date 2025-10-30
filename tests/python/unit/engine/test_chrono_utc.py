"""Test UTC-aware datetime conversion in chrono.h"""
import hg_cpp  # Use C++ implementation
from datetime import datetime, timezone, timedelta
from hgraph import graph, TS, compute_node, MIN_ST, MIN_TD
from hgraph.test import eval_node


def test_naive_datetime_as_utc():
    """Test that naive datetimes are treated as UTC"""
    
    @compute_node
    def return_time(ts: TS[int]) -> TS[datetime]:
        # MIN_ST is a naive datetime that should be treated as UTC
        return MIN_ST
    
    result = eval_node(return_time, [1])
    # Should get back the same time, now as UTC-aware
    assert result == [MIN_ST]


def test_utc_aware_datetime():
    """Test that UTC-aware datetimes are preserved"""
    
    # Create a UTC-aware datetime
    utc_time = datetime(2020, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    
    @compute_node  
    def return_time(ts: TS[int]) -> TS[datetime]:
        return utc_time
    
    result = eval_node(return_time, [1])
    assert len(result) == 1
    # The result should be timezone-aware
    returned_time = result[0]
    if returned_time.tzinfo is not None:
        # If timezone aware, should be UTC
        assert returned_time.tzinfo == timezone.utc
        # Should represent the same moment in time
        assert returned_time == utc_time
    else:
        # If naive (fallback), should still represent the same time
        # when interpreted as UTC
        assert returned_time.replace(tzinfo=timezone.utc) == utc_time


def test_datetime_roundtrip():
    """Test that datetime values round-trip correctly through C++"""
    
    test_times = [
        datetime(1970, 1, 1, 0, 0, 0),  # Unix epoch, naive
        datetime(2020, 6, 15, 14, 30, 45, 123456),  # Random time with microseconds
        datetime(2024, 12, 31, 23, 59, 59, 999999),  # End of year
        MIN_ST,  # Minimum start time
        MIN_ST + MIN_TD,  # One tick after minimum
    ]
    
    for test_time in test_times:
        @compute_node
        def echo_time(ts: TS[int]) -> TS[datetime]:
            return test_time
        
        result = eval_node(echo_time, [1])
        assert len(result) == 1
        
        returned_time = result[0]
        
        # Check that the time is preserved (ignoring timezone differences)
        # The times should represent the same moment
        if returned_time.tzinfo is None:
            # If returned as naive, treat it as UTC for comparison
            returned_utc = returned_time.replace(tzinfo=timezone.utc)
        else:
            returned_utc = returned_time
            
        if test_time.tzinfo is None:
            test_utc = test_time.replace(tzinfo=timezone.utc)
        else:
            test_utc = test_time
        
        # Allow small tolerance for floating point conversion (1 microsecond)
        time_diff = abs((returned_utc - test_utc).total_seconds())
        assert time_diff < 0.000001, f"Time difference too large: {time_diff}s for {test_time}"


def test_different_timezone():
    """Test datetime with non-UTC timezone"""
    
    # Create a datetime in a different timezone (EST = UTC-5)
    est = timezone(timedelta(hours=-5))
    est_time = datetime(2020, 1, 1, 12, 0, 0, tzinfo=est)
    
    @compute_node
    def return_time(ts: TS[int]) -> TS[datetime]:
        return est_time
    
    result = eval_node(return_time, [1])
    assert len(result) == 1
    
    returned_time = result[0]
    
    # The returned time should represent the same moment (17:00 UTC)
    expected_utc = est_time.astimezone(timezone.utc)
    
    if returned_time.tzinfo is None:
        # If naive, it should be in UTC already
        returned_utc = returned_time.replace(tzinfo=timezone.utc)
    else:
        returned_utc = returned_time
    
    # Should be the same moment in time
    time_diff = abs((returned_utc - expected_utc).total_seconds())
    assert time_diff < 0.000001, f"Time difference too large: {time_diff}s"


if __name__ == "__main__":
    test_naive_datetime_as_utc()
    test_utc_aware_datetime()
    test_datetime_roundtrip()
    test_different_timezone()
    print("All tests passed!")
