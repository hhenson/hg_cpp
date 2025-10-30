#!/usr/bin/env python3
"""
Test script to verify UTC timestamp conversion logic.

This tests the Python side of the datetime <-> timestamp conversion 
to verify the logic is correct before building the C++ code.
"""

from datetime import datetime, timezone, timedelta


def test_naive_to_utc_timestamp():
    """Test converting a naive datetime to UTC timestamp"""
    # Create a naive datetime
    naive_dt = datetime(2020, 1, 1, 12, 0, 0)
    print(f"Naive datetime: {naive_dt}")
    print(f"  tzinfo: {naive_dt.tzinfo}")
    
    # Attach UTC timezone
    utc_dt = naive_dt.replace(tzinfo=timezone.utc)
    print(f"UTC-aware datetime: {utc_dt}")
    print(f"  tzinfo: {utc_dt.tzinfo}")
    
    # Get timestamp
    timestamp = utc_dt.timestamp()
    print(f"Timestamp: {timestamp}")
    
    # Verify round-trip
    recovered = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    print(f"Recovered: {recovered}")
    assert recovered == utc_dt
    print("✓ Naive to UTC timestamp works\n")


def test_utc_aware_timestamp():
    """Test converting a UTC-aware datetime to timestamp"""
    # Create a UTC-aware datetime
    utc_dt = datetime(2020, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    print(f"UTC-aware datetime: {utc_dt}")
    
    # Get timestamp
    timestamp = utc_dt.timestamp()
    print(f"Timestamp: {timestamp}")
    
    # Verify round-trip
    recovered = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    print(f"Recovered: {recovered}")
    assert recovered == utc_dt
    print("✓ UTC-aware timestamp works\n")


def test_different_timezone():
    """Test converting datetime in different timezone"""
    # Create datetime in EST (UTC-5)
    est = timezone(timedelta(hours=-5))
    est_dt = datetime(2020, 1, 1, 12, 0, 0, tzinfo=est)
    print(f"EST datetime: {est_dt}")
    print(f"  In UTC: {est_dt.astimezone(timezone.utc)}")
    
    # Get timestamp (always in UTC)
    timestamp = est_dt.timestamp()
    print(f"Timestamp: {timestamp}")
    
    # Recover as UTC
    recovered_utc = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    print(f"Recovered (UTC): {recovered_utc}")
    
    # Should match the UTC version
    assert recovered_utc == est_dt.astimezone(timezone.utc)
    print("✓ Different timezone conversion works\n")


def test_timestamp_comparison():
    """Compare timestamps from different representations of same moment"""
    # All these represent the same moment in time
    utc_dt = datetime(2020, 1, 1, 17, 0, 0, tzinfo=timezone.utc)
    est_dt = datetime(2020, 1, 1, 12, 0, 0, tzinfo=timezone(timedelta(hours=-5)))
    naive_utc_dt = datetime(2020, 1, 1, 17, 0, 0).replace(tzinfo=timezone.utc)
    
    print("Three representations of the same moment:")
    print(f"  UTC: {utc_dt}")
    print(f"  EST: {est_dt}")
    print(f"  Naive->UTC: {naive_utc_dt}")
    
    # All should have the same timestamp
    utc_ts = utc_dt.timestamp()
    est_ts = est_dt.timestamp()
    naive_ts = naive_utc_dt.timestamp()
    
    print(f"\nTimestamps:")
    print(f"  UTC: {utc_ts}")
    print(f"  EST: {est_ts}")
    print(f"  Naive->UTC: {naive_ts}")
    
    assert utc_ts == est_ts == naive_ts
    print("✓ All timestamps match\n")


def test_microsecond_precision():
    """Test that microseconds are preserved"""
    dt_with_us = datetime(2020, 1, 1, 12, 0, 0, 123456, tzinfo=timezone.utc)
    print(f"Datetime with microseconds: {dt_with_us}")
    print(f"  Microseconds: {dt_with_us.microsecond}")
    
    timestamp = dt_with_us.timestamp()
    print(f"Timestamp: {timestamp:.6f}")
    
    recovered = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    print(f"Recovered: {recovered}")
    print(f"  Microseconds: {recovered.microsecond}")
    
    assert recovered == dt_with_us
    assert recovered.microsecond == dt_with_us.microsecond
    print("✓ Microseconds preserved\n")


def test_epoch():
    """Test Unix epoch conversion"""
    epoch = datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    print(f"Unix epoch: {epoch}")
    
    timestamp = epoch.timestamp()
    print(f"Timestamp: {timestamp}")
    
    assert timestamp == 0.0
    print("✓ Unix epoch timestamp is 0.0\n")


if __name__ == "__main__":
    print("Testing UTC timestamp conversion logic\n")
    print("=" * 60)
    
    test_naive_to_utc_timestamp()
    test_utc_aware_timestamp()
    test_different_timezone()
    test_timestamp_comparison()
    test_microsecond_precision()
    test_epoch()
    
    print("=" * 60)
    print("\n✅ All tests passed!")
    print("\nThis verifies the Python logic is correct.")
    print("The C++ implementation should produce the same results.")
