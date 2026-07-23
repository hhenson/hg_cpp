import datetime as dt

import pytest

import hgraph as hg
from hgraph.test import eval_node


def test_temporal_values_are_immutable_hashable_native_scalars():
    civil = hg.CivilDateTime(dt.date(2026, 7, 23), dt.time(12, 34, 56, 789))
    period = hg.Period(years=1, months=2, days=-3)
    zone = hg.ZoneId("America/New_York")

    assert (civil.year, civil.month, civil.day) == (2026, 7, 23)
    assert (civil.hour, civil.minute, civil.second, civil.microsecond) == (
        12,
        34,
        56,
        789,
    )
    assert civil.weekday() == 3
    assert civil.isoweekday() == 4
    assert (period.total_months, period.years, period.months, period.days) == (
        14,
        1,
        2,
        -3,
    )
    assert str(zone) == "America/New_York"
    assert zone == hg.ZoneId("America/New_York")
    assert len({civil, civil}) == 1
    assert len({period, period}) == 1
    assert len({zone, hg.ZoneId("America/New_York")}) == 1

    with pytest.raises(AttributeError):
        civil.year = 2025

    @hg.compute_node
    def identity(value: hg.TS[hg.CivilDateTime]) -> hg.TS[hg.CivilDateTime]:
        return value.value

    assert eval_node(identity, [civil]) == [civil]


def test_range_normalization_algebra_and_python_sequence_contract():
    t0 = dt.datetime(2026, 1, 1)
    t1 = t0 + dt.timedelta(hours=1)
    t2 = t1 + dt.timedelta(hours=1)
    left = hg.InstantRange(t0, t1)
    right = hg.InstantRange(t1, t2)

    assert left.contains(t0)
    assert not left.contains(t1)
    assert hg.InstantRange(t0, t2).contains(left)
    assert left.adjacent(right)
    assert left.mergeable(right)
    assert left.merge(right) == hg.InstantRange(t0, t2)
    assert left.intersection(right).is_empty

    pieces = hg.InstantRange(t0, t2).difference(
        hg.InstantRange(
            t0 + dt.timedelta(minutes=15),
            t1 + dt.timedelta(minutes=45),
        )
    )
    assert len(pieces) == 2
    assert list(pieces) == [
        hg.InstantRange(t0, t0 + dt.timedelta(minutes=15)),
        hg.InstantRange(t1 + dt.timedelta(minutes=45), t2),
    ]
    assert pieces == hg.InstantRangeSet(list(reversed(list(pieces))))
    assert hash(pieces) == hash(hg.InstantRangeSet(list(pieces)))


def test_checked_graph_arithmetic_uses_static_policy_selection():
    jan_31 = dt.date(2025, 1, 31)
    one_month = hg.Period(months=1)

    assert eval_node(
        hg.add_,
        [jan_31],
        [one_month],
        month_end_policy=hg.MonthEndPolicy.CLAMP,
    ) == [dt.date(2025, 2, 28)]
    with pytest.raises(Exception, match="month|day"):
        eval_node(hg.add_, [jan_31], [one_month])

    half_seconds = [
        dt.timedelta(microseconds=500_000),
        dt.timedelta(microseconds=1_500_000),
        dt.timedelta(microseconds=-500_000),
        dt.timedelta(microseconds=-1_500_000),
    ]
    assert eval_node(
        hg.temporal_round,
        half_seconds,
        [dt.timedelta(seconds=1)] * len(half_seconds),
    ) == [
        dt.timedelta(0),
        dt.timedelta(seconds=2),
        dt.timedelta(0),
        dt.timedelta(seconds=-2),
    ]


def test_zone_resolution_uses_the_global_state_provider():
    zone = hg.ZoneId("America/New_York")
    instant = dt.datetime(2025, 1, 15, 12)

    with hg.GlobalContext(hg.GlobalState()):
        hg.set_time_zone_provider()
        zoned = eval_node(hg.at_zone, [instant], [zone])[0]
        assert zoned.instant == instant
        assert zoned.zone == zone
        assert zoned.offset_seconds == -5 * 60 * 60
        assert zoned.civil == hg.CivilDateTime(dt.date(2025, 1, 15), 7)
        with pytest.raises(TypeError):
            hg.ZonedDateTime(instant, zone, zoned.offset_seconds)

        fold = hg.CivilDateTime(dt.date(2025, 11, 2), 1, 30)
        earliest = eval_node(
            hg.resolve_civil,
            [fold],
            [zone],
            ambiguous=hg.AmbiguousTimePolicy.EARLIEST,
            nonexistent=hg.NonexistentTimePolicy.REJECT,
        )[0]
        latest = eval_node(
            hg.resolve_civil,
            [fold],
            [zone],
            ambiguous=hg.AmbiguousTimePolicy.LATEST,
            nonexistent=hg.NonexistentTimePolicy.REJECT,
        )[0]
        assert latest.instant - earliest.instant == dt.timedelta(hours=1)


def test_temporal_schema_identity_is_available_to_python_wiring():
    assert hg.TS[hg.CivilDateTime] != hg.TS[dt.datetime]
    assert hg.TS[hg.Period] != hg.TS[dt.timedelta]
    assert hg.TS[hg.InstantRange] != hg.TS[hg.CivilDateRange]
    assert hg.TSD[hg.ZoneId, hg.TS[hg.ZonedDateTime]]
