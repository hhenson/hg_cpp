RFC 0002: Temporal Types, Zones, and Ranges
===========================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-07-23
:Target: Incremental core foundation

Summary
-------

Distinguish absolute engine time, civil time, named-zone interpretation,
calendar-relative periods, and time ranges in the core type system.
Domain-specific calendars, sessions, and scheduling policy remain extension
concerns.

Type model
----------

``Instant`` and ``Duration``
   UTC timeline values. Existing ``DateTime`` and ``TimeDelta`` remain
   compatibility aliases.

``CivilDate``, ``CivilTime``, and ``CivilDateTime``
   Calendar fields with no zone or offset.

``Period``
   Calendar-relative years, months, and days; it is not implicitly convertible
   to an elapsed ``Duration``.

``ZoneId``
   A validated named-zone identifier.

``ZonedDateTime``
   An instant paired with zone identity and the resolved offset.

``TimeRange[T]``
   An ordered interval with explicit open/closed endpoints and deterministic
   containment, emptiness, intersection, and adjacency.

Zone resolution
---------------

Civil-time resolution must explicitly select ``reject``, ``earliest``, or
``latest`` for an ambiguous fold, and ``reject``, ``next_valid``, or
``previous_valid`` for a nonexistent gap. The default is rejection. A process
or platform local zone must never be inferred silently.

Ownership
---------

``hg_cpp`` owns the general scalar schemas, interval algebra, and eventual
timezone-resolution boundary. Downstream libraries own business dates,
calendars, sessions, day-count conventions, and domain-specific iteration
policy.

Compatibility and portability
-----------------------------

The aliases preserve current engine APIs. A timezone backend must be pinned and
must report its database version for reproducible serialization and replay.
RFC 9557 provides the target form for serializing a timestamp together with
offset and named-zone intent.

Acceptance criteria
-------------------

* Native scalar and TS schemas have C++/Python parity.
* Range construction and boundary algebra are tested.
* Named-zone resolution covers folds, gaps, historical transitions, and every
  explicit policy.
* Duration-versus-period behaviour is tested across DST changes.
* C++ and Python round trips preserve offset and named-zone identity.

Implementation status
---------------------

The proposed initial value/type slice covers aliases, civil values, ``Period``,
syntactic ``ZoneId`` validation, ``ZonedDateTime``, policy enums, and
``TimeRange`` algebra. Timezone database resolution, RFC 9557 serialization,
range iteration/merge, and calendar arithmetic remain later work. Status
remains ``Proposed`` until the accepted implementation scope is merged.

References
----------

* RFC 9557, *Date and Time on the Internet: Timestamps with Additional
  Information*, 2024: https://www.rfc-editor.org/rfc/rfc9557
