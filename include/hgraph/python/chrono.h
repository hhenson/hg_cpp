/*
    nanobind/stl/chrono.h: conversion between std::chrono and python's datetime

    Copyright (c) 2023 Hudson River Trading LLC <opensource@hudson-trading.com> and
                       Trent Houliston <trent@houliston.me> and
                       Wenzel Jakob <wenzel.jakob@epfl.ch>

    All rights reserved. Use of this source code is governed by a
    BSD-style license that can be found in the LICENSE file.
*/
#pragma once

#include <nanobind/nanobind.h>

#if !defined(__STDC_WANT_LIB_EXT1__)
#define __STDC_WANT_LIB_EXT1__ 1 // for gmtime_s
#endif
#include <time.h>

#include <chrono>
#include <cmath>
#include <ctime>
#include <limits>
#include <iostream>

#include <nanobind/stl/detail/chrono.h>

// Casts a std::chrono type (either a duration or a time_point) to/from
// Python timedelta objects, or from a Python float representing seconds.
template <typename type> class duration_caster {
public:
    using rep = typename type::rep;
    using period = typename type::period;
    using duration_t = std::chrono::duration<rep, period>;

    bool from_python(handle src, uint8_t /*flags*/, cleanup_list*) noexcept {
        namespace ch = std::chrono;

        if (!src) return false;

        // support for signed 25 bits is required by the standard
        using days = ch::duration<int_least32_t, std::ratio<86400>>;

        // If invoked with datetime.delta object, unpack it
        int dd, ss, uu;
        try {
            if (unpack_timedelta(src.ptr(), &dd, &ss, &uu)) {
                value = type(ch::duration_cast<duration_t>(
                                 days(dd) + ch::seconds(ss) + ch::microseconds(uu)));
                return true;
            }
        } catch (python_error& e) {
            e.discard_as_unraisable(src.ptr());
            return false;
        }

        // If invoked with a float we assume it is seconds and convert
        int is_float;
#if defined(Py_LIMITED_API)
        is_float = PyType_IsSubtype(Py_TYPE(src.ptr()), &PyFloat_Type);
#else
        is_float = PyFloat_Check(src.ptr());
#endif
        if (is_float) {
            value = type(ch::duration_cast<duration_t>(
                             ch::duration<double>(PyFloat_AsDouble(src.ptr()))));
            return true;
        }
        return false;
    }

    // If this is a duration just return it back
    static const duration_t& get_duration(const duration_t& src) {
        return src;
    }

    // If this is a time_point get the time_since_epoch
    template <typename Clock>
    static duration_t get_duration(
            const std::chrono::time_point<Clock, duration_t>& src) {
        return src.time_since_epoch();
    }

    static handle from_cpp(const type& src, rv_policy, cleanup_list*) noexcept {
        namespace ch = std::chrono;

        // Use overloaded function to get our duration from our source
        // Works out if it is a duration or time_point and get the duration
        auto d = get_duration(src);

        // Declare these special duration types so the conversions happen with the correct primitive types (int)
        using dd_t = ch::duration<int, std::ratio<86400>>;
        using ss_t = ch::duration<int, std::ratio<1>>;
        using us_t = ch::duration<int, std::micro>;

        auto dd = ch::duration_cast<dd_t>(d);
        auto subd = d - dd;
        auto ss = ch::duration_cast<ss_t>(subd);
        auto us = ch::duration_cast<us_t>(subd - ss);
        return pack_timedelta(dd.count(), ss.count(), us.count());
    }

    #if PY_VERSION_HEX < 0x03090000
        NB_TYPE_CASTER(type, io_name("typing.Union[datetime.timedelta, float]",
                                     "datetime.timedelta"))
    #else
        NB_TYPE_CASTER(type, io_name("datetime.timedelta | float",
                                     "datetime.timedelta"))
    #endif
};

// Cast between times on the system clock and datetime.datetime instances
// (also supports datetime.date and datetime.time for Python->C++ conversions)
template <typename Duration>
class type_caster<std::chrono::time_point<std::chrono::system_clock, Duration>> {
public:
    using type = std::chrono::time_point<std::chrono::system_clock, Duration>;
    bool from_python(handle src, uint8_t /*flags*/, cleanup_list*) noexcept {
        namespace ch = std::chrono;

        if (!src)
            return false;

        try {
            // Try to use timestamp() method for UTC normalization
            // This ensures timezone-aware datetimes are correctly converted
            PyObject* timestamp_method = PyObject_GetAttrString(src.ptr(), "timestamp");
            if (timestamp_method && PyCallable_Check(timestamp_method)) {
                // Check if datetime is naive (tzinfo is None)
                PyObject* tzinfo = PyObject_GetAttrString(src.ptr(), "tzinfo");
                bool is_naive = (tzinfo == Py_None);
                Py_XDECREF(tzinfo);
                
                PyObject* dt_to_convert = src.ptr();
                PyObject* utc_dt = nullptr;
                
                // If naive, treat as UTC by attaching timezone.utc
                if (is_naive) {
                    // Import datetime module
                    PyObject* datetime_module = PyImport_ImportModule("datetime");
                    if (datetime_module) {
                        // Get timezone class
                        PyObject* timezone_class = PyObject_GetAttrString(datetime_module, "timezone");
                        if (timezone_class) {
                            // Get timezone.utc
                            PyObject* utc_tz = PyObject_GetAttrString(timezone_class, "utc");
                            if (utc_tz) {
                                // Call replace(tzinfo=timezone.utc) on the datetime
                                PyObject* replace_method = PyObject_GetAttrString(src.ptr(), "replace");
                                if (replace_method) {
                                    PyObject* empty_args = PyTuple_New(0);
                                    PyObject* kwargs = Py_BuildValue("{s:O}", "tzinfo", utc_tz);
                                    utc_dt = PyObject_Call(replace_method, empty_args, kwargs);
                                    Py_DECREF(empty_args);
                                    Py_DECREF(kwargs);
                                    Py_DECREF(replace_method);
                                    if (utc_dt) {
                                        dt_to_convert = utc_dt;
                                    }
                                }
                                Py_DECREF(utc_tz);
                            }
                            Py_DECREF(timezone_class);
                        }
                        Py_DECREF(datetime_module);
                    }
                }
                
                // Call timestamp() to get POSIX seconds (always in UTC)
                PyObject* timestamp_result = PyObject_CallObject(timestamp_method, nullptr);
                Py_DECREF(timestamp_method);
                
                if (timestamp_result) {
                    double posix_seconds = PyFloat_AsDouble(timestamp_result);
                    Py_DECREF(timestamp_result);
                    if (utc_dt) {
                        Py_DECREF(utc_dt);
                    }
                    
                    if (posix_seconds != -1.0 || !PyErr_Occurred()) {
                        // Convert POSIX seconds to time_point
                        auto duration_seconds = ch::duration<double>(posix_seconds);
                        value = type(ch::duration_cast<Duration>(duration_seconds));
                        
                        // Debug output
                        auto us_since_epoch = ch::duration_cast<ch::microseconds>(value.time_since_epoch()).count();
                        std::cerr << "[chrono.h] Converted using timestamp(): " << posix_seconds 
                                  << " seconds (" << us_since_epoch << " us since epoch)" << std::endl;
                        
                        return true;
                    }
                }
                
                if (utc_dt) {
                    Py_DECREF(utc_dt);
                }
                // Clear any Python errors from timestamp attempt
                PyErr_Clear();
            } else {
                Py_XDECREF(timestamp_method);
            }
        } catch (...) {
            // Fall through to legacy method
        }

        // Fallback: use legacy unpack_datetime method
        int yy, mon, dd, hh, min, ss, uu;
        try {
            if (!unpack_datetime(src.ptr(), &yy, &mon, &dd,
                                 &hh, &min, &ss, &uu)) {
                return false;
            }
        } catch (python_error& e) {
            e.discard_as_unraisable(src.ptr());
            return false;
        }
        std::chrono::year_month_day ymd{std::chrono::year{yy}, std::chrono::month{static_cast<unsigned>(mon)}, std::chrono::day{static_cast<unsigned>(dd)}};
        std::chrono::sys_days date_part = ymd;
        auto time_part = std::chrono::hours{hh} + std::chrono::minutes{min} + std::chrono::seconds{ss} + std::chrono::microseconds{uu};

        value = date_part + time_part;

        // Debug output for CI debugging
        auto us_since_epoch = std::chrono::duration_cast<std::chrono::microseconds>(value.time_since_epoch()).count();
        std::cerr << "[chrono.h] Converted (fallback) Python datetime(" << yy << "-" << mon << "-" << dd << " "
                  << hh << ":" << min << ":" << ss << "." << uu << ") to " << us_since_epoch << " us since epoch" << std::endl;

        return true;
    }

    static handle from_cpp(const type& src, rv_policy, cleanup_list*) noexcept {
        namespace ch = std::chrono;

        // Try to create a UTC-aware datetime using fromtimestamp
        try {
            // Convert to POSIX timestamp (seconds since Unix epoch)
            auto duration_since_epoch = src.time_since_epoch();
            auto seconds_since_epoch = ch::duration_cast<ch::duration<double>>(duration_since_epoch);
            double posix_timestamp = seconds_since_epoch.count();
            
            // Import datetime module
            PyObject* datetime_module = PyImport_ImportModule("datetime");
            if (datetime_module) {
                // Get datetime class
                PyObject* datetime_class = PyObject_GetAttrString(datetime_module, "datetime");
                if (datetime_class) {
                    // Get timezone class
                    PyObject* timezone_class = PyObject_GetAttrString(datetime_module, "timezone");
                    if (timezone_class) {
                        // Get timezone.utc
                        PyObject* utc_tz = PyObject_GetAttrString(timezone_class, "utc");
                        if (utc_tz) {
                            // Call datetime.fromtimestamp(posix_timestamp, tz=timezone.utc)
                            PyObject* fromtimestamp_method = PyObject_GetAttrString(datetime_class, "fromtimestamp");
                            if (fromtimestamp_method) {
                                PyObject* args = Py_BuildValue("(d)", posix_timestamp);
                                PyObject* kwargs = Py_BuildValue("{s:O}", "tz", utc_tz);
                                PyObject* result = PyObject_Call(fromtimestamp_method, args, kwargs);
                                Py_DECREF(args);
                                Py_DECREF(kwargs);
                                Py_DECREF(fromtimestamp_method);
                                Py_DECREF(utc_tz);
                                Py_DECREF(timezone_class);
                                Py_DECREF(datetime_class);
                                Py_DECREF(datetime_module);
                                
                                if (result) {
                                    return handle(result);
                                } else {
                                    PyErr_Clear();
                                }
                            } else {
                                Py_DECREF(utc_tz);
                                Py_DECREF(timezone_class);
                                Py_DECREF(datetime_class);
                                Py_DECREF(datetime_module);
                            }
                        } else {
                            Py_DECREF(timezone_class);
                            Py_DECREF(datetime_class);
                            Py_DECREF(datetime_module);
                        }
                    } else {
                        Py_DECREF(datetime_class);
                        Py_DECREF(datetime_module);
                    }
                } else {
                    Py_DECREF(datetime_module);
                }
            }
        } catch (...) {
            // Fall through to legacy method
            PyErr_Clear();
        }

        // Fallback to legacy method: return naive datetime
        auto current_day = std::chrono::floor<std::chrono::days>(src);
        std::chrono::year_month_day ymd{current_day};
        int year = static_cast<int>(ymd.year());
        unsigned int month = static_cast<unsigned int>(ymd.month());
        unsigned int day = static_cast<unsigned int>(ymd.day());

        auto time_of_day = src - current_day;
        std::chrono::hh_mm_ss hms{time_of_day};

        // 4. Extract time components
        long long hour = hms.hours().count();
        long long minute = hms.minutes().count();
        long long second = hms.seconds().count();
        auto subseconds = hms.subseconds(); // This is a duration
        auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(subseconds).count();

        return pack_datetime(year,
                             month,
                             day,
                             hour,
                             minute,
                             second,
                             microseconds);
    }
    #if PY_VERSION_HEX < 0x03090000
        NB_TYPE_CASTER(type, io_name("typing.Union[datetime.datetime, datetime.date, datetime.time]",
                                     "datetime.datetime"))
    #else
        NB_TYPE_CASTER(type, io_name("datetime.datetime | datetime.date | datetime.time",
                                     "datetime.datetime"))
    #endif
};

// Other clocks that are not the system clock are not measured as
// datetime.datetime objects since they are not measured on calendar
// time. So instead we just make them timedeltas; or if they have
// passed us a time as a float, we convert that.
template <typename Clock, typename Duration>
class type_caster<std::chrono::time_point<Clock, Duration>>
  : public duration_caster<std::chrono::time_point<Clock, Duration>> {};

template <typename Rep, typename Period>
class type_caster<std::chrono::duration<Rep, Period>>
  : public duration_caster<std::chrono::duration<Rep, Period>> {};

// Support for date
template<> class type_caster<std::chrono::year_month_day> {
public:
    using type = std::chrono::year_month_day;
    bool from_python(handle src, uint8_t /*flags*/, cleanup_list*) noexcept {
        namespace ch = std::chrono;

        if (!src)
            return false;

        int yy, mon, dd, hh, min, ss, uu;
        try {
            if (!unpack_datetime(src.ptr(), &yy, &mon, &dd,
                                 &hh, &min, &ss, &uu)) {
                return false;
            }
        } catch (python_error& e) {
            e.discard_as_unraisable(src.ptr());
            return false;
        }
        std::chrono::year_month_day ymd{std::chrono::year{yy}, std::chrono::month{static_cast<unsigned>(mon)}, std::chrono::day{static_cast<unsigned>(dd)}};

        value = ymd;
        return true;
    }

    static handle from_cpp(const type& src, rv_policy, cleanup_list*) noexcept {
        namespace ch = std::chrono;

        int year = static_cast<int>(src.year());
        unsigned int month = static_cast<unsigned int>(src.month());
        unsigned int day = static_cast<unsigned int>(src.day());

        return PyDate_FromDate(year,
                             month,
                             day);
    }
    #if PY_VERSION_HEX < 0x03090000
        NB_TYPE_CASTER(type, io_name("typing.Union[datetime.datetime, datetime.date, datetime.time]",
                                     "datetime.datetime"))
    #else
        NB_TYPE_CASTER(type, io_name("datetime.datetime | datetime.date | datetime.time",
                                     "datetime.datetime"))
    #endif
};

NAMESPACE_END(detail)
NAMESPACE_END(NB_NAMESPACE)
