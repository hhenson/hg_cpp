//
// Created by Howard Henson on 03/01/2025.
//

#ifndef TS_H
#define TS_H

#include <hgraph/types/time_series_type.h>

namespace hgraph
{
    template <typename T> struct TimeSeriesValueOutput : TimeSeriesOutput
    {
        using value_type = T;

        TimeSeriesValueOutput() = default;

        [[nodiscard]] nb::object py_value() const override { return nb::cast<nb::object>(_value); }

        [[nodiscard]] nb::object py_delta_value() const override { return py_value(); }

        void apply_result(nb::object value) override {
            if (value.is_none()) { return; }
            _value = nb::cast<T>(value);
            mark_modified();
        }

        const T &value() { return _value; }

        void invalidate() override { mark_invalid(); }

        void copy_from_output(TimeSeriesOutput &output) override {
            TimeSeriesValueOutput<T> &output_t = dynamic_cast<TimeSeriesValueOutput<T> &>(output);
            _value                             = output_t._value;
            mark_modified();
        }

        void copy_from_input(TimeSeriesInput &input) override {

        }

        void clear() override;

      private:
        T _value;
    };

    template<typename T> struct TimeSeriesValueInput : TimeSeriesInput
    {

        [[nodiscard]] TimeSeriesValueOutput<T> &value_output() {
            return dynamic_cast<TimeSeriesValueOutput<T> &>(*output());
        }

        [[nodiscard]] const T& value() {
            return value_output().value();
        }
    };
}  // namespace hgraph

#endif  // TS_H
