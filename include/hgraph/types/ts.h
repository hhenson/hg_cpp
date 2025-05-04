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

        using TimeSeriesOutput::TimeSeriesOutput;

        [[nodiscard]] nb::object py_value() const override { return valid() ? nb::cast(_value) : nb::none(); }

        [[nodiscard]] nb::object py_delta_value() const override { return modified() ? py_value() : nb::none(); }

        void apply_result(nb::handle value) override {
            if (!value.is_valid() || value.is_none()) { return; }
            set_value(nb::cast<T>(value));
        }

        const T &value() { return _value; }

        void set_value(const T &value) {
            _value = value;
            mark_modified();
        }

        void set_value(T &&value) {
            _value = value;
            mark_modified();
        }

        void invalidate() override { mark_invalid(); }

        void copy_from_output(TimeSeriesOutput &output) override {
            auto &output_t = dynamic_cast<TimeSeriesValueOutput<T> &>(output);
            set_value(output_t._value);
        }

        void copy_from_input(TimeSeriesInput &input) override;

      private:
        T _value{};
    };

    template <typename T> struct TimeSeriesValueInput : TimeSeriesInput
    {
        using TimeSeriesInput::TimeSeriesInput;

        [[nodiscard]] TimeSeriesValueOutput<T> &value_output() { return dynamic_cast<TimeSeriesValueOutput<T> &>(*output()); }

        [[nodiscard]] const T &value() { return value_output().value(); }
    };

    template <typename T> void TimeSeriesValueOutput<T>::copy_from_input(TimeSeriesInput &input) {
        TimeSeriesValueInput<T> &input_t = dynamic_cast<TimeSeriesValueInput<T> &>(input);
        set_value(input_t.value());
    }

    void register_ts_with_nanobind(nb::module_ &m);
}  // namespace hgraph

#endif  // TS_H
