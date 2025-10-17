//
// TimeSeriesWindow (TSW) fixed-size (tick-count) implementation
// Ported following TS patterns. Time-window (timedelta) variant can be added later.
//

#ifndef TSW_H
#define TSW_H

#include <hgraph/types/time_series_type.h>
#include <deque>

namespace hgraph
{

    template <typename T> struct TimeSeriesFixedWindowOutput : TimeSeriesOutput
    {
        using value_type = T;

        using TimeSeriesOutput::TimeSeriesOutput;

        // Construct with capacity and min size
        TimeSeriesFixedWindowOutput(const node_ptr &parent, size_t size, size_t min_size)
            : TimeSeriesOutput(parent), _size(size), _min_size(min_size) {
            _buffer.resize(_size);
            _times.resize(_size, engine_time_t{});
        }

        TimeSeriesFixedWindowOutput(const TimeSeriesType::ptr &parent, size_t size, size_t min_size)
            : TimeSeriesOutput(parent), _size(size), _min_size(min_size) {
            _buffer.resize(_size);
            _times.resize(_size, engine_time_t{});
        }

        [[nodiscard]] nb::object py_value() const override;

        [[nodiscard]] nb::object py_delta_value() const override;

        bool can_apply_result(nb::object) override { return !modified(); }

        void apply_result(nb::object value) override;

        void invalidate() override { mark_invalid(); }

        void mark_invalid() override;

        [[nodiscard]] nb::object py_value_times() const;
        [[nodiscard]] engine_time_t first_modified_time() const;

        void copy_from_output(const TimeSeriesOutput &output) override {
            auto &o = dynamic_cast<const TimeSeriesFixedWindowOutput<T> &>(output);
            _buffer  = o._buffer;
            _times   = o._times;
            _start   = o._start;
            _length  = o._length;
            _size    = o._size;
            _min_size = o._min_size;
            mark_modified();
        }

        void copy_from_input(const TimeSeriesInput &input) override;

        [[nodiscard]] bool is_same_type(TimeSeriesType &other) const override {
            return dynamic_cast<TimeSeriesFixedWindowOutput<T> *>(&other) != nullptr;
        }

        // Extra API to mirror Python TSW
        [[nodiscard]] size_t size() const { return _size; }
        [[nodiscard]] size_t min_size() const { return _min_size; }
        [[nodiscard]] bool   has_removed_value() const { return _removed_value.has_value(); }
        [[nodiscard]] T      removed_value() const { return _removed_value.value_or(T{}); }

        [[nodiscard]] size_t len() const { return _length; }

      private:
        std::vector<T>          _buffer{};
        std::vector<engine_time_t> _times{};
        size_t                  _size{0};
        size_t                  _min_size{0};
        size_t                  _start{0};
        size_t                  _length{0};
        std::optional<T>        _removed_value{};
    };

    template <typename T> struct TimeSeriesWindowInput : TimeSeriesInput
    {
        using TimeSeriesInput::TimeSeriesInput;

        [[nodiscard]] TimeSeriesFixedWindowOutput<T>       &output_t() { return dynamic_cast<TimeSeriesFixedWindowOutput<T> &>(*output()); }
        [[nodiscard]] const TimeSeriesFixedWindowOutput<T> &output_t() const { return dynamic_cast<const TimeSeriesFixedWindowOutput<T> &>(*output()); }

        [[nodiscard]] nb::object py_value() const override { return output_t().py_value(); }
        [[nodiscard]] nb::object py_delta_value() const override { return output_t().py_delta_value(); }

        [[nodiscard]] bool modified() const override { return output()->modified(); }
        [[nodiscard]] bool valid() const override { return output()->valid(); }
        [[nodiscard]] bool all_valid() const override { return valid() && output_t().len() >= output_t().min_size(); }
        [[nodiscard]] engine_time_t last_modified_time() const override { return output()->last_modified_time(); }

        [[nodiscard]] nb::object py_value_times() const { return output_t().py_value_times(); }
        [[nodiscard]] engine_time_t first_modified_time() const { return output_t().first_modified_time(); }
        [[nodiscard]] bool has_removed_value() const { return output_t().has_removed_value(); }
        [[nodiscard]] nb::object removed_value() const { return output_t().has_removed_value() ? nb::cast(output_t().removed_value()) : nb::none(); }

        [[nodiscard]] bool is_same_type(TimeSeriesType &other) const override {
            return dynamic_cast<TimeSeriesWindowInput<T> *>(&other) != nullptr;
        }
    };

    template <typename T> void TimeSeriesFixedWindowOutput<T>::copy_from_input(const TimeSeriesInput &input) {
        auto &i = dynamic_cast<const TimeSeriesWindowInput<T> &>(input);
        this->apply_result(i.output_t().py_value());
    }

    // Registration
    void tsw_register_with_nanobind(nb::module_ &m);

}  // namespace hgraph

#endif  // TSW_H
