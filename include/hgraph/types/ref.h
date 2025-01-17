//
// Created by Howard Henson on 03/01/2025.
//

#ifndef REF_H
#define REF_H

#include <hgraph/hgraph_forward_declarations.h>
#include <hgraph/types/time_series_type.h>

namespace hgraph
{

    struct HGRAPH_EXPORT TimeSeriesReference : nb::intrusive_base
    {
        using ptr = nb::ref<TimeSeriesReference>;

        virtual void        bind_input(TimeSeriesInput &ts_input) const              = 0;
        virtual bool        has_peer() const                                         = 0;
        virtual bool        is_valid() const                                         = 0;
        virtual bool        operator==(const TimeSeriesReferenceOutput &other) const = 0;
        virtual std::string to_string() const                                        = 0;

        static ptr make();
        static ptr make(time_series_output_ptr output);
        static ptr make(std::vector<ptr> items);
    };

    struct EmptyTimeSeriesReference final : TimeSeriesReference
    {
        void        bind_input(TimeSeriesInput &ts_input) const override;
        bool        has_peer() const override;
        bool        is_valid() const override;
        bool        operator==(const TimeSeriesReferenceOutput &other) const override;
        std::string to_string() const override;
    };

    struct BoundTimeSeriesReference final : TimeSeriesReference
    {
        BoundTimeSeriesReference(time_series_output_ptr output);

        const TimeSeriesOutput::ptr &output() const;

        void        bind_input(TimeSeriesInput &ts_input) const override;
        bool        has_peer() const override;
        bool        is_valid() const override;
        bool        operator==(const TimeSeriesReferenceOutput &other) const override;
        std::string to_string() const override;

      private:
        TimeSeriesOutput::ptr _output;
    };

    struct UnBoundTimeSeriesReference final : TimeSeriesReference
    {
        UnBoundTimeSeriesReference(std::vector<ptr> items);

        void bind_input(TimeSeriesInput &ts_input) const override;

        bool has_peer() const override;
        bool is_valid() const override;
        bool operator==(const TimeSeriesReferenceOutput &other) const override;

        std::string to_string() const override;

      private:
        std::vector<ptr> _items;
    };

    struct TimeSeriesReferenceOutput : TimeSeriesOutput
    {
        using TimeSeriesOutput::TimeSeriesOutput;

        const TimeSeriesReference::ptr &value() const;

        void set_value(TimeSeriesReference::ptr value);

        void apply_result(nb::object value) override;

        // Registers an input as observing the reference value
        void observe_reference(TimeSeriesInput::ptr input_);

        // Unregisters an input as observing the reference value
        void stop_observing_reference(TimeSeriesInput::ptr input_);

        [[nodiscard]] nb::object py_value() const override;

        [[nodiscard]] nb::object py_delta_value() const override;

        void                     invalidate() override;

        void                     copy_from_output(TimeSeriesOutput &output) override;

        void                     copy_from_input(TimeSeriesInput &input) override;

      private:
        TimeSeriesReference::ptr _value;
        std::unordered_set<TimeSeriesInput *> _reference_observers;
    };

    struct TimeSeriesReferenceInput : TimeSeriesInput
    {
        using TimeSeriesInput::TimeSeriesInput;

        const TimeSeriesReferenceOutput &reference_output() const;

        const TimeSeriesReference::ptr &value() const;

        void start();

      protected:
        void notify_parent(TimeSeriesInput *child, engine_time_t modified_time) override;

    private:
        TimeSeriesReference::ptr _value;
    };

}  // namespace hgraph

#endif  // REF_H
