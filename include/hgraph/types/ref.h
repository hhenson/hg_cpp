//
// Created by Howard Henson on 03/01/2025.
//

#ifndef REF_H
#define REF_H

#include <hgraph/types/time_series_type.h>

namespace hgraph
{

    struct HGRAPH_EXPORT TimeSeriesReference : nb::intrusive_base
    {
        using ptr = nb::ref<TimeSeriesReference>;

        virtual void        bind_input(TimeSeriesInput &ts_input) const              = 0;
        virtual bool        has_output() const                                       = 0;
        virtual bool        is_empty() const                                         = 0;
        virtual bool        is_valid() const                                         = 0;
        virtual bool        operator==(const TimeSeriesReferenceOutput &other) const = 0;
        virtual std::string to_string() const                                        = 0;

        static ptr make();
        static ptr make(time_series_output_ptr output);
        static ptr make(std::vector<ptr> items);
        static ptr make(std::vector<nb::ref<TimeSeriesReferenceInput>> items);

        static void register_with_nanobind(nb::module_ &m);
    };

    struct EmptyTimeSeriesReference final : TimeSeriesReference
    {
        void        bind_input(TimeSeriesInput &ts_input) const override;
        bool        has_output() const override;
        bool        is_empty() const override;
        bool        is_valid() const override;
        bool        operator==(const TimeSeriesReferenceOutput &other) const override;
        std::string to_string() const override;
    };

    struct BoundTimeSeriesReference final : TimeSeriesReference
    {
        explicit BoundTimeSeriesReference(time_series_output_ptr output);

        const TimeSeriesOutput::ptr &output() const;

        void        bind_input(TimeSeriesInput &ts_input) const override;
        bool        has_output() const override;
        bool        is_empty() const override;
        bool        is_valid() const override;
        bool        operator==(const TimeSeriesReferenceOutput &other) const override;
        std::string to_string() const override;

      private:
        TimeSeriesOutput::ptr _output;
    };

    struct UnBoundTimeSeriesReference final : TimeSeriesReference
    {
        explicit UnBoundTimeSeriesReference(std::vector<ptr> items);

        const std::vector<ptr> &items() const;

        void bind_input(TimeSeriesInput &ts_input) const override;
        bool has_output() const override;
        bool is_empty() const override;
        bool is_valid() const override;
        bool operator==(const TimeSeriesReferenceOutput &other) const override;

        std::string to_string() const override;

      private:
        std::vector<ptr> _items;
    };

    struct TimeSeriesReferenceOutput : TimeSeriesOutput
    {
        using TimeSeriesOutput::TimeSeriesOutput;

        ~TimeSeriesReferenceOutput() override;

        const TimeSeriesReference::ptr &value() const;

        TimeSeriesReference::ptr &value();

        void set_value(TimeSeriesReference::ptr value);

        void apply_result(nb::handle value) override;

        // Registers an input as observing the reference value
        void observe_reference(TimeSeriesInput::ptr input_);

        // Unregisters an input as observing the reference value
        void stop_observing_reference(TimeSeriesInput::ptr input_);

        [[nodiscard]] nb::object py_value() const override;

        [[nodiscard]] nb::object py_delta_value() const override;

        void invalidate() override;

        void copy_from_output(TimeSeriesOutput &output) override;

        void copy_from_input(TimeSeriesInput &input) override;

        static void register_with_nanobind(nb::module_ &m);


      private:
        TimeSeriesReference::ptr              _value;
        // Use a raw pointer as we don't have hash implemented on ptr at the moment,
        // So this is a work arround the code managing this also ensures the pointers are incremented
        // and decremented.
        std::unordered_set<TimeSeriesInput *> _reference_observers;
    };

    struct TimeSeriesReferenceInput : TimeSeriesInput
    {
        using ptr = nb::ref<TimeSeriesReferenceInput>;
        using TimeSeriesInput::TimeSeriesInput;

        TimeSeriesReference::ptr value() const;
        TimeSeriesReference::ptr value();

        void start();

        [[nodiscard]] nb::object py_value() const override;

        [[nodiscard]] nb::object py_delta_value() const override;

        // Duplicate binding of another input
        void clone_binding(const TimeSeriesReferenceInput &other);

        [[nodiscard]] bool          modified() const override;
        [[nodiscard]] bool          valid() const override;
        [[nodiscard]] bool          all_valid() const override;
        [[nodiscard]] engine_time_t last_modified_time() const override;
        bool                        bind_output(time_series_output_ptr value) override;
        void                        un_bind_output() override;
        void                        make_active() override;
        void                        make_passive() override;

      protected:
        bool do_bind_output(time_series_output_ptr value) override;
        void do_un_bind_output() override;

      public:
        static void register_with_nanobind(nb::module_ &m);
      protected:
        void notify_parent(TimeSeriesInput *child, engine_time_t modified_time) override;

        std::vector<TimeSeriesReferenceInput::ptr> &items();

        const std::vector<TimeSeriesReferenceInput::ptr> &items() const;

        const TimeSeriesReferenceOutput &reference_output() const;
        TimeSeriesReferenceOutput &reference_output();

      private:
        mutable TimeSeriesReference::ptr _value;
        std::vector<TimeSeriesReferenceInput::ptr> _items;
    };

}  // namespace hgraph

#endif  // REF_H
