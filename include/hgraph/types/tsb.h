//
// Created by Howard Henson on 27/12/2024.
//

#ifndef TSB_H
#define TSB_H
#include <hgraph/types/time_series_type.h>

namespace hgraph
{

    struct TimeSeriesBundleOutputBuilder;
    struct TimeSeriesBundleInputBuilder;

    struct TimeSeriesSchema : nb::intrusive_base
    {
        using ptr = nb::ref<TimeSeriesSchema>;

        explicit TimeSeriesSchema(std::vector<std::string> keys);
        explicit TimeSeriesSchema(std::vector<std::string> keys, const nb::object &type);

        const std::vector<std::string> &keys() const;
        const nb::object               &scalar_type() const;

        static void register_with_nanobind(nb::module_ &m);

      private:
        std::vector<std::string> _keys;
        nb::object               _scalar_type;
    };

    struct TimeSeriesBundleOutput : TimeSeriesOutput
    {
        using ptr = nb::ref<TimeSeriesBundleOutput>;
        // Define an iterator type for the unordered_map
        using iterator       = std::vector<TimeSeriesOutput::ptr>::iterator;
        using const_iterator = std::vector<TimeSeriesOutput::ptr>::const_iterator;

        explicit TimeSeriesBundleOutput(const node_ptr &parent, const TimeSeriesSchema::ptr &schema);
        explicit TimeSeriesBundleOutput(const TimeSeriesType::ptr &parent, const TimeSeriesSchema::ptr &schema);
        TimeSeriesBundleOutput(const TimeSeriesBundleOutput &)            = default;
        TimeSeriesBundleOutput(TimeSeriesBundleOutput &&)                 = default;
        TimeSeriesBundleOutput &operator=(const TimeSeriesBundleOutput &) = default;
        TimeSeriesBundleOutput &operator=(TimeSeriesBundleOutput &&)      = default;
        ~TimeSeriesBundleOutput() override                                = default;

        [[nodiscard]] nb::object py_value() const override;

        [[nodiscard]] nb::object py_delta_value() const override;

        void apply_result(nb::handle value) override;

        // Begin iterator
        iterator       begin();
        const_iterator begin() const;

        // End iterator
        iterator       end();
        const_iterator end() const;

        TimeSeriesOutput       &operator[](const std::string &key);
        const TimeSeriesOutput &operator[](const std::string &key) const;

        TimeSeriesOutput       &operator[](std::size_t ndx);
        const TimeSeriesOutput &operator[](std::size_t ndx) const;

        [[nodiscard]] bool all_valid() const override;

        void invalidate() override;

        void copy_from_output(TimeSeriesOutput &output) override;

        void copy_from_input(TimeSeriesInput &input) override;

        void clear() override;

        // Retrieves valid keys
        std::vector<c_string_ref> keys() const;
        std::vector<c_string_ref> valid_keys() const;
        std::vector<c_string_ref> modified_keys() const;

        // Retrieves valid values
        std::vector<time_series_output_ptr> values() const;
        std::vector<time_series_output_ptr> valid_values() const;
        std::vector<time_series_output_ptr> modified_values() const;

        // Retrieves valid items
        std::vector<std::pair<c_string_ref, time_series_output_ptr>> items() const;
        std::vector<std::pair<c_string_ref, time_series_output_ptr>> valid_items() const;
        std::vector<std::pair<c_string_ref, time_series_output_ptr>> modified_items() const;

        static void register_with_nanobind(nb::module_ &m);

        const TimeSeriesSchema &schema() const;

        bool contains(const std::string &key) const;

        size_t size() const;

      protected:
        friend TimeSeriesBundleOutputBuilder;
        void set_outputs(std::vector<time_series_output_ptr> ts_values);

        // Retrieves valid keys
        std::vector<c_string_ref> keys_with_constraint(const std::function<bool(const TimeSeriesOutput &)> &constraint) const;

        // Retrieves valid values
        std::vector<time_series_output_ptr>
        values_with_constraint(const std::function<bool(const TimeSeriesOutput &)> &constraint) const;

        // Retrieves valid items
        std::vector<std::pair<c_string_ref, time_series_output_ptr>>
        items_with_constraint(const std::function<bool(const TimeSeriesOutput &)> &constraint) const;

      private:
        TimeSeriesSchema::ptr               _schema;
        std::vector<time_series_output_ptr> _ts_values;
    };

    struct TimeSeriesBundleInput : TimeSeriesInput
    {
        using ptr = nb::ref<TimeSeriesBundleInput>;
        using TimeSeriesInput::TimeSeriesInput;

        // Define an iterator type for the unordered_map
        using iterator       = std::vector<TimeSeriesInput::ptr>::iterator;
        using const_iterator = std::vector<TimeSeriesInput::ptr>::const_iterator;

        explicit TimeSeriesBundleInput(const node_ptr &parent, const TimeSeriesSchema::ptr &schema);
        explicit TimeSeriesBundleInput(const TimeSeriesType::ptr &parent, const TimeSeriesSchema::ptr &schema);
        TimeSeriesBundleInput(const TimeSeriesBundleInput &)            = default;
        TimeSeriesBundleInput(TimeSeriesBundleInput &&)                 = default;
        TimeSeriesBundleInput &operator=(const TimeSeriesBundleInput &) = default;
        TimeSeriesBundleInput &operator=(TimeSeriesBundleInput &&)      = default;
        ~TimeSeriesBundleInput() override                               = default;

        [[nodiscard]] nb::object py_value() const override;
        [[nodiscard]] nb::object py_delta_value() const override;

        // Begin iterator
        iterator       begin();
        const_iterator begin() const;

        // End iterator
        iterator       end();
        const_iterator end() const;

        size_t size() const;

        // Retrieves valid keys
        std::vector<c_string_ref> keys() const;
        std::vector<c_string_ref> valid_keys() const;
        std::vector<c_string_ref> modified_keys() const;

        // Retrieves valid values
        std::vector<time_series_input_ptr> values() const;
        std::vector<time_series_input_ptr> valid_values() const;
        std::vector<time_series_input_ptr> modified_values() const;

        // Retrieves valid items
        std::vector<std::pair<c_string_ref, time_series_input_ptr>> items() const;
        std::vector<std::pair<c_string_ref, time_series_input_ptr>> valid_items() const;
        std::vector<std::pair<c_string_ref, time_series_input_ptr>> modified_items() const;

        // Access elements by key
        TimeSeriesInput       &operator[](const std::string &key);
        const TimeSeriesInput &operator[](const std::string &key) const;

        // Access elements by index
        TimeSeriesInput       &operator[](size_t ndx);
        const TimeSeriesInput &operator[](size_t ndx) const;

        [[nodiscard]] bool          modified() const override;
        [[nodiscard]] bool          valid() const override;
        [[nodiscard]] bool          all_valid() const override;
        [[nodiscard]] engine_time_t last_modified_time() const override;
        [[nodiscard]] bool          bound() const override;
        [[nodiscard]] bool          active() const override;
        void                        make_active() override;
        void                        make_passive() override;

        // Check if a key exists
        bool contains(const std::string &key) const;

        // Static method for nanobind registration
        static void register_with_nanobind(nb::module_ &m);

        const TimeSeriesSchema &schema() const;

        void set_subscribe_method(bool subscribe_input) override;

      protected:
        bool do_bind_output(time_series_output_ptr value) override;
        void do_un_bind_output() override;

        friend TimeSeriesBundleInputBuilder;
        void set_inputs(std::vector<time_series_input_ptr> ts_values);

        // Retrieves valid keys
        std::vector<c_string_ref> keys_with_constraint(const std::function<bool(const TimeSeriesInput &)> &constraint) const;

        // Retrieves valid values
        std::vector<time_series_input_ptr>
        values_with_constraint(const std::function<bool(const TimeSeriesInput &)> &constraint) const;

        // Retrieves valid items
        std::vector<std::pair<c_string_ref, time_series_input_ptr>>
        items_with_constraint(const std::function<bool(const TimeSeriesInput &)> &constraint) const;

        nb::object py_value_with_constraint(const std::function<bool(const TimeSeriesInput &)> &constraint) const;

      private:
        // Stores the time-series data
        TimeSeriesSchema::ptr             _schema;
        std::vector<TimeSeriesInput::ptr> _ts_values;
    };

}  // namespace hgraph

#endif  // TSB_H
