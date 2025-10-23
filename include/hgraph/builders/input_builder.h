//
// Created by Howard Henson on 27/12/2024.
//

#ifndef INPUT_BUILDER_H
#define INPUT_BUILDER_H

#include <hgraph/builders/builder.h>

namespace hgraph
{

    // The InputBuilder class implementation

    struct InputBuilder : Builder
    {
        using ptr = nb::ref<InputBuilder>;

        /**
         * Create an instance of InputBuilder using an owning node
         */
        virtual time_series_input_ptr make_instance(node_ptr owning_node) const = 0;

        /**
         * Create an instance of InputBuilder using an parent input
         */
        virtual time_series_input_ptr make_instance(time_series_input_ptr owning_input) const = 0;

        /**
         * Release an instance of the input type.
         * By default, do nothing.
         */
        virtual void release_instance(time_series_input_ptr item) const;

        virtual bool has_reference() const { return false; }

        static void register_with_nanobind(nb::module_ &m);
    };

    struct TimeSeriesSignalInput;
    struct HGRAPH_EXPORT TimeSeriesSignalInputBuilder : InputBuilder
    {
        using ptr = nb::ref<TimeSeriesSignalInputBuilder>;
        using InputBuilder::InputBuilder;

        time_series_input_ptr make_instance(node_ptr owning_node) const override;
        time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override;

        void release_instance(time_series_input_ptr item) const override;
        void release_instance(TimeSeriesSignalInput* item) const;
    };

    template <typename T> struct HGRAPH_EXPORT TimeSeriesValueInputBuilder : InputBuilder
    {
        using ptr = nb::ref<TimeSeriesValueInputBuilder<T>>;
        using InputBuilder::InputBuilder;

        time_series_input_ptr make_instance(node_ptr owning_node) const override;

        time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override;
    };

    struct HGRAPH_EXPORT TimeSeriesRefInputBuilder : InputBuilder
    {
        using InputBuilder::InputBuilder;

        time_series_input_ptr make_instance(node_ptr owning_node) const override;

        time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override;

        bool has_reference() const override { return true; }
    };

    struct HGRAPH_EXPORT TimeSeriesListInputBuilder : InputBuilder
    {
        using ptr = nb::ref<TimeSeriesListInputBuilder>;

        TimeSeriesListInputBuilder(InputBuilder::ptr input_builder, size_t size);

        time_series_input_ptr make_instance(node_ptr owning_node) const override;

        time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override;

        bool has_reference() const override;

        [[nodiscard]] bool is_same_type(const Builder &other) const override;

        void release_instance(time_series_input_ptr item) const override;

      private:
        time_series_input_ptr make_and_set_inputs(TimeSeriesListInput *input) const;

        InputBuilder::ptr input_builder;
        size_t            size;
    };

    struct HGRAPH_EXPORT TimeSeriesBundleInputBuilder : InputBuilder
    {
        using ptr = nb::ref<TimeSeriesBundleInputBuilder>;

        TimeSeriesBundleInputBuilder(time_series_schema_ptr schema, std::vector<InputBuilder::ptr> input_builders);

        time_series_input_ptr make_instance(node_ptr owning_node) const override;

        time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override;

        bool has_reference() const override;

        [[nodiscard]] bool is_same_type(const Builder &other) const override;

        void release_instance(time_series_input_ptr item) const override;

      private:
        time_series_input_ptr          make_and_set_inputs(TimeSeriesBundleInput *input) const;
        time_series_schema_ptr          schema;
        std::vector<InputBuilder::ptr> input_builders;
    };

    struct HGRAPH_EXPORT TimeSeriesSetInputBuilder : InputBuilder
    {
        using ptr = nb::ref<TimeSeriesSetInputBuilder>;
        using InputBuilder::InputBuilder;
    };

    template <typename T> struct HGRAPH_EXPORT TimeSeriesSetInputBuilder_T : TimeSeriesSetInputBuilder
    {
        using TimeSeriesSetInputBuilder::TimeSeriesSetInputBuilder;

        time_series_input_ptr make_instance(node_ptr owning_node) const override;

        time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override;
    };

    // TimeSeriesWindow (TSW) input builder for fixed-size windows
    template <typename T> struct HGRAPH_EXPORT TimeSeriesWindowInputBuilder_T : InputBuilder
    {
        using ptr = nb::ref<TimeSeriesWindowInputBuilder_T<T>>;
        size_t size;
        size_t min_size;

        TimeSeriesWindowInputBuilder_T(size_t size, size_t min_size) : size(size), min_size(min_size) {}

        time_series_input_ptr make_instance(node_ptr owning_node) const override;

        time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override;

        [[nodiscard]] bool is_same_type(const Builder &other) const override {
            if (auto other_b = dynamic_cast<const TimeSeriesWindowInputBuilder_T<T> *>(&other)) {
                return size == other_b->size && min_size == other_b->min_size;
            }
            return false;
        }
    };

    struct HGRAPH_EXPORT TimeSeriesDictInputBuilder : InputBuilder
    {
        using ptr = nb::ref<TimeSeriesDictInputBuilder>;
        input_builder_ptr ts_builder;

        explicit TimeSeriesDictInputBuilder(input_builder_ptr ts_builder);

        bool has_reference() const override { return ts_builder->has_reference(); }
    };

    template <typename T> struct HGRAPH_EXPORT TimeSeriesDictInputBuilder_T : TimeSeriesDictInputBuilder
    {
        using TimeSeriesDictInputBuilder::TimeSeriesDictInputBuilder;

        time_series_input_ptr make_instance(node_ptr owning_node) const override;

        time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override;

        [[nodiscard]] bool is_same_type(const Builder &other) const override;

        void release_instance(time_series_input_ptr item) const override;

    };

}  // namespace hgraph

#endif  // INPUT_BUILDER_H
