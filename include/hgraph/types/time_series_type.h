//
// Created by Howard Henson on 09/12/2024.
//

#ifndef TIME_SERIES_TYPE_H
#define TIME_SERIES_TYPE_H

#include <hgraph/python/pyb.h>
#include <hgraph/util/date_time.h>
#include <hgraph/hgraph_export.h>
#include <hgraph/util/reference_count_subscriber.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>

namespace hgraph
{

    struct HGRAPH_EXPORT TimeSeriesType : nb::intrusive_base
    {
        using ptr = nb::ref<TimeSeriesType>;

        TimeSeriesType()                                  = default;
        TimeSeriesType(const TimeSeriesType &)            = default;
        TimeSeriesType(TimeSeriesType &&)                 = default;
        TimeSeriesType &operator=(const TimeSeriesType &) = default;
        TimeSeriesType &operator=(TimeSeriesType &&)      = default;
        ~TimeSeriesType() override                        = default;

        // Pure virtual methods to be implemented in derived classes

        // Method for owning node
        [[nodiscard]] virtual Node::ptr owning_node() const = 0;

        // Method for owning graph
        [[nodiscard]] virtual Graph::ptr owning_graph() const;

        // Method for value - as python object
        [[nodiscard]] virtual nb::object py_value() const = 0;

        // Method for delta value - as python object
        [[nodiscard]] virtual nb::object py_delta_value() const = 0;

        // Method to check if modified
        [[nodiscard]] virtual bool modified() const = 0;

        // Method to check if valid
        [[nodiscard]] virtual bool valid() const = 0;

        /*
        Is there a valid value associated to this time-series input, or loosely, "has this property
        ever ticked?". Note that it is possible for the time-series to become invalid after it has been made valid.
        The invalidation occurs mostly when working with REF values.
        :return: True if there is a valid value associated with this time-series.
         */
        [[nodiscard]] virtual bool all_valid() const = 0;

        // Method for last modified time
        [[nodiscard]] virtual engine_time_t last_modified_time() const = 0;

        /**
        FOR USE IN LIBRARY CODE.

        Change the owning node / time-series container of this time-series.
        This is used when grafting a time-series input from one node / time-series container to another.
        For example, see use in map implementation.
        */
        virtual void re_parent(Node::ptr parent) = 0;

        // Overload for re_parent with TimeSeries
        virtual void re_parent(TimeSeriesType::ptr parent) = 0;

        static void register_with_nanobind(nb::module_ &m);

    };

    struct TimeSeriesInput;

    struct HGRAPH_EXPORT TimeSeriesOutput : TimeSeriesType
    {
        using ptr = nb::ref<TimeSeriesOutput>;

        [[nodiscard]] Node::ptr owning_node() const override;

        [[nodiscard]] ptr parent_output() const;

        [[nodiscard]] bool has_parent_output() const;

        void re_parent(Node::ptr parent) override;

        void re_parent(ptr parent);

        virtual bool can_apply_result(nb::object value);

        virtual void apply_result(nb::object value) = 0;

        [[nodiscard]] bool modified() const override;

        [[nodiscard]] bool valid() const override;

        [[nodiscard]] bool all_valid() const override;

        [[nodiscard]] engine_time_t last_modified_time() const override;

        virtual void invalidate() = 0;

        virtual void mark_invalid();

        virtual void mark_modified();

        virtual void mark_modified(engine_time_t modified_time);

        void subscribe_node(Node::ptr node);

        void un_subscribe_node(Node::ptr node);

        virtual void copy_from_output(TimeSeriesOutput &output) = 0;

        virtual void copy_from_input(TimeSeriesInput &input) = 0;

        virtual void clear() = 0;

        static void register_with_nanobind(nb::module_ &m);

    protected:
        void _notify(engine_time_t modified_time);

    private:
        using OutputOrNode = std::variant<TimeSeriesOutput::ptr, Node::ptr>;
        std::optional<OutputOrNode> _parent_output_or_node{};
        ReferenceCountSubscriber<Node*> _subscribers{};
        engine_time_t _last_modified_time{MIN_DT};
    };
}  // namespace hgraph

#endif  // TIME_SERIES_TYPE_H
