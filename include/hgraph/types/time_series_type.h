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
#include <variant>

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
        [[nodiscard]] virtual Node &owning_node() = 0;

        [[nodiscard]] virtual const Node &owning_node() const = 0;

        // Method for owning graph
        [[nodiscard]] virtual Graph &owning_graph();

        [[nodiscard]] const Graph &owning_graph() const;

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

        // // Overload for re_parent with TimeSeries
        // virtual void re_parent(TimeSeriesType::ptr parent) = 0;

        static void register_with_nanobind(nb::module_ &m);

    };

    struct TimeSeriesInput;

    struct HGRAPH_EXPORT TimeSeriesOutput : TimeSeriesType
    {
        using ptr = nb::ref<TimeSeriesOutput>;

        [[nodiscard]] Node &owning_node() override;

        [[nodiscard]] const Node &owning_node() const override;

        [[nodiscard]] ptr parent_output() const;

        [[nodiscard]] bool has_parent_output() const;

        void re_parent(Node::ptr parent) override;

        virtual void re_parent(ptr parent);

        virtual bool can_apply_result(std::any value);

        virtual void apply_result(std::any value) = 0;

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

        const Node &_owning_node() const;
    };

    struct HGRAPH_EXPORT TimeSeriesInput : TimeSeriesType
    {
        using ptr = nb::ref<TimeSeriesInput>;

        // Constructor and Destructor
        TimeSeriesInput()           = default;
        ~TimeSeriesInput() override = default;

        // Pure virtual properties and methods

        // The input that this input is bound to. This will be nullptr if this is the root input.
        [[nodiscard]] virtual ptr parent_input() const = 0;

        // True if this input is a child of another input, False otherwise
        [[nodiscard]] virtual bool has_parent_input() const = 0;

        // Is this time-series input bound to an output?
        [[nodiscard]] virtual bool bound() const = 0;

        // True if this input is peered.
        [[nodiscard]] virtual bool has_peer() const = 0;

        // The output bound to this input. If the input is not bound then this will be nullptr.
        [[nodiscard]] virtual time_series_output_ptr output() const = 0;

        // FOR LIBRARY USE ONLY. Binds the output provided to this input.
        virtual bool bind_output(time_series_output_ptr value) = 0;

        // FOR LIBRARY USE ONLY. Unbinds the output from this input.
        virtual void un_bind_output() = 0;

        // Derived classes override this to implement specific behaviors for binding.
        virtual bool do_bind_output(time_series_output_ptr value) = 0;

        // Derived classes override this to implement specific behaviors for unbinding.
        virtual void do_un_bind_output() = 0;

        // An active input will cause the node it is associated with to be scheduled when the value
        // the input represents is modified. Returns True if this input is active.
        [[nodiscard]] virtual bool active() const = 0;

        // Marks the input as being active, causing its node to be scheduled for evaluation when the value changes.
        virtual void make_active() = 0;

        // Marks the input as passive, preventing the associated node from being scheduled for evaluation
        // when the value changes.
        virtual void make_passive() = 0;
    };
}  // namespace hgraph

#endif  // TIME_SERIES_TYPE_H
