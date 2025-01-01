
#ifndef NODE_H
#define NODE_H

#include <any>
#include <hgraph/hgraph_export.h>
#include <hgraph/python/pyb.h>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <hgraph/hgraph_forward_declarations.h>
#include <hgraph/util/date_time.h>
#include <hgraph/util/lifecycle.h>
#include <nanobind/intrusive/ref.h>

namespace hgraph
{

    template <typename Enum> typename std::enable_if<std::is_enum<Enum>::value, Enum>::type operator|(Enum lhs, Enum rhs) {
        using underlying = typename std::underlying_type<Enum>::type;
        return static_cast<Enum>(static_cast<underlying>(lhs) | static_cast<underlying>(rhs));
    }

    template <typename Enum> typename std::enable_if<std::is_enum<Enum>::value, Enum>::type operator&(Enum lhs, Enum rhs) {
        using underlying = typename std::underlying_type<Enum>::type;
        return static_cast<Enum>(static_cast<underlying>(lhs) & static_cast<underlying>(rhs));
    }

    enum class NodeTypeEnum : char8_t {
        NONE             = 0,
        SOURCE_NODE      = 1,
        PUSH_SOURCE_NODE = SOURCE_NODE | (1 << 1),
        PULL_SOURCE_NODE = SOURCE_NODE | (1 << 2),
        COMPUTE_NODE     = 1 << 3,
        SINK_NODE        = 1 << 4
    };

    void node_type_enum_py_register(nb::module_ &m);

    enum class InjectableTypesEnum : char8_t {
        NONE         = 0,
        STATE        = 1,
        SCHEDULER    = 1 << 1,
        OUTPUT       = 1 << 2,
        CLOCK        = 1 << 3,
        ENGINE_API   = 1 << 4,
        REPLAY_STATE = 1 << 5,
        LOGGER       = 1 << 6
    };

    void injectable_type_enum(nb::module_ &m);

    struct HGRAPH_EXPORT NodeSignature : nanobind::intrusive_base
    {
        using ptr = nanobind::ref<NodeSignature>;

        NodeSignature() = default;

        NodeSignature(std::string name, NodeTypeEnum node_type, std::vector<std::string> args,
                      std::optional<std::unordered_map<std::string, nb::object>> time_series_inputs,
                      std::optional<nb::object>                                  time_series_output,
                      std::optional<std::unordered_map<std::string, nb::object>> scalars, nb::object src_location,
                      std::optional<std::unordered_set<std::string>> active_inputs,
                      std::optional<std::unordered_set<std::string>> valid_inputs,
                      std::optional<std::unordered_set<std::string>> all_valid_inputs,
                      std::optional<std::unordered_set<std::string>> context_inputs, InjectableTypesEnum injectable_inputs,
                      std::string wiring_path_name, std::optional<std::string> label, std::optional<std::string> record_replay_id,
                      bool capture_values, bool capture_exception, int64_t trace_back_depth);

        std::string                                                name{};
        NodeTypeEnum                                               node_type{NodeTypeEnum::NONE};
        std::vector<std::string>                                   args{};
        std::optional<std::unordered_map<std::string, nb::object>> time_series_inputs{};
        std::optional<nb::object>                                  time_series_output{};
        std::optional<std::unordered_map<std::string, nb::object>> scalars{};
        nb::object                                                 src_location{nb::none()};
        std::optional<std::unordered_set<std::string>>             active_inputs{};
        std::optional<std::unordered_set<std::string>>             valid_inputs{};
        std::optional<std::unordered_set<std::string>>             all_valid_inputs{};
        std::optional<std::unordered_set<std::string>>             context_inputs{};
        InjectableTypesEnum                                        injectable_inputs{InjectableTypesEnum::NONE};
        std::string                                                wiring_path_name{};
        std::optional<std::string>                                 label{};
        std::optional<std::string>                                 record_replay_id{};
        bool                                                       capture_values{false};
        bool                                                       capture_exception{false};
        int64_t                                                    trace_back_depth{1};

        [[nodiscard]] nb::object get_arg_type(const std::string &arg) const;

        [[nodiscard]] std::string signature() const;

        [[nodiscard]] bool uses_scheduler() const;

        [[nodiscard]] bool uses_clock() const;

        [[nodiscard]] bool uses_engine() const;

        [[nodiscard]] bool uses_state() const;

        [[nodiscard]] bool uses_output_feedback() const;

        [[nodiscard]] bool uses_replay_state() const;

        [[nodiscard]] bool is_source_node() const;

        [[nodiscard]] bool is_push_source_node() const;

        [[nodiscard]] bool is_pull_source_node() const;

        [[nodiscard]] bool is_compute_node() const;

        [[nodiscard]] bool is_sink_node() const;

        [[nodiscard]] bool is_recordable() const;

        static void py_register(nb::module_ &m);
    };

    struct NodeScheduler
    {
        explicit NodeScheduler(Node& node);

        [[nodiscard]] engine_time_t next_scheduled_time() const;
        [[nodiscard]] bool          is_scheduled() const;
        [[nodiscard]] bool          is_scheduled_node() const;
        [[nodiscard]] bool          has_tag(const std::string &tag) const;
        engine_time_t               pop_tag(const std::string &tag, std::optional<engine_time_t> default_time);
        void                        schedule(engine_time_t when, std::optional<std::string> tag, bool on_wall_clock=false);
        void                        schedule(engine_time_delta_t when, std::optional<std::string> tag, bool on_wall_clock=false);
        void                        un_schedule(std::optional<std::string> tag);
        void                        reset();

    protected:
        void _on_alarm(engine_time_t when, std::string tag);

      private:
        Node                                           &_node;
        std::set<std::pair<engine_time_t, std::string>> _scheduled_events;
        std::unordered_map<std::string, engine_time_t>  _tags;
        std::unordered_map<std::string, engine_time_t>  _alarm_tags;
        engine_time_t                                   _last_scheduled_time{MIN_DT};
    };

    struct HGRAPH_EXPORT Node : ComponentLifeCycle
    {
        using ptr = nanobind::ref<Node>;

        Node(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::ptr signature, nb::dict scalars);

        std::vector<nb::ref<TimeSeriesInput>> start_inputs();

        virtual void eval() = 0;
        virtual void notify(engine_time_t modified_time);

        void notify();

        virtual void notify_next_cycle();

        int64_t node_ndx() const;

        const std::vector<int64_t> &owning_graph_id() const;

        const std::vector<int64_t> &node_id() const;

        const NodeSignature &signature() const;

        const nb::dict &scalars() const;

        Graph &graph();

        const Graph &graph() const;

        void set_graph(graph_ptr value);

        TimeSeriesBundleInput &input();

        void set_input(time_series_bundle_input_ptr value);

        TimeSeriesOutput &output();

        void set_output(time_series_output_ptr value);

        TimeSeriesBundleOutput &recordable_state();

        void set_recordable_state(time_series_bundle_output_ptr value);

        std::optional<NodeScheduler> scheduler() const;

        TimeSeriesOutput &error_output();

        void set_error_output(time_series_output_ptr value);

        friend struct Graph;

      private:
        int64_t                       _node_ndx;
        std::vector<int64_t>          _owning_graph_id;
        std::vector<int64_t>          _node_id;
        NodeSignature::ptr            _signature;
        nb::dict                      _scalars;
        graph_ptr                     _graph;
        time_series_bundle_input_ptr  _input;
        time_series_output_ptr        _output;
        time_series_output_ptr        _error_output;
        time_series_bundle_output_ptr _recordable_state;
        std::optional<NodeScheduler>  _scheduler;
        // I am not a fan of this approach to managing the start inputs, but for now keep consistent with current code base in
        // Python.
        std::vector<nb::ref<TimeSeriesInput>> _start_inputs;
    };

    struct PushQueueNode : Node
    {
        using Node::Node;

        void eval() override;

        void enqueue_message(std::any message);

        [[nodiscard]] bool apply_message(std::any message);

        int64_t messages_in_queue() const;

        void set_receiver(sender_receiver_state_ptr value);

      protected:
        void start() override;

      private:
        sender_receiver_state_ptr _receiver;
        int64_t                   _messages_queued{0};
        int64_t                   _messages_dequeued{0};
        bool                      _elide{false};
    };
}  // namespace hgraph

#endif  // NODE_H
