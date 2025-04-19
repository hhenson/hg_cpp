
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
#include <hgraph/python/pyb.h>
#include <hgraph/util/date_time.h>
#include <hgraph/util/lifecycle.h>
#include <hgraph/util/reference_count_subscriber.h>

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

    enum InjectableTypesEnum : int16_t {
        NONE             = 0,
        STATE            = 1,
        RECORDABLE_STATE = 1 << 1,
        SCHEDULER        = 1 << 2,
        OUTPUT           = 1 << 3,
        CLOCK            = 1 << 4,
        ENGINE_API       = 1 << 5,
        LOGGER           = 1 << 6,
        NODE             = 1 << 7
    };

    void injectable_type_enum(nb::module_ &m);

    struct HGRAPH_EXPORT NodeSignature : nanobind::intrusive_base
    {
        using ptr = nanobind::ref<NodeSignature>;

        NodeSignature(std::string name, NodeTypeEnum node_type, std::vector<std::string> args,
                      std::optional<std::unordered_map<std::string, nb::object>> time_series_inputs,
                      std::optional<nb::object> time_series_output, std::optional<nb::dict> scalars, nb::object src_location,
                      std::optional<std::unordered_set<std::string>>                      active_inputs,
                      std::optional<std::unordered_set<std::string>>                      valid_inputs,
                      std::optional<std::unordered_set<std::string>>                      all_valid_inputs,
                      std::optional<std::unordered_set<std::string>>                      context_inputs,
                      std::optional<std::unordered_map<std::string, InjectableTypesEnum>> injectable_inputs, size_t injectables,
                      bool capture_exception, int64_t trace_back_depth, std::string wiring_path_name,
                      std::optional<std::string> label, bool capture_values, std::optional<std::string> record_replay_id);

        std::string                                                         name;
        NodeTypeEnum                                                        node_type;
        std::vector<std::string>                                            args;
        std::optional<std::unordered_map<std::string, nb::object>>          time_series_inputs;
        std::optional<nb::object>                                           time_series_output;
        std::optional<nb::dict>                                             scalars;
        nb::object                                                          src_location;
        std::optional<std::unordered_set<std::string>>                      active_inputs;
        std::optional<std::unordered_set<std::string>>                      valid_inputs;
        std::optional<std::unordered_set<std::string>>                      all_valid_inputs;
        std::optional<std::unordered_set<std::string>>                      context_inputs;
        std::optional<std::unordered_map<std::string, InjectableTypesEnum>> injectable_inputs;
        size_t                                                              injectables;
        bool                                                                capture_exception;
        int64_t                                                             trace_back_depth;
        std::string                                                         wiring_path_name;
        std::optional<std::string>                                          label;
        bool                                                                capture_values;
        std::optional<std::string>                                          record_replay_id;

        [[nodiscard]] nb::object get_arg_type(const std::string &arg) const;

        [[nodiscard]] std::string signature() const;

        [[nodiscard]] bool uses_scheduler() const;

        [[nodiscard]] bool uses_clock() const;

        [[nodiscard]] bool uses_engine() const;

        [[nodiscard]] bool uses_state() const;

        [[nodiscard]] bool uses_recordable_state() const;

        [[nodiscard]] std::optional<std::string> recordable_state_arg() const;

        [[nodiscard]] bool uses_output_feedback() const;

        [[nodiscard]] bool is_source_node() const;

        [[nodiscard]] bool is_push_source_node() const;

        [[nodiscard]] bool is_pull_source_node() const;

        [[nodiscard]] bool is_compute_node() const;

        [[nodiscard]] bool is_sink_node() const;

        [[nodiscard]] bool is_recordable() const;

        [[nodiscard]] nb::dict to_dict() const;
        [[nodiscard]] ptr      copy_with(nb::kwargs kwargs) const;

        static void register_with_nanobind(nb::module_ &m);
    };

    struct NodeScheduler : nanobind::intrusive_base
    {
        using ptr = nanobind::ref<NodeScheduler>;

        explicit NodeScheduler(Node &node);

        [[nodiscard]] engine_time_t next_scheduled_time() const;
        [[nodiscard]] bool          requires_scheduling() const;
        [[nodiscard]] bool          is_scheduled() const;
        [[nodiscard]] bool          is_scheduled_node() const;
        [[nodiscard]] bool          has_tag(const std::string &tag) const;
        engine_time_t               pop_tag(const std::string &tag);
        engine_time_t               pop_tag(const std::string &tag, engine_time_t default_time);
        void                        schedule(engine_time_t when, std::optional<std::string> tag, bool on_wall_clock = false);
        void                        schedule(engine_time_delta_t when, std::optional<std::string> tag, bool on_wall_clock = false);
        void                        un_schedule(const std::string &tag);
        void                        un_schedule();
        void                        reset();
        void                        advance();

        static void register_with_nanobind(nb::module_ &m);

      protected:
        void _on_alarm(engine_time_t when, std::string tag);

      private:
        Node                                           &_node;
        std::set<std::pair<engine_time_t, std::string>> _scheduled_events;
        std::unordered_map<std::string, engine_time_t>  _tags;
        std::unordered_map<std::string, engine_time_t>  _alarm_tags;
        engine_time_t                                   _last_scheduled_time{MIN_DT};
    };

    struct HGRAPH_EXPORT Node : ComponentLifeCycle, Notifiable
    {
        using ptr = nanobind::ref<Node>;

        Node(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::ptr signature, nb::dict scalars);

        void eval();

        void notify(engine_time_t modified_time) override;

        void notify();

        void notify_next_cycle();

        int64_t node_ndx() const;

        const std::vector<int64_t> &owning_graph_id() const;

        std::vector<int64_t> node_id() const;

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

        bool has_recordable_state() const;

        NodeScheduler &scheduler();
        bool           has_scheduler() const;
        void           unset_scheduler();

        TimeSeriesOutput &error_output();

        void set_error_output(time_series_output_ptr value);

        friend struct Graph;

        void add_start_input(nb::ref<TimeSeriesReferenceInput> input);

        static void register_with_nanobind(nb::module_ &m);

        bool has_input() const;
        bool has_output() const;

        std::string repr() const;

        std::string str() const;

      protected:
        virtual void do_eval() = 0;

        void _initialise_inputs();

      private:
        int64_t                       _node_ndx;
        std::vector<int64_t>          _owning_graph_id;
        NodeSignature::ptr            _signature;
        nb::dict                      _scalars;
        graph_ptr                     _graph;
        time_series_bundle_input_ptr  _input;
        time_series_output_ptr        _output;
        time_series_output_ptr        _error_output;
        time_series_bundle_output_ptr _recordable_state;
        NodeScheduler::ptr            _scheduler;
        // I am not a fan of this approach to managing the start inputs, but for now keep consistent with current code base in
        // Python.
        std::vector<nb::ref<TimeSeriesReferenceInput>> _start_inputs;
        std::vector<nb::ref<TimeSeriesInput>>          _check_valid_inputs;
        std::vector<nb::ref<TimeSeriesInput>>          _check_all_valid_inputs;
    };

    struct BasePythonNode : Node
    {
        BasePythonNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::ptr signature, nb::dict scalars,
                       nb::callable eval_fn, nb::callable start_fn, nb::callable stop_fn);

        void _initialise_kwargs();

        void _initialise_state();

      protected:
        void do_eval() override;
        virtual void do_start();
        virtual void do_stop();
        void         initialise() override;
        void         start() override;
        void         stop() override;
        void         dispose() override;

        nb::callable _eval_fn;
        nb::callable _start_fn;
        nb::callable _stop_fn;

        nb::kwargs _kwargs;
    };

    struct PythonNode : BasePythonNode
    {
        using BasePythonNode::BasePythonNode;

      protected:
        void do_eval() override;

        void initialise() override;
        void start() override;
        void stop() override;
        void dispose() override;
    };

    struct PushQueueNode : Node
    {
        using Node::Node;

        void enqueue_message(nb::object message);

        [[nodiscard]] bool apply_message(nb::object message);

        int64_t messages_in_queue() const;

        void set_receiver(sender_receiver_state_ptr value);

      protected:
        void do_eval() override;
        void start() override;

      private:
        sender_receiver_state_ptr _receiver;
        int64_t                   _messages_queued{0};
        int64_t                   _messages_dequeued{0};
        bool                      _elide{false};
    };

    struct PythonGeneratorNode : BasePythonNode
    {
        using BasePythonNode::BasePythonNode;
        nb::iterator generator{};
        nb::object   next_value{};

      protected:
        void do_eval() override;
        void start() override;
    };
}  // namespace hgraph

#endif  // NODE_H
