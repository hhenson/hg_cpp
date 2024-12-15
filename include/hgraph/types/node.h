
#ifndef NODE_H
#define NODE_H

#include <hgraph/python/pyb.h>
#include <hgraph/hgraph_export.h>
#include <vector>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <sstream>

#include <hgraph/util/lifecycle.h>
#include <nanobind/intrusive/ref.h>
#include <hgraph/util/date_time.h>

namespace hgraph {
    struct Graph;

    template<typename Enum>
    typename std::enable_if<std::is_enum<Enum>::value, Enum>::type
    operator|(Enum lhs, Enum rhs) {
        using underlying = typename std::underlying_type<Enum>::type;
        return static_cast<Enum>(
            static_cast<underlying>(lhs) | static_cast<underlying>(rhs)
        );
    }

    template<typename Enum>
    typename std::enable_if<std::is_enum<Enum>::value, Enum>::type
    operator&(Enum lhs, Enum rhs) {
        using underlying = typename std::underlying_type<Enum>::type;
        return static_cast<Enum>(
            static_cast<underlying>(lhs) & static_cast<underlying>(rhs)
        );
    }

    enum class NodeTypeEnum : char8_t {
        NONE = 0,
        SOURCE_NODE = 1,
        PUSH_SOURCE_NODE = SOURCE_NODE | (1 << 1),
        PULL_SOURCE_NODE = SOURCE_NODE | (1 << 2),
        COMPUTE_NODE = 1 << 3,
        SINK_NODE = 1 << 4
    };

    void node_type_enum_py_register(nb::module_ &m);

    enum class InjectableTypesEnum : char8_t {
        NONE = 0,
        STATE = 1,
        SCHEDULER = 1 << 1,
        OUTPUT = 1 << 2,
        CLOCK = 1 << 3,
        ENGINE_API = 1 << 4,
        REPLAY_STATE = 1 << 5,
        LOGGER = 1 << 6
    };

    void injectable_type_enum(nb::module_ &m);

    struct HGRAPH_EXPORT NodeSignature {
        using ptr = nanobind::ref<NodeSignature>;

        std::string name{};
        NodeTypeEnum node_type{NodeTypeEnum::NONE};
        std::vector<std::string> args{};
        std::optional<std::unordered_map<std::string, nb::object> > time_series_inputs{};
        std::optional<nb::object> time_series_output{};
        std::optional<std::unordered_map<std::string, nb::object> > scalars{};
        nb::object src_location{nb::none()};
        std::optional<std::unordered_set<std::string> > active_inputs{};
        std::optional<std::unordered_set<std::string> > valid_inputs{};
        std::optional<std::unordered_set<std::string> > all_valid_inputs{};
        std::optional<std::unordered_set<std::string> > context_inputs{};
        InjectableTypesEnum injectable_inputs{InjectableTypesEnum::NONE};
        std::string wiring_path_name{};
        std::optional<std::string> label{};
        std::optional<std::string> record_replay_id{};
        bool capture_values{false};
        bool capture_exception{false};
        char8_t trace_back_depth{1};

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

    struct TimeSeriesInput;
    struct TimeSeriesBundleInput;
    struct TimeSeriesBundleOutput;
    struct TimeSeriesOutput;

    struct NodeScheduler {
        [[nodiscard]] engine_time_t next_scheduled_time() const;
        [[nodiscard]] bool is_scheduled() const;
        [[nodiscard]] bool is_scheduled_node() const;
        [[nodiscard]] bool has_tag(const std::string& tag) const;
        engine_time_t pop_tag(const std::string& tag, std::optional<engine_time_t> default_time);
        void schedule(engine_time_t when, std::optional<std::string> tag);
        void schedule(engine_time_delta_t when, std::optional<std::string> tag);
        void un_schedule(std::optional<std::string> tag);
        void reset();
    };

    struct HGRAPH_EXPORT Node : ComponentLifeCycle {
        using ptr = nanobind::ref<Node>;

        Node(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature signature, nb::dict scalars);

        int64_t _node_ndx;
        std::vector<int64_t> _owning_graph_id;
        std::vector<int64_t> _node_id;
        NodeSignature::ptr _signature;
        nb::dict _scalars;
        Graph::ptr _graph;
        nb::ref<TimeSeriesBundleInput> _input;
        nb::ref<TimeSeriesOutput> _output;
        nb::ref<TimeSeriesOutput> _error_output;
        nb::ref<TimeSeriesBundleOutput> _recordable_state;
        std::optional<NodeScheduler> _scheduler;

        std::vector<nb::ref<TimeSeriesInput>> start_inputs() const;

        virtual void eval() = 0;
        virtual void notify(engine_time_t modified_time);
        virtual void notify_next_cycle();

        int64_t node_ndx() const { return _node_ndx; }

        const std::vector<int64_t> &owning_graph_id() const { return _owning_graph_id; }

        const std::vector<int64_t> &node_id() const { return _node_id; }

        const NodeSignature::ptr &signature() const { return _signature; }

        const nb::dict &scalars() const { return _scalars; }

        Graph &graph() const { return *_graph; }

        void set_graph(Graph::ptr value) { _graph = std::move(value); }

        TimeSeriesBundleInput &input() const { return *_input; }

        void set_input(nb::ref<TimeSeriesBundleInput> value) { _input = value; }

        nb::ref<TimeSeriesOutput> output() const { return _output; }

        void set_output(nb::ref<TimeSeriesOutput> value) { _output = value; }

        nb::ref<TimeSeriesBundleOutput> recordable_state() const { return _recordable_state; }

        void set_recordable_state(nb::ref<TimeSeriesBundleOutput> value) { _recordable_state = value; }

        std::optional<NodeScheduler> get_scheduler() const { return _scheduler; }

        nb::ref<TimeSeriesOutput> get_error_output() const { return _error_output; }

    };
}

#endif //NODE_H
