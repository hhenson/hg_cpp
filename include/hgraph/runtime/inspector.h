#ifndef HGRAPH_RUNTIME_INSPECTOR_H
#define HGRAPH_RUNTIME_INSPECTOR_H

#include <hgraph/hgraph_export.h>
#include <hgraph/runtime/evaluation_profiler.h>
#include <hgraph/runtime/lifecycle_observer.h>
#include <hgraph/runtime/node.h>
#include <hgraph/util/date_time.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace hgraph
{
    enum class InspectionEntityKind : std::uint8_t
    {
        Graph,
        Node,
    };

    /** One owned graph/node record in an inspection snapshot. */
    struct HGRAPH_EXPORT InspectionEntry
    {
        std::uint64_t id{0};
        std::uint64_t parent_id{0};
        std::vector<std::uint64_t> children{};
        std::string path{};
        std::string label{};
        std::string schema_label{};
        std::string implementation_label{};
        InspectionEntityKind kind{InspectionEntityKind::Node};
        NodeKind node_kind{NodeKind::Compute};
        bool started{false};
        bool stopped{false};
        DateTime evaluation_time{MIN_DT};
        DateTime scheduled_time{MIN_DT};
        NodeStorageMetrics storage{};
        NodeStorageMetrics peak_storage{};
        EvaluationProfilePhase start{};
        EvaluationProfilePhase evaluation{};
        EvaluationProfilePhase stop{};
    };

    /** Self-contained runtime inspection result. */
    struct HGRAPH_EXPORT InspectionSnapshot
    {
        std::uint64_t graph_cycles{0};
        TimeDelta wall_time{0};
        TimeDelta root_evaluation_time{0};
        TimeDelta scheduling_lag_total{0};
        TimeDelta scheduling_lag_max{0};
        std::uint64_t scheduling_lag_samples{0};
        double runtime_load{0.0};
        std::size_t planned_bytes{0};
        std::size_t dynamic_live_bytes{0};
        std::size_t dynamic_reserved_bytes{0};
        std::size_t peak_dynamic_live_bytes{0};
        std::size_t peak_dynamic_reserved_bytes{0};
        std::vector<InspectionEntry> entries{};
    };

    struct HGRAPH_EXPORT InspectorOptions
    {
        std::size_t recent_window{100};
    };

    /**
     * Native graph inspector.
     *
     * Lifecycle callbacks copy runtime state into owned records. ``snapshot``
     * therefore remains valid after nested graph stop/delete/erase and never
     * walks borrowed graph or node pointers. Copies share one collector state
     * so a caller-owned handle can observe an executor-owned observer copy.
     */
    class HGRAPH_EXPORT Inspector final : public LifecycleObserver
    {
      public:
        struct State;

        explicit Inspector(InspectorOptions options = {});
        explicit Inspector(std::size_t recent_window);

        [[nodiscard]] InspectionSnapshot snapshot() const;
        void reset();

        void on_before_start_graph(const GraphView &graph) override;
        void on_after_start_graph(const GraphView &graph) override;
        void on_start_graph_failed(const GraphView &graph) override;
        void on_before_start_node(const NodeView &node) override;
        void on_after_start_node(const NodeView &node) override;
        void on_start_node_failed(const NodeView &node) override;
        void on_before_graph_evaluation(const GraphView &graph) override;
        void on_after_graph_evaluation(const GraphView &graph) override;
        void on_before_node_evaluation(const NodeView &node) override;
        void on_after_node_evaluation(const NodeView &node) override;
        void on_after_graph_push_nodes_evaluation(const GraphView &graph) override;
        void on_before_stop_node(const NodeView &node) override;
        void on_after_stop_node(const NodeView &node) override;
        void on_stop_node_failed(const NodeView &node) override;
        void on_before_stop_graph(const GraphView &graph) override;
        void on_after_stop_graph(const GraphView &graph) override;
        void on_stop_graph_failed(const GraphView &graph) override;

      private:
        InspectorOptions options_{};
        std::shared_ptr<State> state_{};
    };
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_INSPECTOR_H
