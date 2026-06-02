#ifndef HGRAPH_RUNTIME_NODE_H
#define HGRAPH_RUNTIME_NODE_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/metadata/type_binding.h>
#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/time_series/endpoint_schema.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/util/date_time.h>

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace hgraph
{
    class GraphValue;
    class GraphView;
    class NodeBuilder;
    class NodeValue;
    class NodeView;
    struct NodeSchedulerState;

    /** Runtime node category used by graph construction and evaluation. */
    enum class NodeKind
    {
        Compute,
        PushSource,
        PullSource,
        Sink,
        Nested,
    };

    /**
     * Interned node schema descriptor.
     *
     * The schema records the node contract only: input/output time-series
     * shapes, optional local state schema, kind, and readiness selectors. The
     * executable behaviour lives in ``NodeOps`` and its context.
     */
    struct HGRAPH_EXPORT NodeTypeMetaData
    {
        const char *display_name{nullptr};

        const TSValueTypeMetaData *input_schema{nullptr};
        const TSValueTypeMetaData *output_schema{nullptr};
        const TSValueTypeMetaData *error_output_schema{nullptr};
        const TSValueTypeMetaData *recordable_state_schema{nullptr};
        const ValueTypeMetaData   *state_schema{nullptr};
        const ValueTypeMetaData   *scalar_schema{nullptr};

        NodeKind node_kind{NodeKind::Compute};
        bool     uses_scheduler{false};
        // When set, the framework schedules this node for the current cycle during
        // ``start`` (the declarative form of a source doing schedule_now() itself).
        bool     schedule_on_start{false};
        bool     captures_errors{false};

        std::vector<std::size_t> active_inputs{};
        std::vector<std::size_t> valid_inputs{};
        std::vector<std::size_t> all_valid_inputs{};

        [[nodiscard]] std::string_view name() const noexcept;
        [[nodiscard]] bool has_input() const noexcept;
        [[nodiscard]] bool has_output() const noexcept;
        [[nodiscard]] bool has_state() const noexcept;
        [[nodiscard]] bool has_scalars() const noexcept;
        [[nodiscard]] bool has_error_output() const noexcept;
        [[nodiscard]] bool has_recordable_state() const noexcept;
    };

    /**
     * Type-erased node operation table.
     *
     * The table is intentionally passive: it is a context pointer plus
     * function pointers. Runtime policy such as graph scheduling and
     * lifecycle sequencing is driven by ``NodeView`` / ``GraphView`` through
     * these operations.
     */
    struct HGRAPH_EXPORT NodeOps
    {
        const void *context{nullptr};

        void (*attach_graph_impl)(const void *context, void *memory, GraphValue *graph,
                                  std::size_t node_index) = nullptr;
        [[nodiscard]] GraphValue *(*graph_impl)(const void *context, const void *memory) noexcept = nullptr;
        [[nodiscard]] std::size_t (*node_index_impl)(const void *context, const void *memory) noexcept = nullptr;

        [[nodiscard]] bool (*started_impl)(const void *context, const void *memory) noexcept = nullptr;
        void (*start_impl)(const void *context, const NodeView &view, engine_time_t evaluation_time) = nullptr;
        void (*stop_impl)(const void *context, const NodeView &view, engine_time_t evaluation_time) = nullptr;
        void (*evaluate_impl)(const void *context, const NodeView &view, engine_time_t evaluation_time, bool force) = nullptr;
        void (*cleanup_delta_impl)(const void *context, const NodeView &view) = nullptr;

        [[nodiscard]] bool (*has_input_impl)(const void *context, const void *memory) noexcept = nullptr;
        [[nodiscard]] bool (*has_output_impl)(const void *context, const void *memory) noexcept = nullptr;
        [[nodiscard]] bool (*has_state_impl)(const void *context, const void *memory) noexcept = nullptr;
        [[nodiscard]] bool (*has_scalars_impl)(const void *context, const void *memory) noexcept = nullptr;
        [[nodiscard]] bool (*has_scheduler_impl)(const void *context, const void *memory) noexcept = nullptr;
        [[nodiscard]] bool (*has_error_output_impl)(const void *context, const void *memory) noexcept = nullptr;
        [[nodiscard]] bool (*has_recordable_state_impl)(const void *context, const void *memory) noexcept = nullptr;

        [[nodiscard]] TSInputView (*input_view_impl)(const void *context, void *memory,
                                                     engine_time_t evaluation_time) = nullptr;
        [[nodiscard]] TSOutputView (*output_view_impl)(const void *context, void *memory,
                                                       engine_time_t evaluation_time) = nullptr;
        [[nodiscard]] ValueView (*state_view_impl)(const void *context, void *memory) = nullptr;
        [[nodiscard]] ValueView (*scalars_view_impl)(const void *context, void *memory) = nullptr;
        [[nodiscard]] NodeSchedulerState *(*scheduler_state_impl)(const void *context, void *memory) = nullptr;
        [[nodiscard]] TSOutputView (*error_output_view_impl)(const void *context, void *memory,
                                                             engine_time_t evaluation_time) = nullptr;
        [[nodiscard]] TSOutputView (*recordable_state_view_impl)(const void *context, void *memory,
                                                                 engine_time_t evaluation_time) = nullptr;
    };

    using NodeTypeBinding = TypeBinding<NodeTypeMetaData, NodeOps>;
    using NodeStorageRef  = MemoryUtils::StorageRef<NodeTypeBinding>;

    /** User callbacks used by the first-pass native node builder. */
    struct HGRAPH_EXPORT NodeCallbacks
    {
        std::function<void(const NodeView &, engine_time_t)> start{};
        std::function<void(const NodeView &, engine_time_t)> evaluate{};
        std::function<void(const NodeView &, engine_time_t)> stop{};
    };

    /**
     * Borrowed type-erased view over a node allocation.
     *
     * ``NodeView`` carries the interned binding and the node memory. Time-
     * sensitive lifecycle operations and input/output projections receive the
     * evaluation time explicitly. The active input notification target is
     * recovered from the node runtime memory.
     */
    class HGRAPH_EXPORT NodeView
    {
      public:
        NodeView() noexcept;
        NodeView(const NodeTypeBinding *binding, void *memory) noexcept;
        NodeView(const NodeView &) = delete;
        NodeView &operator=(const NodeView &) = delete;
        NodeView(NodeView &&) noexcept = default;
        NodeView &operator=(NodeView &&) noexcept = default;

        [[nodiscard]] bool valid() const noexcept;
        [[nodiscard]] const NodeTypeBinding *binding() const noexcept;
        [[nodiscard]] const NodeTypeMetaData *schema() const noexcept;
        [[nodiscard]] void *data() const noexcept;

        [[nodiscard]] std::string_view label() const noexcept;
        [[nodiscard]] NodeKind node_kind() const noexcept;
        [[nodiscard]] bool started() const noexcept;
        [[nodiscard]] std::size_t node_index() const noexcept;
        [[nodiscard]] GraphValue *graph_value() const noexcept;
        [[nodiscard]] GraphView graph() const;

        [[nodiscard]] bool has_input() const noexcept;
        [[nodiscard]] bool has_output() const noexcept;
        [[nodiscard]] bool has_state() const noexcept;
        [[nodiscard]] bool has_scalars() const noexcept;
        [[nodiscard]] bool has_scheduler() const noexcept;
        [[nodiscard]] bool has_error_output() const noexcept;
        [[nodiscard]] bool has_recordable_state() const noexcept;

        [[nodiscard]] TSInputView input(engine_time_t evaluation_time) const;
        [[nodiscard]] TSOutputView output(engine_time_t evaluation_time) const;
        [[nodiscard]] ValueView state() const;
        [[nodiscard]] ValueView scalars() const;
        /** Borrow this node's persistent scheduler state (only valid when ``has_scheduler``). */
        [[nodiscard]] NodeSchedulerState &scheduler_state() const;
        [[nodiscard]] TSOutputView error_output(engine_time_t evaluation_time) const;
        [[nodiscard]] TSOutputView recordable_state(engine_time_t evaluation_time) const;

        void start(engine_time_t evaluation_time) const;
        void stop(engine_time_t evaluation_time) const;
        void evaluate(engine_time_t evaluation_time, bool force = false) const;
        void cleanup_delta() const;

      private:
        [[nodiscard]] const NodeOps &ops() const;

        NodeStorageRef storage_{};
    };

    /**
     * Owning node value.
     *
     * ``NodeValue`` owns the node allocation through the same
     * ``StorageHandle``/binding pattern used by values and time-series data.
     * The stable active-input notification identity lives in the node runtime
     * storage header and is reached through the node memory pointer.
     */
    class HGRAPH_EXPORT NodeValue final
    {
      public:
        using storage_type = MemoryUtils::StorageHandle<MemoryUtils::InlineStoragePolicy<>, NodeTypeBinding>;

        NodeValue() noexcept;
        explicit NodeValue(const NodeBuilder &builder, std::size_t node_index = 0);
        ~NodeValue();

        NodeValue(const NodeValue &) = delete;
        NodeValue &operator=(const NodeValue &) = delete;
        NodeValue(NodeValue &&other) noexcept;
        NodeValue &operator=(NodeValue &&other) noexcept;

        [[nodiscard]] bool has_value() const noexcept;
        [[nodiscard]] const NodeTypeBinding *binding() const noexcept;
        [[nodiscard]] const NodeTypeMetaData *schema() const noexcept;

        [[nodiscard]] NodeView view();
        [[nodiscard]] NodeView view() const;

        void attach_graph(GraphValue *graph, std::size_t node_index);

      private:
        storage_type storage_{};
    };

    /**
     * Reusable builder for node values.
     *
     * Builders are construction recipes. They cache a binding plus input
     * endpoint annotation and can be reused to create many runtime node
     * instances.
     */
    class HGRAPH_EXPORT NodeBuilder
    {
      public:
        NodeBuilder();

        [[nodiscard]] static NodeBuilder native(NodeTypeMetaData schema,
                                                NodeCallbacks callbacks = {},
                                                TSEndpointSchema input_endpoint = {});

        /**
         * Typed front-end over ``native``: build this node from a static node
         * implementation ``TImplementation`` (a stateless struct with a static
         * ``eval`` taking ``In<>`` / ``Out<>`` / ``State<>`` parameters, plus
         * optional ``start`` / ``stop``). Defined in
         * ``<hgraph/types/static_node.h>``, which must be included to use it.
         */
        template <typename TImplementation>
        NodeBuilder &implementation();

        NodeBuilder &label(std::string label);
        [[nodiscard]] std::string_view label() const noexcept;

        /** Per-instance immutable scalar configuration (its shape is the schema's ``scalar_schema``). */
        NodeBuilder &scalars(Value scalars);
        [[nodiscard]] const Value &scalars() const noexcept;

        [[nodiscard]] const NodeTypeBinding &binding() const;
        [[nodiscard]] const TSEndpointSchema &input_endpoint() const noexcept;
        [[nodiscard]] NodeValue make_node(std::size_t node_index = 0) const;

      private:
        friend class NodeValue;

        NodeBuilder(const NodeTypeBinding &binding, TSEndpointSchema input_endpoint);

        const NodeTypeBinding *binding_{nullptr};
        TSEndpointSchema       input_endpoint_{};
        std::string            label_{};
        Value                  scalars_{};
    };

}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_NODE_H
