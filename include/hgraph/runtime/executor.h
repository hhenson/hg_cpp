#ifndef HGRAPH_RUNTIME_EXECUTOR_H
#define HGRAPH_RUNTIME_EXECUTOR_H

#include <hgraph/runtime/graph.h>

namespace hgraph
{
    class GraphExecutorBuilder;
    class GraphExecutorValue;
    class GraphExecutorView;

    /** Engine execution mode for the first-pass graph executor. */
    enum class GraphExecutorMode
    {
        Simulation,
        RealTime,
    };

    /** Schema/config descriptor for a graph executor. */
    struct HGRAPH_EXPORT GraphExecutorTypeMetaData
    {
        const char *display_name{nullptr};
        GraphExecutorMode mode{GraphExecutorMode::Simulation};

        [[nodiscard]] std::string_view name() const noexcept;
    };

    /** Type-erased executor operation table. */
    struct HGRAPH_EXPORT GraphExecutorOps
    {
        const void *context{nullptr};

        void (*run_impl)(const void *context, const GraphExecutorView &executor) = nullptr;
        void (*request_stop_impl)(const void *context, void *memory) noexcept = nullptr;
        [[nodiscard]] bool (*stop_requested_impl)(const void *context, const void *memory) noexcept = nullptr;
        [[nodiscard]] engine_time_t (*start_time_impl)(const void *context, const void *memory) noexcept = nullptr;
        [[nodiscard]] engine_time_t (*end_time_impl)(const void *context, const void *memory) noexcept = nullptr;
        [[nodiscard]] GraphView (*graph_impl)(const void *context, void *memory) = nullptr;
    };

    using GraphExecutorTypeBinding = TypeBinding<GraphExecutorTypeMetaData, GraphExecutorOps>;
    using GraphExecutorStorageRef  = MemoryUtils::StorageRef<GraphExecutorTypeBinding>;

    /** Borrowed type-erased executor view. */
    class HGRAPH_EXPORT GraphExecutorView
    {
      public:
        GraphExecutorView() noexcept;
        GraphExecutorView(const GraphExecutorTypeBinding *binding, void *memory) noexcept;
        GraphExecutorView(const GraphExecutorView &) = delete;
        GraphExecutorView &operator=(const GraphExecutorView &) = delete;
        GraphExecutorView(GraphExecutorView &&) noexcept = default;
        GraphExecutorView &operator=(GraphExecutorView &&) noexcept = default;

        [[nodiscard]] bool valid() const noexcept;
        [[nodiscard]] const GraphExecutorTypeBinding *binding() const noexcept;
        [[nodiscard]] const GraphExecutorTypeMetaData *schema() const noexcept;
        [[nodiscard]] void *data() const noexcept;

        [[nodiscard]] engine_time_t start_time() const noexcept;
        [[nodiscard]] engine_time_t end_time() const noexcept;
        [[nodiscard]] bool stop_requested() const noexcept;
        [[nodiscard]] GraphView graph() const;

        void run() const;
        void request_stop() const noexcept;

      private:
        [[nodiscard]] const GraphExecutorOps &ops() const;

        GraphExecutorStorageRef storage_{};
    };

    /** Owning graph executor value. */
    class HGRAPH_EXPORT GraphExecutorValue
    {
      public:
        using storage_type = MemoryUtils::StorageHandle<MemoryUtils::InlineStoragePolicy<>, GraphExecutorTypeBinding>;

        GraphExecutorValue() noexcept;
        explicit GraphExecutorValue(const GraphExecutorBuilder &builder);
        ~GraphExecutorValue();

        GraphExecutorValue(const GraphExecutorValue &) = delete;
        GraphExecutorValue &operator=(const GraphExecutorValue &) = delete;
        GraphExecutorValue(GraphExecutorValue &&) noexcept = default;
        GraphExecutorValue &operator=(GraphExecutorValue &&) noexcept = default;

        [[nodiscard]] bool has_value() const noexcept;
        [[nodiscard]] GraphExecutorView view();
        [[nodiscard]] GraphExecutorView view() const;

      private:
        storage_type storage_{};
    };

    /** Reusable construction recipe for graph executors. */
    class HGRAPH_EXPORT GraphExecutorBuilder
    {
      public:
        GraphExecutorBuilder();

        GraphExecutorBuilder &label(std::string label);
        GraphExecutorBuilder &graph_builder(GraphBuilder graph_builder);
        GraphExecutorBuilder &mode(GraphExecutorMode mode) noexcept;
        GraphExecutorBuilder &start_time(engine_time_t start_time) noexcept;
        GraphExecutorBuilder &end_time(engine_time_t end_time) noexcept;

        [[nodiscard]] std::string_view label() const noexcept;
        [[nodiscard]] const GraphBuilder &graph_builder() const noexcept;
        [[nodiscard]] GraphExecutorMode mode() const noexcept;
        [[nodiscard]] engine_time_t start_time() const noexcept;
        [[nodiscard]] engine_time_t end_time() const noexcept;
        [[nodiscard]] const GraphTypeBinding &graph_binding() const;
        [[nodiscard]] const GraphExecutorTypeBinding &binding() const;
        [[nodiscard]] GraphExecutorValue make_executor() const;

      private:
        friend class GraphExecutorValue;

        std::string       label_{};
        GraphBuilder      graph_builder_{};
        GraphExecutorMode mode_{GraphExecutorMode::Simulation};
        engine_time_t     start_time_{MIN_ST};
        engine_time_t     end_time_{MAX_ET};
    };

}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_EXECUTOR_H
