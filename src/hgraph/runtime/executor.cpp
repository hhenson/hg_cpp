#include <hgraph/runtime/executor.h>

#include <deque>
#include <memory>
#include <stdexcept>
#include <utility>

namespace hgraph
{
    namespace
    {
        struct GraphExecutorStorage
        {
            explicit GraphExecutorStorage(const GraphExecutorBuilder &builder)
                : graph(builder.graph_builder().make_graph()),
                  start_time(builder.start_time()),
                  end_time(builder.end_time())
            {
            }

            GraphValue    graph{};
            DateTime start_time{MIN_ST};
            DateTime end_time{MAX_ET};
            bool          stop_requested{false};
        };

        [[nodiscard]] GraphExecutorStorage &storage(void *memory)
        {
            if (memory == nullptr) { throw std::logic_error("GraphExecutor storage is null"); }
            return *MemoryUtils::cast<GraphExecutorStorage>(memory);
        }

        [[nodiscard]] const GraphExecutorStorage &storage(const void *memory)
        {
            if (memory == nullptr) { throw std::logic_error("GraphExecutor storage is null"); }
            return *MemoryUtils::cast<GraphExecutorStorage>(memory);
        }

        void run_impl(const void *, const GraphExecutorView &executor)
        {
            auto graph = executor.graph();
            graph.start(executor.start_time());

            try
            {
                while (!executor.stop_requested())
                {
                    const DateTime next = graph.next_scheduled_time();
                    if (next == MAX_DT || next >= executor.end_time()) { break; }
                    graph.evaluate(next);
                }
                graph.stop();
            }
            catch (...)
            {
                try { graph.stop(); }
                catch (...) {}
                throw;
            }
        }

        void request_stop_impl(const void *, void *memory) noexcept
        {
            storage(memory).stop_requested = true;
        }

        bool stop_requested_impl(const void *, const void *memory) noexcept
        {
            return storage(memory).stop_requested;
        }

        DateTime start_time_impl(const void *, const void *memory) noexcept
        {
            return storage(memory).start_time;
        }

        DateTime end_time_impl(const void *, const void *memory) noexcept
        {
            return storage(memory).end_time;
        }

        GraphView graph_impl(const void *, void *memory)
        {
            return storage(memory).graph.view();
        }

        struct ExecutorRuntimeRegistry
        {
            const GraphExecutorTypeBinding &make_binding(const GraphExecutorBuilder &builder)
            {
                GraphExecutorTypeMetaData meta;
                names.push_back(std::make_unique<std::string>(std::string{builder.label()}));
                if (!names.back()->empty()) { meta.display_name = names.back()->c_str(); }
                meta.mode = builder.mode();

                schemas.push_back(meta);
                return GraphExecutorTypeBinding::intern(schemas.back(), MemoryUtils::plan_for<GraphExecutorStorage>(), ops());
            }

            static const GraphExecutorOps &ops()
            {
                static const GraphExecutorOps table{
                    .context = nullptr,
                    .run_impl = &run_impl,
                    .request_stop_impl = &request_stop_impl,
                    .stop_requested_impl = &stop_requested_impl,
                    .start_time_impl = &start_time_impl,
                    .end_time_impl = &end_time_impl,
                    .graph_impl = &graph_impl,
                };
                return table;
            }

            std::deque<GraphExecutorTypeMetaData>       schemas{};
            std::vector<std::unique_ptr<std::string>>   names{};
        };

        ExecutorRuntimeRegistry &executor_runtime_registry()
        {
            static ExecutorRuntimeRegistry registry;
            return registry;
        }

        void default_run_impl(const void *, const GraphExecutorView &)
        {
            throw std::logic_error("GraphExecutorView::run requires a live executor");
        }

        void default_request_stop_impl(const void *, void *) noexcept {}

        bool default_stop_requested_impl(const void *, const void *) noexcept { return false; }
        DateTime default_start_time_impl(const void *, const void *) noexcept { return MIN_ST; }
        DateTime default_end_time_impl(const void *, const void *) noexcept { return MAX_ET; }

        GraphView default_graph_impl(const void *, void *)
        {
            return GraphView{};
        }

        const GraphExecutorOps &default_executor_ops()
        {
            static const GraphExecutorOps table{
                .context = nullptr,
                .run_impl = &default_run_impl,
                .request_stop_impl = &default_request_stop_impl,
                .stop_requested_impl = &default_stop_requested_impl,
                .start_time_impl = &default_start_time_impl,
                .end_time_impl = &default_end_time_impl,
                .graph_impl = &default_graph_impl,
            };
            return table;
        }

        const GraphExecutorTypeBinding &default_executor_binding()
        {
            static const GraphExecutorTypeMetaData meta{};
            static const GraphExecutorTypeBinding binding{
                .type_meta = &meta,
                .storage_plan = &MemoryUtils::plan_for<std::byte>(),
                .ops = &default_executor_ops(),
            };
            return binding;
        }
    }  // namespace

    std::string_view GraphExecutorTypeMetaData::name() const noexcept
    {
        return display_name != nullptr ? std::string_view{display_name} : std::string_view{};
    }

    GraphExecutorView::GraphExecutorView() noexcept
        : storage_(GraphExecutorStorageRef::empty(default_executor_binding()))
    {
    }

    GraphExecutorView::GraphExecutorView(const GraphExecutorTypeBinding *binding, void *memory) noexcept
        : storage_(binding != nullptr && memory != nullptr ? binding : &default_executor_binding(),
                   binding != nullptr && memory != nullptr ? memory : nullptr)
    {
    }

    bool GraphExecutorView::valid() const noexcept { return storage_.has_value(); }
    const GraphExecutorTypeBinding *GraphExecutorView::binding() const noexcept
    {
        return storage_.binding();
    }
    const GraphExecutorTypeMetaData *GraphExecutorView::schema() const noexcept
    {
        return binding()->type_meta;
    }
    void *GraphExecutorView::data() const noexcept { return storage_.data(); }

    DateTime GraphExecutorView::start_time() const noexcept
    {
        return ops().start_time_impl(ops().context, data());
    }

    DateTime GraphExecutorView::end_time() const noexcept
    {
        return ops().end_time_impl(ops().context, data());
    }

    bool GraphExecutorView::stop_requested() const noexcept
    {
        return ops().stop_requested_impl(ops().context, data());
    }

    GraphView GraphExecutorView::graph() const
    {
        return ops().graph_impl(ops().context, data());
    }

    void GraphExecutorView::run() const
    {
        ops().run_impl(ops().context, *this);
    }

    void GraphExecutorView::request_stop() const noexcept
    {
        ops().request_stop_impl(ops().context, data());
    }

    const GraphExecutorOps &GraphExecutorView::ops() const
    {
        return storage_.binding()->ops_ref();
    }

    GraphExecutorValue::GraphExecutorValue() noexcept = default;

    GraphExecutorValue::GraphExecutorValue(const GraphExecutorBuilder &builder)
    {
        const auto &binding = builder.binding();
        storage_ = storage_type::owning_constructed(binding, [&](void *dst) {
            std::construct_at(MemoryUtils::cast<GraphExecutorStorage>(dst), builder);
        });
    }

    GraphExecutorValue::~GraphExecutorValue() = default;

    bool GraphExecutorValue::has_value() const noexcept { return storage_.has_value(); }

    GraphExecutorView GraphExecutorValue::view()
    {
        return GraphExecutorView{storage_.binding(), storage_.data()};
    }

    GraphExecutorView GraphExecutorValue::view() const
    {
        return GraphExecutorView{storage_.binding(), const_cast<void *>(storage_.data())};
    }

    GraphExecutorBuilder::GraphExecutorBuilder() = default;

    GraphExecutorBuilder &GraphExecutorBuilder::label(std::string label)
    {
        label_ = std::move(label);
        return *this;
    }

    GraphExecutorBuilder &GraphExecutorBuilder::graph_builder(GraphBuilder graph_builder)
    {
        graph_builder_ = std::move(graph_builder);
        return *this;
    }

    GraphExecutorBuilder &GraphExecutorBuilder::mode(GraphExecutorMode mode) noexcept
    {
        mode_ = mode;
        return *this;
    }

    GraphExecutorBuilder &GraphExecutorBuilder::start_time(DateTime start_time) noexcept
    {
        start_time_ = start_time;
        return *this;
    }

    GraphExecutorBuilder &GraphExecutorBuilder::end_time(DateTime end_time) noexcept
    {
        end_time_ = end_time;
        return *this;
    }

    std::string_view GraphExecutorBuilder::label() const noexcept
    {
        return label_;
    }

    const GraphBuilder &GraphExecutorBuilder::graph_builder() const noexcept
    {
        return graph_builder_;
    }

    GraphExecutorMode GraphExecutorBuilder::mode() const noexcept
    {
        return mode_;
    }

    DateTime GraphExecutorBuilder::start_time() const noexcept
    {
        return start_time_;
    }

    DateTime GraphExecutorBuilder::end_time() const noexcept
    {
        return end_time_;
    }

    const GraphTypeBinding &GraphExecutorBuilder::graph_binding() const
    {
        return graph_builder_.binding();
    }

    const GraphExecutorTypeBinding &GraphExecutorBuilder::binding() const
    {
        return executor_runtime_registry().make_binding(*this);
    }

    GraphExecutorValue GraphExecutorBuilder::make_executor() const
    {
        return GraphExecutorValue{*this};
    }

}  // namespace hgraph
