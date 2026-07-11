#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/mock_runtime.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/value/value_builder.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#if defined(_MSC_VER)
#include <malloc.h>
#endif

namespace
{
    std::atomic<bool> g_count_allocations{false};
    std::atomic<std::size_t> g_allocations{0};
    std::atomic<std::size_t> g_allocated_bytes{0};
    std::atomic<const hgraph::ValueView *> g_atomic_value_view_input{nullptr};
    volatile std::uint64_t g_retained_value{0};

    void record_allocation(std::size_t size) noexcept
    {
        if (g_count_allocations.load(std::memory_order_relaxed))
        {
            g_allocations.fetch_add(1, std::memory_order_relaxed);
            g_allocated_bytes.fetch_add(size, std::memory_order_relaxed);
        }
    }

    [[nodiscard]] void *allocate_unaligned(std::size_t size)
    {
        const std::size_t actual_size = std::max<std::size_t>(size, 1);
        if (void *memory = std::malloc(actual_size))
        {
            return memory;
        }
        throw std::bad_alloc{};
    }

    [[nodiscard]] void *allocate_aligned(std::size_t size, std::size_t alignment)
    {
        const std::size_t actual_size = std::max<std::size_t>(size, 1);
#if defined(_MSC_VER)
        if (void *memory = _aligned_malloc(actual_size, alignment))
        {
            return memory;
        }
#else
        void *memory = nullptr;
        if (posix_memalign(&memory, alignment, actual_size) == 0)
        {
            return memory;
        }
#endif
        throw std::bad_alloc{};
    }

    void free_aligned(void *memory) noexcept
    {
#if defined(_MSC_VER)
        _aligned_free(memory);
#else
        std::free(memory);
#endif
    }

    struct AllocationScope
    {
        AllocationScope()
        {
            g_allocations.store(0, std::memory_order_relaxed);
            g_allocated_bytes.store(0, std::memory_order_relaxed);
            g_count_allocations.store(true, std::memory_order_relaxed);
        }

        ~AllocationScope()
        {
            g_count_allocations.store(false, std::memory_order_relaxed);
        }
    };

    struct Sample
    {
        double elapsed_ns{0.0};
        std::size_t allocations{0};
        std::size_t allocated_bytes{0};
        std::uint64_t checksum{0};
    };

    [[nodiscard]] std::size_t env_size(const char *name, std::size_t fallback, std::size_t minimum = 1)
    {
        const char *value = std::getenv(name);
        if (value == nullptr)
        {
            return fallback;
        }

        char *end = nullptr;
        const auto parsed = std::strtoull(value, &end, 10);
        if (end == value || *end != '\0' || parsed > std::numeric_limits<std::size_t>::max())
        {
            throw std::invalid_argument(std::string{name} + " must be a positive integer");
        }
        return std::max<std::size_t>(static_cast<std::size_t>(parsed), minimum);
    }

    template <typename T> [[nodiscard]] T median(std::vector<T> values)
    {
        std::ranges::sort(values);
        return values[values.size() / 2];
    }

    void retain(std::uint64_t value) noexcept
    {
        g_retained_value = value;
        std::atomic_signal_fence(std::memory_order_seq_cst);
    }

    template <typename Operation, typename Check>
    void run_benchmark(std::string_view name, std::size_t default_iterations, std::size_t samples,
                       std::size_t warmup_iterations, Operation &&operation, Check &&check)
    {
        const std::size_t override_iterations = env_size("HGRAPH_TYPE_ERASURE_PERF_ITERATIONS", 0, 0);
        const std::size_t iterations = override_iterations == 0 ? default_iterations : override_iterations;

        check(operation());
        for (std::size_t i = 0; i < warmup_iterations; ++i)
        {
            retain(operation());
        }

        std::vector<Sample> results;
        results.reserve(samples);
        for (std::size_t sample_index = 0; sample_index < samples; ++sample_index)
        {
            std::uint64_t checksum = 0;
            const auto start = std::chrono::steady_clock::now();
            {
                AllocationScope allocations;
                for (std::size_t i = 0; i < iterations; ++i)
                {
                    checksum += operation();
                }
                results.push_back(Sample{
                    .elapsed_ns =
                        std::chrono::duration<double, std::nano>(std::chrono::steady_clock::now() - start).count(),
                    .allocations = g_allocations.load(std::memory_order_relaxed),
                    .allocated_bytes = g_allocated_bytes.load(std::memory_order_relaxed),
                    .checksum = checksum,
                });
            }
            retain(checksum);
        }

        std::vector<double> elapsed_per_op;
        std::vector<std::size_t> allocations;
        std::vector<std::size_t> bytes;
        elapsed_per_op.reserve(results.size());
        allocations.reserve(results.size());
        bytes.reserve(results.size());
        for (const auto &sample : results)
        {
            elapsed_per_op.push_back(sample.elapsed_ns / static_cast<double>(iterations));
            allocations.push_back(sample.allocations);
            bytes.push_back(sample.allocated_bytes);
        }

        const auto checksum = results.front().checksum;
        if (!std::ranges::all_of(results, [checksum](const Sample &sample) { return sample.checksum == checksum; }))
        {
            throw std::runtime_error(std::string{name} + " produced an unstable checksum");
        }

        std::cout << "benchmark"
                  << " name=" << name << " samples=" << samples << " iterations=" << iterations
                  << " median_ns_per_op=" << median(elapsed_per_op)
                  << " min_ns_per_op=" << *std::ranges::min_element(elapsed_per_op)
                  << " max_ns_per_op=" << *std::ranges::max_element(elapsed_per_op)
                  << " median_allocations=" << median(allocations) << " median_allocations_per_op="
                  << static_cast<double>(median(allocations)) / static_cast<double>(iterations)
                  << " median_bytes=" << median(bytes)
                  << " median_bytes_per_op=" << static_cast<double>(median(bytes)) / static_cast<double>(iterations)
                  << " checksum=" << checksum << '\n';
    }

    struct Doubler
    {
        static constexpr auto name = "type_erasure_perf_doubler";
        static hgraph::Port<hgraph::TS<hgraph::Int>> compose(hgraph::Wiring &, hgraph::Port<hgraph::TS<hgraph::Int>> ts)
        {
            using namespace hgraph::stdlib::syntax;
            return (ts * hgraph::Int{2}).template as<hgraph::TS<hgraph::Int>>();
        }
    };

    struct Negator
    {
        static constexpr auto name = "type_erasure_perf_negator";
        static hgraph::Port<hgraph::TS<hgraph::Int>> compose(hgraph::Wiring &, hgraph::Port<hgraph::TS<hgraph::Int>> ts)
        {
            using namespace hgraph::stdlib::syntax;
            return (ts * hgraph::Int{-1}).template as<hgraph::TS<hgraph::Int>>();
        }
    };
}  // namespace

void *operator new(std::size_t size)
{
    record_allocation(size);
    return allocate_unaligned(size);
}

void *operator new[](std::size_t size)
{
    record_allocation(size);
    return allocate_unaligned(size);
}

void *operator new(std::size_t size, std::align_val_t alignment)
{
    record_allocation(size);
    return allocate_aligned(size, static_cast<std::size_t>(alignment));
}

void *operator new[](std::size_t size, std::align_val_t alignment)
{
    record_allocation(size);
    return allocate_aligned(size, static_cast<std::size_t>(alignment));
}

void *operator new(std::size_t size, const std::nothrow_t &) noexcept
{
    try
    {
        return ::operator new(size);
    }
    catch (...)
    {
        return nullptr;
    }
}

void *operator new[](std::size_t size, const std::nothrow_t &) noexcept
{
    try
    {
        return ::operator new[](size);
    }
    catch (...)
    {
        return nullptr;
    }
}

void *operator new(std::size_t size, std::align_val_t alignment, const std::nothrow_t &) noexcept
{
    try
    {
        return ::operator new(size, alignment);
    }
    catch (...)
    {
        return nullptr;
    }
}

void *operator new[](std::size_t size, std::align_val_t alignment, const std::nothrow_t &) noexcept
{
    try
    {
        return ::operator new[](size, alignment);
    }
    catch (...)
    {
        return nullptr;
    }
}

void operator delete(void *memory) noexcept
{
    std::free(memory);
}
void operator delete[](void *memory) noexcept
{
    std::free(memory);
}
void operator delete(void *memory, std::size_t) noexcept
{
    std::free(memory);
}
void operator delete[](void *memory, std::size_t) noexcept
{
    std::free(memory);
}
void operator delete(void *memory, const std::nothrow_t &) noexcept
{
    std::free(memory);
}
void operator delete[](void *memory, const std::nothrow_t &) noexcept
{
    std::free(memory);
}
void operator delete(void *memory, std::align_val_t) noexcept
{
    free_aligned(memory);
}
void operator delete[](void *memory, std::align_val_t) noexcept
{
    free_aligned(memory);
}
void operator delete(void *memory, std::size_t, std::align_val_t) noexcept
{
    free_aligned(memory);
}
void operator delete[](void *memory, std::size_t, std::align_val_t) noexcept
{
    free_aligned(memory);
}
void operator delete(void *memory, std::align_val_t, const std::nothrow_t &) noexcept
{
    free_aligned(memory);
}
void operator delete[](void *memory, std::align_val_t, const std::nothrow_t &) noexcept
{
    free_aligned(memory);
}

int main()
{
    using namespace hgraph;
    using namespace hgraph::testing;

    stdlib::register_standard_operators();
    auto &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("type_erasure_perf_int");
    const auto *ts_int = registry.ts(int_meta);

    const std::size_t samples = env_size("HGRAPH_TYPE_ERASURE_PERF_SAMPLES", 7, 7);
    const std::size_t warmup = env_size("HGRAPH_TYPE_ERASURE_PERF_WARMUP", 64, 0);

    std::cout << std::fixed << std::setprecision(3) << "type_erasure_perf format=1 samples=" << samples
              << " warmup_iterations=" << warmup << '\n';

    Value atomic_value{Int{41}};
    auto atomic_view = atomic_value.view();
    // Loading the source through an atomic pointer makes its binding, schema,
    // and payload opaque on every iteration without disabling optimization of
    // the access path being measured. A local volatile result is insufficient
    // because the compiler can still fold the source-side checked_as work.
    g_atomic_value_view_input.store(&atomic_view, std::memory_order_release);
    run_benchmark(
        "atomic_value_read", 200000, samples, warmup,
        [&] {
            const auto *source = g_atomic_value_view_input.load(std::memory_order_relaxed);
            return static_cast<std::uint64_t>(source->checked_as<Int>());
        },
        [](std::uint64_t value) {
            if (value != 41)
            {
                throw std::runtime_error("atomic value read failed");
            }
        });

    TSOutput scalar_output{*ts_int};
    {
        auto mutation = scalar_output.begin_mutation(MIN_ST);
        if (!mutation.move_value_from(Value{Int{43}}))
        {
            throw std::runtime_error("scalar TS setup failed");
        }
    }
    run_benchmark(
        "scalar_ts_read", 100000, samples, warmup,
        [&] { return static_cast<std::uint64_t>(scalar_output.view(MIN_ST).value().checked_as<Int>()); },
        [](std::uint64_t value) {
            if (value != 43)
            {
                throw std::runtime_error("scalar TS read failed");
            }
        });

    const auto *bundle_meta = registry.tsb("TypeErasurePerfBundle", {{"left", ts_int}, {"right", ts_int}});
    const auto *bundle_binding = ValuePlanFactory::instance().binding_for(bundle_meta->value_schema);
    if (bundle_binding == nullptr)
    {
        throw std::runtime_error("bundle value binding is missing");
    }
    BundleBuilder bundle_builder{*bundle_binding};
    bundle_builder.set("left", Value{Int{47}});
    bundle_builder.set("right", Value{Int{53}});
    TSOutput bundle_output{*bundle_meta};
    {
        auto mutation = bundle_output.begin_mutation(MIN_ST);
        if (!mutation.move_value_from(bundle_builder.build()))
        {
            throw std::runtime_error("composite TS setup failed");
        }
    }
    run_benchmark(
        "fixed_composite_ts_child_read", 50000, samples, warmup,
        [&] {
            auto output_view = bundle_output.view(MIN_ST);
            auto bundle_view = output_view.as_bundle();
            return static_cast<std::uint64_t>(bundle_view.field("left").value().checked_as<Int>());
        },
        [](std::uint64_t value) {
            if (value != 47)
            {
                throw std::runtime_error("composite TS child read failed");
            }
        });

    std::uint64_t node_evaluations = 0;
    NodeTypeMetaData node_schema;
    node_schema.display_name = "type_erasure_perf_node";
    node_schema.node_kind = NodeKind::Sink;
    NodeCallbacks node_callbacks;
    node_callbacks.evaluate = [&node_evaluations](const NodeView &, DateTime) { ++node_evaluations; };
    NodeValue node = NodeBuilder::native(std::move(node_schema), std::move(node_callbacks)).make_node();
    auto node_view = node.view();
    node_view.start(MIN_ST);
    std::uint64_t evaluation_time = 0;
    run_benchmark(
        "erased_native_node_evaluate", 100000, samples, warmup,
        [&] {
            ++evaluation_time;
            return static_cast<std::uint64_t>(
                node_view.evaluate(MIN_ST + TimeDelta{static_cast<TimeDelta::rep>(evaluation_time)}));
        },
        [](std::uint64_t value) {
            if (value != 1)
            {
                throw std::runtime_error("native node evaluation failed");
            }
        });
    node_view.stop(MIN_ST + TimeDelta{static_cast<TimeDelta::rep>(evaluation_time + 1)});
    retain(node_evaluations);

    NodeTypeMetaData graph_node_schema;
    graph_node_schema.display_name = "type_erasure_perf_graph_node";
    graph_node_schema.node_kind = NodeKind::Sink;
    NodeCallbacks graph_node_callbacks;
    graph_node_callbacks.evaluate = [](const NodeView &, DateTime) {};
    GraphBuilder graph_builder;
    graph_builder.label("type_erasure_perf_small_graph")
        .add_node(NodeBuilder::native(std::move(graph_node_schema), std::move(graph_node_callbacks)));
    run_benchmark(
        "small_graph_construct_destroy", 2000, samples, warmup,
        [&] {
            MockRootGraph graph{graph_builder};
            return static_cast<std::uint64_t>(graph.graph().node_count());
        },
        [](std::uint64_t value) {
            if (value != 1)
            {
                throw std::runtime_error("small graph construction failed");
            }
        });

    const std::vector<std::optional<Str>> switch_keys{Str{"a"}, Str{"b"}, Str{"a"}, Str{"b"}};
    const std::vector<std::optional<Int>> switch_inputs{Int{3}, Int{4}, Int{5}, Int{6}};
    const auto switch_cases =
        stdlib::switch_cases({{Value{Str{"a"}}, fn<Doubler>()}, {Value{Str{"b"}}, fn<Negator>()}});
    run_benchmark(
        "alternating_switch_nested_graph_lifecycle", 20, samples, 1,
        [&] {
            const auto result = eval_node<stdlib::switch_>(switch_keys, switch_cases, switch_inputs);
            if (result.size() != 4 || !result[0].has_value() || !result[1].has_value() || !result[2].has_value() ||
                !result[3].has_value())
            {
                throw std::runtime_error("switch lifecycle result shape mismatch");
            }
            const auto a = result[0]->as<Int>();
            const auto b = result[1]->as<Int>();
            const auto c = result[2]->as<Int>();
            const auto d = result[3]->as<Int>();
            if (a != 6 || b != -4 || c != 10 || d != -6)
            {
                throw std::runtime_error("switch lifecycle result mismatch");
            }
            return static_cast<std::uint64_t>(a - b + c - d);
        },
        [](std::uint64_t value) {
            if (value != 26)
            {
                throw std::runtime_error("switch lifecycle checksum failed");
            }
        });

    return 0;
}
