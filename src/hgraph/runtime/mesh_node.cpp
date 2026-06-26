#include <hgraph/runtime/mesh_node.h>

#include <hgraph/runtime/nested_bindings.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/util/date_time.h>
#include <hgraph/util/scope.h>

#include "mapped_child_bindings.h"

#include <ankerl/unordered_dense.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace
    {
        constexpr std::string_view mesh_storage_field_name{"mesh"};
        constexpr std::string_view mesh_subscribe_storage_field_name{"mesh_subscribe"};
        constexpr std::string_view mesh_key_set_storage_field_name{"mesh_key_set"};

        struct ValueKeyHash
        {
            using is_transparent = void;
            [[nodiscard]] std::size_t operator()(const Value &v) const noexcept { return v.hash(); }
            [[nodiscard]] std::size_t operator()(const ValueView &v) const noexcept { return v.hash(); }
        };

        struct ValueKeyEqual
        {
            using is_transparent = void;
            [[nodiscard]] bool operator()(const Value &a, const Value &b) const noexcept { return a.equals(b); }
            [[nodiscard]] bool operator()(const Value &a, const ValueView &b) const noexcept { return a.equals(b); }
            [[nodiscard]] bool operator()(const ValueView &a, const Value &b) const noexcept { return b.equals(a); }
        };

        using ValueSet = ankerl::unordered_dense::set<Value, ValueKeyHash, ValueKeyEqual>;

        // One mesh instance. Declaration order is load-bearing (reverse
        // destruction): the child graph (a subscriber to key_output) tears down
        // before the key output it observes.
        struct MeshEntry
        {
            explicit MeshEntry(Value key_) : key(std::move(key_)) {}

            Value                   key{};
            std::optional<TSOutput> key_output{};
            GraphValue              graph{};
            int                     rank{0};
            // Pause/resume settle state, per cycle:
            bool                    paused{false};       // paused this cycle, awaiting a dependency
            DateTime                settled_time{MIN_DT};// completed (no pause) at this evaluation time
        };

        struct MeshNodeStorage
        {
            MeshNodeStorage()                                   = default;
            MeshNodeStorage(const MeshNodeStorage &)            = delete;
            MeshNodeStorage &operator=(const MeshNodeStorage &) = delete;
            MeshNodeStorage(MeshNodeStorage &&)                 = delete;
            MeshNodeStorage &operator=(MeshNodeStorage &&)      = delete;

            ~MeshNodeStorage() { stop_all_noexcept(); }

            // Value-keyed, pointer-stable instances (a superset of __keys__: it
            // also holds on-demand instances created from inside other instances).
            ankerl::unordered_dense::map<Value, std::unique_ptr<MeshEntry>, ValueKeyHash, ValueKeyEqual> instances{};
            // depends_on -> set of keys that depend on it (reverse edges).
            ankerl::unordered_dense::map<Value, ValueSet, ValueKeyHash, ValueKeyEqual> dependents{};

            std::vector<Value>          graphs_to_remove{};  // lost a dependent; remove if unreferenced
            int                         max_rank{0};
            bool                        primed{false};
            // The instance whose child graph is currently being evaluated; a
            // mesh_subscribe inside it reads this as its "my_key" (the requester).
            Value                       current_eval_key{};

            void stop_all_noexcept() noexcept
            {
                for (auto &[key, entry] : instances)
                {
                    if (entry && entry->graph.has_value())
                    {
                        static_cast<void>(fallback_on_exception(false, [&] {
                            entry->graph.view().stop();
                            return true;
                        }));
                    }
                }
                instances.clear();
                dependents.clear();
                graphs_to_remove.clear();
                max_rank = 0;
                primed = false;
                current_eval_key = Value{};
            }

            [[nodiscard]] std::size_t active_count() const noexcept { return instances.size(); }

            [[nodiscard]] MeshEntry *find(const ValueView &key) noexcept
            {
                auto it = instances.find(key);
                return it == instances.end() ? nullptr : it->second.get();
            }
        };

        struct MeshNodeContext
        {
            MeshNodeSpec spec{};
            std::size_t  storage_offset{0};
        };

        struct MeshSubscribeStorage
        {
            Value requester{};
            Value dependency{};
            bool  has_dependency{false};
            bool  owns_output_binding{false};
        };

        struct MeshSubscribeContext
        {
            std::size_t storage_offset{0};
        };

        struct MeshKeySetStorage
        {
            bool owns_output_binding{false};
        };

        struct MeshKeySetContext
        {
            std::size_t storage_offset{0};
        };

        // Program-lifetime, intentionally-leaked context storage.
        [[nodiscard]] std::vector<std::unique_ptr<MeshNodeContext>> &mesh_node_contexts() noexcept
        {
            static auto *contexts = new std::vector<std::unique_ptr<MeshNodeContext>>;
            return *contexts;
        }

        [[nodiscard]] const MeshNodeContext &register_mesh_node_context(MeshNodeSpec spec, std::size_t storage_offset)
        {
            auto context = std::make_unique<MeshNodeContext>(MeshNodeContext{
                .spec           = std::move(spec),
                .storage_offset = storage_offset,
            });
            const auto *result = context.get();
            mesh_node_contexts().push_back(std::move(context));
            return *result;
        }

        [[nodiscard]] std::vector<std::unique_ptr<MeshSubscribeContext>> &mesh_subscribe_contexts() noexcept
        {
            static auto *contexts = new std::vector<std::unique_ptr<MeshSubscribeContext>>;
            return *contexts;
        }

        [[nodiscard]] const MeshSubscribeContext &register_mesh_subscribe_context(std::size_t storage_offset)
        {
            auto context = std::make_unique<MeshSubscribeContext>(MeshSubscribeContext{
                .storage_offset = storage_offset,
            });
            const auto *result = context.get();
            mesh_subscribe_contexts().push_back(std::move(context));
            return *result;
        }

        [[nodiscard]] std::vector<std::unique_ptr<MeshKeySetContext>> &mesh_key_set_contexts() noexcept
        {
            static auto *contexts = new std::vector<std::unique_ptr<MeshKeySetContext>>;
            return *contexts;
        }

        [[nodiscard]] const MeshKeySetContext &register_mesh_key_set_context(std::size_t storage_offset)
        {
            auto context = std::make_unique<MeshKeySetContext>(MeshKeySetContext{
                .storage_offset = storage_offset,
            });
            const auto *result = context.get();
            mesh_key_set_contexts().push_back(std::move(context));
            return *result;
        }

        MeshNodeStorage &storage_of(const NodeView &view, const MeshNodeContext &context)
        {
            return *MemoryUtils::cast<MeshNodeStorage>(MemoryUtils::advance(view.data(), context.storage_offset));
        }

        const MeshSubscribeContext &mesh_subscribe_context_of(const NodeView &view)
        {
            const NodeTypeBinding *binding = view.binding();
            if (binding == nullptr)
            {
                throw std::logic_error("mesh_subscribe: node has no binding");
            }
            const void *context = binding->ops_ref().extended_view_context;
            if (context == nullptr)
            {
                throw std::logic_error("mesh_subscribe: missing typed storage context");
            }
            return *static_cast<const MeshSubscribeContext *>(context);
        }

        MeshSubscribeStorage &mesh_subscribe_storage_of(const NodeView &view)
        {
            const auto &context = mesh_subscribe_context_of(view);
            return *MemoryUtils::cast<MeshSubscribeStorage>(MemoryUtils::advance(view.data(), context.storage_offset));
        }

        const MeshKeySetContext &mesh_key_set_context_of(const NodeView &view)
        {
            const NodeTypeBinding *binding = view.binding();
            if (binding == nullptr)
            {
                throw std::logic_error("mesh_key_set: node has no binding");
            }
            const void *context = binding->ops_ref().extended_view_context;
            if (context == nullptr)
            {
                throw std::logic_error("mesh_key_set: missing typed storage context");
            }
            return *static_cast<const MeshKeySetContext *>(context);
        }

        MeshKeySetStorage &mesh_key_set_storage_of(const NodeView &view)
        {
            const auto &context = mesh_key_set_context_of(view);
            return *MemoryUtils::cast<MeshKeySetStorage>(MemoryUtils::advance(view.data(), context.storage_offset));
        }

        void queue_graph_removal(MeshNodeStorage &storage, const ValueView &key)
        {
            storage.graphs_to_remove.emplace_back(key);
        }

        void remove_requester_edges(MeshNodeStorage &storage, const ValueView &requester)
        {
            for (auto it = storage.dependents.begin(); it != storage.dependents.end();)
            {
                it->second.erase(Value{requester});
                if (it->second.empty())
                {
                    queue_graph_removal(storage, it->first.view());
                    it = storage.dependents.erase(it);
                }
                else
                {
                    ++it;
                }
            }
        }

        // ---- instance lifecycle ----

        void bind_instance_inputs(const NodeView &view, const MeshNodeContext &context, MeshEntry &entry,
                                  DateTime evaluation_time)
        {
            const MeshNodeSpec &spec = context.spec;
            const TSOutputView key_source = entry.key_output.has_value()
                                                ? entry.key_output->view(evaluation_time)
                                                : TSOutputView{};
            runtime_detail::bind_mapped_child_inputs(view, entry.graph.view(), evaluation_time,
                                                     spec.child, spec.args, entry.key.view(),
                                                     key_source, std::nullopt);
        }

        void bind_instance_output(const NodeView &view, const MeshNodeContext &context, MeshEntry &entry,
                                  DateTime evaluation_time)
        {
            const MeshNodeSpec &spec = context.spec;
            const TSOutputView key_source = entry.key_output.has_value()
                                                ? entry.key_output->view(evaluation_time)
                                                : TSOutputView{};
            runtime_detail::bind_mapped_child_output(view, entry.graph.view(), evaluation_time,
                                                     spec.child.output_binding, spec.args, entry.key.view(),
                                                     key_source, spec.output_binding_mode);
        }

        void clear_instance_output_binding(const NodeView &view, const MeshNodeContext &context,
                                           const MeshEntry &entry, DateTime evaluation_time)
        {
            runtime_detail::clear_mapped_output_element_binding(
                view, evaluation_time, entry.key.view(), context.spec.output_binding_mode);
        }

        void stop_and_clear_all_instances(const NodeView &view, const MeshNodeContext &context,
                                          MeshNodeStorage &storage, DateTime evaluation_time) noexcept
        {
            for (auto &[key, entry] : storage.instances)
            {
                if (!entry) { continue; }
                static_cast<void>(fallback_on_exception(false, [&] {
                    clear_instance_output_binding(view, context, *entry, evaluation_time);
                    return true;
                }));
                if (entry->graph.has_value())
                {
                    static_cast<void>(fallback_on_exception(false, [&] {
                        entry->graph.view().stop();
                        return true;
                    }));
                }
            }
            storage.instances.clear();
            storage.dependents.clear();
            storage.graphs_to_remove.clear();
            storage.max_rank = 0;
            storage.primed = false;
            storage.current_eval_key = Value{};
        }

        MeshEntry &create_instance(const NodeView &view, const MeshNodeContext &context, MeshNodeStorage &storage,
                                   const ValueView &key_view, int rank, DateTime evaluation_time)
        {
            const MeshNodeSpec &spec = context.spec;

            auto  owned  = std::make_unique<MeshEntry>(Value{key_view});
            auto &entry  = *owned;
            entry.rank   = rank;
            entry.paused = true;  // force a first evaluation this cycle (settle loop)

            auto output          = view.output(evaluation_time);
            auto output_dict     = output.as_dict();
            auto output_mutation = output_dict.begin_mutation(evaluation_time);

            auto rollback = UnwindCleanupGuard([&] {
                clear_instance_output_binding(view, context, entry, evaluation_time);
                if (entry.graph.has_value() && entry.graph.view().started()) { entry.graph.view().stop(); }
                (void)output_mutation.erase(entry.key.view());
            });

            entry.graph = spec.child.graph_builder.make_nested_graph(NodeStorageRef{view.binding(), view.data()});
            if (spec.key_output_schema != nullptr)
            {
                entry.key_output.emplace(*spec.key_output_schema);
                auto mutation = entry.key_output->view(evaluation_time).begin_mutation(evaluation_time);
                if (!mutation.copy_value_from(key_view))
                {
                    throw std::logic_error("mesh_: failed to write the key value into the per-key output");
                }
            }

            (void)output_mutation[key_view];

            bind_instance_inputs(view, context, entry, evaluation_time);
            bind_instance_output(view, context, entry, evaluation_time);
            entry.graph.view().start(evaluation_time);
            rollback.release();

            storage.max_rank = std::max(storage.max_rank, rank);
            auto [it, inserted] = storage.instances.emplace(Value{key_view}, std::move(owned));
            (void)inserted;
            return *it->second;
        }

        void remove_instance(const NodeView &view, const MeshNodeContext &context, MeshNodeStorage &storage,
                             const ValueView &key_view, DateTime evaluation_time)
        {
            auto it = storage.instances.find(key_view);
            if (it == storage.instances.end()) { return; }

            clear_instance_output_binding(view, context, *it->second, evaluation_time);
            if (it->second->graph.has_value()) { it->second->graph.view().stop(); }

            auto output          = view.output(evaluation_time);
            auto output_dict     = output.as_dict();
            auto output_mutation = output_dict.begin_mutation(evaluation_time);
            (void)output_mutation.erase(key_view);

            remove_requester_edges(storage, key_view);
            if (auto dep_it = storage.dependents.find(key_view);
                dep_it != storage.dependents.end() && dep_it->second.empty())
            {
                storage.dependents.erase(dep_it);
            }
            storage.instances.erase(it);
        }

        // ---- ranking ----

        [[nodiscard]] bool keys_contains(const NodeView &view, const MeshNodeContext &context,
                                         const ValueView &key, DateTime evaluation_time)
        {
            auto keys_input = walk_ts_path(view.input(evaluation_time).borrowed_ref(),
                                           std::vector<std::size_t>{context.spec.keys_input_index});
            if (!keys_input.valid()) { return false; }
            return keys_input.as_set().contains(key);
        }

        void process_graphs_to_remove(const NodeView &view, const MeshNodeContext &context,
                                      MeshNodeStorage &storage, DateTime evaluation_time)
        {
            if (storage.graphs_to_remove.empty()) { return; }

            std::vector<Value> to_remove;
            to_remove.swap(storage.graphs_to_remove);
            for (const Value &k : to_remove)
            {
                auto       it             = storage.dependents.find(k);
                const bool has_dependents = it != storage.dependents.end() && !it->second.empty();
                if (!has_dependents && !keys_contains(view, context, k.view(), evaluation_time))
                {
                    remove_instance(view, context, storage, k.view(), evaluation_time);
                }
            }
        }

        void re_rank(MeshNodeStorage &storage, const ValueView &key, const ValueView &depends_on,
                     std::vector<Value> &stack)
        {
            MeshEntry *key_entry = storage.find(key);
            MeshEntry *dep_entry = storage.find(depends_on);
            if (key_entry == nullptr || dep_entry == nullptr) { return; }
            if (key_entry->rank > dep_entry->rank) { return; }

            key_entry->rank  = dep_entry->rank + 1;
            storage.max_rank = std::max(storage.max_rank, key_entry->rank);

            stack.push_back(Value{key});
            // Re-rank everything that depends on ``key``.
            if (auto it = storage.dependents.find(key); it != storage.dependents.end())
            {
                for (const Value &dependent : it->second)
                {
                    const bool on_stack = std::any_of(stack.begin(), stack.end(),
                                                      [&](const Value &s) { return s.equals(dependent); });
                    if (on_stack)
                    {
                        throw std::runtime_error("mesh_ has a dependency cycle");
                    }
                    re_rank(storage, dependent.view(), key, stack);
                }
            }
            stack.pop_back();
        }

        // ---- evaluation ----

        // The mesh node is the pause BOUNDARY: it resolves its instances' pauses
        // internally (the settle loop) and always completes, so it returns true.
        bool mesh_evaluate_impl(const void *, const NodeView &view, DateTime evaluation_time)
        {
            if (!view.started()) { return true; }

            auto        mesh_view = view.as<MeshNodeView>();
            const auto &context   = *static_cast<const MeshNodeContext *>(mesh_view.internal_context());
            auto       &storage   = storage_of(view, context);
            const auto &spec      = context.spec;

            auto root_input  = view.input(evaluation_time);
            auto keys_input  = walk_ts_path(root_input.borrowed_ref(),
                                            std::vector<std::size_t>{spec.keys_input_index});

            // 1. __keys__ key-set membership drives instance create/remove.
            if (!keys_input.valid())
            {
                auto output          = view.output(evaluation_time);
                auto output_dict     = output.as_dict();
                auto output_mutation = output_dict.begin_mutation(evaluation_time);
                stop_and_clear_all_instances(view, context, storage, evaluation_time);
                output_mutation.clear();
                return true;
            }

            {
                auto key_set = keys_input.as_set();
                if (!storage.primed)
                {
                    for (const ValueView &k : key_set.values())
                    {
                        if (storage.find(k) == nullptr)
                        {
                            create_instance(view, context, storage, k, storage.max_rank, evaluation_time);
                        }
                    }
                    storage.primed = true;
                }
                else if (keys_input.modified())
                {
                    for (const ValueView &k : key_set.added())
                    {
                        if (storage.find(k) == nullptr)
                        {
                            create_instance(view, context, storage, k, storage.max_rank, evaluation_time);
                        }
                    }
                    for (const ValueView &k : key_set.removed())
                    {
                        auto it = storage.dependents.find(k);
                        const bool has_dependents = it != storage.dependents.end() && !it->second.empty();
                        if (!has_dependents) { remove_instance(view, context, storage, k, evaluation_time); }
                    }
                }
            }

            // 2. Removals queued because a dependent went away (remove_dependency).
            process_graphs_to_remove(view, context, storage, evaluation_time);

            // 3. Settle loop (pause/resume). Evaluate instances in dependency-rank order,
            //    re-scanning until none pauses. A pause has, via add_dependency, created
            //    and/or ranked the missing dependency below the requester; a later pass
            //    evaluates that dependency first and then resumes the paused instance —
            //    so the whole transitive closure settles within this cycle.
            DateTime    next_time = MAX_DT;
            bool        progress  = true;
            std::size_t guard     = 0;
            while (progress)
            {
                progress = false;

                // Snapshot keys by rank: add_dependency can create instances mid-pass,
                // so we must not hold an iterator into instances across an evaluate().
                std::vector<std::pair<int, Value>> order;
                order.reserve(storage.instances.size());
                for (auto &[k, entry] : storage.instances)
                {
                    if (entry) { order.emplace_back(entry->rank, k); }
                }
                std::sort(order.begin(), order.end(),
                          [](const auto &a, const auto &b) { return a.first < b.first; });

                for (const auto &ranked : order)
                {
                    MeshEntry *entry = storage.find(ranked.second.view());
                    if (entry == nullptr || !entry->graph.has_value()) { continue; }
                    if (entry->settled_time == evaluation_time) { continue; }  // already done this cycle

                    bind_instance_inputs(view, context, *entry, evaluation_time);
                    bind_instance_output(view, context, *entry, evaluation_time);

                    auto           child      = entry->graph.view();
                    const DateTime child_next = child.next_scheduled_time();
                    if (child_next != MAX_DT && child_next > evaluation_time)
                    {
                        next_time = std::min(next_time, child_next);
                    }
                    const bool due = child_next <= evaluation_time;
                    if (!due && !entry->paused) { continue; }

                    storage.current_eval_key = entry->key;
                    entry->paused            = false;
                    if (child.evaluate(evaluation_time)) { entry->settled_time = evaluation_time; }
                    else { entry->paused = true; }  // a dependency was created / ranked; re-scan
                    progress = true;

                    if (const DateTime next = child.next_scheduled_time();
                        next != MAX_DT && next > evaluation_time)
                    {
                        next_time = std::min(next_time, next);
                    }
                }

                if (++guard > storage.instances.size() + 64)
                {
                    throw std::runtime_error("mesh_ failed to settle within the cycle");
                }
            }
            storage.current_eval_key = Value{};
            process_graphs_to_remove(view, context, storage, evaluation_time);

            if (next_time < MAX_DT) { view.graph().schedule_node(view.node_index(), next_time); }
            return true;
        }

        // ---- mesh_subscribe node (inside an instance: mesh_(func)[item]) ----
        //
        // Simplified shape: the only wired input is ``item`` (the requested key). The
        // mesh's own TSD output (``self``) and the requester's key (``my_key``) come
        // from the enclosing mesh node via the ``parent_node()`` walk at runtime, not
        // from boundary inputs. The node "takes a key and returns the element type":
        // it forwards ``self[item]`` to its output, pausing (returning false) until the
        // dependency is created, ranked below the requester, and evaluated this cycle.
        constexpr std::size_t subscribe_item_field  = 0;  // TS<K> (active) — the requested key
        constexpr std::size_t subscribe_value_field = 1;  // OUT — seeded with nothing<OUT>, rebound at
                                                          // runtime to self[item] (reads it + makes us
                                                          // reactive to the sibling's ticks)
        constexpr std::size_t key_set_value_field   = 0;  // TSS<K> seeded with nothing<TSS<K>>, rebound to
                                                          // self.key_set() for scheduling.

        [[nodiscard]] std::optional<MeshNodeView> resolve_mesh_node(const NodeView &view)
        {
            GraphView graph = view.graph();
            while (graph.is_nested())
            {
                NodeView parent = graph.as_nested().parent_node();
                if (parent.is<MeshNodeView>()) { return parent.as<MeshNodeView>(); }
                if (!parent.valid()) { break; }
                graph = parent.graph();
            }
            return std::nullopt;
        }

        void forget_subscribe_dependency(MeshSubscribeStorage &storage) noexcept
        {
            storage.requester           = Value{};
            storage.dependency          = Value{};
            storage.has_dependency      = false;
        }

        void remove_subscribe_dependency(const NodeView &view, MeshSubscribeStorage &storage)
        {
            if (!storage.has_dependency) { return; }
            if (std::optional<MeshNodeView> mesh = resolve_mesh_node(view); mesh.has_value())
            {
                mesh->remove_dependency(storage.requester.view(), storage.dependency.view());
            }
            forget_subscribe_dependency(storage);
        }

        void unbind_subscribe_value_input(const NodeView &view, DateTime evaluation_time)
        {
            if (!view.has_input()) { return; }
            auto input  = view.input(evaluation_time);
            auto bundle = input.as_bundle();
            auto value  = bundle.at(subscribe_value_field);
            if (value.is_bindable() && value.bound()) { value.unbind_output(); }
        }

        void passivate_subscribe_value_input(const NodeView &view, DateTime evaluation_time)
        {
            if (!view.has_input()) { return; }
            auto input  = view.input(evaluation_time);
            auto bundle = input.as_bundle();
            auto value  = bundle.at(subscribe_value_field);
            if (value.active()) { value.make_passive(); }
        }

        void clear_subscribe_output(const NodeView &view, MeshSubscribeStorage &storage,
                                    DateTime evaluation_time)
        {
            if (!view.has_output()) { return; }
            auto output = view.output(evaluation_time);
            if (output.forwarding() && storage.owns_output_binding && output.forwarding_bound())
            {
                output.clear_forwarding_target();
            }
            storage.owns_output_binding = false;
        }

        void clear_subscribe_runtime_links(const NodeView &view, MeshSubscribeStorage &storage,
                                           DateTime evaluation_time)
        {
            unbind_subscribe_value_input(view, evaluation_time);
            clear_subscribe_output(view, storage, evaluation_time);
        }

        [[nodiscard]] bool same_subscribe_dependency(const MeshSubscribeStorage &storage,
                                                     const ValueView &requester,
                                                     const ValueView &dependency)
        {
            return storage.has_dependency &&
                   storage.requester.has_value() &&
                   storage.dependency.has_value() &&
                   storage.requester.equals(requester) &&
                   storage.dependency.equals(dependency);
        }

        void publish_subscribe_source(const NodeView &view, MeshSubscribeStorage &storage,
                                      const TSOutputView &source, DateTime evaluation_time)
        {
            auto output = view.output(evaluation_time);
            if (!output.forwarding())
            {
                throw std::logic_error("mesh_subscribe output must be a forwarding endpoint");
            }

            const TSOutputHandle before = output.forwarding_target();
            if (source.bound())
            {
                bind_forwarding_output_to_source(output, source);
                storage.owns_output_binding = output.forwarding_bound();
            }
            else if (output.forwarding_bound())
            {
                output.clear_forwarding_target();
                storage.owns_output_binding = false;
            }

            if (source.bound() && source.valid() && !output.forwarding_target().same_as(before))
            {
                output.begin_mutation(evaluation_time).mark_modified();
            }
        }

        void passivate_key_set_value_input(const NodeView &view, DateTime evaluation_time)
        {
            if (!view.has_input()) { return; }
            auto input  = view.input(evaluation_time);
            auto bundle = input.as_bundle();
            auto value  = bundle.at(key_set_value_field);
            if (value.active()) { value.make_passive(); }
        }

        void publish_key_set_source(const NodeView &view, MeshKeySetStorage &storage,
                                    const TSOutputView &source, DateTime evaluation_time)
        {
            auto output = view.output(evaluation_time);
            if (!output.forwarding())
            {
                throw std::logic_error("mesh_key_set output must be a forwarding endpoint");
            }

            const TSOutputHandle before = output.forwarding_target();
            if (source.bound())
            {
                bind_forwarding_output_to_source(output, source);
                storage.owns_output_binding = output.forwarding_bound();
            }
            else if (output.forwarding_bound())
            {
                output.clear_forwarding_target();
                storage.owns_output_binding = false;
            }

            if (source.bound() && source.valid() && !output.forwarding_target().same_as(before))
            {
                output.begin_mutation(evaluation_time).mark_modified();
            }
        }

        bool mesh_key_set_evaluate_impl(const void *, const NodeView &view, DateTime evaluation_time)
        {
            if (!view.started()) { return true; }

            auto &storage = mesh_key_set_storage_of(view);
            std::optional<MeshNodeView> mesh = resolve_mesh_node(view);
            if (!mesh.has_value())
            {
                throw std::logic_error("mesh_key_set: not evaluated inside a mesh instance");
            }

            auto input  = view.input(evaluation_time);
            auto bundle = input.as_bundle();
            auto self   = mesh->node().output(evaluation_time);
            auto source = self.as_dict().key_set();
            bind_input_to_source(bundle.at(key_set_value_field), source);
            publish_key_set_source(view, storage, source, evaluation_time);
            return true;
        }

        bool mesh_subscribe_evaluate_impl(const void *, const NodeView &view, DateTime evaluation_time)
        {
            if (!view.started()) { return true; }

            auto input   = view.input(evaluation_time);
            auto bundle  = input.as_bundle();
            auto item_in = bundle.at(subscribe_item_field);
            auto &storage = mesh_subscribe_storage_of(view);
            if (!item_in.valid())
            {
                remove_subscribe_dependency(view, storage);
                clear_subscribe_runtime_links(view, storage, evaluation_time);
                return true;
            }

            std::optional<MeshNodeView> mesh = resolve_mesh_node(view);
            if (!mesh.has_value())
            {
                throw std::logic_error("mesh_subscribe: not evaluated inside a mesh instance");
            }

            const Value my_key = mesh->current_key();
            const Value item{item_in.value()};
            if (!my_key.has_value())
            {
                remove_subscribe_dependency(view, storage);
                clear_subscribe_runtime_links(view, storage, evaluation_time);
                return true;
            }

            if (!same_subscribe_dependency(storage, my_key.view(), item.view()))
            {
                remove_subscribe_dependency(view, storage);
                clear_subscribe_runtime_links(view, storage, evaluation_time);
                storage.requester      = my_key;
                storage.dependency     = item;
                storage.has_dependency = true;
            }

            // Register the dependency (creating / ranking the target on demand). If the
            // target is not yet available this cycle, PAUSE: the mesh resolves it in rank
            // order and re-evaluates this instance to resume from here.
            if (!mesh->add_dependency(my_key.view(), item.view())) { return false; }

            // Available — (re)bind our dynamic ``value`` input to self[item] so we forward
            // it AND become reactive to its future ticks (a sibling tick reschedules us via
            // the input notification). Binding is idempotent when already pointed at it.
            auto self = mesh->node().output(evaluation_time);
            auto dict = self.as_dict();
            if (dict.contains(item.view()))
            {
                auto source = dict.at(item.view());
                bind_input_to_source(bundle.at(subscribe_value_field), source);
                publish_subscribe_source(view, storage, source, evaluation_time);
            }
            else
            {
                clear_subscribe_runtime_links(view, storage, evaluation_time);
            }
            return true;
        }

        void mesh_subscribe_stop(const NodeView &view, DateTime evaluation_time)
        {
            auto &storage = mesh_subscribe_storage_of(view);
            remove_subscribe_dependency(view, storage);
            passivate_subscribe_value_input(view, evaluation_time);
            storage.owns_output_binding = false;
        }

        void mesh_key_set_stop(const NodeView &view, DateTime evaluation_time)
        {
            auto &storage = mesh_key_set_storage_of(view);
            passivate_key_set_value_input(view, evaluation_time);
            storage.owns_output_binding = false;
        }

        void mesh_node_stop(const NodeView &view, DateTime evaluation_time)
        {
            auto  mesh_view = view.as<MeshNodeView>();
            auto &storage   = storage_of(view, *static_cast<const MeshNodeContext *>(mesh_view.internal_context()));

            auto output          = view.output(evaluation_time);
            auto output_dict     = output.as_dict();
            auto output_mutation = output_dict.begin_mutation(evaluation_time);

            stop_and_clear_all_instances(view, *static_cast<const MeshNodeContext *>(mesh_view.internal_context()),
                                         storage, evaluation_time);
            output_mutation.clear();
        }
    }  // namespace

    // ---- MeshNodeView ----

    const void *MeshNodeView::node_view_type_id() noexcept
    {
        static const char token{};
        return &token;
    }

    MeshNodeView MeshNodeView::from_node(NodeView view, const void *context)
    {
        if (context == nullptr) { throw std::logic_error("MeshNodeView requires a typed view context"); }
        const auto &typed_context = *static_cast<const MeshNodeContext *>(context);
        void       *storage = MemoryUtils::advance(view.data(), typed_context.storage_offset);
        return MeshNodeView{std::move(view), context, storage};
    }

    const NodeView &MeshNodeView::node() const noexcept { return view_; }

    std::size_t MeshNodeView::active_count() const noexcept
    {
        return MemoryUtils::cast<MeshNodeStorage>(storage_)->active_count();
    }

    Value MeshNodeView::current_key() const
    {
        return MemoryUtils::cast<MeshNodeStorage>(storage_)->current_eval_key;
    }

    bool MeshNodeView::add_dependency(const ValueView &key, const ValueView &depends_on) const
    {
        if (key.equals(depends_on)) { throw std::runtime_error("mesh_ has a dependency cycle"); }

        auto          &storage = *MemoryUtils::cast<MeshNodeStorage>(storage_);
        const auto    &context = *static_cast<const MeshNodeContext *>(context_);
        const DateTime t       = view_.graph().evaluation_time();

        storage.dependents[Value{depends_on}].insert(Value{key});

        MeshEntry *key_entry = storage.find(key);
        if (key_entry == nullptr) { return false; }  // defensive: we should be evaluating it

        MeshEntry *dep_entry = storage.find(depends_on);
        if (dep_entry == nullptr)
        {
            // Create the dependency on demand, same cycle, ranked below the requester;
            // the resolver evaluates it first (lower rank) and then resumes us.
            create_instance(view_, context, storage, depends_on, 0, t);
            std::vector<Value> stack;
            re_rank(storage, key, depends_on, stack);
            return false;
        }

        if (key_entry->rank <= dep_entry->rank)
        {
            // The requester must outrank its dependency; re-rank and re-evaluate in order.
            std::vector<Value> stack;
            re_rank(storage, key, depends_on, stack);
            return false;
        }

        // The dependency exists and is ranked below us: available iff it produced its
        // result this cycle (otherwise pause so the resolver evaluates it first).
        return dep_entry->settled_time == t;
    }

    void MeshNodeView::remove_dependency(const ValueView &key, const ValueView &depends_on) const
    {
        auto &storage = *MemoryUtils::cast<MeshNodeStorage>(storage_);
        auto  it      = storage.dependents.find(depends_on);
        if (it == storage.dependents.end()) { return; }
        it->second.erase(Value{key});
        if (it->second.empty())
        {
            queue_graph_removal(storage, depends_on);
            storage.dependents.erase(it);
        }
    }

    MeshNodeView::MeshNodeView(NodeView view, const void *context, void *storage) noexcept
        : view_(std::move(view)),
          context_(context),
          storage_(storage)
    {
    }

    NodeBuilder mesh_node(NodeTypeMetaData meta, MeshNodeSpec spec)
    {
        meta.node_kind    = NodeKind::Nested;
        meta.valid_inputs = std::vector<std::size_t>{};
        if (spec.output_binding_mode != MapOutputBindingMode::ChildTerminalWritesElement)
        {
            if (meta.output_schema == nullptr || meta.output_schema->kind != TSTypeKind::TSD ||
                meta.output_schema->element_ts() == nullptr)
            {
                throw std::invalid_argument("mesh_node forwarding-element output requires a TSD output schema");
            }
            meta.output_endpoint_schema = TSEndpointSchema::non_peered_dict(
                meta.output_schema,
                TSEndpointSchema::peered(meta.output_schema->element_ts()));
        }

        NodeTypeDescriptor descriptor;
        descriptor.schema = std::move(meta);

        const std::array fields{NodeStorageField{
            .name = mesh_storage_field_name,
            .plan = &MemoryUtils::plan_for<MeshNodeStorage>(),
        }};
        // The mesh field destroys BEFORE the owned TSD output: instance children
        // forward into it (and read it as their self-context).
        descriptor.storage_plan = &node_storage_plan_for(descriptor.schema, {}, fields);

        const auto &context = register_mesh_node_context(
            std::move(spec), descriptor.storage_plan->component(mesh_storage_field_name).offset);

        descriptor.callbacks.stop            = &mesh_node_stop;
        descriptor.ops.evaluate_impl         = &mesh_evaluate_impl;
        descriptor.ops.extended_view_type_id = MeshNodeView::node_view_type_id();
        descriptor.ops.extended_view_context = &context;

        return NodeBuilder::from_descriptor(std::move(descriptor));
    }

    NodeBuilder mesh_subscribe_node(NodeTypeMetaData meta)
    {
        meta.node_kind              = NodeKind::Compute;
        meta.output_endpoint_schema = TSEndpointSchema::peered(meta.output_schema);

        NodeTypeDescriptor descriptor;
        descriptor.schema = std::move(meta);

        const std::array fields{NodeStorageField{
            .name = mesh_subscribe_storage_field_name,
            .plan = &MemoryUtils::plan_for<MeshSubscribeStorage>(),
        }};
        descriptor.storage_plan = &node_storage_plan_for(descriptor.schema, fields);

        const auto &context = register_mesh_subscribe_context(
            descriptor.storage_plan->component(mesh_subscribe_storage_field_name).offset);

        descriptor.callbacks.stop             = &mesh_subscribe_stop;
        descriptor.ops.evaluate_impl          = &mesh_subscribe_evaluate_impl;
        descriptor.ops.extended_view_context  = &context;
        return NodeBuilder::from_descriptor(std::move(descriptor));
    }

    NodeBuilder mesh_key_set_node(NodeTypeMetaData meta)
    {
        meta.node_kind              = NodeKind::Compute;
        meta.schedule_on_start      = true;
        meta.output_endpoint_schema = TSEndpointSchema::peered(meta.output_schema);

        NodeTypeDescriptor descriptor;
        descriptor.schema = std::move(meta);

        const std::array fields{NodeStorageField{
            .name = mesh_key_set_storage_field_name,
            .plan = &MemoryUtils::plan_for<MeshKeySetStorage>(),
        }};
        descriptor.storage_plan = &node_storage_plan_for(descriptor.schema, fields);

        const auto &context = register_mesh_key_set_context(
            descriptor.storage_plan->component(mesh_key_set_storage_field_name).offset);

        descriptor.callbacks.stop             = &mesh_key_set_stop;
        descriptor.ops.evaluate_impl          = &mesh_key_set_evaluate_impl;
        descriptor.ops.extended_view_context  = &context;
        return NodeBuilder::from_descriptor(std::move(descriptor));
    }
}  // namespace hgraph
