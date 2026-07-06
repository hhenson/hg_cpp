#include <hgraph/runtime/race_tsd_node.h>

#include <hgraph/runtime/graph.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/endpoint_schema.h>
#include <hgraph/types/time_series/ts_input/base_view.h>
#include <hgraph/types/time_series/ts_input/bundle_view.h>
#include <hgraph/types/time_series/ts_input/dict_view.h>
#include <hgraph/types/time_series/ts_output/base_view.h>
#include <hgraph/types/time_series_reference.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/util/date_time.h>
#include <hgraph/util/scope.h>

#include <ankerl/unordered_dense.h>

#include <stdexcept>
#include <vector>

namespace hgraph
{
    namespace
    {
        constexpr const char *race_tsd_storage_field_name = "race_tsd";

        struct RaceTsdNotifier final : Notifiable
        {
            GraphValue *graph{nullptr};
            std::size_t node_index{0};

            void notify(DateTime modified_time) override
            {
                if (graph == nullptr) { return; }
                const DateTime when = modified_time != MIN_DT
                                          ? std::max(modified_time, graph->view().evaluation_time())
                                          : graph->view().evaluation_time();
                graph->schedule_node(node_index, when);
            }
        };

        struct RaceTsdEntry
        {
            DateTime                    first_valid{MAX_DT};
            std::vector<TSOutputHandle> subscribed{};
            bool                        seen{false};
        };

        struct ValueKeyHash
        {
            [[nodiscard]] std::size_t operator()(const Value &value) const { return value.view().hash(); }
        };

        struct ValueKeyEq
        {
            [[nodiscard]] bool operator()(const Value &lhs, const Value &rhs) const
            {
                return lhs.view().equals(rhs.view());
            }
        };

        struct RaceTsdStorage
        {
            RaceTsdNotifier notifier{};
            ankerl::unordered_dense::map<Value, RaceTsdEntry, ValueKeyHash, ValueKeyEq> entries{};
            Value winner{};

            void unsubscribe(RaceTsdEntry &entry) noexcept
            {
                for (TSOutputHandle &handle : entry.subscribed)
                {
                    static_cast<void>(fallback_on_exception(false, [&] {
                        handle.data_view().unsubscribe(&notifier);
                        return true;
                    }));
                }
                entry.subscribed.clear();
            }

            void unsubscribe_all() noexcept
            {
                for (auto &[key, entry] : entries) { unsubscribe(entry); }
            }

            ~RaceTsdStorage() noexcept { unsubscribe_all(); }
        };

        struct RaceTsdContext
        {
            std::size_t storage_offset{0};
        };

        /** Subscribe the notifier to every peered target inside ``reference``. */
        void subscribe_reference_targets(RaceTsdStorage &storage, RaceTsdEntry &entry,
                                         const TimeSeriesReference &reference)
        {
            if (reference.is_empty()) { return; }
            if (reference.is_peered())
            {
                TSOutputHandle handle = reference.target_output();
                handle.data_view().subscribe(&storage.notifier);
                entry.subscribed.push_back(std::move(handle));
                return;
            }
            for (const TimeSeriesReference &item : reference.items())
            {
                subscribe_reference_targets(storage, entry, item);
            }
        }

        void publish_reference(const NodeView &view, DateTime evaluation_time, const TimeSeriesReference &reference)
        {
            Value value{reference};
            auto  output   = view.output(evaluation_time);
            auto  mutation = output.begin_mutation(evaluation_time);
            if (!mutation.move_value_from(std::move(value)))
            {
                throw std::logic_error("race_tsd failed to publish the winner reference");
            }
        }

        struct RaceTsdView
        {
            NodeView view;
            void    *storage{nullptr};

            [[nodiscard]] static const void *node_view_type_id() noexcept
            {
                static const char token{};
                return &token;
            }

            [[nodiscard]] static RaceTsdView from_node(NodeView view, const void *context)
            {
                const auto &typed = *static_cast<const RaceTsdContext *>(context);
                void       *storage = MemoryUtils::advance(view.data(), typed.storage_offset);
                return RaceTsdView{std::move(view), storage};
            }
        };

        bool race_tsd_evaluate(const NodeView &view, DateTime evaluation_time, RaceTsdStorage &storage)
        {
            auto  root    = view.input(evaluation_time);
            auto  bundle  = root.as_bundle();
            auto  tsd_ts  = bundle[0];
            auto  tsd     = tsd_ts.as_dict();

            for (auto &[key, entry] : storage.entries) { entry.seen = false; }

            // hgraph's reduce_tsd_with_race loop, over the live key set.
            for (auto &&[key, child] : tsd.items())
            {
                Value key_value{key};
                auto &entry = storage.entries[key_value];
                entry.seen  = true;

                const bool has_reference = child.valid();
                const TimeSeriesReference reference =
                    has_reference ? child.value().checked_as<TimeSeriesReference>() : TimeSeriesReference{};
                const bool target_valid = has_reference && reference.is_valid(evaluation_time);

                if (target_valid)
                {
                    if (entry.first_valid == MAX_DT) { entry.first_valid = evaluation_time; }
                    storage.unsubscribe(entry);
                }
                else
                {
                    entry.first_valid = MAX_DT;
                    storage.unsubscribe(entry);
                }
            }

            // Keys removed from the dict drop out of the race entirely.
            for (auto it = storage.entries.begin(); it != storage.entries.end();)
            {
                if (!it->second.seen)
                {
                    storage.unsubscribe(it->second);
                    it = storage.entries.erase(it);
                }
                else { ++it; }
            }

            const auto winner_it = storage.winner.has_value() ? storage.entries.find(storage.winner)
                                                              : storage.entries.end();
            const bool winner_ok = winner_it != storage.entries.end() && winner_it->second.first_valid != MAX_DT;

            if (!winner_ok)
            {
                const Value *best      = nullptr;
                DateTime     best_time = MAX_DT;
                for (const auto &[key, entry] : storage.entries)
                {
                    if (entry.first_valid < best_time)
                    {
                        best_time = entry.first_valid;
                        best      = &key;
                    }
                }
                if (best != nullptr)
                {
                    storage.winner = Value{best->view()};
                    storage.unsubscribe_all();
                    auto child = tsd.at(storage.winner.view());
                    publish_reference(view, evaluation_time, child.value().checked_as<TimeSeriesReference>());
                }
                else
                {
                    // No winner: subscribe PENDING candidates (valid reference,
                    // invalid target) so a target becoming valid wakes us.
                    storage.winner = Value{};
                    for (auto &&[key, child] : tsd.items())
                    {
                        if (!child.valid()) { continue; }
                        const TimeSeriesReference reference = child.value().checked_as<TimeSeriesReference>();
                        if (reference.is_empty() || reference.is_valid(evaluation_time)) { continue; }
                        auto &entry = storage.entries[Value{key}];
                        if (entry.subscribed.empty())
                        {
                            subscribe_reference_targets(storage, entry, reference);
                        }
                    }
                }
            }
            else
            {
                auto child = tsd.at(storage.winner.view());
                if (child.modified())
                {
                    publish_reference(view, evaluation_time, child.value().checked_as<TimeSeriesReference>());
                }
            }
            return true;
        }

        // Contexts intern by storage offset (immortal - node builders are
        // long-lived registry artifacts).
        const RaceTsdContext &intern_race_context(std::size_t offset)
        {
            static auto *contexts = new std::vector<RaceTsdContext *>{};
            for (const auto *context : *contexts)
            {
                if (context->storage_offset == offset) { return *context; }
            }
            auto *context = new RaceTsdContext{offset};
            contexts->push_back(context);
            return *context;
        }

        bool race_tsd_evaluate_impl(const void *, const NodeView &view, DateTime evaluation_time)
        {
            auto typed = view.as<RaceTsdView>();
            return race_tsd_evaluate(view, evaluation_time, *MemoryUtils::cast<RaceTsdStorage>(typed.storage));
        }

        void race_tsd_start(const NodeView &view, DateTime)
        {
            auto typed = view.as<RaceTsdView>();
            auto &storage = *MemoryUtils::cast<RaceTsdStorage>(typed.storage);
            storage.notifier.graph      = view.graph_value();
            storage.notifier.node_index = view.node_index();
        }

        void race_tsd_stop(const NodeView &view, DateTime)
        {
            auto typed = view.as<RaceTsdView>();
            MemoryUtils::cast<RaceTsdStorage>(typed.storage)->unsubscribe_all();
        }
    }  // namespace

    NodeBuilder make_race_tsd_node(const TSValueTypeMetaData &tsd_schema)
    {
        if (tsd_schema.kind != TSTypeKind::TSD || tsd_schema.element_ts() == nullptr ||
            tsd_schema.element_ts()->kind != TSTypeKind::REF)
        {
            throw std::invalid_argument("race_tsd requires a TSD<K, REF<OUT>> input schema");
        }

        auto       &registry     = TypeRegistry::instance();
        const auto *input_schema = registry.un_named_tsb({{"tsd", &tsd_schema}});

        NodeTypeMetaData schema;
        schema.display_name  = "reduce_tsd_with_race";
        schema.input_schema  = input_schema;
        schema.output_schema = tsd_schema.element_ts();
        schema.node_kind     = NodeKind::Compute;
        schema.active_inputs = std::vector<std::size_t>{0};
        // NO validity gating: an empty/invalid dict still races (to none).
        schema.valid_inputs.emplace();

        NodeTypeDescriptor descriptor;
        descriptor.schema = std::move(schema);

        const std::array fields{NodeStorageField{
            .name = race_tsd_storage_field_name,
            .plan = &MemoryUtils::plan_for<RaceTsdStorage>(),
        }};
        descriptor.storage_plan = &node_storage_plan_for(descriptor.schema, fields);

        const auto &context =
            intern_race_context(descriptor.storage_plan->component(race_tsd_storage_field_name).offset);

        descriptor.callbacks.start          = &race_tsd_start;
        descriptor.callbacks.stop           = &race_tsd_stop;
        descriptor.ops.evaluate_impl        = &race_tsd_evaluate_impl;
        descriptor.ops.extended_view_type_id = RaceTsdView::node_view_type_id();
        descriptor.ops.extended_view_context = &context;

        return NodeBuilder::from_descriptor(std::move(descriptor));
    }
}  // namespace hgraph
