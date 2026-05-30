#include <hgraph/types/time_series/ts_input.h>

#include <hgraph/types/time_series/ts_input/detail.h>

#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/container_ops.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <fmt/format.h>
#include <iterator>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

namespace hgraph
{
    namespace
    {
        inline constexpr std::size_t input_npos = static_cast<std::size_t>(-1);

        [[nodiscard]] constexpr std::size_t ts_kind_index(TSTypeKind kind) noexcept
        {
            return static_cast<std::size_t>(kind);
        }

        [[nodiscard]] std::size_t combine_hash(std::size_t seed, std::size_t value) noexcept
        {
            seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
            return seed;
        }

        [[nodiscard]] bool output_view_bound(const TSOutputView &output) noexcept
        {
            return output.output() != nullptr && output.data_view().valid();
        }

        [[nodiscard]] const TSDataView &empty_ts_data_view() noexcept
        {
            static const TSDataView empty{};
            return empty;
        }

        void validate_endpoint_kind(const TSValueTypeMetaData *schema, TSTypeKind expected, const char *what)
        {
            if (schema == nullptr || schema->kind != expected)
            {
                throw std::invalid_argument(std::string{what} + " requires a matching time-series shape");
            }
        }

        void validate_input_endpoint_schema(const TSEndpointSchema &endpoint_schema, bool root)
        {
            const auto *schema = endpoint_schema.schema();
            if (schema == nullptr) { throw std::invalid_argument("TSInput endpoint annotation requires a schema"); }

            if (root)
            {
                if (schema->kind != TSTypeKind::TSB || !endpoint_schema.is_non_peered())
                {
                    throw std::invalid_argument("TSInput root endpoint annotation must be a non-peered TSB");
                }
            }

            if (endpoint_schema.is_peered()) { return; }
            if (schema->kind != TSTypeKind::TSB && schema->kind != TSTypeKind::TSL)
            {
                throw std::invalid_argument("TSInput non-peered prefixes require TSB or fixed-size TSL schemas");
            }
            if (schema->kind == TSTypeKind::TSL && schema->fixed_size() == 0)
            {
                throw std::invalid_argument("TSInput non-peered TSL prefixes currently require a fixed size");
            }
            for (const auto &child : endpoint_schema.children()) { validate_input_endpoint_schema(child, false); }
        }

        void append_endpoint_key(std::string &key, const TSEndpointSchema &endpoint_schema)
        {
            const auto role = static_cast<std::uint8_t>(endpoint_schema.role());
            key.append(reinterpret_cast<const char *>(&role), sizeof(role));

            const auto schema_bits = reinterpret_cast<std::uintptr_t>(endpoint_schema.schema());
            key.append(reinterpret_cast<const char *>(&schema_bits), sizeof(schema_bits));

            if (endpoint_schema.is_non_peered())
            {
                const auto &children = endpoint_schema.children();
                const auto  size     = children.size();
                key.append(reinterpret_cast<const char *>(&size), sizeof(size));
                for (const auto &child : children) { append_endpoint_key(key, child); }
            }
        }

        [[nodiscard]] std::string plan_cache_key(const TSInputConstructionPlan &plan)
        {
            std::string key;
            key.reserve(128);
            append_endpoint_key(key, plan.endpoint_schema());
            return key;
        }

        [[nodiscard]] std::string binding_cache_key(const TSEndpointSchema           &endpoint_schema,
                                                    const MemoryUtils::StoragePlan   &root_plan,
                                                    std::size_t                       storage_offset)
        {
            std::string key;
            key.reserve(160);
            const auto root_bits = reinterpret_cast<std::uintptr_t>(&root_plan);
            key.append(reinterpret_cast<const char *>(&root_bits), sizeof(root_bits));
            key.append(reinterpret_cast<const char *>(&storage_offset), sizeof(storage_offset));
            append_endpoint_key(key, endpoint_schema);
            return key;
        }

        [[nodiscard]] std::size_t no_endpoint_child_count(const TSValueTypeMetaData *) noexcept { return 0; }
        [[nodiscard]] std::string_view no_endpoint_key_at(const TSValueTypeMetaData *, std::size_t) noexcept { return {}; }
        [[nodiscard]] std::size_t no_endpoint_find_key(const TSValueTypeMetaData *, std::string_view) noexcept
        {
            return input_npos;
        }
        [[nodiscard]] const TSValueTypeMetaData *no_endpoint_child_schema(const TSValueTypeMetaData *,
                                                                          std::size_t) noexcept
        {
            return nullptr;
        }

        [[nodiscard]] std::size_t tsb_endpoint_child_count(const TSValueTypeMetaData *schema) noexcept
        {
            return schema != nullptr ? schema->field_count() : 0;
        }

        [[nodiscard]] std::string_view tsb_endpoint_key_at(const TSValueTypeMetaData *schema,
                                                           std::size_t                index) noexcept
        {
            if (schema == nullptr || index >= schema->field_count()) { return {}; }
            const auto *name = schema->fields()[index].name;
            return name != nullptr ? std::string_view{name} : std::string_view{};
        }

        [[nodiscard]] std::size_t tsb_endpoint_find_key(const TSValueTypeMetaData *schema,
                                                        std::string_view          name) noexcept
        {
            if (schema == nullptr) { return input_npos; }
            for (std::size_t index = 0; index < schema->field_count(); ++index)
            {
                const auto *field_name = schema->fields()[index].name;
                if (field_name != nullptr && name == field_name) { return index; }
            }
            return input_npos;
        }

        [[nodiscard]] const TSValueTypeMetaData *tsb_endpoint_child_schema(const TSValueTypeMetaData *schema,
                                                                           std::size_t                index) noexcept
        {
            return schema != nullptr && index < schema->field_count() ? schema->fields()[index].type : nullptr;
        }

        [[nodiscard]] std::size_t tsl_endpoint_child_count(const TSValueTypeMetaData *schema) noexcept
        {
            return schema != nullptr ? schema->fixed_size() : 0;
        }

        [[nodiscard]] const TSValueTypeMetaData *tsl_endpoint_child_schema(const TSValueTypeMetaData *schema,
                                                                           std::size_t                index) noexcept
        {
            return schema != nullptr && index < schema->fixed_size() ? schema->element_ts() : nullptr;
        }

        [[nodiscard]] const TSValueTypeMetaData *tsd_endpoint_child_schema(const TSValueTypeMetaData *schema,
                                                                           std::size_t) noexcept
        {
            return schema != nullptr ? schema->element_ts() : nullptr;
        }

        [[nodiscard]] TSDataView tsb_target_child_at(TSDataView parent, std::size_t index)
        {
            auto bundle = parent.as_bundle();
            return bundle.at(index);
        }

        [[nodiscard]] TSDataView tsl_target_child_at(TSDataView parent, std::size_t index)
        {
            auto list = parent.as_list();
            return list.at(index);
        }

        [[nodiscard]] TSDataView tsd_target_child_at(TSDataView parent, std::size_t slot)
        {
            auto dict = parent.as_dict();
            return dict.at_slot(slot);
        }

        [[nodiscard]] TimeSeriesReference input_leaf_reference(const TSInputView &view)
        {
            return TimeSeriesReference::empty(view.schema());
        }

        [[nodiscard]] TimeSeriesReference input_tsl_reference(const TSInputView &view)
        {
            const auto *schema = view.schema();
            if (schema == nullptr || schema->fixed_size() == 0) { return TimeSeriesReference::empty(schema); }

            auto list = view.as_list();
            std::vector<TimeSeriesReference> items;
            items.reserve(list.size());
            for (std::size_t index = 0; index < list.size(); ++index)
            {
                items.emplace_back(list.at(index).reference());
            }
            return TimeSeriesReference::non_peered(schema, std::move(items));
        }

        [[nodiscard]] TimeSeriesReference input_tsb_reference(const TSInputView &view)
        {
            const auto *schema = view.schema();
            if (schema == nullptr)
            {
                throw std::logic_error("TSInputView::reference requires a typed TSB input view");
            }

            auto bundle = view.as_bundle();
            std::vector<TimeSeriesReference> items;
            items.reserve(bundle.size());
            for (std::size_t index = 0; index < bundle.size(); ++index)
            {
                items.emplace_back(bundle.at(index).reference());
            }
            return TimeSeriesReference::non_peered(schema, std::move(items));
        }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        [[nodiscard]] nb::object input_tsb_to_python(const void *context, const void *memory);
        [[nodiscard]] nb::object input_tsl_to_python(const void *context, const void *memory);
        [[nodiscard]] nb::object input_tsb_delta_to_python(const void *context,
                                                           const void *memory,
                                                           engine_time_t evaluation_time);
        [[nodiscard]] nb::object input_tsl_delta_to_python(const void *context,
                                                           const void *memory,
                                                           engine_time_t evaluation_time);
#endif

        const detail::TSInputEndpointOps endpoint_ts_ops{
            .name = "TS",
            .child_count = &no_endpoint_child_count,
            .key_at = &no_endpoint_key_at,
            .find_key = &no_endpoint_find_key,
            .child_schema = &no_endpoint_child_schema,
            .reference = &input_leaf_reference,
        };

        const detail::TSInputEndpointOps endpoint_tss_ops{
            .name = "TSS",
            .child_count = &no_endpoint_child_count,
            .key_at = &no_endpoint_key_at,
            .find_key = &no_endpoint_find_key,
            .child_schema = &no_endpoint_child_schema,
            .reference = &input_leaf_reference,
        };

        const detail::TSInputEndpointOps endpoint_tsd_ops{
            .name = "TSD",
            .child_count = &no_endpoint_child_count,
            .key_at = &no_endpoint_key_at,
            .find_key = &no_endpoint_find_key,
            .child_schema = &tsd_endpoint_child_schema,
            .target_child = &tsd_target_child_at,
            .reference = &input_leaf_reference,
        };

        const detail::TSInputEndpointOps endpoint_tsl_ops{
            .name = "TSL",
            .supports_input_projection = true,
            .child_count = &tsl_endpoint_child_count,
            .key_at = &no_endpoint_key_at,
            .find_key = &no_endpoint_find_key,
            .child_schema = &tsl_endpoint_child_schema,
            .target_child = &tsl_target_child_at,
            .reference = &input_tsl_reference,
#if HGRAPH_ENABLE_PYTHON_USER_NODES
            .to_python = &input_tsl_to_python,
            .delta_to_python = &input_tsl_delta_to_python,
#endif
        };

        const detail::TSInputEndpointOps endpoint_tsw_ops{
            .name = "TSW",
            .child_count = &no_endpoint_child_count,
            .key_at = &no_endpoint_key_at,
            .find_key = &no_endpoint_find_key,
            .child_schema = &no_endpoint_child_schema,
            .reference = &input_leaf_reference,
        };

        const detail::TSInputEndpointOps endpoint_tsb_ops{
            .name = "TSB",
            .supports_input_projection = true,
            .named_value_projection = true,
            .value_open = '{',
            .value_close = '}',
            .child_count = &tsb_endpoint_child_count,
            .key_at = &tsb_endpoint_key_at,
            .find_key = &tsb_endpoint_find_key,
            .child_schema = &tsb_endpoint_child_schema,
            .target_child = &tsb_target_child_at,
            .reference = &input_tsb_reference,
#if HGRAPH_ENABLE_PYTHON_USER_NODES
            .to_python = &input_tsb_to_python,
            .delta_to_python = &input_tsb_delta_to_python,
#endif
        };

        const detail::TSInputEndpointOps endpoint_ref_ops{
            .name = "REF",
            .child_count = &no_endpoint_child_count,
            .key_at = &no_endpoint_key_at,
            .find_key = &no_endpoint_find_key,
            .child_schema = &no_endpoint_child_schema,
            .reference = &input_leaf_reference,
        };

        const detail::TSInputEndpointOps endpoint_signal_ops{
            .name = "SIGNAL",
            .child_count = &no_endpoint_child_count,
            .key_at = &no_endpoint_key_at,
            .find_key = &no_endpoint_find_key,
            .child_schema = &no_endpoint_child_schema,
            .reference = &input_leaf_reference,
        };

        [[nodiscard]] const detail::TSInputEndpointOps &input_endpoint_ops_for(const TSValueTypeMetaData *schema)
        {
            static constexpr std::size_t kind_count = ts_kind_index(TSTypeKind::SIGNAL) + 1U;
            static const std::array<const detail::TSInputEndpointOps *, kind_count> table{
                &endpoint_ts_ops,
                &endpoint_tss_ops,
                &endpoint_tsd_ops,
                &endpoint_tsl_ops,
                &endpoint_tsw_ops,
                &endpoint_tsb_ops,
                &endpoint_ref_ops,
                &endpoint_signal_ops,
            };

            if (schema == nullptr) { throw std::logic_error("TSInput endpoint ops require a schema"); }
            const auto index = ts_kind_index(schema->kind);
            if (index >= table.size() || table[index] == nullptr)
            {
                throw std::logic_error("TSInput endpoint ops are not registered for the schema kind");
            }
            return *table[index];
        }

        [[nodiscard]] const detail::TSInputEndpointOps &
        non_peered_input_endpoint_ops_for(const TSEndpointSchema &endpoint_schema)
        {
            if (endpoint_schema.is_peered())
            {
                throw std::logic_error("TSInput non-peered endpoint ops requested for a peered endpoint");
            }

            const auto &ops = input_endpoint_ops_for(endpoint_schema.schema());
            if (!ops.supports_input_projection)
            {
                throw std::logic_error("TSInput non-peered endpoint ops are not available for this shape");
            }
            return ops;
        }

        [[nodiscard]] const TSDataBinding *regular_ts_data_binding_for(const TSValueTypeMetaData *schema)
        {
            return TSDataPlanFactory::instance().binding_for(schema);
        }

        [[nodiscard]] const ValueTypeBinding *regular_value_binding_for(const TSValueTypeMetaData *schema)
        {
            return schema != nullptr ? ValuePlanFactory::instance().binding_for(schema->value_schema) : nullptr;
        }

        [[nodiscard]] const ValueTypeBinding *value_binding_for_data_binding(const TSDataBinding *binding)
        {
            if (binding == nullptr) { return nullptr; }
            const auto &ops = binding->checked_ops();
            const auto *layout = ops.layout_impl(ops.context);
            return layout != nullptr ? layout->value_binding : nullptr;
        }

        [[nodiscard]] const MemoryUtils::StoragePlan &input_storage_plan(const TSEndpointSchema &endpoint_schema);
        [[nodiscard]] const TSDataBinding *input_data_binding_for(const TSEndpointSchema         &endpoint_schema,
                                                                  const MemoryUtils::StoragePlan &root_plan,
                                                                  std::size_t storage_offset);

        [[nodiscard]] std::string child_component_name(const TSValueTypeMetaData *schema, std::size_t index)
        {
            if (schema != nullptr && schema->kind == TSTypeKind::TSB)
            {
                return fmt::format("field_{}", index);
            }
            return fmt::format("element_{}", index);
        }

        [[nodiscard]] std::size_t child_storage_offset(const TSEndpointSchema           &endpoint_schema,
                                                       const MemoryUtils::StoragePlan   &storage_plan,
                                                       std::size_t                       index)
        {
            const auto *schema = endpoint_schema.schema();
            const auto *component = storage_plan.find_component(child_component_name(schema, index));
            if (component == nullptr)
            {
                throw std::logic_error("TSInput storage plan is missing a child component");
            }
            return component->offset;
        }

        [[nodiscard]] std::size_t tracking_offset(const MemoryUtils::StoragePlan &storage_plan)
        {
            const auto *component = storage_plan.find_component("tracking");
            if (component == nullptr) { throw std::logic_error("TSInput storage plan is missing tracking"); }
            return component->offset;
        }

        [[nodiscard]] const MemoryUtils::StoragePlan &input_storage_plan(const TSEndpointSchema &endpoint_schema)
        {
            if (endpoint_schema.is_peered()) { return MemoryUtils::plan_for<detail::TSInputTargetLinkStorage>(); }

            const auto *schema = endpoint_schema.schema();
            auto        builder = MemoryUtils::named_tuple();
            builder.reserve(endpoint_schema.children().size() + 1);
            for (std::size_t index = 0; index < endpoint_schema.children().size(); ++index)
            {
                builder.add_field(child_component_name(schema, index),
                                  input_storage_plan(endpoint_schema.children()[index]));
            }
            builder.add_field("tracking", MemoryUtils::plan_for<TSDataTracking>());
            return builder.build();
        }

        struct InputChild
        {
            const TSValueTypeMetaData *schema{nullptr};
            const TSDataBinding       *input_binding{nullptr};
            const TSDataBinding       *regular_binding{nullptr};
            const ValueTypeBinding    *regular_value_binding{nullptr};
            bool                       target_link{false};
        };

        struct InputBundleDeltaSurface
        {
            IndexedValueOps ops{};
        };

        struct InputListDeltaSurface
        {
            MapValueOps               map_ops{};
            SetValueOps               key_set_ops{};
            const ValueTypeBinding   *ordinal_key_binding{nullptr};
            const ValueTypeBinding   *map_value_binding{nullptr};
            const ValueTypeBinding   *key_set_binding{nullptr};
            std::vector<std::int64_t> ordinal_keys{};
        };

        class InputDeltaSurface
        {
          public:
            enum class Kind : std::uint8_t
            {
                Empty,
                Bundle,
                List,
            };

            InputDeltaSurface() noexcept = default;
            InputDeltaSurface(const InputDeltaSurface &) = delete;
            InputDeltaSurface &operator=(const InputDeltaSurface &) = delete;
            InputDeltaSurface(InputDeltaSurface &&) = delete;
            InputDeltaSurface &operator=(InputDeltaSurface &&) = delete;

            ~InputDeltaSurface() noexcept { destroy(); }

            [[nodiscard]] Kind kind() const noexcept { return kind_; }

            [[nodiscard]] InputBundleDeltaSurface &emplace_bundle()
            {
                destroy();
                std::construct_at(&storage_.bundle);
                kind_ = Kind::Bundle;
                return storage_.bundle;
            }

            [[nodiscard]] InputListDeltaSurface &emplace_list()
            {
                destroy();
                std::construct_at(&storage_.list);
                kind_ = Kind::List;
                return storage_.list;
            }

            [[nodiscard]] InputBundleDeltaSurface &bundle() noexcept { return storage_.bundle; }
            [[nodiscard]] const InputBundleDeltaSurface &bundle() const noexcept { return storage_.bundle; }
            [[nodiscard]] InputListDeltaSurface &list() noexcept { return storage_.list; }
            [[nodiscard]] const InputListDeltaSurface &list() const noexcept { return storage_.list; }

          private:
            union Storage
            {
                InputBundleDeltaSurface bundle;
                InputListDeltaSurface   list;

                Storage() noexcept {}
                ~Storage() noexcept {}
            };

            void destroy() noexcept
            {
                switch (kind_)
                {
                    case Kind::Bundle:
                        std::destroy_at(&storage_.bundle);
                        break;
                    case Kind::List:
                        std::destroy_at(&storage_.list);
                        break;
                    case Kind::Empty:
                        break;
                }
                kind_ = Kind::Empty;
            }

            Kind    kind_{Kind::Empty};
            Storage storage_{};
        };

        struct InputBindingContext
        {
            const TSValueTypeMetaData        *schema{nullptr};
            const detail::TSInputEndpointOps *endpoint_ops{nullptr};
            TSDataLayout                      layout{};
            IndexedTSDataOps                  ts_data_ops{};
            IndexedValueOps                   value_ops{};
            InputDeltaSurface                 delta{};
            const ValueTypeBinding           *value_binding{nullptr};
            const ValueTypeBinding           *delta_binding{nullptr};
            std::vector<InputChild>           children{};
        };

        struct TargetLinkContext
        {
            const TSValueTypeMetaData      *schema{nullptr};
            std::size_t                     storage_offset{0};
            TSDataLayout                    layout{};
            TSDataOps                       ts_data_ops{};
            const TSDataBinding            *regular_binding{nullptr};
        };

        using TargetLinkContextCache = std::unordered_map<std::string, std::unique_ptr<TargetLinkContext>>;
        using InputBindingContextCache = std::unordered_map<std::string, std::unique_ptr<InputBindingContext>>;
        using TSInputBuilderCache = std::unordered_map<std::string, std::unique_ptr<TSInputBuilder>>;

        [[nodiscard]] TargetLinkContextCache &target_link_context_cache()
        {
            static TargetLinkContextCache cache;
            return cache;
        }

        [[nodiscard]] std::mutex &target_link_context_cache_mutex()
        {
            static std::mutex mutex;
            return mutex;
        }

        [[nodiscard]] InputBindingContextCache &input_binding_context_cache()
        {
            static InputBindingContextCache cache;
            return cache;
        }

        [[nodiscard]] std::recursive_mutex &input_binding_context_cache_mutex()
        {
            static std::recursive_mutex mutex;
            return mutex;
        }

        [[nodiscard]] TSInputBuilderCache &input_builder_cache()
        {
            static TSInputBuilderCache cache;
            return cache;
        }

        [[nodiscard]] std::mutex &input_builder_cache_mutex()
        {
            static std::mutex mutex;
            return mutex;
        }

        void clear_input_binding_caches() noexcept
        {
            {
                std::lock_guard lock{target_link_context_cache_mutex()};
                target_link_context_cache().clear();
            }
            {
                std::lock_guard lock{input_binding_context_cache_mutex()};
                input_binding_context_cache().clear();
            }
            {
                std::lock_guard lock{input_builder_cache_mutex()};
                input_builder_cache().clear();
            }
        }

        [[nodiscard]] const InputBindingContext *input_context_for(const TSDataBinding *binding) noexcept;
        [[nodiscard]] const TargetLinkContext *target_context_for(const TSDataBinding *binding) noexcept;

        [[nodiscard]] const void *advance(const void *memory, std::size_t offset) noexcept
        {
            return static_cast<const std::byte *>(memory) + offset;
        }

        [[nodiscard]] void *advance(void *memory, std::size_t offset) noexcept
        {
            return static_cast<std::byte *>(memory) + offset;
        }

        [[nodiscard]] const detail::TSInputTargetLinkStorage *target_storage(const TargetLinkContext *context,
                                                                             const void              *memory) noexcept
        {
            return MemoryUtils::cast<detail::TSInputTargetLinkStorage>(advance(memory, context->storage_offset));
        }

        [[nodiscard]] detail::TSInputTargetLinkStorage *target_storage(const TargetLinkContext *context,
                                                                       void                    *memory) noexcept
        {
            return MemoryUtils::cast<detail::TSInputTargetLinkStorage>(advance(memory, context->storage_offset));
        }

        [[nodiscard]] const detail::TSInputTargetLinkStorage *target_storage(const TSDataView &view) noexcept
        {
            const auto *context = target_context_for(view.binding());
            return context != nullptr && view.data() != nullptr ? target_storage(context, view.data()) : nullptr;
        }

        [[nodiscard]] detail::TSInputTargetLinkStorage *mutable_target_storage(const TSDataView &view)
        {
            const auto *context = target_context_for(view.binding());
            return context != nullptr && view.data() != nullptr
                       ? target_storage(context, const_cast<void *>(view.data()))
                       : nullptr;
        }

        [[nodiscard]] const detail::TSInputTargetLinkStorage *child_target_storage(const InputChild &child,
                                                                                   const void       *memory) noexcept
        {
            if (!child.target_link || memory == nullptr) { return nullptr; }
            const auto *context = target_context_for(child.input_binding);
            return context != nullptr ? target_storage(context, memory) : nullptr;
        }

        [[nodiscard]] const TSDataLayout *input_layout(const void *context) noexcept
        {
            return &static_cast<const InputBindingContext *>(context)->layout;
        }

        [[nodiscard]] const TSDataTracking *input_tracking(const void *context, const void *memory) noexcept
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            return MemoryUtils::cast<TSDataTracking>(advance(memory, state->layout.tracking_offset));
        }

        [[nodiscard]] TSDataTracking *input_mutable_tracking(const void *context, void *memory) noexcept
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            return MemoryUtils::cast<TSDataTracking>(advance(memory, state->layout.tracking_offset));
        }

        [[nodiscard]] const TSDataLayout *target_link_layout(const void *context) noexcept
        {
            return &static_cast<const TargetLinkContext *>(context)->layout;
        }

        [[nodiscard]] const TSDataTracking *target_link_tracking(const void *context, const void *memory) noexcept
        {
            return &target_storage(static_cast<const TargetLinkContext *>(context), memory)->tracking;
        }

        [[nodiscard]] TSDataTracking *target_link_mutable_tracking(const void *context, void *memory) noexcept
        {
            return &target_storage(static_cast<const TargetLinkContext *>(context), memory)->tracking;
        }

        [[nodiscard]] const TSDataBinding *input_element_binding(const void *context,
                                                                 const void *memory,
                                                                 std::size_t index) noexcept
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            if (index >= state->children.size()) { return nullptr; }
            const auto &child = state->children[index];
            if (!child.target_link) { return child.input_binding; }
            const auto *link = child_target_storage(child, memory);
            return link != nullptr && link->bound() ? link->target_output().binding() : child.regular_binding;
        }

        [[nodiscard]] const void *input_element_memory(const void *context,
                                                       const void *memory,
                                                       std::size_t index) noexcept
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            if (index >= state->children.size()) { return nullptr; }
            const auto &child = state->children[index];
            if (!child.target_link) { return memory; }
            const auto *link = child_target_storage(child, memory);
            return link != nullptr && link->bound() ? link->target_output().data_view().data() : nullptr;
        }

        [[nodiscard]] void *input_mutable_element_memory(const void *context, void *memory, std::size_t index) noexcept
        {
            return const_cast<void *>(input_element_memory(context, memory, index));
        }

        [[nodiscard]] bool input_has_current_value(const void *context, const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                const auto *binding = input_element_binding(context, memory, index);
                const auto *data    = input_element_memory(context, memory, index);
                if (binding == nullptr || data == nullptr) { continue; }
                const auto &ops = binding->checked_ops();
                if (ops.has_current_value_impl(ops.context, data)) { return true; }
            }
            return false;
        }

        [[nodiscard]] bool input_all_valid(const void *context, const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            if (!input_has_current_value(context, memory)) { return false; }
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                const auto *binding = input_element_binding(context, memory, index);
                const auto *data    = input_element_memory(context, memory, index);
                if (binding == nullptr || data == nullptr) { return false; }
                const auto &ops = binding->checked_ops();
                if (!ops.all_valid_impl(ops.context, data)) { return false; }
            }
            return true;
        }

        [[nodiscard]] const void *input_value_memory(const void *, const void *memory) noexcept { return memory; }
        [[nodiscard]] void *input_mutable_value_memory(const void *, void *memory) noexcept { return memory; }
        [[nodiscard]] const void *input_delta_memory(const void *, const void *memory) noexcept { return memory; }
        [[nodiscard]] void *input_mutable_delta_memory(const void *, void *memory) noexcept { return memory; }

        void input_cleanup_delta(const void *context, void *memory, engine_time_t modified_time)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                const auto *binding = input_element_binding(context, memory, index);
                const auto *data    = input_element_memory(context, memory, index);
                if (binding == nullptr || data == nullptr) { continue; }
                const auto &ops = binding->checked_ops();
                const auto *tracking = ops.tracking_impl(ops.context, data);
                if (tracking != nullptr && tracking->last_modified_time == modified_time)
                {
                    ops.cleanup_delta_impl(ops.context, const_cast<void *>(data), modified_time);
                }
            }
        }

        [[nodiscard]] std::size_t input_indexed_size(const void *context, const void *) noexcept
        {
            return static_cast<const InputBindingContext *>(context)->children.size();
        }

        [[nodiscard]] const ValueTypeBinding *input_value_element_binding(const void *context,
                                                                          const void *memory,
                                                                          std::size_t index) noexcept
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            if (index >= state->children.size()) { return nullptr; }
            const auto &child = state->children[index];
            if (!child.target_link) { return value_binding_for_data_binding(child.input_binding); }
            const auto *link = child_target_storage(child, memory);
            return link != nullptr && link->bound() ? link->target_view().value().binding() : child.regular_value_binding;
        }

        [[nodiscard]] const void *input_value_element_at(const void *context,
                                                         const void *memory,
                                                         std::size_t index) noexcept
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            if (index >= state->children.size()) { return nullptr; }
            const auto &child = state->children[index];
            if (!child.target_link)
            {
                const auto *binding = child.input_binding;
                const auto &ops = binding->checked_ops();
                return ops.value_memory_impl(ops.context, memory);
            }
            const auto *link = child_target_storage(child, memory);
            return link != nullptr && link->bound() ? link->target_view().value().data() : nullptr;
        }

        [[nodiscard]] ValueView input_value_project_value(const void *context, const void *memory, std::size_t index)
        {
            return ValueView{input_value_element_binding(context, memory, index),
                             input_value_element_at(context, memory, index)};
        }

        [[nodiscard]] Range<ValueView> input_value_make_range(const void *context, const void *memory)
        {
            return Range<ValueView>{.context = context, .memory = memory, .limit = input_indexed_size(context, memory),
                                    .predicate = nullptr, .projector = &input_value_project_value};
        }

        [[nodiscard]] std::size_t input_value_hash(const void *context, const void *memory)
        {
            std::size_t seed = 0;
            const auto  size = input_indexed_size(context, memory);
            for (std::size_t index = 0; index < size; ++index)
            {
                const auto *binding = input_value_element_binding(context, memory, index);
                const auto *child   = input_value_element_at(context, memory, index);
                const auto  value   = child != nullptr && binding != nullptr ? binding->checked_ops().hash(child) : 0;
                seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
            }
            return seed;
        }

        [[nodiscard]] bool input_value_equals(const void *context, const void *lhs, const void *rhs) noexcept
        {
            if (lhs == nullptr || rhs == nullptr) { return lhs == rhs; }
            return fallback_on_exception(false, [&] {
                const auto size = input_indexed_size(context, lhs);
                if (input_indexed_size(context, rhs) != size) { return false; }
                for (std::size_t index = 0; index < size; ++index)
                {
                    const auto *binding = input_value_element_binding(context, lhs, index);
                    const auto *a       = input_value_element_at(context, lhs, index);
                    const auto *b       = input_value_element_at(context, rhs, index);
                    if (a == nullptr || b == nullptr)
                    {
                        if (a != b) { return false; }
                        continue;
                    }
                    if (binding == nullptr || !binding->checked_ops().equals(a, b)) { return false; }
                }
                return true;
            });
        }

        [[nodiscard]] std::partial_ordering input_value_compare(const void *context,
                                                                const void *lhs,
                                                                const void *rhs) noexcept
        {
            if (const auto order = value_ops_detail::null_order(lhs, rhs)) { return *order; }
            return fallback_on_exception(std::partial_ordering::unordered, [&] {
                const auto size = std::min(input_indexed_size(context, lhs), input_indexed_size(context, rhs));
                for (std::size_t index = 0; index < size; ++index)
                {
                    const auto *binding = input_value_element_binding(context, lhs, index);
                    const auto *a       = input_value_element_at(context, lhs, index);
                    const auto *b       = input_value_element_at(context, rhs, index);
                    if (const auto order = value_ops_detail::null_order(a, b)) { return *order; }
                    if (binding == nullptr)
                    {
                        if (a != b) { return std::partial_ordering::unordered; }
                        continue;
                    }
                    const auto order = binding->checked_ops().compare(a, b);
                    if (order != 0) { return order; }
                }
                const auto lhs_size = input_indexed_size(context, lhs);
                const auto rhs_size = input_indexed_size(context, rhs);
                if (lhs_size < rhs_size) { return std::partial_ordering::less; }
                if (lhs_size > rhs_size) { return std::partial_ordering::greater; }
                return std::partial_ordering::equivalent;
            });
        }

        [[nodiscard]] std::string input_value_to_string(const void *context, const void *memory)
        {
            if (memory == nullptr) { return {}; }
            const auto *state = static_cast<const InputBindingContext *>(context);
            const auto &endpoint_ops = *state->endpoint_ops;
            const bool  named = endpoint_ops.named_value_projection;
            fmt::memory_buffer out;
            fmt::format_to(std::back_inserter(out), "{}", endpoint_ops.value_open);
            const auto size = input_indexed_size(context, memory);
            for (std::size_t index = 0; index < size; ++index)
            {
                if (index > 0) { fmt::format_to(std::back_inserter(out), ", "); }
                if (named)
                {
                    const auto key = endpoint_ops.key_at != nullptr ? endpoint_ops.key_at(state->schema, index)
                                                                     : std::string_view{};
                    fmt::format_to(std::back_inserter(out), "{}: ", key);
                }
                const auto *binding = input_value_element_binding(context, memory, index);
                const auto *child   = input_value_element_at(context, memory, index);
                if (binding != nullptr && child != nullptr)
                {
                    fmt::format_to(std::back_inserter(out), "{}", binding->checked_ops().to_string(child));
                }
            }
            fmt::format_to(std::back_inserter(out), "{}", endpoint_ops.value_close);
            return fmt::to_string(out);
        }

        [[nodiscard]] const ValueTypeBinding *input_child_delta_binding(const void *context,
                                                                        const void *memory,
                                                                        std::size_t index)
        {
            const auto *binding = input_element_binding(context, memory, index);
            if (binding == nullptr) { return nullptr; }
            const auto &ops = binding->checked_ops();
            const auto *layout = ops.layout_impl(ops.context);
            return layout != nullptr ? layout->delta_binding : nullptr;
        }

        [[nodiscard]] bool input_child_modified_for_parent_time(const void *context,
                                                                const void *memory,
                                                                std::size_t index)
        {
            const auto *binding = input_element_binding(context, memory, index);
            const auto *data = input_element_memory(context, memory, index);
            if (binding == nullptr || data == nullptr) { return false; }

            const auto &ops = binding->checked_ops();
            const auto *tracking = ops.tracking_impl(ops.context, data);
            return tracking != nullptr && tracking->last_modified_time == input_tracking(context, memory)->last_modified_time;
        }

        [[nodiscard]] ValueView input_child_delta_view(const void *context,
                                                       const void *memory,
                                                       std::size_t index)
        {
            const auto *binding = input_child_delta_binding(context, memory, index);
            if (binding == nullptr) { return {}; }
            if (!input_child_modified_for_parent_time(context, memory, index))
            {
                return ValueView{binding, nullptr};
            }

            const auto *child_binding = input_element_binding(context, memory, index);
            const auto *child_data = input_element_memory(context, memory, index);
            const auto &child_ops = child_binding->checked_ops();
            return ValueView{binding, child_ops.delta_memory_impl(child_ops.context, child_data)};
        }

        [[nodiscard]] std::size_t input_view_hash(ValueView view)
        {
            if (!view.has_value()) { return std::hash<const ValueTypeBinding *>{}(view.binding()); }
            return view.hash();
        }

        [[nodiscard]] const void *input_delta_bundle_element_at(const void *context,
                                                                const void *memory,
                                                                std::size_t index)
        {
            return input_child_delta_view(context, memory, index).data();
        }

        [[nodiscard]] const ValueTypeBinding *input_delta_bundle_element_binding(const void *context,
                                                                                 const void *memory,
                                                                                 std::size_t index) noexcept
        {
            return fallback_on_exception<const ValueTypeBinding *>(nullptr, [&] {
                return input_child_delta_binding(context, memory, index);
            });
        }

        [[nodiscard]] ValueView input_delta_bundle_projector(const void *context,
                                                             const void *memory,
                                                             std::size_t index)
        {
            return input_child_delta_view(context, memory, index);
        }

        [[nodiscard]] Range<ValueView> input_delta_bundle_make_range(const void *context, const void *memory)
        {
            return Range<ValueView>{
                .context = context,
                .memory = memory,
                .limit = input_indexed_size(context, memory),
                .predicate = nullptr,
                .projector = &input_delta_bundle_projector,
            };
        }

        [[nodiscard]] std::size_t input_delta_bundle_hash(const void *context, const void *memory)
        {
            std::size_t seed = 0;
            const auto size = input_indexed_size(context, memory);
            for (std::size_t index = 0; index < size; ++index)
            {
                seed = combine_hash(seed, input_view_hash(input_child_delta_view(context, memory, index)));
            }
            return seed;
        }

        [[nodiscard]] bool input_delta_bundle_equals(const void *context,
                                                     const void *lhs,
                                                     const void *rhs) noexcept
        {
            if (lhs == nullptr || rhs == nullptr) { return lhs == rhs; }
            return fallback_on_exception(false, [&] {
                const auto size = input_indexed_size(context, lhs);
                if (input_indexed_size(context, rhs) != size) { return false; }
                for (std::size_t index = 0; index < size; ++index)
                {
                    if (!input_child_delta_view(context, lhs, index).equals(input_child_delta_view(context, rhs, index)))
                    {
                        return false;
                    }
                }
                return true;
            });
        }

        [[nodiscard]] std::partial_ordering input_delta_bundle_compare(const void *context,
                                                                       const void *lhs,
                                                                       const void *rhs) noexcept
        {
            if (const auto order = value_ops_detail::null_order(lhs, rhs)) { return *order; }
            return fallback_on_exception(std::partial_ordering::unordered, [&] {
                const auto size = input_indexed_size(context, lhs);
                if (input_indexed_size(context, rhs) != size) { return std::partial_ordering::unordered; }
                for (std::size_t index = 0; index < size; ++index)
                {
                    const auto order =
                        input_child_delta_view(context, lhs, index).compare(input_child_delta_view(context, rhs, index));
                    if (order != 0) { return order; }
                }
                return std::partial_ordering::equivalent;
            });
        }

        [[nodiscard]] std::string input_delta_bundle_to_string(const void *context, const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            fmt::memory_buffer out;
            fmt::format_to(std::back_inserter(out), "{{");
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                if (index > 0) { fmt::format_to(std::back_inserter(out), ", "); }
                const auto *name = state->schema->fields()[index].name;
                fmt::format_to(std::back_inserter(out), "{}: {}",
                               name != nullptr ? name : "",
                               input_child_delta_view(context, memory, index).to_string());
            }
            fmt::format_to(std::back_inserter(out), "}}");
            return fmt::to_string(out);
        }

        [[nodiscard]] bool input_delta_child_predicate(const void *context, const void *memory, std::size_t index)
        {
            return input_child_modified_for_parent_time(context, memory, index);
        }

        [[nodiscard]] std::size_t input_delta_map_size(const void *context, const void *memory) noexcept
        {
            return fallback_on_exception(std::size_t{0}, [&] {
                const auto *state = static_cast<const InputBindingContext *>(context);
                std::size_t count = 0;
                for (std::size_t index = 0; index < state->children.size(); ++index)
                {
                    if (input_child_modified_for_parent_time(context, memory, index)) { ++count; }
                }
                return count;
            });
        }

        [[nodiscard]] std::size_t input_nth_modified_child(const InputBindingContext *state,
                                                           const void *memory,
                                                           std::size_t ordinal)
        {
            std::size_t seen = 0;
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                if (!input_child_modified_for_parent_time(state, memory, index)) { continue; }
                if (seen++ == ordinal) { return index; }
            }
            throw std::out_of_range("TSInput TSL delta map index out of range");
        }

        [[nodiscard]] const void *input_delta_map_key_at_index(const void *context,
                                                               const void *memory,
                                                               std::size_t index)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            const auto &delta = state->delta.list();
            return &delta.ordinal_keys[input_nth_modified_child(state, memory, index)];
        }

        [[nodiscard]] const ValueTypeBinding *input_delta_map_key_binding(const void *context,
                                                                          const void *,
                                                                          std::size_t) noexcept
        {
            return static_cast<const InputBindingContext *>(context)->delta.list().ordinal_key_binding;
        }

        [[nodiscard]] const void *input_delta_map_value_at_index(const void *context,
                                                                 const void *memory,
                                                                 std::size_t index)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            return input_child_delta_view(context, memory, input_nth_modified_child(state, memory, index)).data();
        }

        [[nodiscard]] const ValueTypeBinding *input_delta_map_value_binding(const void *context,
                                                                            const void *) noexcept
        {
            return static_cast<const InputBindingContext *>(context)->delta.list().map_value_binding;
        }

        [[nodiscard]] bool input_delta_map_contains(const void *context, const void *memory, const void *key)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            const auto index = *MemoryUtils::cast<std::int64_t>(key);
            return index >= 0 && static_cast<std::size_t>(index) < state->children.size() &&
                   input_child_modified_for_parent_time(context, memory, static_cast<std::size_t>(index));
        }

        [[nodiscard]] const void *input_delta_map_value_at(const void *context, const void *memory, const void *key)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            const auto index = *MemoryUtils::cast<std::int64_t>(key);
            if (index < 0) { return nullptr; }
            const auto slot = static_cast<std::size_t>(index);
            if (slot >= state->children.size() || !input_child_modified_for_parent_time(context, memory, slot))
            {
                return nullptr;
            }
            return input_child_delta_view(context, memory, slot).data();
        }

        [[nodiscard]] ValueView input_delta_map_key_projector(const void *context,
                                                              const void *,
                                                              std::size_t index)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            const auto &delta = state->delta.list();
            return ValueView{delta.ordinal_key_binding, &delta.ordinal_keys[index]};
        }

        [[nodiscard]] ValueView input_delta_map_value_projector(const void *context,
                                                                const void *memory,
                                                                std::size_t index)
        {
            return input_child_delta_view(context, memory, index);
        }

        [[nodiscard]] std::pair<ValueView, ValueView> input_delta_map_kv_projector(const void *context,
                                                                                   const void *memory,
                                                                                   std::size_t index)
        {
            return {input_delta_map_key_projector(context, memory, index),
                    input_delta_map_value_projector(context, memory, index)};
        }

        [[nodiscard]] Range<ValueView> input_delta_map_make_keys_range(const void *context, const void *memory)
        {
            return Range<ValueView>{
                .context = context,
                .memory = memory,
                .limit = input_indexed_size(context, memory),
                .predicate = &input_delta_child_predicate,
                .projector = &input_delta_map_key_projector,
            };
        }

        [[nodiscard]] Range<ValueView> input_delta_map_make_values_range(const void *context, const void *memory)
        {
            return Range<ValueView>{
                .context = context,
                .memory = memory,
                .limit = input_indexed_size(context, memory),
                .predicate = &input_delta_child_predicate,
                .projector = &input_delta_map_value_projector,
            };
        }

        [[nodiscard]] KeyValueRange<ValueView, ValueView> input_delta_map_make_kv_range(const void *context,
                                                                                        const void *memory)
        {
            return KeyValueRange<ValueView, ValueView>{
                .context = context,
                .memory = memory,
                .limit = input_indexed_size(context, memory),
                .predicate = &input_delta_child_predicate,
                .projector = &input_delta_map_kv_projector,
            };
        }

        [[nodiscard]] SetView input_delta_map_key_set(const void *context,
                                                      const ValueTypeBinding *,
                                                      const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            return ValueView{state->delta.list().key_set_binding, memory}.as_set();
        }

        [[nodiscard]] std::size_t input_delta_map_hash(const void *context, const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            const auto &delta = state->delta.list();
            std::size_t result = 0;
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                if (!input_child_modified_for_parent_time(context, memory, index)) { continue; }
                const auto key_hash = delta.ordinal_key_binding->checked_ops().hash(&delta.ordinal_keys[index]);
                const auto value_hash = input_view_hash(input_child_delta_view(context, memory, index));
                result ^= combine_hash(key_hash, value_hash);
            }
            return result;
        }

        [[nodiscard]] bool input_delta_map_equals(const void *context, const void *lhs, const void *rhs) noexcept
        {
            if (lhs == nullptr || rhs == nullptr) { return lhs == rhs; }
            return fallback_on_exception(false, [&] {
                const auto *state = static_cast<const InputBindingContext *>(context);
                if (input_delta_map_size(context, lhs) != input_delta_map_size(context, rhs)) { return false; }
                for (std::size_t index = 0; index < state->children.size(); ++index)
                {
                    if (!input_child_modified_for_parent_time(context, lhs, index)) { continue; }
                    if (!input_child_modified_for_parent_time(context, rhs, index)) { return false; }
                    if (!input_child_delta_view(context, lhs, index).equals(input_child_delta_view(context, rhs, index)))
                    {
                        return false;
                    }
                }
                return true;
            });
        }

        [[nodiscard]] std::partial_ordering input_delta_map_compare(const void *context,
                                                                    const void *lhs,
                                                                    const void *rhs) noexcept
        {
            if (const auto order = value_ops_detail::null_order(lhs, rhs)) { return *order; }
            return input_delta_map_equals(context, lhs, rhs) ? std::partial_ordering::equivalent
                                                             : std::partial_ordering::unordered;
        }

        [[nodiscard]] std::string input_delta_map_to_string(const void *context, const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            const auto &delta = state->delta.list();
            fmt::memory_buffer out;
            fmt::format_to(std::back_inserter(out), "{{");
            bool first = true;
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                if (!input_child_modified_for_parent_time(context, memory, index)) { continue; }
                if (!first) { fmt::format_to(std::back_inserter(out), ", "); }
                first = false;
                fmt::format_to(std::back_inserter(out), "{}: {}",
                               delta.ordinal_keys[index],
                               input_child_delta_view(context, memory, index).to_string());
            }
            fmt::format_to(std::back_inserter(out), "}}");
            return fmt::to_string(out);
        }

        [[nodiscard]] std::size_t input_delta_key_set_hash(const void *context, const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            const auto &delta = state->delta.list();
            std::size_t result = 0;
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                if (!input_child_modified_for_parent_time(context, memory, index)) { continue; }
                result ^= delta.ordinal_key_binding->checked_ops().hash(&delta.ordinal_keys[index]);
            }
            return result;
        }

        [[nodiscard]] bool input_delta_key_set_equals(const void *context,
                                                      const void *lhs,
                                                      const void *rhs) noexcept
        {
            if (lhs == nullptr || rhs == nullptr) { return lhs == rhs; }
            return fallback_on_exception(false, [&] {
                const auto *state = static_cast<const InputBindingContext *>(context);
                for (std::size_t index = 0; index < state->children.size(); ++index)
                {
                    if (input_child_modified_for_parent_time(context, lhs, index) !=
                        input_child_modified_for_parent_time(context, rhs, index))
                    {
                        return false;
                    }
                }
                return true;
            });
        }

        [[nodiscard]] std::partial_ordering input_delta_key_set_compare(const void *context,
                                                                        const void *lhs,
                                                                        const void *rhs) noexcept
        {
            if (const auto order = value_ops_detail::null_order(lhs, rhs)) { return *order; }
            const auto lhs_size = input_delta_map_size(context, lhs);
            const auto rhs_size = input_delta_map_size(context, rhs);
            if (lhs_size < rhs_size) { return std::partial_ordering::less; }
            if (lhs_size > rhs_size) { return std::partial_ordering::greater; }
            return input_delta_key_set_equals(context, lhs, rhs) ? std::partial_ordering::equivalent
                                                                 : std::partial_ordering::unordered;
        }

        [[nodiscard]] std::string input_delta_key_set_to_string(const void *context, const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            const auto &delta = state->delta.list();
            fmt::memory_buffer out;
            fmt::format_to(std::back_inserter(out), "{{");
            bool first = true;
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                if (!input_child_modified_for_parent_time(context, memory, index)) { continue; }
                if (!first) { fmt::format_to(std::back_inserter(out), ", "); }
                first = false;
                fmt::format_to(std::back_inserter(out), "{}", delta.ordinal_keys[index]);
            }
            fmt::format_to(std::back_inserter(out), "}}");
            return fmt::to_string(out);
        }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        [[nodiscard]] nb::object input_delta_bundle_value_to_python(const void *context, const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            nb::dict result;
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                if (!input_child_modified_for_parent_time(context, memory, index)) { continue; }
                const auto *name = state->schema->fields()[index].name;
                if (name == nullptr || *name == '\0') { continue; }
                result[nb::str{name}] = input_child_delta_view(context, memory, index).to_python();
            }
            return result;
        }

        [[nodiscard]] nb::object input_delta_map_value_to_python(const void *context, const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            const auto &delta = state->delta.list();
            nb::dict result;
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                if (!input_child_modified_for_parent_time(context, memory, index)) { continue; }
                result[nb::int_{delta.ordinal_keys[index]}] = input_child_delta_view(context, memory, index).to_python();
            }
            return result;
        }

        [[nodiscard]] nb::object input_delta_key_set_to_python(const void *context, const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            const auto &delta = state->delta.list();
            nb::set result;
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                if (input_child_modified_for_parent_time(context, memory, index))
                {
                    result.add(nb::int_{delta.ordinal_keys[index]});
                }
            }
            return result;
        }

        [[nodiscard]] nb::object child_value_to_python(const TSDataBinding *binding, const void *memory)
        {
            if (binding == nullptr || memory == nullptr) { return nb::none(); }
            const auto &ops = binding->checked_ops();
            if (!ops.has_current_value_impl(ops.context, memory)) { return nb::none(); }
            return ops.to_python_impl(ops.context, memory);
        }

        [[nodiscard]] nb::object child_delta_to_python(const TSDataBinding *binding,
                                                       const void          *memory,
                                                       engine_time_t        evaluation_time)
        {
            if (binding == nullptr || memory == nullptr) { return nb::none(); }
            const auto &ops = binding->checked_ops();
            const auto *tracking = ops.tracking_impl(ops.context, memory);
            if (tracking == nullptr || tracking->last_modified_time != evaluation_time) { return nb::none(); }
            return ops.delta_to_python_impl(ops.context, memory, evaluation_time);
        }

        [[nodiscard]] nb::object input_tsb_to_python(const void *context, const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            nb::dict result;
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                const auto &field = state->schema->fields()[index];
                if (field.name == nullptr) { continue; }
                result[nb::str{field.name}] =
                    child_value_to_python(input_element_binding(context, memory, index),
                                          input_element_memory(context, memory, index));
            }
            return result;
        }

        [[nodiscard]] nb::object input_tsl_to_python(const void *context, const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            nb::list result;
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                result.append(child_value_to_python(input_element_binding(context, memory, index),
                                                    input_element_memory(context, memory, index)));
            }
            return result;
        }

        [[nodiscard]] nb::object input_to_python(const void *context, const void *memory)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            const auto &endpoint_ops = *state->endpoint_ops;
            if (endpoint_ops.to_python == nullptr)
            {
                throw std::logic_error("TSInput non-peered to_python is not available for this endpoint shape");
            }
            return endpoint_ops.to_python(context, memory);
        }

        [[nodiscard]] nb::object input_tsb_delta_to_python(const void *context,
                                                           const void *memory,
                                                           engine_time_t evaluation_time)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            nb::dict result;
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                const auto &field = state->schema->fields()[index];
                if (field.name == nullptr) { continue; }
                auto child_delta = child_delta_to_python(input_element_binding(context, memory, index),
                                                         input_element_memory(context, memory, index),
                                                         evaluation_time);
                if (!child_delta.is_none()) { result[nb::str{field.name}] = child_delta; }
            }
            return result;
        }

        [[nodiscard]] nb::object input_tsl_delta_to_python(const void *context,
                                                           const void *memory,
                                                           engine_time_t evaluation_time)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            nb::dict result;
            for (std::size_t index = 0; index < state->children.size(); ++index)
            {
                auto child_delta = child_delta_to_python(input_element_binding(context, memory, index),
                                                         input_element_memory(context, memory, index),
                                                         evaluation_time);
                if (!child_delta.is_none()) { result[nb::int_{index}] = child_delta; }
            }
            return result;
        }

        [[nodiscard]] nb::object input_delta_to_python(const void *context,
                                                       const void *memory,
                                                       engine_time_t evaluation_time)
        {
            const auto *state = static_cast<const InputBindingContext *>(context);
            const auto &endpoint_ops = *state->endpoint_ops;
            if (endpoint_ops.delta_to_python == nullptr)
            {
                throw std::logic_error("TSInput non-peered delta_to_python is not available for this endpoint shape");
            }
            return endpoint_ops.delta_to_python(context, memory, evaluation_time);
        }
#endif

        [[nodiscard]] bool target_link_has_current_value(const void *context, const void *memory)
        {
            const auto *link = target_storage(static_cast<const TargetLinkContext *>(context), memory);
            const auto  target = link != nullptr ? link->target_view() : TSDataView{};
            return target.valid() && target.has_current_value();
        }

        [[nodiscard]] bool target_link_all_valid(const void *context, const void *memory)
        {
            const auto *link = target_storage(static_cast<const TargetLinkContext *>(context), memory);
            const auto  target = link != nullptr ? link->target_view() : TSDataView{};
            return target.valid() && target.all_valid();
        }

        [[nodiscard]] const void *target_link_value_memory(const void *context, const void *memory)
        {
            const auto *link = target_storage(static_cast<const TargetLinkContext *>(context), memory);
            const auto  target = link != nullptr ? link->target_view() : TSDataView{};
            return target.valid() ? target.value().data() : nullptr;
        }

        [[nodiscard]] const void *target_link_delta_memory(const void *context, const void *memory)
        {
            const auto *link = target_storage(static_cast<const TargetLinkContext *>(context), memory);
            const auto  target = link != nullptr ? link->target_view() : TSDataView{};
            return target.valid() ? target.delta_value(link->tracking.last_modified_time).data() : nullptr;
        }

        void target_link_cleanup_delta(const void *, void *, engine_time_t) {}

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        [[nodiscard]] nb::object target_link_to_python(const void *context, const void *memory)
        {
            const auto *link = target_storage(static_cast<const TargetLinkContext *>(context), memory);
            const auto  target = link != nullptr ? link->target_view() : TSDataView{};
            return target.valid() ? target.value_to_python() : nb::none();
        }

        [[nodiscard]] nb::object target_link_delta_to_python(const void *context,
                                                             const void *memory,
                                                             engine_time_t evaluation_time)
        {
            const auto *link = target_storage(static_cast<const TargetLinkContext *>(context), memory);
            const auto  target = link != nullptr ? link->target_view() : TSDataView{};
            return target.valid() ? target.delta_value_to_python(evaluation_time) : nb::none();
        }
#endif

        [[nodiscard]] const TSDataBinding *make_target_link_binding(const TSEndpointSchema           &endpoint_schema,
                                                                    const MemoryUtils::StoragePlan   &root_plan,
                                                                    std::size_t                       storage_offset)
        {
            const auto key = binding_cache_key(endpoint_schema, root_plan, storage_offset);
            std::lock_guard lock{target_link_context_cache_mutex()};
            auto &cache = target_link_context_cache();
            if (const auto it = cache.find(key); it != cache.end())
            {
                return TypeBinding<TSValueTypeMetaData, TSDataOps>::find(endpoint_schema.schema(),
                                                                          &root_plan,
                                                                          &it->second->ts_data_ops);
            }

            auto context = std::make_unique<TargetLinkContext>();
            context->schema = endpoint_schema.schema();
            context->storage_offset = storage_offset;
            context->regular_binding = regular_ts_data_binding_for(context->schema);

            context->layout.value_binding = ValuePlanFactory::instance().binding_for(context->schema->value_schema);
            context->layout.delta_binding = ValuePlanFactory::instance().binding_for(context->schema->delta_value_schema);
            context->layout.value_offset = 0;
            context->layout.tracking_offset =
                storage_offset;

            context->ts_data_ops = TSDataOps{
                .context = context.get(),
                .kind = context->schema->kind,
                .allows_mutation = true,
                .layout_impl = &target_link_layout,
                .tracking_impl = &target_link_tracking,
                .mutable_tracking_impl = &target_link_mutable_tracking,
                .has_current_value_impl = &target_link_has_current_value,
                .all_valid_impl = &target_link_all_valid,
                .value_memory_impl = &target_link_value_memory,
                .delta_memory_impl = &target_link_delta_memory,
                .cleanup_delta_impl = &target_link_cleanup_delta,
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                .to_python_impl = &target_link_to_python,
                .delta_to_python_impl = &target_link_delta_to_python,
#endif
            };

            const auto &binding = TSDataBinding::intern(*context->schema, root_plan, context->ts_data_ops);
            cache.emplace(key, std::move(context));
            return &binding;
        }

        [[nodiscard]] const TSDataBinding *input_data_binding_for(const TSEndpointSchema         &endpoint_schema,
                                                                  const MemoryUtils::StoragePlan &root_plan,
                                                                  std::size_t storage_offset)
        {
            if (endpoint_schema.is_peered())
            {
                return make_target_link_binding(endpoint_schema, root_plan, storage_offset);
            }

            const auto key = binding_cache_key(endpoint_schema, root_plan, storage_offset);
            std::lock_guard lock{input_binding_context_cache_mutex()};
            auto &cache = input_binding_context_cache();
            if (const auto it = cache.find(key); it != cache.end())
            {
                return TSDataBinding::find(endpoint_schema.schema(), &root_plan, &it->second->ts_data_ops);
            }

            const auto &local_plan = input_storage_plan(endpoint_schema);
            auto context = std::make_unique<InputBindingContext>();
            context->schema = endpoint_schema.schema();
            context->endpoint_ops = &non_peered_input_endpoint_ops_for(endpoint_schema);
            context->layout.tracking_offset = storage_offset + tracking_offset(local_plan);
            context->children.reserve(endpoint_schema.children().size());
            InputListDeltaSurface *list_delta = nullptr;
            if (!context->endpoint_ops->named_value_projection)
            {
                list_delta = &context->delta.emplace_list();
                list_delta->ordinal_keys.reserve(endpoint_schema.children().size());
            }

            for (std::size_t index = 0; index < endpoint_schema.children().size(); ++index)
            {
                const auto &child_schema = endpoint_schema.children()[index];
                const auto child_offset = storage_offset + child_storage_offset(endpoint_schema, local_plan, index);
                const auto *child_binding = input_data_binding_for(child_schema, root_plan, child_offset);
                context->children.push_back(InputChild{
                    .schema = child_schema.schema(),
                    .input_binding = child_binding,
                    .regular_binding = regular_ts_data_binding_for(child_schema.schema()),
                    .regular_value_binding = regular_value_binding_for(child_schema.schema()),
                    .target_link = child_schema.is_peered(),
                });
                if (list_delta != nullptr)
                {
                    list_delta->ordinal_keys.push_back(static_cast<std::int64_t>(index));
                }
            }

            context->value_ops = IndexedValueOps{
                {context.get(), false, &input_value_hash, &input_value_equals, &input_value_compare,
                 &input_value_to_string},
                &input_indexed_size,
                &input_value_element_at,
                &input_value_element_binding,
                &input_value_make_range,
                nullptr,
            };

            context->value_binding = &ValueTypeBinding::intern(*context->schema->value_schema, root_plan,
                                                               context->value_ops);
            context->layout.value_binding = context->value_binding;

            const auto *delta_schema = context->schema->delta_value_schema;
            if (delta_schema == nullptr)
            {
                throw std::logic_error("TSInput data binding requires a delta schema");
            }

            if (context->endpoint_ops->named_value_projection)
            {
                auto &delta = context->delta.emplace_bundle();
                delta.ops = IndexedValueOps{
                    {context.get(), false, &input_delta_bundle_hash, &input_delta_bundle_equals,
                     &input_delta_bundle_compare, &input_delta_bundle_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                     ,
                     &input_delta_bundle_value_to_python
#endif
                    },
                    &input_indexed_size,
                    &input_delta_bundle_element_at,
                    &input_delta_bundle_element_binding,
                    &input_delta_bundle_make_range,
                    nullptr,
                };
                context->delta_binding = &ValueTypeBinding::intern(*delta_schema, root_plan, delta.ops);
            }
            else
            {
                auto &delta = *list_delta;
                delta.ordinal_key_binding = ValuePlanFactory::instance().binding_for(delta_schema->key_type);
                delta.map_value_binding = input_child_delta_binding(context.get(), nullptr, 0);
                if (delta.ordinal_key_binding == nullptr || delta.map_value_binding == nullptr)
                {
                    throw std::logic_error("TSInput fixed-list delta bindings are not resolved");
                }

                delta.map_ops = MapValueOps{
                    {{context.get(), false, &input_delta_map_hash, &input_delta_map_equals,
                      &input_delta_map_compare, &input_delta_map_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                      ,
                      &input_delta_map_value_to_python
#endif
                     },
                     &input_delta_map_size,
                     &input_delta_map_key_at_index,
                     &input_delta_map_key_binding,
                     &input_delta_map_make_keys_range,
                     nullptr},
                    &input_delta_map_contains,
                    &input_delta_map_value_at,
                    &input_delta_map_value_at_index,
                    &input_delta_map_value_binding,
                    &input_delta_map_make_keys_range,
                    &input_delta_map_make_values_range,
                    &input_delta_map_make_kv_range,
                    &input_delta_map_key_set,
                };

                delta.key_set_ops = SetValueOps{
                    {{context.get(), false, &input_delta_key_set_hash, &input_delta_key_set_equals,
                      &input_delta_key_set_compare, &input_delta_key_set_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                      ,
                      &input_delta_key_set_to_python
#endif
                     },
                     &input_delta_map_size,
                     &input_delta_map_key_at_index,
                     &input_delta_map_key_binding,
                     &input_delta_map_make_keys_range,
                     nullptr},
                    &input_delta_map_contains,
                };

                const auto *key_set_schema = TypeRegistry::instance().set(delta_schema->key_type);
                delta.key_set_binding = &ValueTypeBinding::intern(*key_set_schema, root_plan, delta.key_set_ops);
                context->delta_binding = &ValueTypeBinding::intern(*delta_schema, root_plan, delta.map_ops);
            }
            context->layout.delta_binding = context->delta_binding;

            context->ts_data_ops = IndexedTSDataOps{};
            TSDataOps &base_ops = context->ts_data_ops;
            base_ops = TSDataOps{
                .context = context.get(),
                .kind = context->schema->kind,
                .allows_mutation = true,
                .layout_impl = &input_layout,
                .tracking_impl = &input_tracking,
                .mutable_tracking_impl = &input_mutable_tracking,
                .has_current_value_impl = &input_has_current_value,
                .all_valid_impl = &input_all_valid,
                .value_memory_impl = &input_value_memory,
                .mutable_value_memory_impl = &input_mutable_value_memory,
                .delta_memory_impl = &input_delta_memory,
                .mutable_delta_memory_impl = &input_mutable_delta_memory,
                .cleanup_delta_impl = &input_cleanup_delta,
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                .to_python_impl = &input_to_python,
                .delta_to_python_impl = &input_delta_to_python,
#endif
            };
            context->ts_data_ops.size_impl = &input_indexed_size;
            context->ts_data_ops.element_binding_impl = &input_element_binding;
            context->ts_data_ops.element_memory_impl = &input_element_memory;
            context->ts_data_ops.mutable_element_memory_impl = &input_mutable_element_memory;

            const auto &binding = TSDataBinding::intern(*context->schema, root_plan, context->ts_data_ops);
            cache.emplace(key, std::move(context));
            return &binding;
        }

        [[nodiscard]] const InputBindingContext *input_context_for(const TSDataBinding *binding) noexcept
        {
            if (binding == nullptr) { return nullptr; }

            // The authoritative lookup is through the ops context. Input
            // bindings always use ``input_layout`` while regular TSData and
            // TargetLink bindings do not.
            const auto *ops = binding->ops;
            return ops != nullptr && ops->layout_impl == &input_layout
                       ? static_cast<const InputBindingContext *>(ops->context)
                       : nullptr;
        }

        [[nodiscard]] const TargetLinkContext *target_context_for(const TSDataBinding *binding) noexcept
        {
            if (binding == nullptr) { return nullptr; }
            const auto *ops = binding->ops;
            return ops != nullptr && ops->layout_impl == &target_link_layout
                       ? static_cast<const TargetLinkContext *>(ops->context)
                       : nullptr;
        }

        [[nodiscard]] const TSDataBinding *input_data_binding_for(const TSEndpointSchema &endpoint_schema)
        {
            const auto &root_plan = input_storage_plan(endpoint_schema);
            return input_data_binding_for(endpoint_schema, root_plan, 0);
        }

    }  // namespace

    namespace detail
    {
        bool output_view_bound(const TSOutputView &output) noexcept
        {
            return ::hgraph::output_view_bound(output);
        }

        const TSDataView &empty_ts_data_view() noexcept
        {
            return ::hgraph::empty_ts_data_view();
        }

        void validate_input_view_kind(const TSValueTypeMetaData *schema, TSTypeKind expected, const char *what)
        {
            ::hgraph::validate_endpoint_kind(schema, expected, what);
        }

        const TSInputEndpointOps &input_endpoint_ops_for(const TSValueTypeMetaData *schema)
        {
            return ::hgraph::input_endpoint_ops_for(schema);
        }

        const TSDataBinding *input_data_binding_for(const TSEndpointSchema &endpoint_schema)
        {
            return ::hgraph::input_data_binding_for(endpoint_schema);
        }

        const TSDataBinding *regular_ts_data_binding_for(const TSValueTypeMetaData *schema)
        {
            return ::hgraph::regular_ts_data_binding_for(schema);
        }

        bool is_target_link_binding(const TSDataBinding *binding) noexcept
        {
            return ::hgraph::target_context_for(binding) != nullptr;
        }

        const TSInputTargetLinkStorage *target_link_storage(const TSDataView &view) noexcept
        {
            return ::hgraph::target_storage(view);
        }

        TSInputTargetLinkStorage *mutable_target_link_storage(const TSDataView &view)
        {
            return ::hgraph::mutable_target_storage(view);
        }

        const TSValueTypeMetaData *target_link_schema(const TSDataView &view) noexcept
        {
            const auto *context = ::hgraph::target_context_for(view.binding());
            return context != nullptr ? context->schema : nullptr;
        }

        TSInputChildProjection input_child_projection(const TSDataView &parent, std::size_t index)
        {
            const auto *context = ::hgraph::input_context_for(parent.binding());
            if (context == nullptr || index >= context->children.size())
            {
                throw std::logic_error("TSInput child projection requires non-peered input storage");
            }

            const auto &child = context->children[index];
            if (child.target_link)
            {
                TSDataView link{child.input_binding, parent.data()};
                const auto *storage = ::hgraph::target_storage(link);
                if (storage != nullptr && storage->bound())
                {
                    return TSInputChildProjection{storage->target_view(), std::move(link)};
                }
                return TSInputChildProjection{TSDataView{child.regular_binding, static_cast<const void *>(nullptr)},
                                              std::move(link)};
            }

            return TSInputChildProjection{TSDataView{child.input_binding, parent.data()}, {}};
        }

        void TSInputSchedulingNotifier::notify(engine_time_t modified_time)
        {
            if (target != nullptr) { target->notify(modified_time); }
        }

        TSInputActiveTarget::TSInputActiveTarget() noexcept
        {
        }

        TSInputActiveTarget::TSInputActiveTarget(TSInputActiveTarget *parent_, std::size_t slot_) noexcept
            : parent(parent_),
              slot(slot_)
        {
        }

        TSInputActiveTarget::~TSInputActiveTarget() noexcept
        {
            unsubscribe();
        }

        TSInputActiveTarget *TSInputActiveTarget::child_at(std::size_t slot_) const noexcept
        {
            if (const auto it = children.find(slot_); it != children.end()) { return it->second.get(); }
            return nullptr;
        }

        bool TSInputActiveTarget::has_any_active() const noexcept
        {
            if (active) { return true; }
            return std::ranges::any_of(children, [](const auto &entry) {
                return entry.second && entry.second->has_any_active();
            });
        }

        TSInputActiveTarget &TSInputActiveTarget::ensure_child(std::size_t slot_)
        {
            auto &child = children[slot_];
            if (!child) { child = std::make_unique<TSInputActiveTarget>(this, slot_); }
            return *child;
        }

        void TSInputActiveTarget::subscribe(const TSDataView &observed_, Notifiable *target_notifier)
        {
            if (observed_.valid() && observed_.data() == observed.data() && observed_.binding() == observed.binding())
            {
                notifier.target = target_notifier;
                return;
            }

            unsubscribe();
            notifier.target = target_notifier;
            if (target_notifier == nullptr || !observed_.valid()) { return; }
            observed = observed_.borrowed_ref();
            observed.subscribe(&notifier);
        }

        void TSInputActiveTarget::unsubscribe() noexcept
        {
            if (!observed.valid()) { return; }
            [[maybe_unused]] auto reset_observed = make_scope_exit([this]() noexcept { observed = {}; });
            [[maybe_unused]] auto unsubscribe_observer =
                make_scope_exit<true>([this] { observed.unsubscribe(&notifier); });
        }

    }  // namespace detail

    TSInputConstructionPlan::TSInputConstructionPlan(const TSValueTypeMetaData &root_schema,
                                                     TSEndpointSchema           endpoint_schema)
        : schema_(&root_schema),
          endpoint_schema_(std::move(endpoint_schema))
    {
        if (!time_series_schema_equivalent(&root_schema, endpoint_schema_.schema()))
        {
            throw std::invalid_argument("TSInput construction annotation schema does not match the root schema");
        }
        validate_input_endpoint_schema(endpoint_schema_, true);
    }

    const TSValueTypeMetaData &TSInputConstructionPlan::schema() const noexcept
    {
        return *schema_;
    }

    const TSEndpointSchema &TSInputConstructionPlan::endpoint_schema() const noexcept
    {
        return endpoint_schema_;
    }

    TSInputConstructionPlan TSInputPlanFactory::compile(const TSValueTypeMetaData &root_schema,
                                                        const TSEndpointSchema    &endpoint_schema)
    {
        return TSInputConstructionPlan{root_schema, endpoint_schema};
    }

    TSInputBuilder::TSInputBuilder(TSInputConstructionPlan plan)
        : plan_(std::move(plan))
    {
    }

    const TSValueTypeMetaData &TSInputBuilder::schema() const noexcept
    {
        return plan_.schema();
    }

    TSInput TSInputBuilder::make_input() const
    {
        return TSInput{*this};
    }

    const TSInputBuilder *TSInputBuilderFactory::builder_for(const TSValueTypeMetaData &root_schema,
                                                            const TSEndpointSchema    &endpoint_schema)
    {
        return builder_for(TSInputPlanFactory::compile(root_schema, endpoint_schema));
    }

    const TSInputBuilder &TSInputBuilderFactory::checked_builder_for(const TSValueTypeMetaData &root_schema,
                                                                    const TSEndpointSchema    &endpoint_schema)
    {
        return checked_builder_for(TSInputPlanFactory::compile(root_schema, endpoint_schema));
    }

    const TSInputBuilder *TSInputBuilderFactory::builder_for(const TSInputConstructionPlan &plan)
    {
        if (plan.endpoint_schema().schema() == nullptr || plan.endpoint_schema().schema()->kind != TSTypeKind::TSB ||
            !plan.endpoint_schema().is_non_peered())
        {
            return nullptr;
        }

        const auto key = plan_cache_key(plan);
        std::lock_guard lock{input_builder_cache_mutex()};
        auto &cache = input_builder_cache();
        if (const auto it = cache.find(key); it != cache.end()) { return it->second.get(); }

        auto builder = std::unique_ptr<TSInputBuilder>(new TSInputBuilder(plan));
        const auto *result = builder.get();
        cache.emplace(key, std::move(builder));
        return result;
    }

    const TSInputBuilder &TSInputBuilderFactory::checked_builder_for(const TSInputConstructionPlan &plan)
    {
        if (const auto *builder = builder_for(plan); builder != nullptr) { return *builder; }
        throw std::invalid_argument("TSInputBuilderFactory requires a non-peered TSB root annotation");
    }

    void TSInputBuilderFactory::reset() noexcept
    {
        clear_input_binding_caches();
    }

    TSInput::TSInput() noexcept = default;

    TSInput::TSInput(const TSInputBuilder &builder)
        : builder_(&builder)
    {
        rebuild_from_plan(builder.plan_);
    }

    TSInput::TSInput(const TSInputConstructionPlan &plan)
    {
        rebuild_from_plan(plan);
    }

    TSInput::TSInput(const TSInput &other)
        : builder_(other.builder_),
          schema_(other.schema_),
          data_(other.data_)
    {
    }

    TSInput &TSInput::operator=(const TSInput &other)
    {
        if (this == &other) { return *this; }
        builder_ = other.builder_;
        schema_ = other.schema_;
        data_ = other.data_;
        active_root_.reset();
        return *this;
    }

    TSInput::TSInput(TSInput &&other) noexcept
        : builder_(std::exchange(other.builder_, nullptr)),
          schema_(std::exchange(other.schema_, nullptr)),
          data_(std::move(other.data_)),
          active_root_(std::move(other.active_root_))
    {
    }

    TSInput &TSInput::operator=(TSInput &&other) noexcept
    {
        if (this != &other)
        {
            builder_ = std::exchange(other.builder_, nullptr);
            schema_ = std::exchange(other.schema_, nullptr);
            data_ = std::move(other.data_);
            active_root_ = std::move(other.active_root_);
        }
        return *this;
    }

    TSInput::~TSInput() = default;

    bool TSInput::has_value() const noexcept
    {
        return data_.has_value();
    }

    const TSValueTypeMetaData *TSInput::schema() const noexcept
    {
        return schema_;
    }

    TSInputView TSInput::view(Notifiable *scheduling_notifier, engine_time_t evaluation_time)
    {
        return TSInputView{this, data_.view(), {}, nullptr, scheduling_notifier, evaluation_time};
    }

    TSInputView TSInput::view(Notifiable *scheduling_notifier, engine_time_t evaluation_time) const
    {
        return TSInputView{const_cast<TSInput *>(this), data_.view(), {}, nullptr, scheduling_notifier, evaluation_time};
    }

    void TSInput::rebuild_from_plan(const TSInputConstructionPlan &plan)
    {
        schema_ = &plan.schema();
        const auto *binding = detail::input_data_binding_for(plan.endpoint_schema());
        if (binding == nullptr) { throw std::logic_error("TSInput could not resolve input data binding"); }
        data_ = TSData{*binding};
        active_root_.reset();
    }

    void TSInput::make_active(std::vector<std::size_t> path, TSDataView observed, Notifiable *target_notifier)
    {
        if (!active_root_) { active_root_ = std::make_unique<detail::TSInputActiveTarget>(); }
        auto *active = active_root_.get();
        for (const auto slot : path) { active = &active->ensure_child(slot); }
        active->active = true;
        active->subscribe(observed, target_notifier);
    }

    void TSInput::make_passive(const std::vector<std::size_t> &path)
    {
        auto *active = active_root_.get();
        for (const auto slot : path)
        {
            if (active == nullptr) { return; }
            active = active->child_at(slot);
        }
        if (active == nullptr || !active->active) { return; }

        active->unsubscribe();
        active->active = false;

        while (active != nullptr && !active->has_any_active())
        {
            auto *parent = active->parent;
            if (parent == nullptr)
            {
                active_root_.reset();
                return;
            }
            const auto slot = active->slot;
            active = parent;
            active->children.erase(slot);
        }
    }

    bool TSInput::active(const std::vector<std::size_t> &path) const noexcept
    {
        auto *active = active_root_.get();
        for (const auto slot : path)
        {
            if (active == nullptr) { return false; }
            active = active->child_at(slot);
        }
        return active != nullptr && active->active;
    }

}  // namespace hgraph
