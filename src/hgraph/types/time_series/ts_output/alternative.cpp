#include <hgraph/types/time_series/ts_output/alternative.h>

#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/endpoint_schema.h>
#include <hgraph/types/time_series/ts_data/proxy.h>
#include <hgraph/types/time_series/ts_input/detail.h>
#include <hgraph/types/time_series/ts_input/target_link.h>
#include <hgraph/types/time_series_reference.h>
#include <hgraph/types/value/value.h>

#include <array>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <unordered_map>
#include <utility>

namespace hgraph::detail
{
    namespace
    {
        struct ToRefBuildContext
        {
            const TSOutput *output{nullptr};
        };

        [[nodiscard]] constexpr std::size_t ts_kind_index(TSTypeKind kind) noexcept
        {
            return static_cast<std::size_t>(kind);
        }

        [[nodiscard]] constexpr std::size_t endpoint_role_index(TSEndpointRole role) noexcept
        {
            return static_cast<std::size_t>(role);
        }

        [[nodiscard]] engine_time_t concrete_reference_time(engine_time_t time) noexcept
        {
            return time != MIN_DT ? time : MIN_ST;
        }

        [[nodiscard]] bool schema_equivalent_after_dereference(const TSValueTypeMetaData *lhs,
                                                               const TSValueTypeMetaData *rhs)
        {
            auto &registry = TypeRegistry::instance();
            return time_series_schema_equivalent(registry.dereference(lhs), registry.dereference(rhs));
        }

        [[nodiscard]] const TSDataBinding &checked_ts_data_binding(const TSValueTypeMetaData &schema)
        {
            const auto *binding = TSDataPlanFactory::instance().binding_for(&schema);
            if (binding == nullptr)
            {
                throw std::logic_error("TSOutput to-REF alternative could not resolve requested TSData binding");
            }
            return *binding;
        }

        [[nodiscard]] const TSDataBinding &checked_endpoint_binding(const TSEndpointSchema &endpoint_schema)
        {
            const auto *binding = input_data_binding_for(endpoint_schema);
            if (binding == nullptr)
            {
                throw std::logic_error("TSOutput from-REF alternative could not resolve endpoint TSData binding");
            }
            return *binding;
        }

        [[nodiscard]] bool field_name_equal(const TSFieldMetaData &lhs, const TSFieldMetaData &rhs) noexcept
        {
            const std::string_view lname = lhs.name != nullptr ? std::string_view{lhs.name} : std::string_view{};
            const std::string_view rname = rhs.name != nullptr ? std::string_view{rhs.name} : std::string_view{};
            return lname == rname;
        }

        [[nodiscard]] bool is_to_ref_shape(const TSValueTypeMetaData *source_schema,
                                           const TSValueTypeMetaData *requested_schema);

        [[nodiscard]] bool to_ref_shape_matches_unsupported(const TSValueTypeMetaData *,
                                                            const TSValueTypeMetaData *) noexcept
        {
            return false;
        }

        [[nodiscard]] bool to_ref_shape_matches_ref(const TSValueTypeMetaData *source_schema,
                                                    const TSValueTypeMetaData *requested_schema)
        {
            const auto *target_schema = requested_schema->referenced_ts();
            return target_schema != nullptr && target_schema->kind != TSTypeKind::REF &&
                   schema_equivalent_after_dereference(target_schema, source_schema);
        }

        [[nodiscard]] bool to_ref_shape_matches_bundle(const TSValueTypeMetaData *source_schema,
                                                       const TSValueTypeMetaData *requested_schema)
        {
            if (source_schema->field_count() != requested_schema->field_count()) { return false; }
            for (std::size_t index = 0; index < requested_schema->field_count(); ++index)
            {
                if (!field_name_equal(source_schema->fields()[index], requested_schema->fields()[index]))
                {
                    return false;
                }
                if (!is_to_ref_shape(source_schema->fields()[index].type, requested_schema->fields()[index].type))
                {
                    return false;
                }
            }
            return true;
        }

        [[nodiscard]] bool to_ref_shape_matches_list(const TSValueTypeMetaData *source_schema,
                                                     const TSValueTypeMetaData *requested_schema)
        {
            return source_schema->fixed_size() == requested_schema->fixed_size() &&
                   requested_schema->fixed_size() != 0 &&
                   is_to_ref_shape(source_schema->element_ts(), requested_schema->element_ts());
        }

        [[nodiscard]] bool to_ref_shape_matches_dict(const TSValueTypeMetaData *source_schema,
                                                     const TSValueTypeMetaData *requested_schema)
        {
            return source_schema->key_type() == requested_schema->key_type() &&
                   is_to_ref_shape(source_schema->element_ts(), requested_schema->element_ts());
        }

        using ToRefShapeMatchesFn = bool (*)(const TSValueTypeMetaData *, const TSValueTypeMetaData *);

        [[nodiscard]] ToRefShapeMatchesFn to_ref_shape_matcher_for(TSTypeKind kind) noexcept
        {
            static constexpr std::size_t kind_count = ts_kind_index(TSTypeKind::SIGNAL) + 1U;
            static const std::array<ToRefShapeMatchesFn, kind_count> table{
                &to_ref_shape_matches_unsupported,
                &to_ref_shape_matches_unsupported,
                &to_ref_shape_matches_dict,
                &to_ref_shape_matches_list,
                &to_ref_shape_matches_unsupported,
                &to_ref_shape_matches_bundle,
                &to_ref_shape_matches_ref,
                &to_ref_shape_matches_unsupported,
            };

            const auto index = ts_kind_index(kind);
            return index < table.size() ? table[index] : &to_ref_shape_matches_unsupported;
        }

        [[nodiscard]] bool is_to_ref_shape(const TSValueTypeMetaData *source_schema,
                                           const TSValueTypeMetaData *requested_schema)
        {
            if (source_schema == nullptr || requested_schema == nullptr) { return false; }
            if (requested_schema->kind != TSTypeKind::REF && source_schema->kind != requested_schema->kind)
            {
                return false;
            }
            return to_ref_shape_matcher_for(requested_schema->kind)(source_schema, requested_schema);
        }

        using AlternativeRouteMatchesFn = bool (*)(const TSValueTypeMetaData *, const TSValueTypeMetaData &);

        [[nodiscard]] bool alternative_route_matches_to_ref(const TSValueTypeMetaData *source_schema,
                                                            const TSValueTypeMetaData &requested_schema)
        {
            return is_to_ref_shape(source_schema, &requested_schema);
        }

        [[nodiscard]] bool alternative_route_matches_from_ref(const TSValueTypeMetaData *source_schema,
                                                              const TSValueTypeMetaData &requested_schema) noexcept
        {
            return source_schema != nullptr && source_schema->kind == TSTypeKind::REF &&
                   requested_schema.kind != TSTypeKind::REF;
        }

        [[nodiscard]] TSOutputView source_child_view(const TSOutputView &parent, const TSDataView &child)
        {
            return TSOutputView{parent.output(), child.borrowed_ref(), parent.evaluation_time()};
        }

        [[nodiscard]] TSEndpointSchema from_ref_endpoint_schema_for(const TSValueTypeMetaData *schema);

        [[nodiscard]] TSEndpointSchema peered_from_ref_endpoint_schema_for(const TSValueTypeMetaData *schema)
        {
            return TSEndpointSchema::peered(schema);
        }

        [[nodiscard]] TSEndpointSchema bundle_from_ref_endpoint_schema_for(const TSValueTypeMetaData *schema)
        {
            std::vector<TSEndpointSchema> children;
            children.reserve(schema->field_count());
            for (std::size_t index = 0; index < schema->field_count(); ++index)
            {
                children.push_back(from_ref_endpoint_schema_for(schema->fields()[index].type));
            }
            return TSEndpointSchema::non_peered(schema, std::move(children));
        }

        [[nodiscard]] TSEndpointSchema list_from_ref_endpoint_schema_for(const TSValueTypeMetaData *schema)
        {
            if (schema->fixed_size() == 0) { return TSEndpointSchema::peered(schema); }
            return TSEndpointSchema::non_peered_list(schema, from_ref_endpoint_schema_for(schema->element_ts()));
        }

        using FromRefEndpointSchemaForFn = TSEndpointSchema (*)(const TSValueTypeMetaData *);

        [[nodiscard]] FromRefEndpointSchemaForFn from_ref_endpoint_schema_builder_for(TSTypeKind kind) noexcept
        {
            static constexpr std::size_t kind_count = ts_kind_index(TSTypeKind::SIGNAL) + 1U;
            static const std::array<FromRefEndpointSchemaForFn, kind_count> table{
                &peered_from_ref_endpoint_schema_for,
                &peered_from_ref_endpoint_schema_for,
                &peered_from_ref_endpoint_schema_for,
                &list_from_ref_endpoint_schema_for,
                &peered_from_ref_endpoint_schema_for,
                &bundle_from_ref_endpoint_schema_for,
                &peered_from_ref_endpoint_schema_for,
                &peered_from_ref_endpoint_schema_for,
            };

            const auto index = ts_kind_index(kind);
            return index < table.size() ? table[index] : &peered_from_ref_endpoint_schema_for;
        }

        [[nodiscard]] TSEndpointSchema from_ref_endpoint_schema_for(const TSValueTypeMetaData *schema)
        {
            if (schema == nullptr)
            {
                throw std::invalid_argument("TSOutput from-REF endpoint schema requires a time-series schema");
            }
            return from_ref_endpoint_schema_builder_for(schema->kind)(schema);
        }

        [[nodiscard]] TSDataView endpoint_child_view(const TSDataView &parent, std::size_t index)
        {
            auto projection = input_child_projection(parent, index);
            if (projection.target_link.valid())
            {
                return TSOutputAlternativeStore::child_view_with_parent(parent, projection.target_link, index);
            }
            if (projection.visible.valid())
            {
                return TSOutputAlternativeStore::child_view_with_parent(parent, projection.visible, index);
            }
            return {};
        }

        void unbind_target_link_at(const TSDataView &target, engine_time_t modified_time)
        {
            auto *link = mutable_target_link_storage(target);
            if (link == nullptr)
            {
                throw std::logic_error("TSOutput from-REF target unbinding requires TargetLink storage");
            }
            link->unbind();
            link->record_target_modified(modified_time);
        }

        void bind_target_link_at(const TSDataView &target, const TSOutputView &output, engine_time_t modified_time)
        {
            bind_target_link(target, output);
            auto *link = mutable_target_link_storage(target);
            if (link == nullptr)
            {
                throw std::logic_error("TSOutput from-REF target binding requires TargetLink storage");
            }
            link->record_target_modified(modified_time);
        }

        [[nodiscard]] TSOutputView output_child_view(const TSOutputView &parent,
                                                     const TSValueTypeMetaData &parent_schema,
                                                     std::size_t index)
        {
            const auto &ops = input_endpoint_ops_for(&parent_schema);
            if (ops.target_child == nullptr || ops.child_schema == nullptr)
            {
                throw std::logic_error("TSOutput from-REF cannot project a child from this output schema");
            }
            auto child = ops.target_child(parent.data_view(), index);
            if (!child.valid()) { throw std::logic_error("TSOutput from-REF output child projection failed"); }
            return source_child_view(parent, child);
        }

        void unbind_from_ref_data(const TSDataView &target,
                                  const TSEndpointSchema &endpoint_schema,
                                  engine_time_t modified_time);

        void apply_output_to_from_ref_data(const TSDataView &target,
                                           const TSEndpointSchema &endpoint_schema,
                                           const TSOutputView &output,
                                           engine_time_t modified_time);

        void apply_reference_to_from_ref_data(const TSDataView &target,
                                              const TSEndpointSchema &endpoint_schema,
                                              const TimeSeriesReference &reference,
                                              engine_time_t modified_time);

        using FromRefUnbindFn = void (*)(const TSDataView &, const TSEndpointSchema &, engine_time_t);
        using FromRefOutputApplyFn = void (*)(
            const TSDataView &,
            const TSEndpointSchema &,
            const TSOutputView &,
            engine_time_t);
        using FromRefNonPeeredReferenceApplyFn = void (*)(
            const TSDataView &,
            const TSEndpointSchema &,
            const TimeSeriesReference &,
            engine_time_t);

        struct FromRefRoleOps
        {
            FromRefUnbindFn                  unbind{nullptr};
            FromRefOutputApplyFn             apply_output{nullptr};
            FromRefNonPeeredReferenceApplyFn apply_non_peered_reference{nullptr};
        };

        void unbind_from_ref_peered(const TSDataView &target,
                                    const TSEndpointSchema &,
                                    engine_time_t modified_time)
        {
            unbind_target_link_at(target, modified_time);
        }

        void unbind_from_ref_non_peered(const TSDataView &target,
                                        const TSEndpointSchema &endpoint_schema,
                                        engine_time_t modified_time)
        {
            for (std::size_t index = 0; index < endpoint_schema.child_count(); ++index)
            {
                auto child = endpoint_child_view(target, index);
                unbind_from_ref_data(child, endpoint_schema.child(index), modified_time);
            }
        }

        void apply_output_to_from_ref_peered(const TSDataView &target,
                                             const TSEndpointSchema &,
                                             const TSOutputView &output,
                                             engine_time_t modified_time)
        {
            bind_target_link_at(target, output, modified_time);
        }

        void apply_output_to_from_ref_non_peered(const TSDataView &target,
                                                 const TSEndpointSchema &endpoint_schema,
                                                 const TSOutputView &output,
                                                 engine_time_t modified_time)
        {
            for (std::size_t index = 0; index < endpoint_schema.child_count(); ++index)
            {
                auto child = endpoint_child_view(target, index);
                auto child_output = output_child_view(output, *endpoint_schema.schema(), index);
                apply_output_to_from_ref_data(child, endpoint_schema.child(index), child_output, modified_time);
            }
        }

        void apply_non_peered_reference_to_peered_from_ref_data(const TSDataView &,
                                                               const TSEndpointSchema &,
                                                               const TimeSeriesReference &,
                                                               engine_time_t)
        {
            throw std::invalid_argument("TSOutput from-REF cannot apply a non-peered reference to a peered leaf");
        }

        void apply_non_peered_reference_to_non_peered_from_ref_data(
            const TSDataView &target,
            const TSEndpointSchema &endpoint_schema,
            const TimeSeriesReference &reference,
            engine_time_t modified_time)
        {
            if (reference.items().size() != endpoint_schema.child_count())
            {
                throw std::invalid_argument("TSOutput from-REF non-peered reference has the wrong child count");
            }

            for (std::size_t index = 0; index < endpoint_schema.child_count(); ++index)
            {
                auto child = endpoint_child_view(target, index);
                apply_reference_to_from_ref_data(child, endpoint_schema.child(index), reference[index], modified_time);
            }
        }

        [[nodiscard]] const FromRefRoleOps &from_ref_role_ops_for(TSEndpointRole role) noexcept
        {
            static const std::array<FromRefRoleOps, 2> table{{
                {
                    &unbind_from_ref_peered,
                    &apply_output_to_from_ref_peered,
                    &apply_non_peered_reference_to_peered_from_ref_data,
                },
                {
                    &unbind_from_ref_non_peered,
                    &apply_output_to_from_ref_non_peered,
                    &apply_non_peered_reference_to_non_peered_from_ref_data,
                },
            }};

            const auto index = endpoint_role_index(role);
            return index < table.size() ? table[index] : table[0];
        }

        void unbind_from_ref_data(const TSDataView &target,
                                  const TSEndpointSchema &endpoint_schema,
                                  engine_time_t modified_time)
        {
            from_ref_role_ops_for(endpoint_schema.role()).unbind(target, endpoint_schema, modified_time);
        }

        void apply_output_to_from_ref_data(const TSDataView &target,
                                           const TSEndpointSchema &endpoint_schema,
                                           const TSOutputView &output,
                                           engine_time_t modified_time)
        {
            from_ref_role_ops_for(endpoint_schema.role()).apply_output(target, endpoint_schema, output, modified_time);
        }

        void apply_reference_to_from_ref_data(const TSDataView &target,
                                              const TSEndpointSchema &endpoint_schema,
                                              const TimeSeriesReference &reference,
                                              engine_time_t modified_time)
        {
            if (reference.is_empty())
            {
                unbind_from_ref_data(target, endpoint_schema, modified_time);
                return;
            }

            if (reference.is_peered())
            {
                const auto &output = TSOutputAlternativeStore::peered_reference_target(reference);
                apply_output_to_from_ref_data(target, endpoint_schema, output.view(modified_time), modified_time);
                return;
            }

            from_ref_role_ops_for(endpoint_schema.role()).apply_non_peered_reference(target, endpoint_schema, reference,
                                                                                    modified_time);
        }

        [[nodiscard]] const TSDataBinding &to_ref_ts_data_binding_for(const TSValueTypeMetaData &schema);
        void populate_to_ref_data(const TSDataView           &target,
                                  const TSOutputView         &source_view,
                                  const TSValueTypeMetaData  &target_schema,
                                  engine_time_t               modified_time,
                                  const ToRefBuildContext    &build_context);

        void build_to_ref_proxy_value(TSDProxy      &,
                                      std::size_t,
                                      const TSDataView &target,
                                      const TSDataView &source,
                                      engine_time_t  modified_time,
                                      const void    *context)
        {
            const auto *build_context = static_cast<const ToRefBuildContext *>(context);
            if (build_context == nullptr || build_context->output == nullptr)
            {
                throw std::logic_error("TSOutput to-REF proxy value builder requires an output context");
            }
            if (!source.valid()) { throw std::logic_error("TSOutput to-REF proxy source child is not live"); }
            if (target.schema() == nullptr)
            {
                throw std::logic_error("TSOutput to-REF proxy target child is not typed");
            }
            if (target.has_current_value()) { return; }

            populate_to_ref_data(target.borrowed_ref(),
                                 TSOutputView{build_context->output, source.borrowed_ref(), modified_time},
                                 *target.schema(),
                                 modified_time,
                                 *build_context);
        }

        [[nodiscard]] const TSDataBinding &to_ref_regular_binding_for(const TSValueTypeMetaData &schema)
        {
            return checked_ts_data_binding(schema);
        }

        [[nodiscard]] const TSDataBinding &to_ref_dict_binding_for(const TSValueTypeMetaData &schema)
        {
            return tsd_proxy_binding_for(schema, to_ref_ts_data_binding_for(*schema.element_ts()));
        }

        using ToRefBindingForFn = const TSDataBinding &(*)(const TSValueTypeMetaData &);

        [[nodiscard]] ToRefBindingForFn to_ref_binding_for_kind(TSTypeKind kind) noexcept
        {
            static constexpr std::size_t kind_count = ts_kind_index(TSTypeKind::SIGNAL) + 1U;
            static const std::array<ToRefBindingForFn, kind_count> table{
                &to_ref_regular_binding_for,
                &to_ref_regular_binding_for,
                &to_ref_dict_binding_for,
                &to_ref_regular_binding_for,
                &to_ref_regular_binding_for,
                &to_ref_regular_binding_for,
                &to_ref_regular_binding_for,
                &to_ref_regular_binding_for,
            };

            const auto index = ts_kind_index(kind);
            return index < table.size() ? table[index] : &to_ref_regular_binding_for;
        }

        [[nodiscard]] const TSDataBinding &to_ref_ts_data_binding_for(const TSValueTypeMetaData &schema)
        {
            return to_ref_binding_for_kind(schema.kind)(schema);
        }

        void populate_to_ref_unsupported(const TSDataView &,
                                         const TSOutputView &,
                                         const TSValueTypeMetaData &,
                                         engine_time_t,
                                         const ToRefBuildContext &)
        {
            throw std::logic_error("TSOutput to-REF alternative encountered unsupported requested schema");
        }

        void populate_to_ref_dict(const TSDataView           &target,
                                  const TSOutputView         &source_view,
                                  const TSValueTypeMetaData  &,
                                  engine_time_t               modified_time,
                                  const ToRefBuildContext    &build_context)
        {
            bind_tsd_proxy(target.borrowed_ref(),
                           source_view.data_view().as_dict(),
                           &build_to_ref_proxy_value,
                           &build_context,
                           modified_time);
        }

        void populate_to_ref_ref(const TSDataView           &target,
                                 const TSOutputView         &source_view,
                                 const TSValueTypeMetaData  &target_schema,
                                 engine_time_t               modified_time,
                                 const ToRefBuildContext    &)
        {
            auto reference = Value{TSOutputAlternativeStore::peered_reference_as(target_schema.referenced_ts(),
                                                                                 source_view.handle())};
            auto mutation = target.begin_mutation(modified_time);
            if (!mutation.copy_value_from(reference.view()))
            {
                throw std::logic_error("TSOutput to-REF alternative could not copy reference value");
            }
        }

        void populate_to_ref_bundle(const TSDataView           &target,
                                    const TSOutputView         &source_view,
                                    const TSValueTypeMetaData  &target_schema,
                                    engine_time_t               modified_time,
                                    const ToRefBuildContext    &build_context)
        {
            auto target_bundle = target.as_bundle();
            auto source_bundle = source_view.data_view().as_bundle();
            for (std::size_t index = 0; index < target_schema.field_count(); ++index)
            {
                auto target_child = target_bundle.at(index);
                auto source_child_data = source_bundle.at(index);
                auto source_child = source_child_view(source_view, source_child_data);
                populate_to_ref_data(target_child, source_child, *target_schema.fields()[index].type,
                                     modified_time, build_context);
            }
        }

        void populate_to_ref_list(const TSDataView           &target,
                                  const TSOutputView         &source_view,
                                  const TSValueTypeMetaData  &target_schema,
                                  engine_time_t               modified_time,
                                  const ToRefBuildContext    &build_context)
        {
            auto target_list = target.as_list();
            auto source_list = source_view.data_view().as_list();
            for (std::size_t index = 0; index < target_schema.fixed_size(); ++index)
            {
                auto target_child = target_list.at(index);
                auto source_child_data = source_list.at(index);
                auto source_child = source_child_view(source_view, source_child_data);
                populate_to_ref_data(target_child, source_child, *target_schema.element_ts(), modified_time,
                                     build_context);
            }
        }

        using ToRefPopulateFn = void (*)(
            const TSDataView &,
            const TSOutputView &,
            const TSValueTypeMetaData &,
            engine_time_t,
            const ToRefBuildContext &);

        [[nodiscard]] ToRefPopulateFn to_ref_populator_for(TSTypeKind kind) noexcept
        {
            static constexpr std::size_t kind_count = ts_kind_index(TSTypeKind::SIGNAL) + 1U;
            static const std::array<ToRefPopulateFn, kind_count> table{
                &populate_to_ref_unsupported,
                &populate_to_ref_unsupported,
                &populate_to_ref_dict,
                &populate_to_ref_list,
                &populate_to_ref_unsupported,
                &populate_to_ref_bundle,
                &populate_to_ref_ref,
                &populate_to_ref_unsupported,
            };

            const auto index = ts_kind_index(kind);
            return index < table.size() ? table[index] : &populate_to_ref_unsupported;
        }

        void populate_to_ref_data(const TSDataView           &target,
                                  const TSOutputView         &source_view,
                                  const TSValueTypeMetaData  &target_schema,
                                  engine_time_t               modified_time,
                                  const ToRefBuildContext    &build_context)
        {
            to_ref_populator_for(target_schema.kind)(target, source_view, target_schema, modified_time, build_context);
        }
    }  // namespace

    struct TSOutputAlternativeStore::ToRefAlternativeState final
    {
        ToRefAlternativeState(const TSValueTypeMetaData &requested_schema, const TSOutputView &source)
            : requested_schema{&requested_schema},
              data{to_ref_ts_data_binding_for(requested_schema)}
        {
            rebind(source);
        }

        ToRefAlternativeState(const ToRefAlternativeState &) = delete;
        ToRefAlternativeState &operator=(const ToRefAlternativeState &) = delete;
        ~ToRefAlternativeState() = default;

        const TSValueTypeMetaData *requested_schema{nullptr};
        TSData                     data{};
        TSOutputHandle             source{};
        ToRefBuildContext          build_context{};

        [[nodiscard]] TSOutputHandle handle(const TSOutput *output) noexcept
        {
            return TSOutputHandle{output, data.view()};
        }

        void rebind(const TSOutputView &new_source)
        {
            source               = new_source.handle();
            build_context.output = new_source.output();
            refresh(new_source.evaluation_time());
        }

      private:
        void refresh(engine_time_t modified_time)
        {
            if (requested_schema == nullptr || !source.bound()) { return; }
            modified_time = concrete_reference_time(modified_time);
            auto target = data.view();
            populate_to_ref_data(target, source.view(modified_time), *requested_schema, modified_time, build_context);
        }
    };

    struct TSOutputAlternativeStore::RefLinkAlternativeState final
    {
        struct SourceNotifier final : Notifiable
        {
            explicit SourceNotifier(RefLinkAlternativeState &owner) noexcept
                : owner{&owner}
            {
            }

            void notify(engine_time_t modified_time) override
            {
                if (owner != nullptr) { owner->refresh(modified_time); }
            }

            RefLinkAlternativeState *owner{nullptr};
        };

        RefLinkAlternativeState(const TSValueTypeMetaData &requested_schema, const TSOutputView &source)
            : requested_schema{&requested_schema},
              endpoint_schema{from_ref_endpoint_schema_for(&requested_schema)},
              data{checked_endpoint_binding(endpoint_schema)},
              notifier{*this}
        {
            rebind(source);
        }

        RefLinkAlternativeState(const RefLinkAlternativeState &) = delete;
        RefLinkAlternativeState &operator=(const RefLinkAlternativeState &) = delete;

        ~RefLinkAlternativeState() noexcept
        {
            unsubscribe_source();
        }

        const TSValueTypeMetaData *requested_schema{nullptr};
        TSEndpointSchema           endpoint_schema{};
        TSData                     data{};
        TSOutputHandle             source{};
        SourceNotifier             notifier;

        [[nodiscard]] TSOutputHandle handle(const TSOutput *output) noexcept
        {
            return TSOutputHandle{output, data.view()};
        }

        void rebind(const TSOutputView &new_source)
        {
            const auto next_source = new_source.handle();
            if (!source.same_as(next_source))
            {
                unsubscribe_source();
                source = next_source;
                subscribe_source();
            }
            refresh(new_source.evaluation_time());
        }

      private:
        void subscribe_source()
        {
            if (source.bound()) { source.data_view().subscribe(&notifier); }
        }

        void unsubscribe_source() noexcept
        {
            if (!source.bound()) { return; }
            static_cast<void>(fallback_on_exception(false, [&] {
                source.data_view().unsubscribe(&notifier);
                return true;
            }));
        }

        void refresh(engine_time_t modified_time)
        {
            if (modified_time == MIN_DT || requested_schema == nullptr || !source.bound()) { return; }

            auto source_view = source.view(modified_time);
            auto target = data.view();
            if (!source_view.valid())
            {
                unbind_from_ref_data(target, endpoint_schema, modified_time);
                return;
            }

            const auto &reference = source_view.value().checked_as<TimeSeriesReference>();
            apply_reference_to_from_ref_data(target, endpoint_schema, reference, modified_time);
        }
    };

    TSOutputAlternativeStore::TSOutputAlternativeStore() noexcept = default;
    TSOutputAlternativeStore::TSOutputAlternativeStore(TSOutputAlternativeStore &&) noexcept = default;
    TSOutputAlternativeStore &TSOutputAlternativeStore::operator=(TSOutputAlternativeStore &&) noexcept = default;
    TSOutputAlternativeStore::~TSOutputAlternativeStore() noexcept = default;

    std::size_t TSOutputAlternativeStore::AlternativeKeyHash::operator()(const AlternativeKey &key) const noexcept
    {
        auto combine = [](std::size_t seed, const void *value) noexcept {
            const auto h = std::hash<const void *>{}(value);
            return seed ^ (h + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U));
        };

        std::size_t seed = 0;
        seed = combine(seed, key.source_output);
        seed = combine(seed, key.source_binding);
        seed = combine(seed, key.source_data);
        seed = combine(seed, key.requested_schema);
        return seed;
    }

    TSOutputAlternativeStore::AlternativeKey TSOutputAlternativeStore::key_for(
        const TSOutputView &source,
        const TSValueTypeMetaData &requested_schema) noexcept
    {
        return AlternativeKey{
            .source_output    = source.output(),
            .source_binding   = source.data_view().binding(),
            .source_data      = source.data_view().data(),
            .requested_schema = &requested_schema,
        };
    }

    TSOutputHandle TSOutputAlternativeStore::binding_for(const TSOutputView &source,
                                                         const TSValueTypeMetaData &requested_schema)
    {
        struct AlternativeRoute
        {
            using BindFn = TSOutputHandle (TSOutputAlternativeStore::*)(
                const AlternativeKey &,
                const TSOutputView &,
                const TSValueTypeMetaData &);

            AlternativeRouteMatchesFn matches{nullptr};
            BindFn                    bind{nullptr};
        };

        static constexpr std::array<AlternativeRoute, 2> routes{{
            {&alternative_route_matches_to_ref, &TSOutputAlternativeStore::to_ref_binding},
            {&alternative_route_matches_from_ref, &TSOutputAlternativeStore::from_ref_binding},
        }};

        const auto *source_schema = source.schema();
        if (source_schema == nullptr)
        {
            throw std::invalid_argument("TSOutput alternative binding requires a typed source view");
        }

        const auto key = key_for(source, requested_schema);
        for (const auto &route : routes)
        {
            if (route.matches(source_schema, requested_schema))
            {
                return (this->*route.bind)(key, source, requested_schema);
            }
        }

        throw std::logic_error("TSOutput structural reference alternatives are not implemented yet");
    }

    TimeSeriesReference TSOutputAlternativeStore::peered_reference_as(const TSValueTypeMetaData *target_schema,
                                                                      TSOutputHandle target)
    {
        return TimeSeriesReference::peered_as(target_schema, target);
    }

    const TSOutputHandle &TSOutputAlternativeStore::peered_reference_target(const TimeSeriesReference &reference)
    {
        return reference.target_output();
    }

    TSDataView TSOutputAlternativeStore::child_view_with_parent(const TSDataView &parent,
                                                                const TSDataView &child,
                                                                std::size_t child_id)
    {
        return TSDataView{child.binding(), child.data(), parent, child_id};
    }

    TSOutputHandle TSOutputAlternativeStore::to_ref_binding(const AlternativeKey &key,
                                                            const TSOutputView &source,
                                                            const TSValueTypeMetaData &requested_schema)
    {
        auto it = to_ref_alternatives_.find(key);
        if (it == to_ref_alternatives_.end())
        {
            auto state = std::make_unique<ToRefAlternativeState>(requested_schema, source);
            it = to_ref_alternatives_.emplace(key, std::move(state)).first;
        }
        else
        {
            if (it->second->requested_schema != &requested_schema)
            {
                throw std::logic_error("TSOutput to-REF alternative cache key resolved to the wrong requested schema");
            }
            it->second->rebind(source);
        }
        return it->second->handle(source.output());
    }

    TSOutputHandle TSOutputAlternativeStore::from_ref_binding(const AlternativeKey &key,
                                                              const TSOutputView &source,
                                                              const TSValueTypeMetaData &requested_schema)
    {
        auto it = ref_link_alternatives_.find(key);
        if (it == ref_link_alternatives_.end())
        {
            auto state = std::make_unique<RefLinkAlternativeState>(requested_schema, source);
            it = ref_link_alternatives_.emplace(key, std::move(state)).first;
        }
        else
        {
            if (it->second->requested_schema != &requested_schema)
            {
                throw std::logic_error("TSOutput from-REF alternative cache key resolved to the wrong requested schema");
            }
            it->second->rebind(source);
        }
        return it->second->handle(source.output());
    }
}  // namespace hgraph::detail
