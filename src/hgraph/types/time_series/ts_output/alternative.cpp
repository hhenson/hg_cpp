#include <hgraph/types/time_series/ts_output/alternative.h>

#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/endpoint_schema.h>
#include <hgraph/types/time_series/ts_data/proxy.h>
#include <hgraph/types/time_series_reference.h>
#include <hgraph/types/value/value.h>

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

        [[nodiscard]] bool field_name_equal(const TSFieldMetaData &lhs, const TSFieldMetaData &rhs) noexcept
        {
            const std::string_view lname = lhs.name != nullptr ? std::string_view{lhs.name} : std::string_view{};
            const std::string_view rname = rhs.name != nullptr ? std::string_view{rhs.name} : std::string_view{};
            return lname == rname;
        }

        [[nodiscard]] bool is_to_ref_shape(const TSValueTypeMetaData *source_schema,
                                           const TSValueTypeMetaData *requested_schema)
        {
            if (source_schema == nullptr || requested_schema == nullptr) { return false; }

            if (requested_schema->kind == TSTypeKind::REF)
            {
                const auto *target_schema = requested_schema->referenced_ts();
                return target_schema != nullptr && target_schema->kind != TSTypeKind::REF &&
                       schema_equivalent_after_dereference(target_schema, source_schema);
            }

            if (source_schema->kind != requested_schema->kind) { return false; }

            switch (requested_schema->kind)
            {
                case TSTypeKind::TSB:
                    if (source_schema->field_count() != requested_schema->field_count()) { return false; }
                    for (std::size_t index = 0; index < requested_schema->field_count(); ++index)
                    {
                        if (!field_name_equal(source_schema->fields()[index], requested_schema->fields()[index]))
                        {
                            return false;
                        }
                        if (!is_to_ref_shape(source_schema->fields()[index].type,
                                             requested_schema->fields()[index].type))
                        {
                            return false;
                        }
                    }
                    return true;

                case TSTypeKind::TSL:
                    return source_schema->fixed_size() == requested_schema->fixed_size() &&
                           requested_schema->fixed_size() != 0 &&
                           is_to_ref_shape(source_schema->element_ts(), requested_schema->element_ts());

                case TSTypeKind::TSD:
                    return source_schema->key_type() == requested_schema->key_type() &&
                           is_to_ref_shape(source_schema->element_ts(), requested_schema->element_ts());

                default:
                    return false;
            }
        }

        [[nodiscard]] TSOutputView source_child_view(const TSOutputView &parent, TSDataView child)
        {
            return TSOutputView{parent.output(), child, parent.evaluation_time()};
        }

        [[nodiscard]] const TSDataBinding &to_ref_ts_data_binding_for(const TSValueTypeMetaData &schema);
        void populate_to_ref_data(TSDataView                  target,
                                  TSOutputView                source_view,
                                  const TSValueTypeMetaData  &target_schema,
                                  engine_time_t               modified_time,
                                  const ToRefBuildContext    &build_context);

        void build_to_ref_proxy_value(TSDProxy      &,
                                      std::size_t,
                                      TSDataView     target,
                                      TSDataView     source,
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

            populate_to_ref_data(target,
                                 TSOutputView{build_context->output, source, modified_time},
                                 *target.schema(),
                                 modified_time,
                                 *build_context);
        }

        [[nodiscard]] const TSDataBinding &to_ref_ts_data_binding_for(const TSValueTypeMetaData &schema)
        {
            if (schema.kind == TSTypeKind::TSD)
            {
                return tsd_proxy_binding_for(schema, to_ref_ts_data_binding_for(*schema.element_ts()));
            }
            return checked_ts_data_binding(schema);
        }

        void populate_to_ref_data(TSDataView                  target,
                                  TSOutputView                source_view,
                                  const TSValueTypeMetaData  &target_schema,
                                  engine_time_t               modified_time,
                                  const ToRefBuildContext    &build_context)
        {
            if (target_schema.kind == TSTypeKind::TSD)
            {
                bind_tsd_proxy(target,
                               source_view.data_view().as_dict(),
                               &build_to_ref_proxy_value,
                               &build_context,
                               modified_time);
                return;
            }

            if (target_schema.kind == TSTypeKind::REF)
            {
                auto reference = Value{TSOutputAlternativeStore::peered_reference_as(target_schema.referenced_ts(),
                                                                                     source_view.handle())};
                auto mutation = target.begin_mutation(modified_time);
                if (!mutation.copy_value_from(reference.view()))
                {
                    throw std::logic_error("TSOutput to-REF alternative could not copy reference value");
                }
                return;
            }

            if (target_schema.kind == TSTypeKind::TSB)
            {
                auto target_bundle = target.as_bundle();
                auto source_bundle = source_view.data_view().as_bundle();
                for (std::size_t index = 0; index < target_schema.field_count(); ++index)
                {
                    populate_to_ref_data(target_bundle.at(index),
                                         source_child_view(source_view, source_bundle.at(index)),
                                         *target_schema.fields()[index].type,
                                         modified_time,
                                         build_context);
                }
                return;
            }

            if (target_schema.kind == TSTypeKind::TSL)
            {
                auto target_list = target.as_list();
                auto source_list = source_view.data_view().as_list();
                for (std::size_t index = 0; index < target_schema.fixed_size(); ++index)
                {
                    populate_to_ref_data(target_list.at(index),
                                         source_child_view(source_view, source_list.at(index)),
                                         *target_schema.element_ts(),
                                         modified_time,
                                         build_context);
                }
                return;
            }

            throw std::logic_error("TSOutput to-REF alternative encountered unsupported requested schema");
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
            auto target = data.view();
            populate_to_ref_data(target, source.view(modified_time), *requested_schema, modified_time, build_context);
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
        const auto *source_schema = source.schema();
        if (source_schema == nullptr)
        {
            throw std::invalid_argument("TSOutput alternative binding requires a typed source view");
        }

        const auto key = key_for(source, requested_schema);
        if (is_to_ref_shape(source_schema, &requested_schema))
        {
            return to_ref_binding(key, source, requested_schema);
        }

        if (source_schema->kind == TSTypeKind::REF && requested_schema.kind != TSTypeKind::REF)
        {
            throw std::logic_error("TSOutput REF dereference alternatives are not implemented yet");
        }

        throw std::logic_error("TSOutput structural reference alternatives are not implemented yet");
    }

    TimeSeriesReference TSOutputAlternativeStore::peered_reference_as(const TSValueTypeMetaData *target_schema,
                                                                      TSOutputHandle target)
    {
        return TimeSeriesReference::peered_as(target_schema, target);
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
}  // namespace hgraph::detail
