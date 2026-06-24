#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_HIGHER_ORDER_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_HIGHER_ORDER_IMPL_H

#include <hgraph/lib/std/operators/conversion.h>  // nothing (placeholder for the mesh_subscribe value input)
#include <hgraph/lib/std/operators/higher_order.h>
#include <hgraph/lib/std/operators/impl/reduce_layout.h>
#include <hgraph/runtime/map_node.h>
#include <hgraph/runtime/mesh_node.h>
#include <hgraph/runtime/reduce_node.h>
#include <hgraph/runtime/switch_node.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/subgraph_wiring.h>
#include <hgraph/types/wired_fn.h>
#include <hgraph/util/scope.h>

#include <array>
#include <cstddef>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <typeindex>
#include <typeinfo>
#include <utility>
#include <vector>

namespace hgraph::stdlib
{
    /**
     * Default overloads for the higher-order operators — ordinary registered
     * candidates (a graph overload per kind, like any other operator family).
     * User specialisations register alongside and are selected by the standard
     * pattern ranking + ``requires_`` gating, e.g. a concrete overload gated on
     * the wired function's identity.
     */

    namespace higher_order_impl_detail
    {
        inline void bind_graph_output(ResolutionMap &resolution,
                                      const TSValueTypeMetaData *output,
                                      std::string_view legacy_var = {})
        {
            if (output == nullptr) { return; }
            if (!legacy_var.empty() && resolution.find_ts(legacy_var) == nullptr)
            {
                resolution.bind_ts(legacy_var, output);
            }
            if (resolution.find_ts("__out__") == nullptr)
            {
                resolution.bind_ts("__out__", output);
            }
        }

        struct lifted_reduce_tsl_node_tag
        {
        };

        [[nodiscard]] inline const LiftedKernel *resolve_lifted_kernel_for_schemas(
            const WiredFn &func,
            std::span<const TSValueTypeMetaData *const> input_schemas,
            const TSValueTypeMetaData *expected_output = nullptr)
        {
            if (func.lifted != nullptr) { return func.lifted->valid() ? func.lifted : nullptr; }
            if (func.operator_name.empty() || func.arity != input_schemas.size()) { return nullptr; }

            std::vector<WiringArg> args;
            args.reserve(input_schemas.size());
            for (const TSValueTypeMetaData *schema : input_schemas)
            {
                WiringArg arg;
                arg.kind = WiringArg::Kind::TimeSeries;
                arg.port.schema = schema;
                args.push_back(std::move(arg));
            }

            try
            {
                ResolvedOperatorCall resolved =
                    OperatorRegistry::instance().resolve(func.operator_name,
                                                         std::span<const WiringArg>{args.data(), args.size()},
                                                         func.has_output,
                                                         expected_output);
                const LiftedKernel *kernel = resolved.impl != nullptr ? resolved.impl->lifted_kernel : nullptr;
                return kernel != nullptr && kernel->valid() ? kernel : nullptr;
            }
            catch (...)
            {
                return nullptr;
            }
        }

        // A fixed-size TSL second argument (shared requires_ of the TSL overloads;
        // the dynamic-TSL/TSD reductions are separate, future overloads).
        [[nodiscard]] inline bool reduce_ts_is_fixed_tsl(OperatorCallContext context, std::size_t arity)
        {
            if (context.args.size() != arity || context.args[1].kind != WiringArg::Kind::TimeSeries)
            {
                return false;
            }
            const auto *schema = context.args[1].port.schema;
            return schema != nullptr && schema->fixed_size() > 0;
        }

        [[nodiscard]] inline const LiftedKernel *lifted_reduce_tsl_kernel(OperatorCallContext context)
        {
            if (!reduce_ts_is_fixed_tsl(context, 2) || context.args[1].from_variadic_tail)
            {
                return nullptr;
            }

            auto       &registry   = TypeRegistry::instance();
            const auto *collection = registry.dereference(context.args[1].port.schema);
            if (collection == nullptr || collection->kind != TSTypeKind::TSL || collection->fixed_size() == 0)
            {
                return nullptr;
            }
            const auto *element = registry.dereference(collection->element_ts());
            if (element == nullptr || element->kind != TSTypeKind::TS)
            {
                return nullptr;
            }

            const WiredFn *func = context.scalar_as<WiredFn>("func");
            if (func == nullptr) { return nullptr; }

            std::array<const TSValueTypeMetaData *, 2> input_schemas{element, element};
            const LiftedKernel *kernel =
                resolve_lifted_kernel_for_schemas(*func,
                                                  std::span<const TSValueTypeMetaData *const>{input_schemas.data(),
                                                                                              input_schemas.size()},
                                                  element);
            if (kernel == nullptr || kernel->arity != 2 || !kernel->associative || !kernel->has_identity())
            {
                return nullptr;
            }

            if (!time_series_schema_equivalent(kernel->input_schema(0), element) ||
                !time_series_schema_equivalent(kernel->input_schema(1), element) ||
                !time_series_schema_equivalent(kernel->output_schema(), element))
            {
                return nullptr;
            }
            return kernel;
        }

        [[nodiscard]] inline WiringPortRef wire_lifted_reduce_tsl(Wiring &w,
                                                                  const WiredFn &func,
                                                                  const LiftedKernel *kernel,
                                                                  const WiringPortRef &ts)
        {
            if (kernel == nullptr || !kernel->valid() || !kernel->has_identity())
            {
                throw std::invalid_argument("reduce: lifted fast path requires a lifted function with an identity");
            }

            auto       &registry   = TypeRegistry::instance();
            const auto *collection = registry.dereference(ts.schema);
            if (collection == nullptr || collection->kind != TSTypeKind::TSL || collection->fixed_size() == 0)
            {
                throw std::invalid_argument("reduce: lifted fast path requires a fixed-size TSL input");
            }
            const auto *element = registry.dereference(collection->element_ts());
            if (element == nullptr || element->kind != TSTypeKind::TS)
            {
                throw std::invalid_argument("reduce: lifted fast path requires scalar TS elements");
            }

            const auto *input_schema = registry.un_named_tsb({{"ts", ts.schema}});
            const auto *output_schema = element;
            std::array<WiringPortRef, 1> inputs{ts};

            NodeTypeMetaData node_schema;
            node_schema.display_name = "reduce_lifted";
            node_schema.input_schema = input_schema;
            node_schema.output_schema = output_schema;
            node_schema.node_kind = NodeKind::Compute;
            node_schema.valid_inputs = std::vector<std::size_t>{0};

            NodeCallbacks callbacks;
            callbacks.evaluate = [kernel](const NodeView &view, DateTime evaluation_time) {
                auto input = view.input(evaluation_time);
                auto root = input.as_bundle();
                auto ts_field = root.field("ts");
                auto list = ts_field.as_list();

                Value accumulator = kernel->identity_value();
                for (std::size_t i = 0; i < list.size(); ++i)
                {
                    auto item = list[i];
                    if (!item.valid()) { continue; }

                    std::array<ValueView, 2> args{accumulator.view(), item.value()};
                    accumulator = kernel->eval(std::span<const ValueView>{args.data(), args.size()});
                }

                auto output = view.output(evaluation_time);
                auto mutation = output.begin_mutation(evaluation_time);
                if (!mutation.copy_value_from(accumulator.view()))
                {
                    throw std::logic_error("reduce: lifted fast path failed to copy the result");
                }
            };

            NodeBuilder builder = NodeBuilder::native(std::move(node_schema), std::move(callbacks));
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                input_schema, std::span<const WiringPortRef>{inputs.data(), inputs.size()}));

            WiringPortRef out = w.add_node(std::type_index(typeid(lifted_reduce_tsl_node_tag)),
                                           std::move(builder),
                                           std::span<const WiringPortRef>{inputs.data(), inputs.size()},
                                           Value{func});
            return out;
        }

        inline void resolve_lifted_reduce_tsl_output(ResolutionMap &resolution, OperatorCallContext context)
        {
            const LiftedKernel *kernel = lifted_reduce_tsl_kernel(context);
            if (kernel == nullptr) { return; }
            bind_graph_output(resolution, kernel->output_schema());
        }

        struct reduce_lifted_tsl
        {
            static constexpr auto name = "reduce_lifted_tsl";

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_lifted_reduce_tsl_output(resolution, context);
            }

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return lifted_reduce_tsl_kernel(context) != nullptr;
            }

            static auto compose(Wiring &w,
                                Scalar<"func", WiredFn> func,
                                NamedPort<"ts", TSL<TS<ScalarVar<"T">>>> ts)
            {
                WiringPortRef ts_ref = ts.erased();
                auto &registry = TypeRegistry::instance();
                const auto *collection = registry.dereference(ts_ref.schema);
                if (collection == nullptr || collection->kind != TSTypeKind::TSL)
                {
                    throw std::invalid_argument("reduce: lifted fast path requires a fixed-size TSL input");
                }
                const TSValueTypeMetaData *element = collection->element_ts();
                std::array<const TSValueTypeMetaData *, 2> input_schemas{element, element};
                const LiftedKernel *kernel =
                    resolve_lifted_kernel_for_schemas(func.value(),
                                                      std::span<const TSValueTypeMetaData *const>{
                                                          input_schemas.data(), input_schemas.size()},
                                                      element);
                return wire_lifted_reduce_tsl(w, func.value(), kernel, ts_ref);
            }
        };

        /**
         * The shared fixed-TSL reduction wiring, mirroring Python ``_reduce_tsl``:
         * every leaf is ``default(ts[i], zero)`` — an element that has not ticked
         * yet counts as ``zero`` — then the linear/tree layout combines the
         * leaves. ``default`` is dispatched through the registry at the resolved
         * element schema.
         */
        [[nodiscard]] inline WiringPortRef reduce_tsl_wire(Wiring &w, const WiredFn &combiner,
                                                           const WiringPortRef &ts, const WiringPortRef &zero)
        {
            if (!combiner.valid())
            {
                throw std::invalid_argument("reduce: 'func' must be a wirable function (fn<X>())");
            }
            if (combiner.arity != 2 || !combiner.has_output)
            {
                throw std::invalid_argument(
                    "reduce: the combiner must take (lhs, rhs) time-series inputs and produce an output");
            }

            const TSValueTypeMetaData *schema  = ts.schema;
            const TSValueTypeMetaData *element = schema->element_ts();
            const std::size_t          size    = schema->fixed_size();

            const auto ts_arg = [](WiringPortRef ref) {
                WiringArg arg;
                arg.kind = WiringArg::Kind::TimeSeries;
                arg.port = std::move(ref);
                return arg;
            };

            std::vector<WiringPortRef> elements;
            elements.reserve(size);
            for (std::size_t i = 0; i < size; ++i)
            {
                const std::array<WiringArg, 2> leaf_args{
                    ts_arg(subgraph_wiring_detail::tsl_element_ref(ts, i, element)),
                    ts_arg(zero),
                };
                elements.push_back(
                    wire_operator(w, "default", {leaf_args.data(), leaf_args.size()}, true).output.erased());
            }
            return operator_impl_detail::reduce_layout(w, combiner, std::move(elements));
        }

        /**
         * Raw variadic reduction: no derived zero and no default() leaves. This
         * is used when a graph overload passes a VarIn tail to reduce_ directly;
         * the dispatcher has packed that tail into a structural TSL but tagged
         * the WiringArg so ordinary collection reduce keeps its zero semantics.
         */
        [[nodiscard]] inline WiringPortRef reduce_variadic_tsl_wire(Wiring &w, const WiredFn &combiner,
                                                                    const WiringPortRef &ts)
        {
            if (!combiner.valid())
            {
                throw std::invalid_argument("reduce: 'func' must be a wirable function (fn<X>())");
            }
            if (combiner.arity != 2 || !combiner.has_output)
            {
                throw std::invalid_argument(
                    "reduce: the combiner must take (lhs, rhs) time-series inputs and produce an output");
            }

            const TSValueTypeMetaData *schema  = ts.schema;
            const TSValueTypeMetaData *element = schema->element_ts();
            const std::size_t          size    = schema->fixed_size();

            std::vector<WiringPortRef> elements;
            elements.reserve(size);
            for (std::size_t i = 0; i < size; ++i)
            {
                elements.push_back(subgraph_wiring_detail::tsl_element_ref(ts, i, element));
            }
            return operator_impl_detail::reduce_layout(w, combiner, std::move(elements));
        }

        inline void resolve_reduce_tsl_output(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (resolution.find_ts("__out__") != nullptr ||
                context.args.size() < 2 ||
                context.args[1].kind != WiringArg::Kind::TimeSeries)
            {
                return;
            }
            const auto *schema = TypeRegistry::instance().dereference(context.args[1].port.schema);
            if (schema == nullptr || schema->kind != TSTypeKind::TSL) { return; }
            bind_graph_output(resolution, schema->element_ts(), "V");
        }

        struct reduce_variadic_tsl
        {
            static constexpr auto name = "reduce_variadic_tsl";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return reduce_ts_is_fixed_tsl(context, 2) && context.args[1].from_variadic_tail;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_reduce_tsl_output(resolution, context);
            }

            static WiringPortRef compose(Wiring &w, Scalar<"func", WiredFn> func,
                                         NamedPort<"ts", TSL<TsVar<"V">>> ts)
            {
                return reduce_variadic_tsl_wire(w, func.value(), ts.erased());
            }
        };

        /**
         * ``reduce(func, ts: TSL[V, SIZE]) -> V`` — the zero is derived from the
         * operation: ``zero(item_tp, func)`` (op-aware: ``add_`` -> 0, ``mul_``
         * -> 1, ...). A combiner with no registered zero (e.g. a custom
         * sub-graph) is a wiring-time error, like Python's ``KeyError`` — use
         * the explicit-zero arity instead.
         */
        struct reduce_tsl
        {
            static constexpr auto name = "reduce_tsl";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return reduce_ts_is_fixed_tsl(context, 2) && !context.args[1].from_variadic_tail;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_reduce_tsl_output(resolution, context);
            }

            static WiringPortRef compose(Wiring &w, Scalar<"func", WiredFn> func,
                                         NamedPort<"ts", TSL<TsVar<"V">>> ts)
            {
                const TSValueTypeMetaData *element = ts.erased().schema->element_ts();

                const std::array<WiringArg, 1> zero_args{operator_dispatch_detail::make_wiring_arg(func)};
                const WiringPortRef            zero =
                    wire_operator(w, "zero", {zero_args.data(), zero_args.size()}, true, element).output.erased();

                return reduce_tsl_wire(w, func.value(), ts.erased(), zero);
            }
        };

        /**
         * ``reduce(func, ts: TSL[V, SIZE], zero) -> V`` — the explicit-zero
         * arity (Python's third parameter): ``zero`` is a plain value wired as
         * ``const(zero)`` at the element schema. Required when the combiner has
         * no registered ``zero`` overload (e.g. a custom sub-graph).
         */
        struct reduce_tsl_zero
        {
            static constexpr auto name = "reduce_tsl_zero";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return reduce_ts_is_fixed_tsl(context, 3);
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_reduce_tsl_output(resolution, context);
            }

            static WiringPortRef compose(Wiring &w, Scalar<"func", WiredFn> func,
                                         NamedPort<"ts", TSL<TsVar<"V">>> ts,
                                         Scalar<"zero", ScalarVar<"Z">> zero_value)
            {
                const TSValueTypeMetaData *element = ts.erased().schema->element_ts();

                WiringArg zero_arg;
                zero_arg.kind         = WiringArg::Kind::Scalar;
                zero_arg.scalar_value = Value{zero_value.value()};
                zero_arg.scalar_meta  = zero_arg.scalar_value.schema();
                const WiringPortRef zero =
                    wire_operator(w, "const", {&zero_arg, 1}, true, element).output.erased();

                return reduce_tsl_wire(w, func.value(), ts.erased(), zero);
            }
        };
    }  // namespace higher_order_impl_detail

    namespace higher_order_impl_detail
    {
        struct reduce_tsd_node_tag
        {
        };

        /**
         * The dynamic-TSD reduce wiring core (see *Nested Graphs > reduce over
         * dynamic TSD*): compile the binary combiner once, add ONE reduce node
         * whose outer inputs are ``[ts (TSD), zero (element)]`` and whose
         * forwarding output publishes the root aggregate.
         */
        [[nodiscard]] inline WiringPortRef wire_reduce_tsd(Wiring &w, const Scalar<"func", WiredFn> &func,
                                                           WiringPortRef ts, WiringPortRef zero)
        {
            const WiredFn &combiner = func.value();
            if (!combiner.valid() || combiner.arity != 2 || !combiner.has_output)
            {
                throw std::invalid_argument(
                    "reduce: 'func' must be a wirable (lhs, rhs) -> value function (fn<X>())");
            }

            auto       &registry   = TypeRegistry::instance();
            const auto *tsd_schema = registry.dereference(ts.schema);
            if (tsd_schema == nullptr || tsd_schema->kind != TSTypeKind::TSD)
            {
                throw std::invalid_argument("reduce: the collection input must be a TSD");
            }
            const auto *element = tsd_schema->element_ts();

            const std::array<const TSValueTypeMetaData *, 2> schemas{element, element};
            CompiledSubGraph combiner_graph = combiner.compile({schemas.data(), schemas.size()});
            if (combiner_graph.output_schema == nullptr || !combiner_graph.output_binding.has_value())
            {
                throw std::invalid_argument("reduce: the combiner must produce an output");
            }
            if (combiner_graph.output_binding->kind != NestedGraphOutputBinding::Kind::ChildOutput)
            {
                throw std::invalid_argument(
                    "reduce: pass-through combiner outputs are not supported by dynamic TSD reduce yet");
            }
            if (!time_series_schema_equivalent(registry.dereference(combiner_graph.output_schema),
                                               registry.dereference(element)))
            {
                throw std::invalid_argument(
                    "reduce: the combiner output schema must match the collection's element schema");
            }

            ReduceNodeSpec spec;
            spec.child.graph_builder  = std::move(combiner_graph.graph_builder);
            spec.child.input_bindings = std::move(combiner_graph.input_bindings);
            spec.child.output_binding = combiner_graph.output_binding;

            const auto *input_schema =
                TypeRegistry::instance().un_named_tsb({{"ts", ts.schema}, {"zero", zero.schema}});
            const auto *output_schema = element;

            const std::array<WiringPortRef, 2> inputs{std::move(ts), std::move(zero)};

            WiringNodeSchema node_schema;
            node_schema.input  = input_schema;
            node_schema.output = output_schema;

            WiringPortRef out = w.add_node(
                std::type_index(typeid(reduce_tsd_node_tag)), node_schema,
                std::span<const WiringPortRef>{inputs.data(), inputs.size()}, Value{combiner},
                [&]() {
                    NodeTypeMetaData meta;
                    meta.display_name  = "reduce";
                    meta.input_schema  = input_schema;
                    meta.output_schema = output_schema;

                    NodeBuilder builder = reduce_node(std::move(meta), std::move(spec));
                    builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                        input_schema, std::span<const WiringPortRef>{inputs.data(), inputs.size()}));
                    return builder;
                });
            return out;
        }

        inline void resolve_reduce_tsd_output(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (resolution.find_ts("__out__") != nullptr ||
                context.args.size() < 2 ||
                context.args[1].kind != WiringArg::Kind::TimeSeries)
            {
                return;
            }
            const auto *schema = TypeRegistry::instance().dereference(context.args[1].port.schema);
            if (schema == nullptr || schema->kind != TSTypeKind::TSD) { return; }
            bind_graph_output(resolution, schema->element_ts(), "V");
        }

        [[nodiscard]] inline bool reduce_ts_is_tsd(OperatorCallContext context, std::size_t expected_args)
        {
            if (context.args.size() != expected_args) { return false; }
            if (context.args.size() < 2 || context.args[1].kind != WiringArg::Kind::TimeSeries) { return false; }
            const auto *schema = TypeRegistry::instance().dereference(context.args[1].port.schema);
            return schema != nullptr && schema->kind == TSTypeKind::TSD;
        }

        /** ``reduce(func, ts: TSD[K, V]) -> V`` — the zero is ``zero(item_tp, func)``. */
        struct reduce_tsd
        {
            static constexpr auto name = "reduce_tsd";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return reduce_ts_is_tsd(context, 2);
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_reduce_tsd_output(resolution, context);
            }

            static WiringPortRef compose(Wiring &w, Scalar<"func", WiredFn> func,
                                         NamedPort<"ts", TSD<ScalarVar<"K">, TsVar<"V">>> ts)
            {
                const auto *element =
                    TypeRegistry::instance().dereference(ts.erased().schema)->element_ts();

                const std::array<WiringArg, 1> zero_args{operator_dispatch_detail::make_wiring_arg(func)};
                const WiringPortRef            zero =
                    wire_operator(w, "zero", {zero_args.data(), zero_args.size()}, true, element).output.erased();

                return wire_reduce_tsd(w, func, ts.erased(), zero);
            }
        };

        /** ``reduce(func, ts: TSD[K, V], zero) -> V`` — the explicit-zero arity. */
        struct reduce_tsd_zero
        {
            static constexpr auto name = "reduce_tsd_zero";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return reduce_ts_is_tsd(context, 3);
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_reduce_tsd_output(resolution, context);
            }

            static WiringPortRef compose(Wiring &w, Scalar<"func", WiredFn> func,
                                         NamedPort<"ts", TSD<ScalarVar<"K">, TsVar<"V">>> ts,
                                         Scalar<"zero", ScalarVar<"Z">> zero_value)
            {
                const auto *element =
                    TypeRegistry::instance().dereference(ts.erased().schema)->element_ts();

                WiringArg zero_arg;
                zero_arg.kind         = WiringArg::Kind::Scalar;
                zero_arg.scalar_value = Value{zero_value.value()};
                zero_arg.scalar_meta  = zero_arg.scalar_value.schema();
                const WiringPortRef zero =
                    wire_operator(w, "const", {&zero_arg, 1}, true, element).output.erased();

                return wire_reduce_tsd(w, func, ts.erased(), zero);
            }
        };

        struct switch_node_tag
        {
        };

        [[nodiscard]] inline const TSValueTypeMetaData *switch_branch_output_child_schema(
            const TSValueTypeMetaData &schema,
            std::size_t index)
        {
            switch (schema.kind)
            {
                case TSTypeKind::TSB:
                    if (index >= schema.field_count())
                    {
                        throw std::out_of_range("switch_: branch output source path is out of range for TSB");
                    }
                    return schema.fields()[index].type;

                case TSTypeKind::TSL:
                    if (schema.fixed_size() == 0)
                    {
                        throw std::invalid_argument(
                            "switch_: branch output source path requires fixed-size TSL prefixes");
                    }
                    if (index >= schema.fixed_size())
                    {
                        throw std::out_of_range("switch_: branch output source path is out of range for TSL");
                    }
                    return schema.element_ts();

                default:
                    throw std::invalid_argument(
                        "switch_: branch output source path can only traverse TSB or fixed-size TSL");
            }
        }

        [[nodiscard]] inline std::size_t switch_branch_output_child_count(
            const TSValueTypeMetaData &schema)
        {
            switch (schema.kind)
            {
                case TSTypeKind::TSB:
                    return schema.field_count();

                case TSTypeKind::TSL:
                    if (schema.fixed_size() == 0)
                    {
                        throw std::invalid_argument(
                            "switch_: branch output source path requires fixed-size TSL prefixes");
                    }
                    return schema.fixed_size();

                default:
                    throw std::invalid_argument(
                        "switch_: branch output source path can only traverse TSB or fixed-size TSL");
            }
        }

        [[nodiscard]] inline TSEndpointSchema switch_branch_output_endpoint_schema_for(
            const TSValueTypeMetaData *schema,
            const std::vector<std::size_t> &source_path,
            std::size_t depth = 0)
        {
            if (schema == nullptr)
            {
                throw std::invalid_argument("switch_: branch output binding requires an output schema");
            }
            if (depth == source_path.size()) { return TSEndpointSchema::peered(schema); }

            const auto selected = source_path[depth];
            const auto count    = switch_branch_output_child_count(*schema);
            if (selected >= count)
            {
                throw std::out_of_range("switch_: branch output source path is out of range");
            }

            std::vector<TSEndpointSchema> children;
            children.reserve(count);
            for (std::size_t index = 0; index < count; ++index)
            {
                const auto *child_schema = switch_branch_output_child_schema(*schema, index);
                children.push_back(index == selected
                                       ? switch_branch_output_endpoint_schema_for(child_schema, source_path, depth + 1)
                                       : TSEndpointSchema::owned(child_schema));
            }
            return TSEndpointSchema::non_peered(schema, std::move(children));
        }

        /**
         * Compile one branch over the outer time-series slots. ``named_slots``
         * are the keyword arguments as ``(name, outer slot)`` pairs — resolved
         * per branch against ITS parameter names (branches may name and order
         * their parameters differently, like Python). A branch consumes the key
         * only when its first parameter is named ``key``.
         */
        [[nodiscard]] inline SingleNestedGraphNodeSpec compile_switch_branch(
            const WiredFn &branch,
            const TSValueTypeMetaData *key_schema,
            std::span<const TSValueTypeMetaData *const> slot_schemas,
            std::size_t positional_count,
            std::span<const std::pair<std::string, std::size_t>> named_slots,
            const TSValueTypeMetaData *&output_schema)
        {
            if (!branch.valid())
            {
                throw std::invalid_argument("switch_: every branch must be a wirable function (fn<X>())");
            }

            std::vector<std::size_t> positional_slots(positional_count);
            for (std::size_t i = 0; i < positional_count; ++i) { positional_slots[i] = i; }

            const auto bound_slots = bind_wired_fn_args<std::size_t>(
                "switch_", branch, {positional_slots.data(), positional_slots.size()}, named_slots);

            std::vector<const TSValueTypeMetaData *> schemas;
            schemas.reserve(branch.arity);
            if (bound_slots.takes_leading_key) { schemas.push_back(key_schema); }
            for (const std::size_t slot : bound_slots.ordered) { schemas.push_back(slot_schemas[slot]); }

            CompiledSubGraph compiled = branch.compile({schemas.data(), schemas.size()});

            if (compiled.output_schema == nullptr)
            {
                throw std::invalid_argument("switch_: every branch must produce an output");
            }
            if (output_schema == nullptr) { output_schema = compiled.output_schema; }
            else if (!time_series_schema_equivalent(output_schema, compiled.output_schema))
            {
                throw std::invalid_argument("switch_: all branches must produce the same output schema");
            }

            SingleNestedGraphNodeSpec spec;
            spec.graph_builder  = std::move(compiled.graph_builder);
            spec.input_bindings = std::move(compiled.input_bindings);
            spec.output_binding = compiled.output_binding;
            if (spec.output_binding.has_value() &&
                spec.output_binding->kind == NestedGraphOutputBinding::Kind::ChildOutput)
            {
                const NestedGraphEndpoint &source = spec.output_binding->source;
                NodeBuilder &terminal = spec.graph_builder.node_at(source.node);
                const NodeTypeMetaData *terminal_meta = terminal.binding().type_meta;
                terminal.output_endpoint(
                    switch_branch_output_endpoint_schema_for(terminal_meta != nullptr
                                                                 ? terminal_meta->output_schema
                                                                 : nullptr,
                                                             source.path));
            }

            // Re-target boundary ordinals onto the outer input root: ordinal 0
            // is the key when the branch consumes it; every other parameter
            // maps through its resolved outer slot (+1 past the key input).
            const std::size_t offset = bound_slots.takes_leading_key ? 1 : 0;
            for (NestedGraphInputBinding &binding : spec.input_bindings)
            {
                const std::size_t ordinal = binding.source_path[0];
                if (bound_slots.takes_leading_key && ordinal == 0) { binding.source_path[0] = 0; }
                else { binding.source_path[0] = 1 + bound_slots.ordered[ordinal - offset]; }
            }
            return spec;
        }

        /** The shared switch wiring: compile every branch, then add one switch node. */
        [[nodiscard]] inline WiringPortRef wire_switch(Wiring &w, WiringPortRef key, const SwitchCases &cases,
                                                       std::vector<WiringPortRef> ts,
                                                       std::vector<std::pair<std::string, WiringPortRef>> kwargs)
        {
            if (cases.cases.empty() && !cases.default_branch.has_value())
            {
                throw std::invalid_argument("switch_: requires at least one case");
            }

            // Outer time-series slots: positional args, then keyword args in
            // call order. Branches resolve their parameters onto these slots
            // (keywords by each branch's own parameter names).
            const std::size_t positional_count = ts.size();
            std::vector<std::pair<std::string, std::size_t>> named_slots;
            named_slots.reserve(kwargs.size());
            for (std::size_t i = 0; i < kwargs.size(); ++i)
            {
                named_slots.emplace_back(kwargs[i].first, positional_count + i);
                ts.push_back(kwargs[i].second);
            }

            std::vector<const TSValueTypeMetaData *> ts_schemas;
            ts_schemas.reserve(ts.size());
            for (const WiringPortRef &port : ts) { ts_schemas.push_back(port.schema); }

            const TSValueTypeMetaData *output_schema = nullptr;
            SwitchNodeSpec             spec;
            spec.reload_on_ticked = cases.reload_on_ticked;
            spec.branches.reserve(cases.cases.size());
            for (const SwitchCase &entry : cases.cases)
            {
                spec.branches.push_back(SwitchBranch{
                    .key  = entry.key,
                    .spec = compile_switch_branch(entry.branch, key.schema,
                                                  {ts_schemas.data(), ts_schemas.size()}, positional_count,
                                                  {named_slots.data(), named_slots.size()}, output_schema),
                });
            }
            if (cases.default_branch.has_value())
            {
                spec.default_branch = compile_switch_branch(*cases.default_branch, key.schema,
                                                            {ts_schemas.data(), ts_schemas.size()},
                                                            positional_count,
                                                            {named_slots.data(), named_slots.size()},
                                                            output_schema);
            }

            // Outer node inputs: [key, ts...].
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
            fields.reserve(1 + ts.size());
            fields.emplace_back("key", key.schema);
            for (std::size_t i = 0; i < ts_schemas.size(); ++i)
            {
                fields.emplace_back(std::to_string(i), ts_schemas[i]);
            }
            const auto *input_schema = TypeRegistry::instance().un_named_tsb(fields);

            std::vector<WiringPortRef> inputs;
            inputs.reserve(1 + ts.size());
            inputs.push_back(std::move(key));
            for (WiringPortRef &port : ts) { inputs.push_back(std::move(port)); }

            WiringNodeSchema node_schema;
            node_schema.input  = input_schema;
            node_schema.output = output_schema;

            WiringPortRef out = w.add_node(
                std::type_index(typeid(switch_node_tag)), node_schema,
                std::span<const WiringPortRef>{inputs.data(), inputs.size()}, Value{cases},
                [&]() {
                    NodeTypeMetaData meta;
                    meta.display_name  = "switch_";
                    meta.input_schema  = input_schema;
                    meta.output_schema = output_schema;

                    NodeBuilder builder = switch_node(std::move(meta), std::move(spec));
                    builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                        input_schema, std::span<const WiringPortRef>{inputs.data(), inputs.size()}));
                    return builder;
                });
            return out;
        }

        /**
         * The output schema is whatever the branches compute — discover it for
         * the resolver by compiling the first branch (a side-effect-free
         * wiring-time computation), unless an explicit output schema already
         * bound ``O``.
         */
        inline void resolve_switch_output(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (resolution.find_ts("__out__") != nullptr) { return; }

            const SwitchCases *cases = context.scalar_as<SwitchCases>("cases");
            if (cases == nullptr) { return; }
            const WiredFn *branch = !cases->cases.empty()
                                        ? &cases->cases.front().branch
                                        : (cases->default_branch.has_value() ? &*cases->default_branch : nullptr);
            if (branch == nullptr) { return; }

            if (context.args.empty() || context.args[0].kind != WiringArg::Kind::TimeSeries) { return; }
            const TSValueTypeMetaData *key_schema = context.args[0].port.schema;

            std::vector<const TSValueTypeMetaData *> slot_schemas;
            for (std::size_t i = 2; i < context.args.size(); ++i)
            {
                if (context.args[i].kind != WiringArg::Kind::TimeSeries) { return; }
                slot_schemas.push_back(context.args[i].port.schema);
            }
            const std::size_t positional_count = slot_schemas.size();
            std::vector<std::pair<std::string, std::size_t>> named_slots;
            for (const auto &[name, port] : context.kwargs)
            {
                named_slots.emplace_back(name, slot_schemas.size());
                slot_schemas.push_back(port.schema);
            }

            try
            {
                const TSValueTypeMetaData *output_schema = nullptr;
                (void)compile_switch_branch(*branch, key_schema, {slot_schemas.data(), slot_schemas.size()},
                                            positional_count, {named_slots.data(), named_slots.size()},
                                            output_schema);
                bind_graph_output(resolution, output_schema, "O");
            }
            catch (...)
            {
                // Leave unresolved; the real wiring path reports the error.
            }
        }

        /** ``switch_(key, cases, *ts, **kwargs)`` — keywords resolve per branch. */
        struct switch_impl
        {
            static constexpr auto name = "switch_impl";

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_switch_output(resolution, context);
            }

            static WiringPortRef compose(Wiring &w, NamedPort<"key", TS<ScalarVar<"K">>> key,
                                         Scalar<"cases", SwitchCases> cases, VarIn<"ts", TsVar<"TS">> ts,
                                         VarKwIn<"kwargs"> kwargs)
            {
                return wire_switch(w, key.erased(), cases.value(),
                                   std::vector<WiringPortRef>{ts.begin(), ts.end()},
                                   std::vector<std::pair<std::string, WiringPortRef>>{kwargs.begin(),
                                                                                      kwargs.end()});
            }
        };
    }  // namespace higher_order_impl_detail

    namespace higher_order_impl_detail
    {
        struct map_node_tag
        {
        };
        struct mesh_node_tag
        {
        };
        struct lifted_map_tsl_node_tag
        {
        };

        /**
         * Classify the ``map_`` time-series arguments, Python-style: every
         * TSD argument is **multiplexed** (the key types must all agree —
         * the live key set is their union); everything else broadcasts whole.
         * The argument list has already been resolved onto ``func``'s
         * parameter order, so argument ``i`` feeds ``func`` parameter ``i``
         * (after the optional key).
         */
        struct MapArgClassification
        {
            std::vector<bool>                        is_multiplexed{};   ///< per ts arg (call order)
            std::vector<bool>                        exclude_from_keys{};///< per ts arg: ``no_key`` tag
            std::vector<const TSValueTypeMetaData *> child_schemas{};    ///< per ts arg: element or whole schema
            const ValueTypeMetaData                 *key_meta{nullptr};
        };

        /**
         * Classification honours the wiring-time argument tags (Python's
         * wrappers): ``pass_through`` forces broadcast whatever the kind;
         * ``no_key`` keeps the TSD multiplexed but excludes it from key-set
         * inference.
         */
        [[nodiscard]] inline MapArgClassification classify_map_args(
            std::span<const TSValueTypeMetaData *const> ts_schemas,
            std::span<const std::uint8_t> arg_tags)
        {
            auto &registry = TypeRegistry::instance();

            MapArgClassification result;
            result.is_multiplexed.reserve(ts_schemas.size());
            result.exclude_from_keys.reserve(ts_schemas.size());
            result.child_schemas.reserve(ts_schemas.size());
            for (std::size_t i = 0; i < ts_schemas.size(); ++i)
            {
                const auto tag = i < arg_tags.size() ? static_cast<WiringPortRef::ArgTag>(arg_tags[i])
                                                     : WiringPortRef::ArgTag::None;
                const auto *deref = registry.dereference(ts_schemas[i]);
                if (tag != WiringPortRef::ArgTag::PassThrough && deref != nullptr &&
                    deref->kind == TSTypeKind::TSD)
                {
                    if (result.key_meta == nullptr) { result.key_meta = deref->key_type(); }
                    else if (deref->key_type() != result.key_meta)
                    {
                        throw std::invalid_argument(
                            "map_: every multiplexed TSD must share the same key type");
                    }
                    result.is_multiplexed.push_back(true);
                    result.exclude_from_keys.push_back(tag == WiringPortRef::ArgTag::NoKey);
                    result.child_schemas.push_back(deref->element_ts());
                }
                else
                {
                    if (tag == WiringPortRef::ArgTag::NoKey)
                    {
                        throw std::invalid_argument("map_: 'no_key' applies to multiplexed TSD inputs only");
                    }
                    result.is_multiplexed.push_back(false);
                    result.exclude_from_keys.push_back(false);
                    result.child_schemas.push_back(ts_schemas[i]);
                }
            }
            if (result.key_meta == nullptr)
            {
                throw std::invalid_argument("map_: at least one input must be a multiplexed TSD");
            }
            return result;
        }

        /**
         * Compile the ``map_`` child template over the classified time-series
         * args, deriving the boundary-arg source table and the ``TSD<K, OUT>``
         * output schema. The caller has already resolved name-based key
         * consumption, so this helper sees a schema count that either matches
         * ``func`` directly or has one extra slot for the key.
         */
        [[nodiscard]] inline MapNodeSpec compile_map_child(const WiredFn &func,
                                                           std::span<const TSValueTypeMetaData *const> ts_schemas,
                                                           std::span<const std::uint8_t> arg_tags,
                                                           const TSValueTypeMetaData *&output_schema)
        {
            if (!func.valid())
            {
                throw std::invalid_argument("map_: 'func' must be a wirable function (fn<X>())");
            }

            auto &registry = TypeRegistry::instance();

            const MapArgClassification classified = classify_map_args(ts_schemas, arg_tags);

            const std::size_t base_arity = ts_schemas.size();
            const bool        takes_key  = func.arity == base_arity + 1;
            if (!takes_key && func.arity != base_arity)
            {
                throw std::invalid_argument(
                    "map_: 'func' must take one parameter per time-series argument, with an optional key "
                    "already resolved by name");
            }

            const auto *key_ts = registry.ts(classified.key_meta);

            std::vector<const TSValueTypeMetaData *> schemas;
            schemas.reserve(func.arity);
            if (takes_key) { schemas.push_back(key_ts); }
            schemas.insert(schemas.end(), classified.child_schemas.begin(), classified.child_schemas.end());

            CompiledSubGraph compiled = func.compile({schemas.data(), schemas.size()});
            if (compiled.output_schema == nullptr)
            {
                throw std::invalid_argument("map_: 'func' must produce an output (sink maps are not supported yet)");
            }
            if (!compiled.output_binding.has_value() ||
                compiled.output_binding->kind != NestedGraphOutputBinding::Kind::ChildOutput ||
                !compiled.output_binding->source.path.empty())
            {
                throw std::invalid_argument(
                    "map_: the function output must be a whole node output (pass-through and sub-path outputs "
                    "are not supported yet)");
            }
            const auto *out = compiled.output_schema;
            if (registry.dereference(out) != out)
            {
                throw std::invalid_argument(
                    "map_: a reference-valued function output cannot back a TSD element");
            }
            if (out->kind == TSTypeKind::TSD || (out->kind == TSTypeKind::TSL && out->fixed_size() == 0))
            {
                throw std::invalid_argument(
                    "map_: the function output cannot be embedded as a TSD element yet (TSD / dynamic-TSL "
                    "elements are a recorded deferral)");
            }
            output_schema = registry.tsd(classified.key_meta, out);

            MapNodeSpec spec;
            spec.child.graph_builder  = std::move(compiled.graph_builder);
            spec.child.input_bindings = std::move(compiled.input_bindings);
            spec.child.output_binding = compiled.output_binding;
            spec.key_output_schema    = takes_key ? key_ts : nullptr;

            // The design intent: every key has a REAL element instantiated in
            // the parent's owned TSD output, and the child's terminal node
            // WRITES THROUGH to it — its output is re-homed as a forwarding
            // endpoint that the map node points at the parent element. No copy.
            spec.child.graph_builder.node_at(spec.child.output_binding->source.node)
                .output_endpoint(TSEndpointSchema::peered(out));

            spec.args.reserve(func.arity);
            if (takes_key) { spec.args.push_back(MapArgSource{.kind = MapArgSourceKind::Key}); }
            for (std::size_t i = 0; i < ts_schemas.size(); ++i)
            {
                if (classified.is_multiplexed[i])
                {
                    spec.args.push_back(MapArgSource{.kind = MapArgSourceKind::Element, .outer_index = i});
                    spec.multiplexed_inputs.push_back(i);
                }
                else
                {
                    spec.args.push_back(MapArgSource{.kind = MapArgSourceKind::OuterInput, .outer_index = i});
                }
            }
            return spec;
        }

        /**
         * The shared map wiring over the **func-parameter-ordered** time-series
         * list (positional + keyword arguments already resolved onto the
         * function's parameters): compile the child template, add one map node.
         */
        [[nodiscard]] inline WiringPortRef wire_map(Wiring &w, const Scalar<"func", WiredFn> &func,
                                                    std::string_view key_arg,
                                                    std::vector<WiringPortRef> ordered,
                                                    std::optional<WiringPortRef> keys = std::nullopt)
        {
            std::vector<const TSValueTypeMetaData *> ts_schemas;
            std::vector<std::uint8_t>                arg_tags;
            ts_schemas.reserve(ordered.size());
            arg_tags.reserve(ordered.size());
            for (const WiringPortRef &port : ordered)
            {
                ts_schemas.push_back(port.schema);
                arg_tags.push_back(static_cast<std::uint8_t>(port.arg_tag));
            }

            const TSValueTypeMetaData *output_schema = nullptr;
            MapNodeSpec spec = compile_map_child(func.value(), {ts_schemas.data(), ts_schemas.size()},
                                                 {arg_tags.data(), arg_tags.size()}, output_schema);

            // The lifecycle key set: an explicit ``__keys__`` when supplied,
            // else derived from the multiplexed dicts — ``keys_(tsd)`` for
            // one, ``union(keys_(tsd)...)`` for several (the Python wiring:
            // ``__keys__ = union(*key_sets)``); ``no_key``-tagged dicts are
            // excluded from the inference. The runtime is always keys-driven;
            // there is no in-node union scan.
            auto &registry = TypeRegistry::instance();
            if (!keys.has_value())
            {
                // Each multiplexed dict contributes its ZERO-COPY key-set
                // projection (no node); several union through the union
                // operator — the Python wiring ``__keys__ = union(*key_sets)``.
                std::vector<WiringArg> key_set_args;
                key_set_args.reserve(spec.multiplexed_inputs.size());
                for (const std::size_t mux_index : spec.multiplexed_inputs)
                {
                    if (static_cast<WiringPortRef::ArgTag>(arg_tags[mux_index]) ==
                        WiringPortRef::ArgTag::NoKey)
                    {
                        continue;
                    }
                    WiringArg key_set_arg;
                    key_set_arg.kind = WiringArg::Kind::TimeSeries;
                    key_set_arg.port = subgraph_wiring_detail::tsd_key_set_ref(ordered[mux_index]);
                    key_set_args.push_back(std::move(key_set_arg));
                }
                if (key_set_args.empty())
                {
                    throw std::invalid_argument(
                        "map_: every multiplexed input is no_key — supply an explicit '__keys__'");
                }
                if (key_set_args.size() == 1) { keys = key_set_args.front().port; }
                else
                {
                    keys = wire_operator(w, "union", {key_set_args.data(), key_set_args.size()}, true)
                               .output.erased();
                }
            }
            if (registry.dereference(keys->schema) != registry.tss(output_schema->key_type()))
            {
                throw std::invalid_argument("map_: '__keys__' must be a TSS of the mapped key type");
            }
            spec.keys_input_index = ordered.size();

            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
            fields.reserve(ts_schemas.size() + 1);
            for (std::size_t i = 0; i < ts_schemas.size(); ++i)
            {
                fields.emplace_back(std::to_string(i), ts_schemas[i]);
            }
            fields.emplace_back("__keys__", keys->schema);
            const auto *input_schema = TypeRegistry::instance().un_named_tsb(fields);

            std::vector<WiringPortRef> inputs = std::move(ordered);
            inputs.push_back(std::move(*keys));

            WiringNodeSchema node_schema;
            node_schema.input  = input_schema;
            node_schema.output = output_schema;

            // The call configuration joins the interning identity: equal
            // inputs with a different function, key-arg name, or argument
            // tags must not dedup to one node. Unlike SwitchCases (a declared
            // operator parameter), MapCallConfig is built here, so register
            // the scalar at the point of use.
            (void)scalar_descriptor<MapCallConfig>::value_meta();
            WiringPortRef out = w.add_node(
                std::type_index(typeid(map_node_tag)), node_schema,
                std::span<const WiringPortRef>{inputs.data(), inputs.size()},
                Value{MapCallConfig{func.value(), Str{key_arg}, arg_tags}},
                [&]() {
                    NodeTypeMetaData meta;
                    meta.display_name  = "map_";
                    meta.input_schema  = input_schema;
                    meta.output_schema = output_schema;

                    NodeBuilder builder = map_node(std::move(meta), std::move(spec));
                    builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                        input_schema, std::span<const WiringPortRef>{inputs.data(), inputs.size()}));
                    return builder;
                });
            return out;
        }

        // The mesh wiring-time scope (the enclosing mesh a mesh_(func)[k] resolves to)
        // lives on the global wiring singleton ``OperatorRegistry`` — see its
        // push_mesh_scope / pop_mesh_scope / resolve_mesh_scope. wire_mesh pushes it
        // around the child compile; the child compiles in a fresh Wiring, so the scope
        // cannot live on a Wiring instance (and the build is single-threaded, so it is a
        // plain global, not a thread-local).
        struct mesh_subscribe_node_tag
        {
        };

        /**
         * ``mesh_`` wiring. Mirrors ``wire_map``: compile the child, derive the key set
         * (``__keys__`` or the union of multiplexed key sets), then add one mesh
         * node. The mesh node owns the same ``TSD<K, OUT>`` output as ``map_``; with
         * no internal ``mesh_(func)[k]`` requests it is observably ``map_``. The
         * spec additionally always carries a per-instance ``TS<K>`` key output
         * (read by the self-context once cross-instance access lands).
         */
        [[nodiscard]] inline WiringPortRef wire_mesh(Wiring &w, const Scalar<"func", WiredFn> &func,
                                                     std::string_view key_arg,
                                                     std::vector<WiringPortRef> ordered,
                                                     std::optional<WiringPortRef> keys = std::nullopt)
        {
            std::vector<const TSValueTypeMetaData *> ts_schemas;
            std::vector<std::uint8_t>                arg_tags;
            ts_schemas.reserve(ordered.size());
            arg_tags.reserve(ordered.size());
            for (const WiringPortRef &port : ordered)
            {
                ts_schemas.push_back(port.schema);
                arg_tags.push_back(static_cast<std::uint8_t>(port.arg_tag));
            }

            // The element type is statically known from the function signature; push it
            // as the mesh scope so a mesh_(func)[k] in the body resolves to this mesh.
            const TSValueTypeMetaData *element_schema = func.value().output_schema();
            if (element_schema == nullptr)
            {
                throw std::invalid_argument(
                    "mesh_: 'func' must have a statically-known output type (an operator with a "
                    "type-var output cannot be a mesh function)");
            }

            const TSValueTypeMetaData *output_schema = nullptr;
            MapNodeSpec                map_spec;
            {
                OperatorRegistry::instance().push_mesh_scope(element_schema, std::string{});
                auto pop = make_scope_exit([] noexcept { OperatorRegistry::instance().pop_mesh_scope(); });
                map_spec = compile_map_child(func.value(), {ts_schemas.data(), ts_schemas.size()},
                                             {arg_tags.data(), arg_tags.size()}, output_schema);
            }

            auto &registry = TypeRegistry::instance();

            // Key-set derivation — identical to map_ (explicit __keys__, else the
            // union of the multiplexed dict key sets).
            if (!keys.has_value())
            {
                std::vector<WiringArg> key_set_args;
                key_set_args.reserve(map_spec.multiplexed_inputs.size());
                for (const std::size_t mux_index : map_spec.multiplexed_inputs)
                {
                    if (static_cast<WiringPortRef::ArgTag>(arg_tags[mux_index]) == WiringPortRef::ArgTag::NoKey)
                    {
                        continue;
                    }
                    WiringArg key_set_arg;
                    key_set_arg.kind = WiringArg::Kind::TimeSeries;
                    key_set_arg.port = subgraph_wiring_detail::tsd_key_set_ref(ordered[mux_index]);
                    key_set_args.push_back(std::move(key_set_arg));
                }
                if (key_set_args.empty())
                {
                    throw std::invalid_argument(
                        "mesh_: every multiplexed input is no_key — supply an explicit '__keys__'");
                }
                if (key_set_args.size() == 1) { keys = key_set_args.front().port; }
                else
                {
                    keys = wire_operator(w, "union", {key_set_args.data(), key_set_args.size()}, true)
                               .output.erased();
                }
            }
            if (registry.dereference(keys->schema) != registry.tss(output_schema->key_type()))
            {
                throw std::invalid_argument("mesh_: '__keys__' must be a TSS of the mapped key type");
            }

            // Transcribe the map child spec into a mesh spec (a superset).
            MeshNodeSpec spec;
            spec.child              = std::move(map_spec.child);
            spec.args               = std::move(map_spec.args);
            spec.multiplexed_inputs = std::move(map_spec.multiplexed_inputs);
            spec.keys_input_index   = ordered.size();
            // Every mesh instance owns a TS<K> key output, independent of whether
            // func takes a key. mesh_subscribe reads the current requester key from
            // the enclosing mesh evaluation context.
            spec.key_output_schema  = registry.ts(output_schema->key_type());

            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
            fields.reserve(ts_schemas.size() + 1);
            for (std::size_t i = 0; i < ts_schemas.size(); ++i)
            {
                fields.emplace_back(std::to_string(i), ts_schemas[i]);
            }
            fields.emplace_back("__keys__", keys->schema);
            const auto *input_schema = registry.un_named_tsb(fields);

            std::vector<WiringPortRef> inputs = std::move(ordered);
            inputs.push_back(std::move(*keys));

            WiringNodeSchema node_schema;
            node_schema.input  = input_schema;
            node_schema.output = output_schema;

            (void)scalar_descriptor<MapCallConfig>::value_meta();
            WiringPortRef out = w.add_node(
                std::type_index(typeid(mesh_node_tag)), node_schema,
                std::span<const WiringPortRef>{inputs.data(), inputs.size()},
                Value{MapCallConfig{func.value(), Str{key_arg}, arg_tags}},
                [&]() {
                    NodeTypeMetaData meta;
                    meta.display_name  = "mesh_";
                    meta.input_schema  = input_schema;
                    meta.output_schema = output_schema;

                    NodeBuilder builder = mesh_node(std::move(meta), std::move(spec));
                    builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                        input_schema, std::span<const WiringPortRef>{inputs.data(), inputs.size()}));
                    return builder;
                });
            return out;
        }

        // ``mesh_(func)[k]`` access — wires a mesh_subscribe node that reads the sibling
        // instance ``[k]``. The output type comes from the enclosing mesh scope (resolved
        // innermost, or by name). The node takes ``item`` (the requested key) and returns
        // the mesh element type; at runtime it forwards ``self[item]``, pausing until that
        // sibling has produced its result this cycle.
        [[nodiscard]] inline WiringPortRef mesh_ref_erased(Wiring &w, const WiringPortRef &key,
                                                           const WiringPortRef &value_placeholder,
                                                           std::string_view name = {})
        {
            const TSValueTypeMetaData *out_schema = OperatorRegistry::instance().resolve_mesh_scope(name);
            if (out_schema == nullptr)
            {
                throw std::logic_error(
                    "mesh_(func)[k] used outside a mesh scope (no enclosing mesh is being wired)");
            }

            // {item, value}: ``item`` is the requested key (wired); ``value`` starts at a
            // never-ticking ``nothing<OUT>`` placeholder and is rebound at runtime to
            // self[item] (forwards it and makes the node reactive to the sibling's ticks).
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields{
                {"item", key.schema}, {"value", out_schema}};
            const auto *input_schema = TypeRegistry::instance().un_named_tsb(fields);

            WiringNodeSchema node_schema;
            node_schema.input  = input_schema;
            node_schema.output = out_schema;

            std::array<WiringPortRef, 2> inputs{key, value_placeholder};
            return w.add_node(
                std::type_index(typeid(mesh_subscribe_node_tag)), node_schema,
                std::span<const WiringPortRef>{inputs.data(), inputs.size()}, Value{},
                [&]() {
                    NodeTypeMetaData meta;
                    meta.display_name  = "mesh_subscribe";
                    meta.input_schema  = input_schema;
                    meta.output_schema = out_schema;
                    NodeBuilder builder = mesh_subscribe_node(std::move(meta));
                    builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                        input_schema, std::span<const WiringPortRef>{inputs.data(), inputs.size()}));
                    return builder;
                });
        }

        struct OrderedMapSchemas
        {
            std::vector<const TSValueTypeMetaData *> schemas{};
            std::vector<std::uint8_t>               arg_tags{};   ///< per schema: WiringPortRef::ArgTag
            bool                                    takes_key{false};
        };

        [[nodiscard]] inline std::optional<OrderedMapSchemas> ordered_map_schemas(OperatorCallContext context,
                                                                                  std::string_view default_key_arg)
        {
            const WiredFn *func = context.scalar_as<WiredFn>("func");
            if (func == nullptr) { return std::nullopt; }

            // Normalized args interleave the operator's own scalars (``func``,
            // the keyword-only ``__key_arg__``) with the time-series tail —
            // only the time-series args are func arguments. Ordering works on
            // the full PORT REFS so the wiring-time argument tags ride along.
            std::vector<WiringPortRef> positional;
            for (const WiringArg &argument : context.args)
            {
                if (argument.kind == WiringArg::Kind::TimeSeries) { positional.push_back(argument.port); }
            }
            std::vector<std::pair<std::string, WiringPortRef>> named;
            named.reserve(context.kwargs.size());
            for (const auto &[name, port] : context.kwargs)
            {
                if (name == "__keys__") { continue; }   // map_'s own argument, not func's
                named.emplace_back(name, port);
            }

            const std::string *key_override = context.scalar_as<Str>("__key_arg__");

            try
            {
                auto bound = bind_wired_fn_args<WiringPortRef>(
                    "map_", *func, {positional.data(), positional.size()}, {named.data(), named.size()},
                    key_override != nullptr ? std::string_view{*key_override} : default_key_arg);

                OrderedMapSchemas ordered;
                ordered.takes_key = bound.takes_leading_key;
                ordered.schemas.reserve(bound.ordered.size());
                ordered.arg_tags.reserve(bound.ordered.size());
                for (const WiringPortRef &ref : bound.ordered)
                {
                    ordered.schemas.push_back(ref.schema);
                    ordered.arg_tags.push_back(static_cast<std::uint8_t>(ref.arg_tag));
                }
                return ordered;
            }
            catch (const std::invalid_argument &)
            {
                return std::nullopt;
            }
        }

        /** Bind the output var ``O`` for the resolver: ``TSD<K, OUT(func)>``. */
        inline void resolve_map_output(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (resolution.find_ts("__out__") != nullptr) { return; }

            const WiredFn *func = context.scalar_as<WiredFn>("func");
            if (func == nullptr) { return; }
            auto ordered = ordered_map_schemas(context, "key");
            if (!ordered.has_value()) { return; }

            try
            {
                const TSValueTypeMetaData *output_schema = nullptr;
                (void)compile_map_child(*func, {ordered->schemas.data(), ordered->schemas.size()},
                                        {ordered->arg_tags.data(), ordered->arg_tags.size()}, output_schema);
                bind_graph_output(resolution, output_schema, "O");
            }
            catch (...)
            {
                // Leave unresolved; the real wiring path reports the error.
            }
        }

        /** The first collection in ``func`` parameter order decides which map kernel applies. */
        /** The first NON-pass-through collection decides the map kernel. */
        [[nodiscard]] inline const TSValueTypeMetaData *first_map_collection(OperatorCallContext context)
        {
            auto &registry = TypeRegistry::instance();
            auto ordered = ordered_map_schemas(context, "key");
            if (!ordered.has_value()) { ordered = ordered_map_schemas(context, "ndx"); }
            if (!ordered.has_value()) { return nullptr; }
            for (std::size_t i = 0; i < ordered->schemas.size(); ++i)
            {
                if (i < ordered->arg_tags.size() &&
                    static_cast<WiringPortRef::ArgTag>(ordered->arg_tags[i]) ==
                        WiringPortRef::ArgTag::PassThrough)
                {
                    continue;   // pass_through never selects the kernel
                }
                const auto *schema = registry.dereference(ordered->schemas[i]);
                if (schema != nullptr &&
                    (schema->kind == TSTypeKind::TSD ||
                     (schema->kind == TSTypeKind::TSL && schema->fixed_size() > 0)))
                {
                    return schema;
                }
            }
            return nullptr;
        }

        /**
         * ``map_(func, *args, **kwargs)`` over TSDs — keyed runtime children,
         * the Python shape: no fixed anchor parameter; positional + keyword
         * arguments resolve onto ``func``'s parameters, every TSD multiplexes
         * (union key set), everything else broadcasts.
         */
        /**
         * Split the ``__keys__`` special out of the collected kwargs — it is
         * an argument of ``map_`` itself (the explicit demultiplexing key
         * set), not of ``func``.
         */
        [[nodiscard]] inline std::optional<WiringPortRef> split_keys_kwarg(
            std::vector<std::pair<std::string, WiringPortRef>> &named)
        {
            for (auto it = named.begin(); it != named.end(); ++it)
            {
                if (it->first == "__keys__")
                {
                    std::optional<WiringPortRef> keys{std::move(it->second)};
                    named.erase(it);
                    return keys;
                }
            }
            return std::nullopt;
        }

        struct map_impl_tsd
        {
            static constexpr auto name = "map_impl";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                const auto *collection = first_map_collection(context);
                return collection != nullptr && collection->kind == TSTypeKind::TSD;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_map_output(resolution, context);
            }

            static std::vector<std::pair<std::string_view, Value>> defaults()
            {
                return {{"__key_arg__", Value{Str{"key"}}}};
            }

            static WiringPortRef compose(Wiring &w, Scalar<"func", WiredFn> func,
                                         VarIn<"args", TsVar<"B">> positional,
                                         Scalar<"__key_arg__", Str> key_arg, VarKwIn<"kwargs"> kwargs)
            {
                const std::vector<WiringPortRef> pos{positional.begin(), positional.end()};
                std::vector<std::pair<std::string, WiringPortRef>> named{kwargs.begin(), kwargs.end()};
                std::optional<WiringPortRef> keys = split_keys_kwarg(named);
                auto bound = bind_wired_fn_args<WiringPortRef>("map_", func.value(),
                                                               {pos.data(), pos.size()},
                                                               {named.data(), named.size()},
                                                               key_arg.value());
                return wire_map(w, func, key_arg.value(), std::move(bound.ordered), std::move(keys));
            }
        };

        /**
         * ``mesh_(func, *args, **kwargs)`` over TSDs. Same call shape as
         * ``map_impl_tsd``; lowers to a mesh node via ``wire_mesh``. With no
         * cross-instance ``mesh_(func)[k]`` requests this is observably ``map_``.
         */
        // mesh output resolution: TSD<K, OUT> where OUT is the function's statically
        // known output schema. Unlike map_, this does NOT compile the child — a
        // mesh_(func)[k] in the body needs the mesh scope (only pushed by wire_mesh),
        // so compiling here would fail. func->output_schema() gives OUT directly.
        inline void resolve_mesh_output(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (resolution.find_ts("__out__") != nullptr) { return; }
            const WiredFn *func = context.scalar_as<WiredFn>("func");
            if (func == nullptr) { return; }
            const TSValueTypeMetaData *element = func->output_schema();
            if (element == nullptr) { return; }
            auto ordered = ordered_map_schemas(context, "key");
            if (!ordered.has_value()) { return; }
            try
            {
                const MapArgClassification classified =
                    classify_map_args({ordered->schemas.data(), ordered->schemas.size()},
                                      {ordered->arg_tags.data(), ordered->arg_tags.size()});
                const auto *output_schema = TypeRegistry::instance().tsd(classified.key_meta, element);
                bind_graph_output(resolution, output_schema, "O");
            }
            catch (...)
            {
                // Leave unresolved; the real wiring path reports the error.
            }
        }

        struct mesh_impl_tsd
        {
            static constexpr auto name = "mesh_impl";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                const auto *collection = first_map_collection(context);
                return collection != nullptr && collection->kind == TSTypeKind::TSD;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_mesh_output(resolution, context);
            }

            static std::vector<std::pair<std::string_view, Value>> defaults()
            {
                return {{"__key_arg__", Value{Str{"key"}}}};
            }

            static WiringPortRef compose(Wiring &w, Scalar<"func", WiredFn> func,
                                         VarIn<"args", TsVar<"B">> positional,
                                         Scalar<"__key_arg__", Str> key_arg, VarKwIn<"kwargs"> kwargs)
            {
                const std::vector<WiringPortRef> pos{positional.begin(), positional.end()};
                std::vector<std::pair<std::string, WiringPortRef>> named{kwargs.begin(), kwargs.end()};
                std::optional<WiringPortRef> keys = split_keys_kwarg(named);
                auto bound = bind_wired_fn_args<WiringPortRef>("mesh_", func.value(),
                                                               {pos.data(), pos.size()},
                                                               {named.data(), named.size()},
                                                               key_arg.value());
                return wire_mesh(w, func, key_arg.value(), std::move(bound.ordered), std::move(keys));
            }
        };
        /**
         * ``map_`` over a fixed-size TSL is a **wiring-time expansion** —
         * Python's ``_map_no_index``: one inline application of ``func`` per
         * index (key = ``const(i)`` when ``func`` takes it first), assembled
         * into a structural TSL output. No runtime node is involved.
         */
        /** A tail arg multiplexes in the TSL form when it is a fixed TSL of the SAME size. */
        [[nodiscard]] inline bool tsl_arg_is_multiplexed(const TSValueTypeMetaData *schema, std::size_t size)
        {
            const auto *deref = TypeRegistry::instance().dereference(schema);
            return deref != nullptr && deref->kind == TSTypeKind::TSL && deref->fixed_size() == size;
        }

        struct LiftedMapTslPlan
        {
            const LiftedKernel *kernel{nullptr};
            const TSValueTypeMetaData *output_schema{nullptr};
            std::vector<bool> multiplexed{};
            std::vector<std::uint8_t> arg_tags{};
            std::size_t size{0};
        };

        [[nodiscard]] inline std::optional<LiftedMapTslPlan> lifted_map_tsl_plan(
            const WiredFn &func,
            std::span<const TSValueTypeMetaData *const> schemas,
            std::span<const std::uint8_t> arg_tags,
            bool takes_key)
        {
            if (takes_key || !func.has_output || func.arity != schemas.size()) { return std::nullopt; }

            auto &registry = TypeRegistry::instance();
            std::size_t size = 0;
            for (std::size_t i = 0; i < schemas.size(); ++i)
            {
                const auto tag = i < arg_tags.size() ? static_cast<WiringPortRef::ArgTag>(arg_tags[i])
                                                     : WiringPortRef::ArgTag::None;
                if (tag == WiringPortRef::ArgTag::NoKey) { return std::nullopt; }
                if (tag == WiringPortRef::ArgTag::PassThrough) { continue; }
                const auto *schema = registry.dereference(schemas[i]);
                if (schema != nullptr && schema->kind == TSTypeKind::TSL && schema->fixed_size() > 0)
                {
                    size = schema->fixed_size();
                    break;
                }
            }
            if (size == 0) { return std::nullopt; }

            LiftedMapTslPlan plan;
            plan.size = size;
            plan.multiplexed.reserve(schemas.size());
            plan.arg_tags.assign(arg_tags.begin(), arg_tags.end());

            std::vector<const TSValueTypeMetaData *> expected_schemas;
            expected_schemas.reserve(schemas.size());
            for (std::size_t i = 0; i < schemas.size(); ++i)
            {
                const auto tag = i < arg_tags.size() ? static_cast<WiringPortRef::ArgTag>(arg_tags[i])
                                                     : WiringPortRef::ArgTag::None;
                const bool multiplexed = tag != WiringPortRef::ArgTag::PassThrough &&
                                         tsl_arg_is_multiplexed(schemas[i], size);
                const TSValueTypeMetaData *expected =
                    multiplexed ? registry.dereference(schemas[i])->element_ts() : schemas[i];
                expected_schemas.push_back(expected);
                plan.multiplexed.push_back(multiplexed);
            }

            const LiftedKernel *kernel =
                resolve_lifted_kernel_for_schemas(func,
                                                  std::span<const TSValueTypeMetaData *const>{expected_schemas.data(),
                                                                                              expected_schemas.size()});
            if (kernel == nullptr || kernel->arity != schemas.size()) { return std::nullopt; }

            for (std::size_t i = 0; i < expected_schemas.size(); ++i)
            {
                const TSValueTypeMetaData *expected = expected_schemas[i];
                if (!time_series_schema_equivalent(kernel->input_schema(i), expected))
                {
                    return std::nullopt;
                }
            }
            plan.kernel = kernel;
            plan.output_schema = registry.tsl(kernel->output_schema(), size);
            return plan;
        }

        [[nodiscard]] inline std::optional<LiftedMapTslPlan> lifted_map_tsl_plan(OperatorCallContext context)
        {
            const WiredFn *func = context.scalar_as<WiredFn>("func");
            if (func == nullptr) { return std::nullopt; }
            auto ordered = ordered_map_schemas(context, "ndx");
            if (!ordered.has_value()) { return std::nullopt; }
            return lifted_map_tsl_plan(*func,
                                       {ordered->schemas.data(), ordered->schemas.size()},
                                       {ordered->arg_tags.data(), ordered->arg_tags.size()},
                                       ordered->takes_key);
        }

        [[nodiscard]] inline WiringPortRef wire_lifted_map_tsl(Wiring &w,
                                                               const WiredFn &func,
                                                               std::string_view key_arg,
                                                               std::vector<WiringPortRef> ordered)
        {
            std::vector<const TSValueTypeMetaData *> schemas;
            std::vector<std::uint8_t> arg_tags;
            schemas.reserve(ordered.size());
            arg_tags.reserve(ordered.size());
            for (const WiringPortRef &port : ordered)
            {
                schemas.push_back(port.schema);
                arg_tags.push_back(static_cast<std::uint8_t>(port.arg_tag));
            }

            std::optional<LiftedMapTslPlan> plan =
                lifted_map_tsl_plan(func,
                                    {schemas.data(), schemas.size()},
                                    {arg_tags.data(), arg_tags.size()},
                                    /*takes_key=*/false);
            if (!plan.has_value())
            {
                throw std::invalid_argument("map_: lifted TSL fast path requires a compatible lifted scalar function");
            }

            auto &registry = TypeRegistry::instance();
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
            fields.reserve(schemas.size());
            for (std::size_t i = 0; i < schemas.size(); ++i)
            {
                fields.emplace_back(std::to_string(i), schemas[i]);
            }
            const auto *input_schema = registry.un_named_tsb(fields);

            NodeTypeMetaData node_schema;
            node_schema.display_name = "map_lifted_tsl";
            node_schema.input_schema = input_schema;
            node_schema.output_schema = plan->output_schema;
            node_schema.node_kind = NodeKind::Compute;

            const LiftedKernel *kernel = plan->kernel;
            std::vector<bool> multiplexed = plan->multiplexed;
            const std::size_t size = plan->size;
            NodeCallbacks callbacks;
            callbacks.evaluate =
                [kernel, multiplexed = std::move(multiplexed), size](const NodeView &view,
                                                                     DateTime evaluation_time) {
                    auto input_root = view.input(evaluation_time);
                    auto bundle = input_root.as_bundle();
                    auto output_root = view.output(evaluation_time);
                    auto output = output_root.as_list();

                    for (std::size_t i = 0; i < size; ++i)
                    {
                        std::vector<ValueView> values;
                        values.reserve(multiplexed.size());
                        bool ready = true;
                        for (std::size_t arg = 0; arg < multiplexed.size(); ++arg)
                        {
                            auto input = bundle[arg];
                            if (multiplexed[arg])
                            {
                                auto list = input.as_list();
                                auto item = list[i];
                                if (!item.valid())
                                {
                                    ready = false;
                                    break;
                                }
                                values.emplace_back(item.value());
                            }
                            else
                            {
                                if (!input.valid())
                                {
                                    ready = false;
                                    break;
                                }
                                values.emplace_back(input.value());
                            }
                        }
                        if (!ready) { continue; }

                        Value result = kernel->eval(std::span<const ValueView>{values.data(), values.size()});
                        auto output_item = output[i];
                        auto mutation = output_item.begin_mutation(evaluation_time);
                        if (!mutation.copy_value_from(result.view()))
                        {
                            throw std::logic_error("map_: lifted TSL fast path failed to copy an element result");
                        }
                    }
                };

            NodeBuilder builder = NodeBuilder::native(std::move(node_schema), std::move(callbacks));
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                input_schema, std::span<const WiringPortRef>{ordered.data(), ordered.size()}));

            (void)scalar_descriptor<MapCallConfig>::value_meta();
            WiringPortRef out = w.add_node(std::type_index(typeid(lifted_map_tsl_node_tag)),
                                           std::move(builder),
                                           std::span<const WiringPortRef>{ordered.data(), ordered.size()},
                                           Value{MapCallConfig{func, Str{key_arg}, arg_tags}});
            return out;
        }

        [[nodiscard]] inline WiringPortRef wire_map_tsl(Wiring &w, const WiredFn &func, bool takes_key,
                                                        std::vector<WiringPortRef> ordered)
        {
            auto &registry = TypeRegistry::instance();

            // The first fixed TSL anchors the size; every same-size fixed TSL
            // multiplexes per index, the rest broadcast whole.
            std::size_t size = 0;
            for (const WiringPortRef &port : ordered)
            {
                if (port.arg_tag == WiringPortRef::ArgTag::NoKey)
                {
                    throw std::invalid_argument("map_: 'no_key' applies to TSD maps only");
                }
                if (port.arg_tag == WiringPortRef::ArgTag::PassThrough) { continue; }
                const auto *schema = registry.dereference(port.schema);
                if (schema != nullptr && schema->kind == TSTypeKind::TSL && schema->fixed_size() > 0)
                {
                    size = schema->fixed_size();
                    break;
                }
            }
            if (size == 0)
            {
                throw std::invalid_argument("map_: at least one input must be a fixed-size TSL");
            }
            if (!func.has_output)
            {
                throw std::invalid_argument("map_: 'func' must produce an output");
            }

            const auto *key_meta = scalar_descriptor<Int>::value_meta();
            const auto *key_ts   = registry.ts(key_meta);

            std::vector<WiringPortRef> children;
            children.reserve(size);
            for (std::size_t i = 0; i < size; ++i)
            {
                std::vector<WiringPortRef> args;
                args.reserve(func.arity);
                if (takes_key)
                {
                    WiringArg key_arg;
                    key_arg.kind         = WiringArg::Kind::Scalar;
                    key_arg.scalar_value = Value{static_cast<Int>(i)};
                    key_arg.scalar_meta  = key_meta;
                    args.push_back(wire_operator(w, "const", {&key_arg, 1}, true, key_ts).output.erased());
                }
                for (const WiringPortRef &tail : ordered)
                {
                    if (tail.arg_tag != WiringPortRef::ArgTag::PassThrough &&
                        tsl_arg_is_multiplexed(tail.schema, size))
                    {
                        const auto *tail_element = registry.dereference(tail.schema)->element_ts();
                        args.push_back(subgraph_wiring_detail::tsl_element_ref(tail, i, tail_element));
                    }
                    else { args.push_back(tail); }
                }

                WiringPortRef out = func.wire(w, {args.data(), args.size()});
                if (!children.empty() &&
                    !time_series_schema_equivalent(registry.dereference(out.schema),
                                                   registry.dereference(children.front().schema)))
                {
                    throw std::invalid_argument(
                        "map_: 'func' produced differing output schemas across the TSL indices");
                }
                children.push_back(std::move(out));
            }

            const auto *output_schema = registry.tsl(children.front().schema, size);
            return WiringPortRef::structural_source(output_schema, std::move(children));
        }

        /** Bind the output var ``O`` for the resolver: ``TSL<OUT(func), SIZE>``. */
        inline void resolve_map_tsl_output(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (resolution.find_ts("__out__") != nullptr) { return; }

            const WiredFn *func = context.scalar_as<WiredFn>("func");
            if (func == nullptr) { return; }

            auto &registry = TypeRegistry::instance();
            auto ordered = ordered_map_schemas(context, "ndx");
            if (!ordered.has_value()) { return; }

            try
            {
                auto tag_at = [&](std::size_t index) {
                    return index < ordered->arg_tags.size()
                               ? static_cast<WiringPortRef::ArgTag>(ordered->arg_tags[index])
                               : WiringPortRef::ArgTag::None;
                };

                // A no_key TSL still anchors the size here so resolution
                // succeeds and compose can reject it with the clear message.
                std::size_t size = 0;
                for (std::size_t i = 0; i < ordered->schemas.size(); ++i)
                {
                    if (tag_at(i) == WiringPortRef::ArgTag::PassThrough) { continue; }
                    const auto *deref = registry.dereference(ordered->schemas[i]);
                    if (deref != nullptr && deref->kind == TSTypeKind::TSL && deref->fixed_size() > 0)
                    {
                        size = deref->fixed_size();
                        break;
                    }
                }
                if (size == 0) { return; }

                std::vector<const TSValueTypeMetaData *> schemas;
                schemas.reserve(func->arity);
                if (ordered->takes_key) { schemas.push_back(registry.ts(scalar_descriptor<Int>::value_meta())); }
                for (std::size_t i = 0; i < ordered->schemas.size(); ++i)
                {
                    if (tag_at(i) != WiringPortRef::ArgTag::PassThrough &&
                        tsl_arg_is_multiplexed(ordered->schemas[i], size))
                    {
                        schemas.push_back(registry.dereference(ordered->schemas[i])->element_ts());
                    }
                    else { schemas.push_back(ordered->schemas[i]); }
                }

                CompiledSubGraph compiled = func->compile({schemas.data(), schemas.size()});
                if (compiled.output_schema != nullptr)
                {
                    bind_graph_output(resolution, registry.tsl(compiled.output_schema, size), "O");
                }
            }
            catch (...)
            {
                // Leave unresolved; the real wiring path reports the error.
            }
        }

        inline void resolve_lifted_map_tsl_output(ResolutionMap &resolution, OperatorCallContext context)
        {
            auto plan = lifted_map_tsl_plan(context);
            if (plan.has_value() && plan->output_schema != nullptr)
            {
                bind_graph_output(resolution, plan->output_schema, "O");
            }
        }

        struct map_lifted_tsl
        {
            static constexpr auto name = "map_lifted_tsl";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return lifted_map_tsl_plan(context).has_value();
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_lifted_map_tsl_output(resolution, context);
            }

            static std::vector<std::pair<std::string_view, Value>> defaults()
            {
                return {{"__key_arg__", Value{Str{"ndx"}}}};
            }

            static auto compose(Wiring &w, Scalar<"func", WiredFn> func,
                                VarIn<"args", TsVar<"B">> positional,
                                Scalar<"__key_arg__", Str> key_arg, VarKwIn<"kwargs"> kwargs)
            {
                const std::vector<WiringPortRef> pos{positional.begin(), positional.end()};
                std::vector<std::pair<std::string, WiringPortRef>> named{kwargs.begin(), kwargs.end()};
                if (split_keys_kwarg(named).has_value())
                {
                    throw std::invalid_argument("map_: '__keys__' applies to TSD maps only");
                }
                auto bound = bind_wired_fn_args<WiringPortRef>("map_", func.value(),
                                                               {pos.data(), pos.size()},
                                                               {named.data(), named.size()},
                                                               key_arg.value());
                if (bound.takes_leading_key)
                {
                    throw std::invalid_argument("map_: lifted TSL fast path does not support index arguments yet");
                }
                return wire_lifted_map_tsl(w, func.value(), key_arg.value(), std::move(bound.ordered));
            }
        };

        /**
         * ``map_(func, *args, **kwargs)`` over a fixed TSL — wiring-time
         * expansion over the first fixed TSL in ``func`` parameter order. Any
         * fixed TSL of the SAME size multiplexes per index; everything else
         * broadcasts whole.
         */
        struct map_impl_tsl
        {
            static constexpr auto name = "map_impl";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                const auto *collection = first_map_collection(context);
                return collection != nullptr && collection->kind == TSTypeKind::TSL &&
                       !lifted_map_tsl_plan(context).has_value();
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_map_tsl_output(resolution, context);
            }

            static std::vector<std::pair<std::string_view, Value>> defaults()
            {
                return {{"__key_arg__", Value{Str{"ndx"}}}};
            }

            static WiringPortRef compose(Wiring &w, Scalar<"func", WiredFn> func,
                                         VarIn<"args", TsVar<"B">> positional,
                                         Scalar<"__key_arg__", Str> key_arg, VarKwIn<"kwargs"> kwargs)
            {
                const std::vector<WiringPortRef> pos{positional.begin(), positional.end()};
                std::vector<std::pair<std::string, WiringPortRef>> named{kwargs.begin(), kwargs.end()};
                if (split_keys_kwarg(named).has_value())
                {
                    throw std::invalid_argument("map_: '__keys__' applies to TSD maps only");
                }
                auto bound = bind_wired_fn_args<WiringPortRef>("map_", func.value(),
                                                               {pos.data(), pos.size()},
                                                               {named.data(), named.size()},
                                                               key_arg.value());
                return wire_map_tsl(w, func.value(), bound.takes_leading_key, std::move(bound.ordered));
            }
        };
    }  // namespace higher_order_impl_detail

    /**
     * ``mesh_(func)[k]`` cross-instance access, called inside a mesh function (or a
     * sub-graph wired within the mesh's scope). Returns the result of the sibling
     * instance for key ``k``, creating it on demand and recording a dependency so the
     * mesh evaluates it first (same cycle, via pause/resume). ``OutS`` is the mesh
     * element type; ``name`` optionally selects an enclosing named mesh.
     */
    template <typename OutS, typename KeyPort>
    [[nodiscard]] Port<OutS> mesh_ref(Wiring &w, KeyPort key, std::string_view name = {})
    {
        // A never-ticking placeholder seeds the dynamic ``value`` input; mesh_subscribe
        // rebinds it to the sibling output (self[item]) at runtime.
        Port<OutS> placeholder = wire<nothing, OutS>(w);
        return Port<OutS>{
            w, higher_order_impl_detail::mesh_ref_erased(w, key.erased(), placeholder.erased(), name)};
    }

    inline void register_higher_order_operators()
    {
        register_graph_overload<reduce_, higher_order_impl_detail::reduce_variadic_tsl>();
        register_graph_overload<reduce_, higher_order_impl_detail::reduce_lifted_tsl>();
        register_graph_overload<reduce_, higher_order_impl_detail::reduce_tsl>();
        register_graph_overload<reduce_, higher_order_impl_detail::reduce_tsl_zero>();
        register_graph_overload<reduce_, higher_order_impl_detail::reduce_tsd>();
        register_graph_overload<reduce_, higher_order_impl_detail::reduce_tsd_zero>();

        register_graph_overload<switch_, higher_order_impl_detail::switch_impl>();

        register_graph_overload<map_, higher_order_impl_detail::map_impl_tsd>();
        register_graph_overload<map_, higher_order_impl_detail::map_lifted_tsl>();
        register_graph_overload<map_, higher_order_impl_detail::map_impl_tsl>();

        register_graph_overload<mesh_, higher_order_impl_detail::mesh_impl_tsd>();
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_HIGHER_ORDER_IMPL_H
