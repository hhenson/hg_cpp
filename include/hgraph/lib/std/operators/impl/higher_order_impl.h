#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_HIGHER_ORDER_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_HIGHER_ORDER_IMPL_H

#include <hgraph/lib/std/operators/higher_order.h>
#include <hgraph/runtime/map_node.h>
#include <hgraph/runtime/reduce_node.h>
#include <hgraph/runtime/switch_node.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/subgraph_wiring.h>
#include <hgraph/types/wired_fn.h>

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
        // The static reduction layout, mirroring Python _reduce_tsl: a linear
        // chain for small sizes, otherwise balanced binary pairing with an
        // odd-element carry (over_run). Combination is one WiredFn application
        // per reduction node.
        [[nodiscard]] inline WiringPortRef reduce_layout(Wiring &w, const WiredFn &func,
                                                         std::vector<WiringPortRef> elements)
        {
            auto combine = [&](const WiringPortRef &lhs, const WiringPortRef &rhs) {
                const std::array<WiringPortRef, 2> args{lhs, rhs};
                return func.wire(w, std::span<const WiringPortRef>{args.data(), args.size()});
            };

            if (elements.size() < 4)
            {
                WiringPortRef out = elements.front();
                for (std::size_t i = 1; i < elements.size(); ++i) { out = combine(out, elements[i]); }
                return out;
            }

            std::vector<WiringPortRef> outs;
            outs.reserve(elements.size() / 2);
            for (std::size_t i = 0; i + 1 < elements.size(); i += 2)
            {
                outs.push_back(combine(elements[i], elements[i + 1]));
            }
            std::optional<WiringPortRef> over_run;
            if (elements.size() % 2 == 1) { over_run = elements.back(); }

            while (outs.size() > 1)
            {
                std::size_t count = outs.size();
                if (count % 2 == 1)
                {
                    if (over_run.has_value())
                    {
                        outs.push_back(*over_run);
                        over_run.reset();
                        ++count;
                    }
                    else
                    {
                        over_run = outs.back();
                        outs.pop_back();
                        --count;
                    }
                }
                std::vector<WiringPortRef> next;
                next.reserve(count / 2);
                for (std::size_t i = 0; i + 1 < count; i += 2) { next.push_back(combine(outs[i], outs[i + 1])); }
                outs = std::move(next);
            }
            return over_run.has_value() ? combine(outs.front(), *over_run) : outs.front();
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
            return reduce_layout(w, combiner, std::move(elements));
        }

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
                return reduce_ts_is_fixed_tsl(context, 2);
            }

            static Port<TsVar<"V">> compose(Wiring &w, Scalar<"func", WiredFn> func, NamedPort<"ts", TSL<TsVar<"V">>> ts)
            {
                const TSValueTypeMetaData *element = ts.erased().schema->element_ts();

                const std::array<WiringArg, 1> zero_args{operator_dispatch_detail::make_wiring_arg(func)};
                const WiringPortRef            zero =
                    wire_operator(w, "zero", {zero_args.data(), zero_args.size()}, true, element).output.erased();

                return Port<TsVar<"V">>{w, reduce_tsl_wire(w, func.value(), ts.erased(), zero)};
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

            static Port<TsVar<"V">> compose(Wiring &w, Scalar<"func", WiredFn> func, NamedPort<"ts", TSL<TsVar<"V">>> ts,
                                            Scalar<"zero", ScalarVar<"Z">> zero_value)
            {
                const TSValueTypeMetaData *element = ts.erased().schema->element_ts();

                WiringArg zero_arg;
                zero_arg.kind         = WiringArg::Kind::Scalar;
                zero_arg.scalar_value = Value{zero_value.value()};
                zero_arg.scalar_meta  = zero_arg.scalar_value.schema();
                const WiringPortRef zero =
                    wire_operator(w, "const", {&zero_arg, 1}, true, element).output.erased();

                return Port<TsVar<"V">>{w, reduce_tsl_wire(w, func.value(), ts.erased(), zero)};
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
        [[nodiscard]] inline Port<void> wire_reduce_tsd(Wiring &w, const Scalar<"func", WiredFn> &func,
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
            return Port<void>{w, std::move(out)};
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

            static Port<TsVar<"V">> compose(Wiring &w, Scalar<"func", WiredFn> func,
                                            NamedPort<"ts", TSD<ScalarVar<"K">, TsVar<"V">>> ts)
            {
                const auto *element =
                    TypeRegistry::instance().dereference(ts.erased().schema)->element_ts();

                const std::array<WiringArg, 1> zero_args{operator_dispatch_detail::make_wiring_arg(func)};
                const WiringPortRef            zero =
                    wire_operator(w, "zero", {zero_args.data(), zero_args.size()}, true, element).output.erased();

                auto out = wire_reduce_tsd(w, func, ts.erased(), zero);
                return Port<TsVar<"V">>{w, out.erased()};
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

            static Port<TsVar<"V">> compose(Wiring &w, Scalar<"func", WiredFn> func,
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

                auto out = wire_reduce_tsd(w, func, ts.erased(), zero);
                return Port<TsVar<"V">>{w, out.erased()};
            }
        };

        struct switch_node_tag
        {
        };

        /**
         * Compile one branch over the outer time-series slots. ``named_slots``
         * are the keyword arguments as ``(name, outer slot)`` pairs — resolved
         * per branch against ITS parameter names (branches may name and order
         * their parameters differently, like Python). A branch may consume the
         * key (arity == ts_count + 1, key first) or not (arity == ts_count).
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
        [[nodiscard]] inline Port<void> wire_switch(Wiring &w, WiringPortRef key, const SwitchCases &cases,
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
            return Port<void>{w, std::move(out)};
        }

        /**
         * The output schema is whatever the branches compute — discover it for
         * the resolver by compiling the first branch (a side-effect-free
         * wiring-time computation), unless an explicit output schema already
         * bound ``O``.
         */
        inline void resolve_switch_output(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (resolution.find_ts("O") != nullptr) { return; }

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
                if (output_schema != nullptr) { resolution.bind_ts("O", output_schema); }
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

            static Port<TsVar<"O">> compose(Wiring &w, NamedPort<"key", TS<ScalarVar<"K">>> key,
                                            Scalar<"cases", SwitchCases> cases, VarIn<"ts", TsVar<"TS">> ts,
                                            VarKwIn<"kwargs"> kwargs)
            {
                auto out = wire_switch(w, key.erased(), cases.value(),
                                       std::vector<WiringPortRef>{ts.begin(), ts.end()},
                                       std::vector<std::pair<std::string, WiringPortRef>>{kwargs.begin(),
                                                                                          kwargs.end()});
                return Port<TsVar<"O">>{w, out.erased()};
            }
        };
    }  // namespace higher_order_impl_detail

    namespace higher_order_impl_detail
    {
        struct map_node_tag
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
            std::vector<const TSValueTypeMetaData *> child_schemas{};    ///< per ts arg: element or whole schema
            const ValueTypeMetaData                 *key_meta{nullptr};
        };

        [[nodiscard]] inline MapArgClassification classify_map_args(
            std::span<const TSValueTypeMetaData *const> ts_schemas)
        {
            auto &registry = TypeRegistry::instance();

            MapArgClassification result;
            result.is_multiplexed.reserve(ts_schemas.size());
            result.child_schemas.reserve(ts_schemas.size());
            for (std::size_t i = 0; i < ts_schemas.size(); ++i)
            {
                const auto *deref = registry.dereference(ts_schemas[i]);
                if (deref != nullptr && deref->kind == TSTypeKind::TSD)
                {
                    if (result.key_meta == nullptr) { result.key_meta = deref->key_type(); }
                    else if (deref->key_type() != result.key_meta)
                    {
                        throw std::invalid_argument(
                            "map_: every multiplexed TSD must share the same key type");
                    }
                    result.is_multiplexed.push_back(true);
                    result.child_schemas.push_back(deref->element_ts());
                }
                else
                {
                    result.is_multiplexed.push_back(false);
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
         * output schema. ``func`` may take the key first
         * (arity = ts-arg count + 1).
         */
        [[nodiscard]] inline MapNodeSpec compile_map_child(const WiredFn &func,
                                                           std::span<const TSValueTypeMetaData *const> ts_schemas,
                                                           const TSValueTypeMetaData *&output_schema)
        {
            if (!func.valid())
            {
                throw std::invalid_argument("map_: 'func' must be a wirable function (fn<X>())");
            }

            auto &registry = TypeRegistry::instance();

            const MapArgClassification classified = classify_map_args(ts_schemas);

            const std::size_t base_arity = ts_schemas.size();
            const bool        takes_key  = func.arity == base_arity + 1;
            if (!takes_key && func.arity != base_arity)
            {
                throw std::invalid_argument(
                    "map_: 'func' must take one parameter per time-series argument — optionally preceded by "
                    "the key");
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
        [[nodiscard]] inline Port<void> wire_map(Wiring &w, const Scalar<"func", WiredFn> &func,
                                                 std::vector<WiringPortRef> ordered,
                                                 std::optional<WiringPortRef> keys = std::nullopt)
        {
            std::vector<const TSValueTypeMetaData *> ts_schemas;
            ts_schemas.reserve(ordered.size());
            for (const WiringPortRef &port : ordered) { ts_schemas.push_back(port.schema); }

            const TSValueTypeMetaData *output_schema = nullptr;
            MapNodeSpec spec = compile_map_child(func.value(), {ts_schemas.data(), ts_schemas.size()},
                                                 output_schema);

            // The lifecycle key set: an explicit ``__keys__`` when supplied,
            // else derived from the multiplexed dicts — ``keys_(tsd)`` for
            // one, ``union(keys_(tsd)...)`` for several (the Python wiring:
            // ``__keys__ = union(*key_sets)``). The runtime is always
            // keys-driven; there is no in-node union scan.
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
                    WiringArg key_set_arg;
                    key_set_arg.kind = WiringArg::Kind::TimeSeries;
                    key_set_arg.port = subgraph_wiring_detail::tsd_key_set_ref(ordered[mux_index]);
                    key_set_args.push_back(std::move(key_set_arg));
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

            WiringPortRef out = w.add_node(
                std::type_index(typeid(map_node_tag)), node_schema,
                std::span<const WiringPortRef>{inputs.data(), inputs.size()},
                Value{func.value()},
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
            return Port<void>{w, std::move(out)};
        }

        struct OrderedMapSchemas
        {
            std::vector<const TSValueTypeMetaData *> schemas{};
            bool                                    takes_key{false};
        };

        [[nodiscard]] inline std::optional<OrderedMapSchemas> ordered_map_schemas(OperatorCallContext context)
        {
            const WiredFn *func = context.scalar_as<WiredFn>("func");
            if (func == nullptr) { return std::nullopt; }

            std::vector<const TSValueTypeMetaData *> positional;
            for (std::size_t i = 1; i < context.args.size(); ++i)
            {
                if (context.args[i].kind != WiringArg::Kind::TimeSeries) { return std::nullopt; }
                positional.push_back(context.args[i].port.schema);
            }
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> named;
            named.reserve(context.kwargs.size());
            for (const auto &[name, port] : context.kwargs)
            {
                if (name == "__keys__") { continue; }   // map_'s own argument, not func's
                named.emplace_back(name, port.schema);
            }

            try
            {
                OrderedMapSchemas ordered;
                auto bound = bind_wired_fn_args<const TSValueTypeMetaData *>(
                    "map_", *func, {positional.data(), positional.size()}, {named.data(), named.size()},
                    true);
                ordered.schemas   = std::move(bound.ordered);
                ordered.takes_key = bound.takes_leading_key;
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
            if (resolution.find_ts("O") != nullptr) { return; }

            const WiredFn *func = context.scalar_as<WiredFn>("func");
            if (func == nullptr) { return; }
            auto ordered = ordered_map_schemas(context);
            if (!ordered.has_value()) { return; }

            try
            {
                const TSValueTypeMetaData *output_schema = nullptr;
                (void)compile_map_child(*func, {ordered->schemas.data(), ordered->schemas.size()}, output_schema);
                if (output_schema != nullptr) { resolution.bind_ts("O", output_schema); }
            }
            catch (...)
            {
                // Leave unresolved; the real wiring path reports the error.
            }
        }

        /** The first collection in ``func`` parameter order decides which map kernel applies. */
        [[nodiscard]] inline const TSValueTypeMetaData *first_map_collection(OperatorCallContext context)
        {
            auto &registry = TypeRegistry::instance();
            auto ordered = ordered_map_schemas(context);
            if (!ordered.has_value()) { return nullptr; }
            for (const auto *schema_in : ordered->schemas)
            {
                const auto *schema = registry.dereference(schema_in);
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

            static Port<TsVar<"O">> compose(Wiring &w, Scalar<"func", WiredFn> func,
                                            VarIn<"args", TsVar<"B">> positional, VarKwIn<"kwargs"> kwargs)
            {
                const std::vector<WiringPortRef> pos{positional.begin(), positional.end()};
                std::vector<std::pair<std::string, WiringPortRef>> named{kwargs.begin(), kwargs.end()};
                std::optional<WiringPortRef> keys = split_keys_kwarg(named);
                auto bound = bind_wired_fn_args<WiringPortRef>("map_", func.value(),
                                                               {pos.data(), pos.size()},
                                                               {named.data(), named.size()});
                auto out = wire_map(w, func, std::move(bound.ordered), std::move(keys));
                return Port<TsVar<"O">>{w, out.erased()};
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

        [[nodiscard]] inline Port<void> wire_map_tsl(Wiring &w, const WiredFn &func, bool takes_key,
                                                     std::vector<WiringPortRef> ordered)
        {
            auto &registry = TypeRegistry::instance();

            // The first fixed TSL anchors the size; every same-size fixed TSL
            // multiplexes per index, the rest broadcast whole.
            std::size_t size = 0;
            for (const WiringPortRef &port : ordered)
            {
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
                    if (tsl_arg_is_multiplexed(tail.schema, size))
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
            return Port<void>{w, WiringPortRef::structural_source(output_schema, std::move(children))};
        }

        /** Bind the output var ``O`` for the resolver: ``TSL<OUT(func), SIZE>``. */
        inline void resolve_map_tsl_output(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (resolution.find_ts("O") != nullptr) { return; }

            const WiredFn *func = context.scalar_as<WiredFn>("func");
            if (func == nullptr) { return; }

            auto &registry = TypeRegistry::instance();
            auto ordered = ordered_map_schemas(context);
            if (!ordered.has_value()) { return; }

            try
            {
                std::size_t size = 0;
                for (const auto *schema_in : ordered->schemas)
                {
                    const auto *deref = registry.dereference(schema_in);
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
                for (const auto *schema_in : ordered->schemas)
                {
                    if (tsl_arg_is_multiplexed(schema_in, size))
                    {
                        schemas.push_back(registry.dereference(schema_in)->element_ts());
                    }
                    else { schemas.push_back(schema_in); }
                }

                CompiledSubGraph compiled = func->compile({schemas.data(), schemas.size()});
                if (compiled.output_schema != nullptr)
                {
                    resolution.bind_ts("O", registry.tsl(compiled.output_schema, size));
                }
            }
            catch (...)
            {
                // Leave unresolved; the real wiring path reports the error.
            }
        }

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
                return collection != nullptr && collection->kind == TSTypeKind::TSL;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_map_tsl_output(resolution, context);
            }

            static Port<TsVar<"O">> compose(Wiring &w, Scalar<"func", WiredFn> func,
                                            VarIn<"args", TsVar<"B">> positional, VarKwIn<"kwargs"> kwargs)
            {
                const std::vector<WiringPortRef> pos{positional.begin(), positional.end()};
                std::vector<std::pair<std::string, WiringPortRef>> named{kwargs.begin(), kwargs.end()};
                if (split_keys_kwarg(named).has_value())
                {
                    throw std::invalid_argument("map_: '__keys__' applies to TSD maps only");
                }
                auto bound = bind_wired_fn_args<WiringPortRef>("map_", func.value(),
                                                               {pos.data(), pos.size()},
                                                               {named.data(), named.size()});
                auto out = wire_map_tsl(w, func.value(), bound.takes_leading_key, std::move(bound.ordered));
                return Port<TsVar<"O">>{w, out.erased()};
            }
        };
    }  // namespace higher_order_impl_detail

    inline void register_higher_order_operators()
    {
        register_graph_overload<reduce_, higher_order_impl_detail::reduce_tsl>();
        register_graph_overload<reduce_, higher_order_impl_detail::reduce_tsl_zero>();
        register_graph_overload<reduce_, higher_order_impl_detail::reduce_tsd>();
        register_graph_overload<reduce_, higher_order_impl_detail::reduce_tsd_zero>();

        register_graph_overload<switch_, higher_order_impl_detail::switch_impl>();

        register_graph_overload<map_, higher_order_impl_detail::map_impl_tsd>();
        register_graph_overload<map_, higher_order_impl_detail::map_impl_tsl>();
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_HIGHER_ORDER_IMPL_H
