#ifndef HGRAPH_LIB_STD_OPERATORS_COLLECTION_H
#define HGRAPH_LIB_STD_OPERATORS_COLLECTION_H

#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/time_series/ts_delta.h>

#include <array>
#include <cstddef>
#include <span>
#include <stdexcept>
#include <type_traits>
#include <typeindex>
#include <utility>
#include <vector>

namespace hgraph::stdlib
{
    /**
     * Collection operator **definitions** (markers only): aggregations / reductions, set
     * operations and ``TSD`` (dictionary) re-shaping. Mirrors the Python ``hgraph``
     * aggregation operators (``_operators.py``) and the TSD / mapping operators
     * (``_tsd_and_mapping.py``). The aggregations and set operations are **variadic**:
     * unary = over a collection / running, n-ary = element-wise.
     */

    // ---- Aggregations / reductions ----

    /** ``sum_`` — running / element-wise sum. */
    struct sum_ : Operator<"sum_", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``mean`` — running / element-wise mean. */
    struct mean : Operator<"mean", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``std_`` — running / element-wise standard deviation. */
    struct std_ : Operator<"std", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``var_`` — running / element-wise variance. */
    struct var_ : Operator<"var", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    // ---- Set operations (variadic over the inputs) ----

    /** ``union_`` — set union of the inputs. */
    struct union_ : Operator<"union", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``intersection_`` — set intersection of the inputs. */
    struct intersection_ : Operator<"intersection", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``difference_`` — set difference (``lhs`` minus the rest). */
    struct difference_ : Operator<"difference", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``symmetric_difference_`` — set symmetric difference of the inputs. */
    struct symmetric_difference_ : Operator<"symmetric_difference", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    // ---- TSD / dictionary re-shaping ----

    /** ``keys_`` — the keys of a dictionary (as a ``TSS`` / set). */
    struct keys_ : Operator<"keys_", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``values_`` — the values of a dictionary. */
    struct values_ : Operator<"values_", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``rekey`` — re-key the input dictionary using a mapping time-series. */
    struct rekey : Operator<"rekey", In<"ts", TsVar<"S">>, In<"new_keys", TsVar<"K">>, Out<TsVar<"O">>>
    {
    };

    /** ``flip`` — swap keys and values of a dictionary. */
    struct flip : Operator<"flip", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``partition`` — split a ``TSD[K, V]`` into ``TSD[K1, TSD[K, V]]`` using a mapping. */
    struct partition : Operator<"partition", In<"ts", TsVar<"S">>, In<"partitions", TsVar<"P">>, Out<TsVar<"O">>>
    {
    };

    /** ``unpartition`` — merge a nested ``TSD[K1, TSD[K, V]]`` back into ``TSD[K, V]``. */
    struct unpartition : Operator<"unpartition", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``flip_keys`` — invert the outer/inner keys of a nested ``TSD[K, TSD[K1, V]]``. */
    struct flip_keys : Operator<"flip_keys", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``collapse_keys`` — collapse a nested ``TSD[K, TSD[K1, V]]`` to ``TSD[Tuple[K, K1], V]``. */
    struct collapse_keys : Operator<"collapse_keys", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``uncollapse_keys`` — the inverse of ``collapse_keys`` (optional ``remove_empty`` flag). */
    struct uncollapse_keys : Operator<"uncollapse_keys", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    namespace collection_detail
    {
        template <typename T>
        struct is_tsl_schema : std::false_type
        {
        };

        template <typename ElementSchema, std::size_t FixedSize>
        struct is_tsl_schema<TSL<ElementSchema, FixedSize>> : std::true_type
        {
        };

        template <typename T>
        inline constexpr bool is_tsl_schema_v = is_tsl_schema<T>::value;

        template <typename T>
        struct tsl_schema_traits;

        template <typename ElementSchema, std::size_t FixedSize>
        struct tsl_schema_traits<TSL<ElementSchema, FixedSize>>
        {
            using element_schema = ElementSchema;
            static constexpr std::size_t fixed_size = FixedSize;
        };

        template <typename T>
        struct port_schema
        {
            using type = void;
        };

        template <typename Schema>
        struct port_schema<Port<Schema>>
        {
            using type = Schema;
        };

        template <typename T>
        using port_schema_t = typename port_schema<std::remove_cvref_t<T>>::type;

        template <typename First, typename...>
        struct first_type
        {
            using type = First;
        };

        template <typename... T>
        using first_type_t = typename first_type<T...>::type;

        template <typename First, typename... Rest>
        inline constexpr bool typed_ports_same_v =
            !std::is_void_v<port_schema_t<First>> &&
            (std::is_same_v<port_schema_t<First>, port_schema_t<Rest>> && ...);

        struct to_tsl_node_tag
        {
        };

        inline void evaluate_to_tsl(const NodeView &view, engine_time_t evaluation_time)
        {
            auto root         = view.input(evaluation_time);
            auto bundle       = root.as_bundle();
            auto list_field   = bundle.field("ts");
            auto input_list   = list_field.as_list();
            auto output_root  = view.output(evaluation_time);
            auto output_list  = output_root.as_list();
            const auto count  = input_list.size();

            for (std::size_t index = 0; index < count; ++index)
            {
                auto input_child  = input_list.at(index);
                auto output_child = output_list.at(index);
                if (!input_child.valid()) { continue; }

                if (!output_child.valid())
                {
                    apply_current_value(output_child, input_child.value());
                }
                else if (input_child.modified())
                {
                    apply_delta(output_child, input_child.delta_value());
                }
            }
        }

        [[nodiscard]] inline NodeBuilder to_tsl_node_builder(const TSValueTypeMetaData &output_schema)
        {
            if (output_schema.kind != TSTypeKind::TSL || output_schema.fixed_size() == 0 ||
                output_schema.element_ts() == nullptr)
            {
                throw std::logic_error("to_tsl requires a fixed-size TSL output schema");
            }

            auto       &registry     = TypeRegistry::instance();
            const auto *input_schema = registry.un_named_tsb({{"ts", &output_schema}});

            NodeTypeMetaData schema;
            schema.display_name     = "to_tsl";
            schema.input_schema     = input_schema;
            schema.output_schema    = &output_schema;
            schema.node_kind        = NodeKind::Compute;
            schema.active_inputs    = {0};
            schema.all_valid_inputs = {0};

            NodeCallbacks callbacks;
            callbacks.evaluate = &evaluate_to_tsl;

            auto list_endpoint =
                TSEndpointSchema::non_peered_list(&output_schema, TSEndpointSchema::peered(output_schema.element_ts()));
            auto input_endpoint = TSEndpointSchema::non_peered(input_schema, {std::move(list_endpoint)});

            NodeBuilder builder = NodeBuilder::native(std::move(schema), std::move(callbacks),
                                                       std::move(input_endpoint));
            builder.label("to_tsl");
            return builder;
        }

        inline void validate_to_tsl_inputs(const TSValueTypeMetaData &element_schema,
                                           std::span<const WiringPortRef> refs)
        {
            for (const WiringPortRef &ref : refs)
            {
                if (!graph_wiring_detail::input_accepts_output_schema(&element_schema, ref.schema))
                {
                    throw std::logic_error("to_tsl input port schema does not match the TSL element schema");
                }
            }
        }

        inline void validate_inferred_to_tsl_inputs(const TSValueTypeMetaData &element_schema,
                                                    std::span<const WiringPortRef> refs)
        {
            for (const WiringPortRef &ref : refs)
            {
                if (ref.schema == nullptr || !time_series_schema_equivalent(&element_schema, ref.schema))
                {
                    throw std::logic_error(
                        "to_tsl without an explicit schema requires all input schemas to match exactly");
                }
            }
        }

        template <std::size_t Size>
        [[nodiscard]] std::vector<WiringInputRef> to_tsl_inputs(const std::array<WiringPortRef, Size> &refs)
        {
            std::vector<WiringInputRef> inputs;
            inputs.reserve(Size);
            for (std::size_t index = 0; index < Size; ++index)
            {
                inputs.push_back(WiringInputRef{
                    .source      = refs[index],
                    .target_path = {0, index},
                });
            }
            return inputs;
        }

        [[nodiscard]] inline WiringPortRef add_to_tsl_node(Wiring &w,
                                                           const TSValueTypeMetaData &output_schema,
                                                           std::vector<WiringInputRef> inputs)
        {
            NodeBuilder builder = to_tsl_node_builder(output_schema);
            return w.add_node(std::type_index(typeid(to_tsl_node_tag)), std::move(builder), inputs, Value{});
        }
    }  // namespace collection_detail

    /**
     * Wire several time-series ports into a fixed non-peered ``TSL``.
     *
     * ``to_tsl<TS<Str>>(w, a, b)`` and ``to_tsl<TSL<TS<Str>>>(w, a, b)`` both return
     * ``Port<TSL<TS<Str>, 2>>``. If every input is already a typed ``Port<S>``, the
     * element schema can be inferred and the same fixed-size typed port is returned.
     * If the inputs are erased ports and no schema is supplied, the result is an
     * erased ``Port<void>`` with a runtime fixed TSL schema inferred from matching
     * input schemas.
     */
    template <typename OutOrElementSchema = void, typename... Ports>
    [[nodiscard]] auto to_tsl(Wiring &w, const Ports &...ports)
    {
        static_assert(sizeof...(Ports) > 0, "to_tsl requires at least one input");
        static_assert((graph_wiring_detail::is_port<std::remove_cvref_t<Ports>>::value && ...),
                      "to_tsl inputs must be time-series Ports");

        constexpr std::size_t size = sizeof...(Ports);
        std::array<WiringPortRef, size> refs{ports.erased()...};

        if constexpr (std::is_void_v<OutOrElementSchema>)
        {
            if constexpr (collection_detail::typed_ports_same_v<Ports...>)
            {
                using FirstPort = collection_detail::first_type_t<Ports...>;
                using Element   = collection_detail::port_schema_t<FirstPort>;
                using Result    = TSL<Element, size>;

                const auto *output_schema = schema_descriptor<Result>::ts_meta();
                collection_detail::validate_to_tsl_inputs(*output_schema->element_ts(), refs);
                WiringPortRef out =
                    collection_detail::add_to_tsl_node(w, *output_schema, collection_detail::to_tsl_inputs(refs));
                return Port<Result>{out.node, out.path};
            }
            else
            {
                const auto *element_schema = refs.front().schema;
                if (element_schema == nullptr)
                {
                    throw std::logic_error("to_tsl cannot infer an output schema from an untyped input");
                }
                collection_detail::validate_inferred_to_tsl_inputs(*element_schema, refs);
                const auto *output_schema = TypeRegistry::instance().tsl(element_schema, size);
                WiringPortRef out =
                    collection_detail::add_to_tsl_node(w, *output_schema, collection_detail::to_tsl_inputs(refs));
                return Port<void>{out.node, out.path, out.schema};
            }
        }
        else if constexpr (collection_detail::is_tsl_schema_v<OutOrElementSchema>)
        {
            using Traits = collection_detail::tsl_schema_traits<OutOrElementSchema>;
            static_assert(Traits::fixed_size == 0 || Traits::fixed_size == size,
                          "to_tsl explicit fixed TSL output size must match the input count");
            using Result = TSL<typename Traits::element_schema,
                               Traits::fixed_size == 0 ? size : Traits::fixed_size>;

            const auto *output_schema = schema_descriptor<Result>::ts_meta();
            collection_detail::validate_to_tsl_inputs(*output_schema->element_ts(), refs);
            WiringPortRef out =
                collection_detail::add_to_tsl_node(w, *output_schema, collection_detail::to_tsl_inputs(refs));
            return Port<Result>{out.node, out.path};
        }
        else
        {
            using Result = TSL<OutOrElementSchema, size>;

            const auto *output_schema = schema_descriptor<Result>::ts_meta();
            collection_detail::validate_to_tsl_inputs(*output_schema->element_ts(), refs);
            WiringPortRef out =
                collection_detail::add_to_tsl_node(w, *output_schema, collection_detail::to_tsl_inputs(refs));
            return Port<Result>{out.node, out.path};
        }
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_COLLECTION_H
