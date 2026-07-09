#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_SERIES_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_SERIES_IMPL_H

#include <hgraph/lib/std/operators/arithmetic.h>
#include <hgraph/lib/std/operators/container.h>
#include <hgraph/types/operator_type_resolution.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/series.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/value.h>

#include <arrow/array.h>
#include <arrow/compute/api.h>
#include <arrow/scalar.h>

#include <stdexcept>
#include <string>

namespace hgraph::stdlib
{
    using namespace hgraph::operator_type_resolution;

    namespace series_impl_detail
    {
        [[nodiscard]] inline const ValueTypeMetaData *series_meta()
        {
            return TypeRegistry::instance().value_type("series");
        }

        [[nodiscard]] inline bool is_series_arg(OperatorCallContext context, std::size_t index)
        {
            const auto *schema = time_series_schema_at(context, index);
            return schema != nullptr && schema->kind == TSTypeKind::TS && schema->value_schema == series_meta();
        }

        /** A ``arrow::Datum`` for one operand: the Series' array, or a scalar
            built from an ``int``/``float`` TS value. */
        [[nodiscard]] inline arrow::Datum operand_datum(const TSInputView &input)
        {
            const auto *schema = input.schema();
            if (schema->value_schema == series_meta())
            {
                return arrow::Datum{input.value().checked_as<Series>().array};
            }
            auto &registry = TypeRegistry::instance();
            if (schema->value_schema == registry.value_type("int"))
            {
                return arrow::Datum{arrow::MakeScalar(static_cast<std::int64_t>(input.value().checked_as<Int>()))};
            }
            if (schema->value_schema == registry.value_type("float"))
            {
                return arrow::Datum{arrow::MakeScalar(input.value().checked_as<Float>())};
            }
            throw std::invalid_argument("series operator operand must be a Series, int or float");
        }

        [[nodiscard]] inline Series call_binary(const char *fn, const arrow::Datum &lhs, const arrow::Datum &rhs)
        {
            auto result = arrow::compute::CallFunction(fn, {lhs, rhs});
            if (!result.ok())
            {
                throw std::runtime_error(std::string{"arrow "} + fn + " failed: " + result.status().ToString());
            }
            return Series{.array = result->make_array()};
        }

        [[nodiscard]] inline arrow::Datum to_float(const arrow::Datum &value)
        {
            auto cast = arrow::compute::Cast(value, arrow::float64());
            if (!cast.ok()) { throw std::runtime_error("arrow cast to float failed: " + cast.status().ToString()); }
            return *cast;
        }
    }  // namespace series_impl_detail

    /** Elementwise Series arithmetic via arrow compute. ``Div`` is TRUE
        division (int/int -> float, hgraph semantics), so both operands cast
        to float first. Handles Series (+) Series and Series (+) scalar (and
        scalar first) - at least one operand is a Series. */
    template <fixed_string FnName, bool Div>
    struct series_binary_impl
    {
        static constexpr const char *name = FnName.value;

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return series_impl_detail::is_series_arg(context, 0) || series_impl_detail::is_series_arg(context, 1);
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (output_bound(resolution)) { return; }
            if (!series_impl_detail::is_series_arg(context, 0) && !series_impl_detail::is_series_arg(context, 1))
            {
                return;
            }
            bind_output(
                resolution, TypeRegistry::instance().ts(series_impl_detail::series_meta()));
        }

        static void eval(In<"lhs", TsVar<"L">> lhs, In<"rhs", TsVar<"R">> rhs, Out<TsVar<"__out__">> out)
        {
            const auto &erased = static_cast<const TSOutputView &>(out);
            auto        lhs_d  = series_impl_detail::operand_datum(lhs);
            auto        rhs_d  = series_impl_detail::operand_datum(rhs);
            Series      result = Div
                                     ? series_impl_detail::call_binary("divide", series_impl_detail::to_float(lhs_d),
                                                                       series_impl_detail::to_float(rhs_d))
                                     : series_impl_detail::call_binary(FnName.value, lhs_d, rhs_d);
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(Value{std::move(result)}));
        }
    };

    /** getitem_(series, index): the element at ``index`` (a scalar TS). */
    struct series_getitem_impl
    {
        static constexpr auto name = "getitem_series";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return series_impl_detail::is_series_arg(context, 0) &&
                   time_series_arg_matches<AnyTS>(context, 1);
        }

        static void eval(In<"ts", TS<ScalarVar<"S">>> ts, In<"key", TS<Int>> key, Out<TsVar<"O">> out)
        {
            const auto  series = ts.base().value().checked_as<Series>();
            const auto  index  = key.value();
            if (!series.has_value() || index < 0 || index >= series.array->length())
            {
                throw std::out_of_range("Series index out of range");
            }
            auto scalar = series.array->GetScalar(index);
            if (!scalar.ok()) { throw std::runtime_error("Series element read failed"); }
            const auto &erased = static_cast<const TSOutputView &>(out);
            // The element's C++ type follows the bound output (int/float).
            auto &registry = TypeRegistry::instance();
            Value result;
            if (erased.schema()->value_schema == registry.value_type("float"))
            {
                auto v = arrow::compute::Cast(arrow::Datum{*scalar}, arrow::float64());
                if (!v.ok()) { throw std::runtime_error("Series element cast failed"); }
                result = Value{std::static_pointer_cast<arrow::DoubleScalar>(v->scalar())->value};
            }
            else
            {
                auto v = arrow::compute::Cast(arrow::Datum{*scalar}, arrow::int64());
                if (!v.ok()) { throw std::runtime_error("Series element cast failed"); }
                result = Value{static_cast<Int>(std::static_pointer_cast<arrow::Int64Scalar>(v->scalar())->value)};
            }
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(std::move(result)));
        }
    };

    /** contains_(series, item): membership via arrow ``is_in``. */
    struct series_contains_impl
    {
        static constexpr auto name = "contains_series";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return series_impl_detail::is_series_arg(context, 0);
        }

        static void eval(In<"ts", TS<ScalarVar<"S">>> ts, In<"item", TS<ScalarVar<"I">>> item, Out<TS<Bool>> out)
        {
            const auto series = ts.base().value().checked_as<Series>();
            if (!series.has_value()) { out.set(false); return; }
            // is_in(item, value_set=series) -> is the item a member.
            arrow::compute::SetLookupOptions options{arrow::Datum{series.array}};
            auto item_datum = series_impl_detail::operand_datum(item);
            auto found = arrow::compute::CallFunction("is_in", {item_datum}, &options);
            if (!found.ok()) { throw std::runtime_error("arrow is_in failed: " + found.status().ToString()); }
            out.set(std::static_pointer_cast<arrow::BooleanScalar>(found->scalar())->value);
        }
    };

    void register_series_operators();
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_SERIES_IMPL_H
