#ifndef HGRAPH_CPP_ROOT_VALUE_VISITOR_H
#define HGRAPH_CPP_ROOT_VALUE_VISITOR_H

#include <hgraph/types/detail/visitor.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/value/specialized_views.h>

#include <functional>
#include <optional>
#include <stdexcept>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

namespace hgraph
{
    namespace detail
    {
        struct ValueVisitorAccess
        {
            [[nodiscard]] static ValueView borrow(const ValueView &view) noexcept { return view.borrowed_ref(); }

            template <typename View>
            [[nodiscard]] static View project(const ValueView &view)
            {
                return View{view.borrowed_ref(), TrustedValueKind{}};
            }
        };

        template <typename SpecialisedView, typename Visitor>
        decltype(auto) invoke_value_visitor(Visitor &visitor, const ValueView &view)
        {
            if constexpr (std::invocable<Visitor &, SpecialisedView>)
            {
                return std::invoke(visitor, ValueVisitorAccess::project<SpecialisedView>(view));
            }
            else
            {
                return std::invoke(visitor, ValueVisitorAccess::borrow(view));
            }
        }

        template <typename Value, typename Handler> struct AtomicCase
        {
            using value_type   = Value;
            using handler_type = Handler;
            using result_type  = std::invoke_result_t<handler_type &, const value_type &>;

            handler_type handler;

            [[nodiscard]] bool matches(const AtomicView &view) const noexcept
            {
                return view.type().ops() == &ops_for<value_type>();
            }

            decltype(auto) invoke(const AtomicView &view)
            {
                return std::invoke(handler, view.template as<value_type>());
            }
        };

        template <typename T> struct IsAtomicCase : std::false_type
        {
        };

        template <typename Value, typename Handler>
        struct IsAtomicCase<AtomicCase<Value, Handler>> : std::true_type
        {
        };

        template <typename T>
        inline constexpr bool is_atomic_case_v = IsAtomicCase<std::remove_cvref_t<T>>::value;

        template <typename... Cases> struct AtomicCasePackTraits;

        template <> struct AtomicCasePackTraits<>
        {
            static constexpr bool distinct = true;
        };

        template <typename First, typename... Rest> struct AtomicCasePackTraits<First, Rest...>
        {
            using result_type = typename First::result_type;

            static constexpr bool distinct =
                ((!std::same_as<typename First::value_type, typename Rest::value_type>) && ...) &&
                AtomicCasePackTraits<Rest...>::distinct;
            static constexpr bool same_result =
                (std::same_as<result_type, typename Rest::result_type> && ...);
        };

        template <typename Tuple, std::size_t... I>
        [[nodiscard]] consteval bool tuple_elements_are_atomic_cases(std::index_sequence<I...>)
        {
            return (is_atomic_case_v<std::tuple_element_t<I, Tuple>> && ...);
        }

        template <typename Result, std::size_t I, std::size_t CaseCount, typename Tuple, typename OnMiss>
        Result dispatch_atomic_cases(const AtomicView &value, Tuple &options, OnMiss &on_miss)
        {
            if constexpr (I < CaseCount)
            {
                auto &candidate = std::get<I>(options);
                if (candidate.matches(value)) { return candidate.invoke(value); }
                return dispatch_atomic_cases<Result, I + 1, CaseCount>(value, options, on_miss);
            }
            else
            {
                return std::invoke(on_miss);
            }
        }

        template <typename Result> struct AtomicTryResult
        {
            using type = std::optional<Result>;
        };

        template <> struct AtomicTryResult<void>
        {
            using type = bool;
        };

        template <typename Result>
        using atomic_try_result_t = typename AtomicTryResult<Result>::type;

        template <typename Result, std::size_t I, std::size_t CaseCount, typename Tuple>
        atomic_try_result_t<Result> try_dispatch_atomic_cases(const AtomicView &value, Tuple &cases)
        {
            if constexpr (I < CaseCount)
            {
                auto &candidate = std::get<I>(cases);
                if (candidate.matches(value))
                {
                    if constexpr (std::is_void_v<Result>)
                    {
                        candidate.invoke(value);
                        return true;
                    }
                    else
                    {
                        return std::optional<Result>{candidate.invoke(value)};
                    }
                }
                return try_dispatch_atomic_cases<Result, I + 1, CaseCount>(value, cases);
            }
            else if constexpr (std::is_void_v<Result>)
            {
                return false;
            }
            else
            {
                return std::nullopt;
            }
        }

        template <typename Tuple, std::size_t... I>
        decltype(auto) visit_atomic_with_default(const AtomicView &value, Tuple &options, std::index_sequence<I...>)
        {
            using Cases   = AtomicCasePackTraits<std::tuple_element_t<I, Tuple>...>;
            using Default = std::tuple_element_t<sizeof...(I), Tuple>;

            static_assert(Cases::distinct, "atomic visitor cannot contain duplicate cases for the same type");
            static_assert(std::invocable<Default &, AtomicView>,
                          "atomic visitor default handler must accept AtomicView");

            using Result = std::invoke_result_t<Default &, AtomicView>;
            static_assert((std::same_as<Result, typename std::tuple_element_t<I, Tuple>::result_type> && ...),
                          "every atomic visitor branch must return the same type");
            static_assert(visitor_result_safe_v<Result>,
                          "atomic visitor cannot return a reference or lazy hgraph range that may borrow "
                          "from the visited value; return an owned value instead");

            auto on_miss = [&]() -> Result
            {
                return std::invoke(std::get<sizeof...(I)>(options), ValueVisitorAccess::project<AtomicView>(value));
            };
            return dispatch_atomic_cases<Result, 0, sizeof...(I)>(value, options, on_miss);
        }

        template <typename Tuple, std::size_t... I>
        decltype(auto) visit_atomic_without_default(const AtomicView &value, Tuple &cases, std::index_sequence<I...>)
        {
            using Cases  = AtomicCasePackTraits<std::tuple_element_t<I, Tuple>...>;
            using Result = typename Cases::result_type;

            static_assert(Cases::distinct, "atomic visitor cannot contain duplicate cases for the same type");
            static_assert(Cases::same_result, "every atomic visitor branch must return the same type");
            static_assert(visitor_result_safe_v<Result>,
                          "atomic visitor cannot return a reference or lazy hgraph range that may borrow "
                          "from the visited value; return an owned value instead");

            auto on_miss = [&]() -> Result
            {
                throw std::invalid_argument("atomic visitor has no case for value type '" +
                                            std::string{value.schema()->name()} + "'");
            };
            return dispatch_atomic_cases<Result, 0, sizeof...(I)>(value, cases, on_miss);
        }

        template <typename Tuple, std::size_t... I>
        auto try_visit_atomic_cases(const AtomicView &value, Tuple &cases, std::index_sequence<I...>)
        {
            using Cases  = AtomicCasePackTraits<std::tuple_element_t<I, Tuple>...>;
            using Result = typename Cases::result_type;

            static_assert(Cases::distinct, "atomic visitor cannot contain duplicate cases for the same type");
            static_assert(Cases::same_result, "every atomic visitor branch must return the same type");
            static_assert(visitor_result_safe_v<Result>,
                          "atomic visitor cannot return a reference or lazy hgraph range that may borrow "
                          "from the visited value; return an owned value instead");
            static_assert(std::is_void_v<Result> || std::is_move_constructible_v<Result>,
                          "a non-void try_visit_atomic result must be movable so std::optional can own it");

            return try_dispatch_atomic_cases<Result, 0, sizeof...(I)>(value, cases);
        }
    }  // namespace detail

    /**
     * Associate one exact registered scalar type with its handler.
     *
     * The wrapper makes type selection explicit and avoids the implicit
     * conversions that an ordinary callable overload set could admit.
     */
    template <typename T, typename Handler>
    [[nodiscard]] auto atomic_case(Handler &&handler)
    {
        using Value         = std::remove_cvref_t<T>;
        using StoredHandler = std::decay_t<Handler>;

        static_assert(std::is_object_v<Value> && !std::is_array_v<Value>,
                      "atomic_case<T> requires a non-array object type");
        static_assert(std::invocable<StoredHandler &, const Value &>,
                      "atomic_case<T> handler must accept const T&");

        return detail::AtomicCase<Value, StoredHandler>{StoredHandler{std::forward<Handler>(handler)}};
    }

    /**
     * Visit an atomic value using exact typed cases.
     *
     * A final ordinary callable is the optional ``AtomicView`` default. With
     * no default, an unhandled registered scalar type raises
     * ``std::invalid_argument``. Every branch returns void or the same exact
     * safe value type.
     */
    template <typename... Options>
    decltype(auto) visit_atomic(const AtomicView &value, Options &&...options)
    {
        constexpr std::size_t option_count = sizeof...(Options);
        if constexpr (option_count == 0)
        {
            static_assert(option_count > 0, "visit_atomic requires at least one typed case or a default handler");
        }
        else
        {
            using OptionTuple = std::tuple<std::decay_t<Options>...>;
            using LastOption  = std::tuple_element_t<option_count - 1, OptionTuple>;
            constexpr bool has_default      = !detail::is_atomic_case_v<LastOption>;
            constexpr std::size_t case_count = has_default ? option_count - 1 : option_count;
            constexpr bool valid_cases =
                detail::tuple_elements_are_atomic_cases<OptionTuple>(std::make_index_sequence<case_count>{});

            if constexpr (!valid_cases)
            {
                static_assert(valid_cases,
                              "visit_atomic requires typed atomic_case<T> options followed by at most one default");
            }
            else
            {
                if (!value.valid())
                {
                    throw std::invalid_argument("cannot visit an atomic value without a live payload");
                }

                auto stored = OptionTuple{std::forward<Options>(options)...};
                if constexpr (has_default)
                {
                    return detail::visit_atomic_with_default(value, stored,
                                                             std::make_index_sequence<case_count>{});
                }
                else
                {
                    return detail::visit_atomic_without_default(value, stored,
                                                                std::make_index_sequence<case_count>{});
                }
            }
        }
    }

    /**
     * Attempt exact typed visitation without treating an unlisted atomic type
     * as an error. Returns ``std::optional<R>`` for value-returning handlers
     * and ``bool`` for void handlers.
     */
    template <typename... Cases>
    auto try_visit_atomic(const AtomicView &value, Cases &&...cases)
    {
        constexpr std::size_t case_count = sizeof...(Cases);
        if constexpr (case_count == 0)
        {
            static_assert(case_count > 0, "try_visit_atomic requires at least one typed case");
        }
        else
        {
            using CaseTuple = std::tuple<std::decay_t<Cases>...>;
            constexpr bool valid_cases =
                detail::tuple_elements_are_atomic_cases<CaseTuple>(std::make_index_sequence<case_count>{});

            if constexpr (!valid_cases)
            {
                static_assert(valid_cases, "try_visit_atomic accepts only atomic_case<T> options");
            }
            else
            {
                if (!value.valid())
                {
                    throw std::invalid_argument("cannot visit an atomic value without a live payload");
                }

                auto stored = CaseTuple{std::forward<Cases>(cases)...};
                return detail::try_visit_atomic_cases(value, stored, std::make_index_sequence<case_count>{});
            }
        }
    }

    /**
     * Visit a live erased value according to its semantic value shape.
     *
     * ``Any`` is a transparent owning box rather than a visitor alternative:
     * every populated Any layer is peeled before dispatch. An empty Any has no
     * value to visit and is rejected.
     *
     * A shape-specific handler takes precedence over a ``ValueView`` catch-all.
     * Every reachable handler must return void or the same safe value type.
     * References and lazy hgraph ranges are rejected because the selected
     * wrapper is a temporary borrowed cursor.
     */
    template <typename... Handlers>
    decltype(auto) visit(const ValueView &value, Handlers &&...handlers)
    {
        static_assert(sizeof...(Handlers) > 0, "value visit requires at least one handler");
        auto visitor  = detail::VisitorOverload<std::decay_t<Handlers>...>{std::forward<Handlers>(handlers)...};
        using Visitor = decltype(visitor);

        constexpr bool complete = detail::visitor_branch_invocable_v<Visitor, AtomicView, ValueView> &&
                                  detail::visitor_branch_invocable_v<Visitor, TupleView, ValueView> &&
                                  detail::visitor_branch_invocable_v<Visitor, BundleView, ValueView> &&
                                  detail::visitor_branch_invocable_v<Visitor, ListView, ValueView> &&
                                  detail::visitor_branch_invocable_v<Visitor, SetView, ValueView> &&
                                  detail::visitor_branch_invocable_v<Visitor, MapView, ValueView> &&
                                  detail::visitor_branch_invocable_v<Visitor, CyclicBufferView, ValueView> &&
                                  detail::visitor_branch_invocable_v<Visitor, QueueView, ValueView>;

        if constexpr (!complete)
        {
            static_assert(complete, "value visitor must handle every concrete value kind directly or "
                                    "through a ValueView catch-all");
        }
        else
        {
            using Result = detail::visitor_branch_result_t<Visitor, AtomicView, ValueView>;
            static_assert(detail::visitor_result_safe_v<Result>,
                          "value visitor cannot return a reference or lazy hgraph range that may borrow "
                          "from a temporary view; consume the range in the handler or return an owned collection");
            static_assert(detail::visitor_branch_same_result_v<Result, Visitor, TupleView, ValueView> &&
                              detail::visitor_branch_same_result_v<Result, Visitor, BundleView, ValueView> &&
                              detail::visitor_branch_same_result_v<Result, Visitor, ListView, ValueView> &&
                              detail::visitor_branch_same_result_v<Result, Visitor, SetView, ValueView> &&
                              detail::visitor_branch_same_result_v<Result, Visitor, MapView, ValueView> &&
                              detail::visitor_branch_same_result_v<Result, Visitor, CyclicBufferView, ValueView> &&
                              detail::visitor_branch_same_result_v<Result, Visitor, QueueView, ValueView>,
                          "every value visitor branch must return the same type");

            auto current = detail::ValueVisitorAccess::borrow(value);
            for (;;)
            {
                if (!current.valid()) { throw std::invalid_argument("cannot visit a value without a live payload"); }

                const auto kind = current.schema()->try_value_kind();
                if (!kind.has_value()) { throw std::invalid_argument("cannot visit a value with an unknown value kind"); }

                switch (*kind)
                {
                case ValueTypeKind::Atomic:
                    return detail::invoke_value_visitor<AtomicView>(visitor, current);
                case ValueTypeKind::Tuple:
                    return detail::invoke_value_visitor<TupleView>(visitor, current);
                case ValueTypeKind::Bundle:
                    return detail::invoke_value_visitor<BundleView>(visitor, current);
                case ValueTypeKind::List:
                    return detail::invoke_value_visitor<ListView>(visitor, current);
                case ValueTypeKind::Set:
                    return detail::invoke_value_visitor<SetView>(visitor, current);
                case ValueTypeKind::Map:
                    return detail::invoke_value_visitor<MapView>(visitor, current);
                case ValueTypeKind::CyclicBuffer:
                    return detail::invoke_value_visitor<CyclicBufferView>(visitor, current);
                case ValueTypeKind::Queue:
                    return detail::invoke_value_visitor<QueueView>(visitor, current);
                case ValueTypeKind::Any: {
                    auto boxed = current.as_any();
                    if (!boxed.has_value()) { throw std::invalid_argument("cannot visit an empty Any value"); }
                    current = boxed.get();
                    break;
                }
                }
            }
        }
    }
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_VALUE_VISITOR_H
