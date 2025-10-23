//
// Created by Howard Henson on 04/05/2025.
//

#ifndef TSS_H
#define TSS_H

#include <hgraph/python/hashable.h>
#include <hgraph/types/constants.h>
#include <hgraph/types/feature_extension.h>
#include <hgraph/types/ts.h>

namespace hgraph
{

    struct SetDelta : nb::intrusive_base
    {
        using ptr = nb::ref<SetDelta>;

        virtual ~SetDelta() = default;

        /**
         * Get the elements that were added in this delta
         * @return Reference to the set of added elements
         */
        [[nodiscard]] virtual nb::object py_added() const = 0;

        /**
         * Get the elements that were removed in this delta
         * @return Reference to the set of removed elements
         */
        [[nodiscard]] virtual nb::object py_removed() const = 0;

        [[nodiscard]] virtual nb::object py_type() const = 0;

        [[nodiscard]] virtual size_t hash() const = 0;

        // Default to False, then the overrides will sort out the rest
        [[nodiscard]] virtual bool operator==(const SetDelta &other) const = 0;

        static void register_with_nanobind(nb::module_ &m);
    };

    template <typename T> struct SetDelta_T : SetDelta
    {
        using ptr             = nb::ref<SetDelta_T<T>>;
        using scalar_type     = T;
        using collection_type = std::unordered_set<T>;

        template <typename U = T>
            requires(!std::is_same_v<U, nb::object>)
        SetDelta_T(collection_type added, collection_type removed);

        template <typename U = T>
            requires(std::is_same_v<U, nb::object>)
        SetDelta_T(collection_type added, collection_type removed, nb::object tp);

        [[nodiscard]] nb::object py_added() const override;
        [[nodiscard]] nb::object py_removed() const override;

        [[nodiscard]] const collection_type &added() const;
        [[nodiscard]] const collection_type &removed() const;

        [[nodiscard]] bool operator==(const SetDelta &other) const override;
        [[nodiscard]] bool operator==(const SetDelta_T<T> &other) const;

        [[nodiscard]] size_t hash() const override;

        [[nodiscard]] nb::ref<SetDelta_T<T>> operator+(const SetDelta_T<T> &other) const;

        [[nodiscard]] nb::object py_type() const override;

      private:
        collection_type                                                               _added;
        collection_type                                                               _removed;
        std::conditional_t<std::is_same_v<T, nb::object>, nb::object, std::monostate> _tp;
    };

    template <typename T> SetDelta_T<T>::ptr make_set_delta(std::unordered_set<T> added, std::unordered_set<T> removed) {
        auto v{new SetDelta_T<T>(std::move(added), std::move(removed))};
        return typename SetDelta_T<T>::ptr(v);
    }

    template <>
    inline SetDelta_T<nb::object>::ptr make_set_delta(std::unordered_set<nb::object> added,
                                                      std::unordered_set<nb::object> removed) {
        nb::object tp;
        if (!added.empty()) {
            tp = nb::borrow(*added.begin()->type());
        } else if (!removed.empty()) {
            tp = nb::borrow(*removed.begin()->type());
        } else {
            tp = get_object();
        }
        return new SetDelta_T<nb::object>(added, removed, tp);
    }

    template <typename T_TS>
        requires TimeSeriesT<T_TS>
    struct TimeSeriesSet : T_TS
    {
        using T_TS::T_TS;
        [[nodiscard]] virtual bool       py_contains(const nb::object &item) const    = 0;
        [[nodiscard]] virtual size_t     size() const                                 = 0;
        [[nodiscard]] virtual bool       empty() const                                = 0;
        [[nodiscard]] virtual nb::object py_values() const                            = 0;
        [[nodiscard]] virtual nb::object py_added() const                             = 0;
        [[nodiscard]] virtual bool       py_was_added(const nb::object &item) const   = 0;
        [[nodiscard]] virtual nb::object py_removed() const                           = 0;
        [[nodiscard]] virtual bool       py_was_removed(const nb::object &item) const = 0;
    };

    struct TimeSeriesSetOutput : TimeSeriesSet<TimeSeriesOutput>
    {
        using ptr = nb::ref<TimeSeriesSetOutput>;

        explicit TimeSeriesSetOutput(const node_ptr &parent);
        explicit TimeSeriesSetOutput(const TimeSeriesType::ptr &parent);

        virtual void py_remove(const nb::object &key) = 0;
        virtual void py_add(const nb::object &key)    = 0;

        [[nodiscard]] virtual TimeSeriesValueOutput<bool>::ptr get_contains_output(const nb::object &item,
                                                                                   const nb::object &requester) = 0;
        virtual void release_contains_output(const nb::object &item, const nb::object &requester)               = 0;

        [[nodiscard]] TimeSeriesValueOutput<bool>::ptr &is_empty_output();

        void invalidate() override;

      private:
        nb::ref<TimeSeriesValueOutput<bool>> _is_empty_ref_output;
    };

    struct TimeSeriesSetInput : TimeSeriesSet<TimeSeriesInput>
    {
        using TimeSeriesSet<TimeSeriesInput>::TimeSeriesSet;
        TimeSeriesSetOutput &set_output() const;

        bool do_bind_output(TimeSeriesOutput::ptr &output) override;
        void do_un_bind_output(bool unbind_refs) override;

        [[nodiscard]] nb::object py_added() const override;
        [[nodiscard]] bool       py_was_added(const nb::object &item) const override;
        [[nodiscard]] nb::object py_removed() const override;
        [[nodiscard]] bool       py_was_removed(const nb::object &item) const override;

        [[nodiscard]] const TimeSeriesSetOutput &prev_output() const;
        [[nodiscard]] bool                       has_prev_output() const;

      protected:
        virtual void reset_prev();
        void         _add_reset_prev() const;

      private:
        TimeSeriesSetOutput::ptr _prev_output;
        mutable bool             _pending_reset_prev{false};
    };

    struct TimeSeriesDictOutput;


    template <typename T_Key> struct TimeSeriesSetOutput_T : TimeSeriesSetOutput
    {
        using element_type    = T_Key;
        using collection_type = std::unordered_set<T_Key>;
        using set_delta       = SetDelta_T<T_Key>;
        using set_delta_ptr   = nb::ref<set_delta>;

        static constexpr bool is_py_object = std::is_same_v<T_Key, nb::object>;

        explicit TimeSeriesSetOutput_T(const node_ptr &parent);
        explicit TimeSeriesSetOutput_T(const TimeSeriesType::ptr &parent);

        [[nodiscard]] nb::object             py_value() const override;
        [[nodiscard]] const collection_type &value() const;

        void set_value(collection_type added, collection_type removed);

        void set_value(const SetDelta_T<T_Key> &delta);

        void set_value(const nb::object &delta);

        [[nodiscard]] nb::object py_delta_value() const override;

        void py_set_value(nb::object value) override;
        void apply_result(nb::object value) override;

        void clear() override;

        void copy_from_output(const TimeSeriesOutput &output) override;
        void copy_from_input(const TimeSeriesInput &input) override;

        [[nodiscard]] bool py_contains(const nb::object &item) const override;
        [[nodiscard]] bool contains(const element_type &item) const;

        [[nodiscard]] size_t size() const override;

        [[nodiscard]] nb::object py_values() const override;

        [[nodiscard]] nb::object             py_added() const override;
        [[nodiscard]] const collection_type &added() const;
        [[nodiscard]] bool                   has_added() const;
        [[nodiscard]] bool                   py_was_added(const nb::object &item) const override;
        [[nodiscard]] bool                   was_added(const element_type &item) const;
        void                                 py_add(const nb::object &key) override;
        void                                 add(const element_type &key);

        [[nodiscard]] nb::object             py_removed() const override;
        [[nodiscard]] const collection_type &removed() const;
        [[nodiscard]] bool                   has_removed() const;
        [[nodiscard]] bool                   py_was_removed(const nb::object &item) const override;
        [[nodiscard]] bool                   was_removed(const element_type &item) const;
        void                                 py_remove(const nb::object &key) override;
        void                                 remove(const element_type &key);

        [[nodiscard]] bool empty() const override;

        [[nodiscard]] TimeSeriesValueOutput<bool>::ptr get_contains_output(const nb::object &item,
                                                                           const nb::object &requester) override;
        void release_contains_output(const nb::object &item, const nb::object &requester) override;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override {
            return dynamic_cast<const TimeSeriesSetOutput_T<T_Key> *>(other) != nullptr;
        }

        using TimeSeriesOutput::mark_modified;
        void mark_modified(engine_time_t modified_time) override;

        // void post_modify() override;

        void _reset_value();

      protected:

        void _add(const element_type &item);
        void _remove(const element_type &item);
        void _post_modify();
        void _reset();

      private:
        collection_type                      _value;
        collection_type                      _added;
        collection_type                      _removed;
        FeatureOutputExtension<element_type> _contains_ref_outputs;

        // These are caches and not a key part of the object and could be constructed in a "const" function.
        mutable nb::frozenset _py_value{};
        mutable nb::frozenset _py_added{};
        mutable nb::frozenset _py_removed{};
    };

    template <typename T> struct TimeSeriesSetInput_T : TimeSeriesSetInput
    {
        using TimeSeriesSetInput::TimeSeriesSetInput;
        using element_type    = typename TimeSeriesSetOutput_T<T>::element_type;
        using collection_type = typename TimeSeriesSetOutput_T<T>::collection_type;
        using set_delta       = typename TimeSeriesSetOutput_T<T>::set_delta;
        using set_delta_ptr   = nb::ref<typename TimeSeriesSetOutput_T<T>::set_delta>;

        [[nodiscard]] nb::object py_value() const override;
        [[nodiscard]] nb::object py_delta_value() const override;

        [[nodiscard]] bool   py_contains(const nb::object &item) const override;
        [[nodiscard]] size_t size() const override;
        [[nodiscard]] bool   empty() const override;

        [[nodiscard]] nb::object py_values() const override;
        [[nodiscard]] nb::object py_added() const override;

        [[nodiscard]] bool       py_was_added(const nb::object &item) const override;
        [[nodiscard]] nb::object py_removed() const override;
        [[nodiscard]] bool       py_was_removed(const nb::object &item) const override;

        [[nodiscard]] const collection_type &value() const;
        [[nodiscard]] set_delta_ptr          delta_value() const;
        [[nodiscard]] bool                   contains(const element_type &item) const;
        [[nodiscard]] const collection_type &values() const;

        [[nodiscard]] const collection_type &added() const;

        [[nodiscard]] bool was_added(const element_type &item) const;

        [[nodiscard]] const collection_type &removed() const;

        [[nodiscard]] bool was_removed(const element_type &item) const;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override;

      protected:
        const TimeSeriesSetOutput_T<element_type> &prev_output_t() const;
        const TimeSeriesSetOutput_T<element_type> &set_output_t() const;
        void                                       reset_prev() override;

        // These are caches of values
        collection_type         _empty{};
        mutable collection_type _added{};  // Use this when we have a previous bound value
        mutable collection_type _removed{};
    };

    void tss_register_with_nanobind(nb::module_ &m);

}  // namespace hgraph

#endif  // TSS_H
