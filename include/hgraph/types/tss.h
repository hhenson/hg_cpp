//
// Created by Howard Henson on 04/05/2025.
//

#ifndef TSS_H
#define TSS_H

#include <hgraph/types/feature_extension.h>
#include <hgraph/types/ts.h>

namespace hgraph
{

    struct SetDelta
    {
        virtual ~SetDelta() = default;

        /**
         * Get the elements that were added in this delta
         * @return Reference to the set of added elements
         */
        [[nodiscard]] virtual nb::object py_added_elements() const = 0;

        /**
         * Get the elements that were removed in this delta
         * @return Reference to the set of removed elements
         */
        [[nodiscard]] virtual nb::object py_removed_elements() const = 0;

        static void register_with_nanobind(nb::module_ &m);
    };

    struct SetDelta_Object : SetDelta
    {

        SetDelta_Object(nb::object added_elements, nb::object removed_elements);

        [[nodiscard]] nb::object py_added_elements() const override;
        [[nodiscard]] nb::object py_removed_elements() const override;

      private:
        nb::object _added_elements;
        nb::object _removed_elements;
    };

    template <typename T> struct SetDeltaImpl : SetDelta
    {
        using ptr             = nb::ref<SetDeltaImpl<T>>;
        using scalar_type     = T;
        using collection_type = std::unordered_set<T>;

        SetDeltaImpl(collection_type added_elements, collection_type removed_elements)
            : _added_elements(std::move(added_elements)), _removed_elements(std::move(removed_elements)) {}

        [[nodiscard]] nb::object py_added_elements() const override { return nb::cast(_added_elements); }
        [[nodiscard]] nb::object py_removed_elements() const override { return nb::cast(_removed_elements); }

        [[nodiscard]] collection_type &added_elements() { return _added_elements; }
        [[nodiscard]] collection_type &removed_elements() { return _removed_elements; }

      private:
        collection_type _added_elements;
        collection_type _removed_elements;
    };

    template <typename T_TS>
        requires TimeSeriesT<T_TS>
    struct TimeSeriesSet : T_TS
    {
        using T_TS::T_TS;
        [[nodiscard]] virtual bool             py_contains(const nb::object &item) const    = 0;
        [[nodiscard]] virtual size_t           size() const                                 = 0;
        [[nodiscard]] virtual const nb::object py_values() const                            = 0;
        [[nodiscard]] virtual const nb::object py_added() const                             = 0;
        [[nodiscard]] virtual bool             py_was_added(const nb::object &item) const   = 0;
        [[nodiscard]] virtual const nb::object py_removed() const                           = 0;
        [[nodiscard]] virtual bool             py_was_removed(const nb::object &item) const = 0;
    };

    struct TimeSeriesSetOutput : TimeSeriesSet<TimeSeriesOutput>
    {
        using ptr = nb::ref<TimeSeriesSetOutput>;
        using TimeSeriesSet<TimeSeriesOutput>::TimeSeriesSet;

        virtual void py_remove(const nb::object &key) = 0;
        virtual void py_add(const nb::object &key)    = 0;

        void invalidate() override;
    };

    struct TimeSeriesSetInput : TimeSeriesSet<TimeSeriesInput>
    {
        using TimeSeriesSet<TimeSeriesInput>::TimeSeriesSet;
        TimeSeriesSetOutput &set_output() const;

        bool do_bind_output(TimeSeriesOutput::ptr output) override;
        void do_un_bind_output() override;

        [[nodiscard]] nb::object       py_value() const override;
        [[nodiscard]] nb::object       py_delta_value() const override;
        [[nodiscard]] bool             py_contains(const nb::object &item) const override;
        [[nodiscard]] size_t           size() const override;
        [[nodiscard]] const nb::object py_values() const override;
        [[nodiscard]] const nb::object py_added() const override;
        [[nodiscard]] bool             py_was_added(const nb::object &item) const override;
        [[nodiscard]] const nb::object py_removed() const override;
        [[nodiscard]] bool             py_was_removed(const nb::object &item) const override;

        [[nodiscard]] const TimeSeriesSetOutput &prev_output() const;
        [[nodiscard]] bool                       has_prev_output() const;

      protected:
        virtual void reset_prev();

      private:
        TimeSeriesSetOutput::ptr _prev_output;
    };

    template <typename T_Key> struct TimeSeriesSetOutput_T : TimeSeriesSetOutput
    {
        using element_type       = T_Key;
        using collection_type    = std::unordered_set<T_Key>;
        using set_delta          = SetDeltaImpl<element_type>;

        static constexpr bool is_py_object = std::is_same_v<T_Key, nb::object>;

        using TimeSeriesSetOutput::TimeSeriesSetOutput;

        [[nodiscard]] nb::object             py_value() const override;
        [[nodiscard]] const collection_type &value() const { return _value; }

        [[nodiscard]] nb::object       py_delta_value() const override;
        void                           apply_result(nb::object value) override;
        void                           clear() override;
        void                           copy_from_output(const TimeSeriesOutput &output) override;
        void                           copy_from_input(const TimeSeriesInput &input) override;
        [[nodiscard]] bool             py_contains(const nb::object &item) const override;
        [[nodiscard]] bool             contains(const element_type &item) const;
        [[nodiscard]] size_t           size() const override;
        [[nodiscard]] const nb::object py_values() const override;
        [[nodiscard]] const nb::object py_added() const override;
        [[nodiscard]] const collection_type &added() const;
        [[nodiscard]] bool             has_added() const;
        [[nodiscard]] bool             py_was_added(const nb::object &item) const override;
        [[nodiscard]] bool             was_added(const element_type &item) const;
        [[nodiscard]] const nb::object py_removed() const override;
        [[nodiscard]] const collection_type &removed() const;
        [[nodiscard]] bool             has_removed() const;
        [[nodiscard]] bool             py_was_removed(const nb::object &item) const override;
        [[nodiscard]] bool             was_removed(const element_type &item) const;
        void                           py_remove(const nb::object &key) override;
        void                           remove(const element_type &key);
        void                           py_add(const nb::object &key) override;
        void                           add(const element_type &key);
        [[nodiscard]] bool             empty() const;

      protected:
        void _add(const element_type &item);
        void _remove(const element_type &item);
        void _post_modify();

      private:
        collection_type                                       _value;
        collection_type                                       _added;
        collection_type                                       _removed;
        nb::ref<TimeSeriesValueOutput<bool>>                  _is_empty_ref_output;
        std::unique_ptr<FeatureOutputExtension<element_type>> _contains_ref_outputs;

        // These are caches and not a key part of the object and could be constructed in a "const" function.
        mutable nb::set _py_value{};
        mutable nb::set _py_added{};
        mutable nb::set _py_removed{};
    };


    template <typename T> struct TimeSeriesSetInput_T : TimeSeriesSetInput
    {
        using TimeSeriesSetInput::TimeSeriesSetInput;
        using element_type    = typename TimeSeriesSetOutput_T<T>::element_type;
        using collection_type = typename TimeSeriesSetOutput_T<T>::collection_type;
        using set_delta       = typename TimeSeriesSetOutput_T<T>::set_delta;

        [[nodiscard]] const collection_type &value() const { return bound() ? set_output_t().value() : _empty; }
        [[nodiscard]] set_delta              delta_value() const { return set_delta(added(), removed()); }
        [[nodiscard]] bool contains(const element_type &item) const { return bound() ? set_output_t().contains(item) : false; }
        [[nodiscard]] collection_type       &values() const { return *const_cast<collection_type *>(&value()); }
        [[nodiscard]] const collection_type &added() const {
            if (bound()) {
                if (has_prev_output() && _added.empty()) {
                    auto &prev = prev_output_t();
                    // Get the set of elements that would have been present in previous cycle
                    auto prev_state = prev.value();
                    prev_state.insert(prev.removed().begin(), prev.removed().end());
                    for (const auto &item : prev.added()) { prev_state.erase(item); }
                    // Added elements are those in current values but not in previous state
                    for (const auto &item : values()) {
                        if (prev_state.find(item) == prev_state.end()) { _added.insert(item); }
                    }
                    return _added;
                }
                return sampled() ? values() : set_output_t().added();
            }
            _added.clear();
            return _added;
        }

        [[nodiscard]] bool was_added(const element_type &item) const {
            if (has_prev_output()) { return set_output_t().was_added(item) && !prev_output_t().contains(item); }
            if (sampled()) { return contains(item); }
            return set_output_t().was_added(item);
        }

        [[nodiscard]] const collection_type &removed() const {
            if (bound()) {
                if (has_prev_output() && _removed.empty()) {
                    auto &prev {prev_output_t()};
                    // Get the set of elements that would have been present in previous cycle
                    collection_type prev_state{ prev.value()};
                    prev_state.insert(prev.removed().begin(), prev.removed().end());
                    for (const auto &item : prev.added()) { prev_state.erase(item); }
                    // Removed elements are those in previous state but not in current values
                    for (const auto &item : prev_state) {
                        if (values().find(item) == values().end()) { _removed.insert(item); }
                    }
                    return _removed;
                }
                return sampled() ? _empty : set_output_t().removed();
            }
            return _empty;
        }

        [[nodiscard]] bool was_removed(const element_type &item) const {
            if (has_prev_output()) {
                return prev_output_t().contains(item) && !contains(item);
            } else if (sampled()) {
                return false;
            } else {
                return set_output_t().was_removed(item);
            }
        }

      protected:
        const TimeSeriesSetOutput_T<element_type> &prev_output_t() const {
            return reinterpret_cast<const TimeSeriesSetOutput_T<element_type> &>(prev_output());
        }

        const TimeSeriesSetOutput_T<element_type> &set_output_t() const {
            return reinterpret_cast<const TimeSeriesSetOutput_T<element_type> &>(*output());
        }

        void reset_prev() override {
            TimeSeriesSetInput::reset_prev();
            _added.clear();
            _removed.clear();
        }

        // These are caches of values
        collection_type _empty;
        mutable collection_type _added;  // Use this when we have a previous bound value
        mutable collection_type _removed;
    };

    void tss_register_with_nanobind(nb::module_ &m);

}  // namespace hgraph

#endif  // TSS_H
