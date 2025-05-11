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
        using collection_type    = std::conditional_t<std::is_same_v<T_Key, nb::object>, nb::set, std::unordered_set<T_Key>>;
        using py_collection_type = std::conditional_t<std::is_same_v<T_Key, nb::object>, std::monostate, nb::set>;
        using set_delta          = SetDeltaImpl<element_type>;

        static constexpr bool is_py_object = std::is_same_v<T_Key, nb::object>;

        using TimeSeriesSetOutput::TimeSeriesSetOutput;

        [[nodiscard]] nb::object             py_value() const override;
        [[nodiscard]] const collection_type &value() const { return _value; }

        [[nodiscard]] nb::object       py_delta_value() const override;
        void                           apply_result(nb::object value) override;
        void                           clear() override;
        void                           copy_from_output(TimeSeriesOutput &output) override;
        void                           copy_from_input(TimeSeriesInput &input) override;
        [[nodiscard]] bool             py_contains(const nb::object &item) const override;
        [[nodiscard]] size_t           size() const override;
        [[nodiscard]] const nb::object py_values() const override;
        [[nodiscard]] const nb::object py_added() const override;
        [[nodiscard]] bool             has_added();
        [[nodiscard]] bool             py_was_added(const nb::object &item) const override;
        [[nodiscard]] const nb::object py_removed() const override;
        [[nodiscard]] bool             has_removed();
        [[nodiscard]] bool             py_was_removed(const nb::object &item) const override;
        void                           py_remove(const nb::object &key) override;
        void                           py_add(const nb::object &key) override;
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

        // If we are already using nb::object then we don't need these values, the simplist
        // approach to minimising the impact here is to use the monostate, it takes up 1 byte
        // So will hopefully not cause any wasted space
        py_collection_type _py_value;
        py_collection_type _py_added;
        py_collection_type _py_removed;
    };

    // template <typename T> struct TimeSeriesSetOutput_T : TimeSeriesSetOutput
    // {
    //     using element_type    = T;
    //     using collection_type = std::unordered_set<T>;
    //     using set_delta       = SetDeltaImpl<element_type>;
    //
    //     using TimeSeriesSetOutput::TimeSeriesSetOutput;
    //
    //     [[nodiscard]] nb::object py_value() const override { return nb::cast(_value); }
    //
    //     [[nodiscard]] const collection_type &value() const { return value; }
    //
    //     [[nodiscard]] nb::object py_delta_value() const override { return nb::cast(delta_value()); }
    //
    //     [[nodiscard]] set_delta delta_value() const { return set_delta(_added, _removed); }
    //
    //     void apply_result(nb::handle value) override {
    //         if (value.is_none()) { return; }
    //
    //         if (nb::isinstance<SetDelta>(value)) {
    //             auto &delta = nb::cast<SetDelta &>(value);
    //             _added      = collection_type();
    //             for (const auto &e : delta.py_added_elements()) {
    //                 auto v{nb::cast<element_type>(e)};
    //                 if (!_value.contains(v)) {
    //                     _added.insert(v);
    //                     _value.insert(v);
    //                 }
    //             }
    //
    //             _removed = collection_type();
    //             for (const auto &e : delta.py_removed_elements()) {
    //                 auto v{nb::cast<element_type>(e)};
    //                 if (_value.contains(v)) {
    //                     _removed.insert(v);
    //                     _value.erase(v);
    //                 }
    //             }
    //
    //             if (std::any_of(_removed.begin(), _removed.end(), [this](const auto &item) { return _added.contains(item); })) {
    //                 throw std::runtime_error("Cannot remove and add the same element");
    //             }
    //         } else {
    //             auto removed{nb::module_::import_("hgraph").attr("Removed")};
    //             auto input_set = nb::cast<nb::set>(value);
    //
    //             _added   = collection_type();
    //             _removed = collection_type();
    //
    //             for (const auto &item : input_set) {
    //                 if (!nb::isinstance(item, removed)) {
    //                     auto e{nb::cast<element_type>(item)};
    //                     if (!_value.contains(e)) {
    //                         _added.insert(e);
    //                         _value.insert(e);
    //                     }
    //                 } else {
    //                     auto e{nb::cast<element_type>(item.attr("item"))};
    //                     if (_value.contains(item)) {
    //                         if (_added.contains(item)) { throw std::runtime_error("Cannot remove and add the same element"); }
    //                         _removed.add(item);
    //                         _value.discard(item);
    //                     }
    //                 }
    //             }
    //         }
    //
    //         if (!_added.empty() || !_removed.empty() || !valid()) { mark_modified(); }
    //     }
    //
    //     void clear() override {
    //         _value.clear();
    //         _added.clear();
    //         _removed.clear();
    //     }
    //
    //     void copy_from_output(TimeSeriesOutput &output) override {
    //         auto &output_obj = dynamic_cast<TimeSeriesSetOutput_T<T> &>(output);
    //
    //         _added.clear();
    //         _removed.clear();
    //
    //         // Calculate added elements (elements in output but not in current value)
    //         for (const auto &item : output_obj._value) {
    //             if (!_value.contains(item)) { _added.insert(item); }
    //         }
    //
    //         // Calculate removed elements (elements in current value but not in output)
    //         for (const auto &item : _value) {
    //             if (!output_obj._value.contains(item)) { _removed.insert(item); }
    //         }
    //
    //         if (_added.size() > 0 || _removed.size() > 0) {
    //             _value = collection_type(output_obj._value);
    //             mark_modified();
    //         }
    //     }
    //
    //     void copy_from_input(TimeSeriesInput &input) override;
    //
    //     [[nodiscard]] bool py_contains(const nb::handle &item) const override { return contains(nb::cast<element_type>(item)); }
    //
    //     [[nodiscard]] bool contains(const element_type &item) const { return _value.find(item) != _value.end(); }
    //
    //     [[nodiscard]] size_t size() const override { return _value.size(); }
    //
    //     [[nodiscard]] const nb::object py_values() const override { return nb::cast(_value); }
    //     [[nodiscard]] collection_type  values() const { return _value; }
    //
    //     [[nodiscard]] const nb::object       py_added() const override { return nb::cast(_added); }
    //     [[nodiscard]] const collection_type &added() const { return _added; }
    //
    //     [[nodiscard]] bool py_was_added(const nb::handle &item) const override { return was_added(nb::cast<element_type>(item));
    //     }
    //     [[nodiscard]] bool was_added(const element_type &item) const { return _added.find(item) != _added.end(); }
    //
    //     [[nodiscard]] const nb::object       py_removed() const override { return nb::cast(_removed); }
    //     [[nodiscard]] const collection_type &removed() const { return _removed; }
    //
    //     [[nodiscard]] bool py_was_removed(const nb::handle &item) const override {
    //         return was_removed(nb::cast<element_type>(item));
    //     }
    //     [[nodiscard]] bool was_removed(const element_type &item) const { return _removed.find(item) != _removed.end(); }
    //
    //   private:
    //     collection_type                                       _value;
    //     collection_type                                       _added;
    //     collection_type                                       _removed;
    //     nb::ref<TimeSeriesOutput>                             _is_empty_ref_output;
    //     std::unique_ptr<FeatureOutputExtension<element_type>> _contains_ref_outputs;
    // };

    template <typename T> struct TimeSeriesSetInput_T : TimeSeriesSetInput
    {
        using TimeSeriesSetInput::TimeSeriesSetInput;
        using element_type    = typename TimeSeriesSetOutput_T<T>::element_type;
        using collection_type = typename TimeSeriesSetOutput_T<T>::collection_type;
        using set_delta       = typename TimeSeriesSetOutput_T<T>::set_delta;

        [[nodiscard]] const collection_type &value() const { return bound() ? set_output_t().value() : _empty; }
        [[nodiscard]] set_delta        delta_value() const { return set_delta(added(), removed()); }
        [[nodiscard]] bool contains(const element_type &item) const { return bound() ? set_output_t().contains(item) : false; }
        [[nodiscard]] collection_type       &values() const { return value(); }
        [[nodiscard]] const collection_type &added() const {
            if (bound()) {
                if (has_prev_output() && _added.empty()) {
                    auto &prev = prev_output_t();
                    // Get the set of elements that would have been present in previous cycle
                    auto prev_state = prev.values();
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
            return collection_type();
        }

        [[nodiscard]] bool was_added(const element_type &item) const {
            if (has_prev_output()) { return set_output_t().was_added(item) && !prev_output_t().contains(item); }
            if (sampled()) { return contains(item); }
            return set_output_t().was_added(item);
        }

        [[nodiscard]] const collection_type &removed() const {
            if (bound()) {
                if (has_prev_output() && _removed.empty()) {
                    auto &prev = prev_output_t();
                    // Get the set of elements that would have been present in previous cycle
                    auto prev_state = prev.values();
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
        TimeSeriesSetOutput_T<element_type> &prev_output_t() const {
            return dynamic_cast<TimeSeriesSetOutput_T<element_type> &>(prev_output());
        }

        TimeSeriesSetOutput_T<element_type> &set_output_t() const {
            return dynamic_cast<TimeSeriesSetOutput_T<element_type> &>(*output());
        }

        void reset_prev() override {
            TimeSeriesSetInput::reset_prev();
            _added.clear();
            _removed.clear();
        }

        collection_type _empty;
        collection_type _added;  // Use this when we have a previous bound value
        collection_type _removed;
    };

    void tss_register_with_nanobind(nb::module_ &m);

}  // namespace hgraph

#endif  // TSS_H
