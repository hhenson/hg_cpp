//
// Created by Howard Henson on 10/05/2025.
//

#ifndef TSD_H
#define TSD_H

#include <hgraph/types/tss.h>

namespace hgraph
{
    // TSDKeyObserver: Used to track additions and removals of parent keys.
    // Since the TSD is dynamic, the inputs associated to an output needs to be updated when a key is added or removed
    // in order to correctly manage it's internal state.
    template <typename K> struct TSDKeyObserver
    {
        // Called when a key is added
        virtual void on_key_added(const K &key) = 0;

        // Called when a key is removed
        virtual void on_key_removed(const K &key) = 0;

        virtual ~TSDKeyObserver() = default;
    };

    template <typename T_TS>
        requires TimeSeriesT<T_TS>
    struct TimeSeriesDict : T_TS
    {
        using ts_type = T_TS;
        using T_TS::T_TS;

        [[nodiscard]] virtual size_t size() const = 0;

        // Create a set of Python-based API, for non-object-based instances there will
        // be typed analogues.
        [[nodiscard]] virtual bool     py_contains(const nb::object &item) const = 0;
        [[nodiscard]] virtual ts_type &get_item(const nb::object &)              = 0;

        [[nodiscard]] virtual nb::iterator py_keys() const   = 0;
        [[nodiscard]] virtual nb::iterator py_values() const = 0;
        [[nodiscard]] virtual nb::iterator py_items() const  = 0;

        [[nodiscard]] virtual nb::iterator py_modified_keys() const            = 0;
        [[nodiscard]] virtual nb::iterator py_modified_values() const          = 0;
        [[nodiscard]] virtual nb::iterator py_modified_items() const           = 0;
        [[nodiscard]] virtual bool         was_modified(const nb::object &key) = 0;

        [[nodiscard]] virtual nb::iterator py_valid_keys() const   = 0;
        [[nodiscard]] virtual nb::iterator py_valid_values() const = 0;
        [[nodiscard]] virtual nb::iterator py_valid_items() const  = 0;

        [[nodiscard]] virtual nb::iterator py_added_keys() const            = 0;
        [[nodiscard]] virtual nb::iterator py_added_values() const          = 0;
        [[nodiscard]] virtual nb::iterator py_added_items() const           = 0;
        [[nodiscard]] virtual bool         has_added() const                = 0;
        [[nodiscard]] virtual bool         was_added(const nb::object &key) = 0;

        [[nodiscard]] virtual nb::iterator py_removed_keys() const            = 0;
        [[nodiscard]] virtual nb::iterator py_removed_values() const          = 0;
        [[nodiscard]] virtual nb::iterator py_removed_items() const           = 0;
        [[nodiscard]] virtual bool         has_removed() const                = 0;
        [[nodiscard]] virtual bool         was_removed(const nb::object &key) = 0;

        [[nodiscard]] virtual TimeSeriesSet<T_TS> &key_set() = 0;
    };

    struct TimeSeriesDictOutput : TimeSeriesDict<TimeSeriesOutput>
    {
        using ptr = nb::ref<TimeSeriesDictOutput>;
        using TimeSeriesDict::TimeSeriesDict;

        void apply_result(nb::object value) override;
        bool can_apply_result(nb::object value) override;

        virtual void                   set_item(const nb::object &key, const nb::object &value)     = 0;
        virtual void                   del_item(const nb::object &key)                              = 0;
        virtual nb::object             pop(const nb::object &key, const nb::object &default_value)  = 0;
        virtual time_series_output_ptr py_get_ref(const nb::object &key, const void *requester)     = 0;
        virtual void                   py_release_ref(const nb::object &key, const void *requester) = 0;

        void mark_child_modified(TimeSeriesOutput &child, engine_time_t modified_time) override;
        void mark_modified(engine_time_t modified_time) override;

        

      protected:
        virtual void              clear_on_end_of_evaluation_cycle();
        virtual TimeSeriesOutput &get_or_create(const nb::object &key) = 0;

        // Try to reduce duplicate code when we move to the template use void* for the key in these temp structures.
        // With remove, we need to hold the actual values as they are removed from the set and won't be referenceable.
        using key_value_pair = std::tuple<void *, TimeSeriesOutput *>;
        using change_set     = std::unordered_set<key_value_pair>;
        // Hold a pointer to the reverse, this makes an assumption that the key will not be copied once inside the forward map.
        // This is true of the std::unordered_map, but may not be true of all maps, so if the main map changes, this assumption
        // needs to be validated.
        using reverse_map = std::unordered_map<TimeSeriesOutput *, void *>;
        reverse_map        _reverse_ts_values;
        change_set         _modified_items;
        change_set         _added_items;
        nb::dict           _value;
        nb::dict           _delta_value;
        output_builder_ptr _ts_builder;
        output_builder_ptr _ts_ref_builder;

        void add_added_item(void *key, TimeSeriesOutput *value);
        void add_modified_value(TimeSeriesOutput *value);
        void remove_value(TimeSeriesOutput *value);
    };

    struct TimeSeriesDictInput : TimeSeriesDict<TimeSeriesInput>
    {
        using ptr = nb::ref<TimeSeriesDictInput>;
        using TimeSeriesDict::TimeSeriesDict;
    };

    template <typename T_Key> struct TimeSeriesDictOutput_T : TimeSeriesDictOutput
    {
        using key_type            = T_Key;
        using map_type            = std::unordered_map<key_type, time_series_output_ptr>;
        using item_iterator       = map_type::iterator;
        using const_item_iterator = map_type::const_iterator;

        explicit TimeSeriesDictOutput_T(const node_ptr &parent);
        explicit TimeSeriesDictOutput_T(const time_series_type_ptr &parent);

        [[nodiscard]] nb::object py_value() const override;
        [[nodiscard]] nb::object py_delta_value() const override;

        void clear() override;
        void invalidate() override;
        void copy_from_output(TimeSeriesOutput &output) override;
        void copy_from_input(TimeSeriesInput &input) override;

        [[nodiscard]] size_t       size() const override;
        [[nodiscard]] bool         py_contains(const nb::object &item) const override;
        [[nodiscard]] ts_type     &get_item(const nb::object &) override;
        [[nodiscard]] nb::iterator py_keys() const override;
        [[nodiscard]] nb::iterator py_values() const override;
        [[nodiscard]] nb::iterator py_items() const override;
        [[nodiscard]] nb::iterator py_modified_keys() const override;
        [[nodiscard]] nb::iterator py_modified_values() const override;
        [[nodiscard]] nb::iterator py_modified_items() const override;
        [[nodiscard]] nb::iterator py_valid_keys() const override;
        [[nodiscard]] nb::iterator py_valid_values() const override;
        [[nodiscard]] nb::iterator py_valid_items() const override;
        [[nodiscard]] nb::iterator py_added_keys() const override;
        [[nodiscard]] nb::iterator py_added_values() const override;
        [[nodiscard]] nb::iterator py_added_items() const override;
        [[nodiscard]] nb::iterator py_removed_keys() const override;
        [[nodiscard]] nb::iterator py_removed_values() const override;
        [[nodiscard]] nb::iterator py_removed_items() const override;

        [[nodiscard]] TimeSeriesSetOutput &key_set() override;

        void                   set_item(const nb::object &key, const nb::object &value) override;
        void                   del_item(const nb::object &key) override;
        nb::object             pop(const nb::object &key, const nb::object &default_value) override;
        time_series_output_ptr py_get_ref(const nb::object &key, const void *requester) override;
        void                   py_release_ref(const nb::object &key, const void *requester) override;
        time_series_output_ptr get_ref(const key_type &key, const void *requester)
            requires(!std::is_same_v<key_type, nb::object>);
        void release_ref(const key_type &key, const void *requester)
            requires(!std::is_same_v<key_type, nb::object>);

      protected:
        void              clear_on_end_of_evaluation_cycle() override;
        TimeSeriesOutput &get_or_create(const nb::object &key) override;
        TimeSeriesOutput &_get_or_create(key_type key);
        const key_type   &key_from_value(TimeSeriesOutput *value) const;

        
      private:
        void _initialise();

        void _create(const key_type &key);

        void add_key_observer(TSDKeyObserver<key_type> *observer);
        void remove_key_observer(TSDKeyObserver<key_type> *observer);

        map_type _ts_values;
        map_type _removed_values;  // This ensures we hold onto the values until we are sure no one needs to reference them.
        FeatureOutputExtension<key_type>      _ref_ts_feature;
        std::vector<TSDKeyObserver<key_type>> _key_observers;
    };

    template <typename T_Key> struct TimeSeriesDictInput_T : TimeSeriesDictInput
    {
        using key_type = T_Key;
        using TimeSeriesDictInput::TimeSeriesDictInput;

        [[nodiscard]] size_t       size() const override;
        [[nodiscard]] bool         py_contains(const nb::object &item) const override;
        [[nodiscard]] ts_type     &get_item(const nb::object &item) override;
        [[nodiscard]] nb::iterator py_keys() const override;
        [[nodiscard]] nb::iterator py_values() const override;
        [[nodiscard]] nb::iterator py_items() const override;
        [[nodiscard]] nb::iterator py_modified_keys() const override;
        [[nodiscard]] nb::iterator py_modified_values() const override;
        [[nodiscard]] nb::iterator py_modified_items() const override;
        [[nodiscard]] nb::iterator py_valid_keys() const override;
        [[nodiscard]] nb::iterator py_valid_values() const override;
        [[nodiscard]] nb::iterator py_valid_items() const override;
        [[nodiscard]] nb::iterator py_added_keys() const override;
        [[nodiscard]] nb::iterator py_added_values() const override;
        [[nodiscard]] nb::iterator py_added_items() const override;
        [[nodiscard]] nb::iterator py_removed_keys() const override;
        [[nodiscard]] nb::iterator py_removed_values() const override;
        [[nodiscard]] nb::iterator py_removed_items() const override;

        [[nodiscard]] TimeSeriesSet<TimeSeriesInput> &key_set() override;

        [[nodiscard]] bool was_modified(const nb::object &key) override;
        [[nodiscard]] bool has_added() const override;
        [[nodiscard]] bool was_added(const nb::object &key) override;
        [[nodiscard]] bool has_removed() const override;
        [[nodiscard]] bool was_removed(const nb::object &key) override;
    };

    void tsd_register_with_nanobind(nb::module_ &m);

}  // namespace hgraph

#endif  // TSD_H
