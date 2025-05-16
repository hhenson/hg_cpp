//
// Created by Howard Henson on 10/05/2025.
//

#ifndef TSD_H
#define TSD_H

#include <hgraph/types/tss.h>
#include <ranges>

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
        [[nodiscard]] virtual nb::object py_get_item(const nb::object &item) const = 0;

        [[nodiscard]] virtual nb::iterator py_keys() const   = 0;
        [[nodiscard]] virtual nb::iterator py_values() const = 0;
        [[nodiscard]] virtual nb::iterator py_items() const  = 0;

        [[nodiscard]] virtual nb::iterator py_modified_keys() const                     = 0;
        [[nodiscard]] virtual nb::iterator py_modified_values() const                   = 0;
        [[nodiscard]] virtual nb::iterator py_modified_items() const                    = 0;
        [[nodiscard]] virtual bool         py_was_modified(const nb::object &key) const = 0;

        [[nodiscard]] virtual nb::iterator py_valid_keys() const   = 0;
        [[nodiscard]] virtual nb::iterator py_valid_values() const = 0;
        [[nodiscard]] virtual nb::iterator py_valid_items() const  = 0;

        [[nodiscard]] virtual nb::iterator py_added_keys() const                     = 0;
        [[nodiscard]] virtual nb::iterator py_added_values() const                   = 0;
        [[nodiscard]] virtual nb::iterator py_added_items() const                    = 0;
        [[nodiscard]] virtual bool         has_added() const                         = 0;
        [[nodiscard]] virtual bool         py_was_added(const nb::object &key) const = 0;

        [[nodiscard]] virtual nb::iterator py_removed_keys() const                     = 0;
        [[nodiscard]] virtual nb::iterator py_removed_values() const                   = 0;
        [[nodiscard]] virtual nb::iterator py_removed_items() const                    = 0;
        [[nodiscard]] virtual bool         has_removed() const                         = 0;
        [[nodiscard]] virtual bool         py_was_removed(const nb::object &key) const = 0;

        [[nodiscard]] virtual TimeSeriesSet<T_TS>       &key_set()       = 0;
        [[nodiscard]] virtual const TimeSeriesSet<T_TS> &key_set() const = 0;
    };

    struct TimeSeriesDictOutput : TimeSeriesDict<TimeSeriesOutput>
    {
        using ptr = nb::ref<TimeSeriesDictOutput>;
        using TimeSeriesDict::TimeSeriesDict;

        virtual void                   py_set_item(const nb::object &key, const nb::object &value)    = 0;
        virtual void                   py_del_item(const nb::object &key)                             = 0;
        virtual nb::object             py_pop(const nb::object &key, const nb::object &default_value) = 0;
        virtual time_series_output_ptr py_get_ref(const nb::object &key, const void *requester)       = 0;
        virtual void                   py_release_ref(const nb::object &key, const void *requester)   = 0;

      protected:
        nb::dict _value;
        nb::dict _delta_value;
    };

    struct TimeSeriesDictInput : TimeSeriesDict<TimeSeriesInput>
    {
        using ptr = nb::ref<TimeSeriesDictInput>;
        using TimeSeriesDict::TimeSeriesDict;
    };

    template <typename T_Key> struct TimeSeriesDictOutput_T : TimeSeriesDictOutput
    {
        using key_type            = T_Key;
        using value_type          = time_series_output_ptr;
        using map_type            = std::unordered_map<key_type, value_type>;
        using item_iterator       = typename map_type::iterator;
        using const_item_iterator = typename map_type::const_iterator;
        using key_set_type        = TimeSeriesSetOutput_T<key_type>;
        using constrained_view =
            std::ranges::filter_view<std::ranges::ref_view<map_type>, std::function<bool(const typename map_type::value_type &)>>;
        // TODO: Currently we are only exposing simple types and nb::object, so this simple strategy is not overly expensive,
        //  If we start using more complicated native types, we may wish to use a pointer so something to that effect to
        //  Track keys. The values have a light weight reference counting cost to store as value_type so leave for the moment as
        //  well.
        using reverse_map = std::unordered_map<value_type, key_type>;

        explicit TimeSeriesDictOutput_T(const node_ptr &parent);
        explicit TimeSeriesDictOutput_T(const time_series_type_ptr &parent);

        void apply_result(nb::object value) override;
        bool can_apply_result(nb::object value) override;

        void mark_child_modified(TimeSeriesOutput &child, engine_time_t modified_time) override;
        void mark_modified(engine_time_t modified_time) override;

        [[nodiscard]] nb::object py_value() const override;
        [[nodiscard]] nb::object py_delta_value() const override;

        void clear() override;
        void invalidate() override;
        void copy_from_output(const TimeSeriesOutput &output) override;
        void copy_from_input(const TimeSeriesInput &input) override;

        [[nodiscard]] auto size() const -> size_t override;

        [[nodiscard]] bool py_contains(const nb::object &item) const override;
        [[nodiscard]] bool contains(const key_type &item) const;

        [[nodiscard]] nb::object py_get_item(const nb::object &item) const override;
        [[nodiscard]] ts_type &operator[](const key_type &item);
        [[nodiscard]] ts_type &operator[](const key_type &item) const;

        [[nodiscard]] const_item_iterator begin() const;
        [[nodiscard]] item_iterator       begin();
        [[nodiscard]] const_item_iterator end() const;
        [[nodiscard]] item_iterator       end();

        [[nodiscard]] nb::iterator py_keys() const override;
        [[nodiscard]] nb::iterator py_values() const override;
        [[nodiscard]] nb::iterator py_items() const override;

        [[nodiscard]] constrained_view modified_items() const;

        [[nodiscard]] nb::iterator py_modified_keys() const override;
        [[nodiscard]] nb::iterator py_modified_values() const override;
        [[nodiscard]] nb::iterator py_modified_items() const override;
        [[nodiscard]] bool         py_was_modified(const nb::object &key) const override;
        [[nodiscard]] bool         was_modified(const key_type &key) const;

        [[nodiscard]] nb::iterator py_valid_keys() const override;
        [[nodiscard]] nb::iterator py_valid_values() const override;
        [[nodiscard]] nb::iterator py_valid_items() const override;

        [[nodiscard]] nb::iterator py_added_keys() const override;
        [[nodiscard]] nb::iterator py_added_values() const override;
        [[nodiscard]] nb::iterator py_added_items() const override;
        [[nodiscard]] bool         py_was_added(const nb::object &key) const override;
        [[nodiscard]] bool         was_added(const key_type &key) const;

        [[nodiscard]] nb::iterator py_removed_keys() const override;
        [[nodiscard]] nb::iterator py_removed_values() const override;
        [[nodiscard]] nb::iterator py_removed_items() const override;
        [[nodiscard]] bool         py_was_removed(const nb::object &key) const override;
        [[nodiscard]] bool         was_removed(const key_type &key) const;

        [[nodiscard]] TimeSeriesSet<ts_type>            &key_set() override;
        [[nodiscard]] const TimeSeriesSet<ts_type>      &key_set() const override;
        [[nodiscard]] TimeSeriesSetOutput_T<key_type> &key_set_t();

        void py_set_item(const nb::object &key, const nb::object &value) override;

        void py_del_item(const nb::object &key) override;
        void erase(const key_type &key);

        nb::object py_pop(const nb::object &key, const nb::object &default_value) override;

        time_series_output_ptr py_get_ref(const nb::object &key, const void *requester) override;
        void                   py_release_ref(const nb::object &key, const void *requester) override;
        time_series_output_ptr get_ref(const key_type &key, const void *requester)
            requires(!std::is_same_v<key_type, nb::object>);
        void release_ref(const key_type &key, const void *requester)
            requires(!std::is_same_v<key_type, nb::object>);

      protected:
        void              clear_on_end_of_evaluation_cycle();
        TimeSeriesOutput &_get_or_create(const key_type &key);
        const key_type   &key_from_value(TimeSeriesOutput *value) const;
        void              _create(const key_type &key);

        void add_added_item(key_type key, value_type value);
        void add_modified_value(value_type value);
        void remove_value(key_type key);

      private:
        void _initialise();

        void         add_key_observer(TSDKeyObserver<key_type> *observer);
        void         remove_key_observer(TSDKeyObserver<key_type> *observer);
        key_set_type _key_set;
        map_type     _ts_values;

        reverse_map _reverse_ts_values;
        map_type    _modified_items;
        map_type    _added_items;
        map_type    _removed_values;  // This ensures we hold onto the values until we are sure no one needs to reference them.

        output_builder_ptr _ts_builder;
        output_builder_ptr _ts_ref_builder;

        FeatureOutputExtension<key_type>      _ref_ts_feature;
        std::vector<TSDKeyObserver<key_type>> _key_observers;
    };

    template <typename T_Key> struct TimeSeriesDictInput_T : TimeSeriesDictInput
    {
        using key_type = T_Key;
        using TimeSeriesDictInput::TimeSeriesDictInput;
        using map_type            = std::unordered_map<key_type, time_series_input_ptr>;
        using item_iterator       = map_type::iterator;
        using const_item_iterator = map_type::const_iterator;

        const_item_iterator begin() const;
        item_iterator       begin();

        const_item_iterator end() const;
        item_iterator       end();

        [[nodiscard]] size_t size() const override;

        [[nodiscard]] bool py_contains(const nb::object &item) const override;
        [[nodiscard]] bool contains(const key_type &item) const;

        [[nodiscard]] nb::object py_get_item(const nb::object &item) const override;
        [[nodiscard]] ts_type &operator[](const key_type &item);

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

        [[nodiscard]] bool py_was_modified(const nb::object &key) const override;
        [[nodiscard]] bool has_added() const override;
        [[nodiscard]] bool py_was_added(const nb::object &key) const override;
        [[nodiscard]] bool has_removed() const override;
        [[nodiscard]] bool py_was_removed(const nb::object &key) const override;
    };

    void tsd_register_with_nanobind(nb::module_ &m);

}  // namespace hgraph

#endif  // TSD_H
