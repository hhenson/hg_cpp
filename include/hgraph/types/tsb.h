
#ifndef TSB_H
#define TSB_H

#include <hgraph/types/ts_indexed.h>

namespace hgraph
{

    struct TimeSeriesBundleOutputBuilder;
    struct TimeSeriesBundleInputBuilder;

    struct TimeSeriesSchema : nb::intrusive_base
    {
        using ptr = nb::ref<TimeSeriesSchema>;

        explicit TimeSeriesSchema(std::vector<std::string> keys);
        explicit TimeSeriesSchema(std::vector<std::string> keys, nb::object type);

        const std::vector<std::string> &keys() const;
        const nb::object               &scalar_type() const;

        static void register_with_nanobind(nb::module_ &m);

      private:
        std::vector<std::string> _keys;
        nb::object               _scalar_type;
    };

    template <typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    struct TimeSeriesBundle : T_TS
    {
        using bundle_type = TimeSeriesBundle<T_TS>;
        using ptr         = nb::ref<bundle_type>;
        using typename T_TS::index_ts_type;
        using typename T_TS::ts_type;

        using key_collection_type     = std::vector<c_string_ref>;
        using raw_key_collection_type = std::vector<std::string>;
        using raw_key_iterator        = raw_key_collection_type::iterator;
        using raw_key_const_iterator  = raw_key_collection_type::const_iterator;
        using key_iterator            = key_collection_type::iterator;
        using key_const_iterator      = key_collection_type::const_iterator;

        using key_value_collection_type = std::vector<std::pair<c_string_ref, typename ts_type::ptr>>;

        explicit TimeSeriesBundle(const node_ptr &parent, TimeSeriesSchema::ptr schema);
        explicit TimeSeriesBundle(const TimeSeriesType::ptr &parent, TimeSeriesSchema::ptr schema);
        TimeSeriesBundle(const TimeSeriesBundle &)            = default;
        TimeSeriesBundle(TimeSeriesBundle &&)                 = default;
        TimeSeriesBundle &operator=(const TimeSeriesBundle &) = default;
        TimeSeriesBundle &operator=(TimeSeriesBundle &&)      = default;
        ~TimeSeriesBundle() override                          = default;

        [[nodiscard]] nb::object py_value() const override;

        [[nodiscard]] nb::object py_delta_value() const override;

        // Default iterator iterates over keys to keep this more consistent with Python (c.f. dict)
        [[nodiscard]] raw_key_const_iterator begin() const;

        [[nodiscard]] raw_key_const_iterator end() const;

        using index_ts_type::operator[];

        [[nodiscard]] typename ts_type::ptr &operator[](const std::string &key);

        [[nodiscard]] const typename ts_type::ptr &operator[](const std::string &key) const;

        [[nodiscard]] bool contains(const std::string &key) const;

        [[nodiscard]] const TimeSeriesSchema &schema() const;

        [[nodiscard]]  TimeSeriesSchema &schema();

        // Retrieves valid keys
        [[nodiscard]] key_collection_type keys() const;

        [[nodiscard]] key_collection_type valid_keys() const;

        [[nodiscard]] key_collection_type modified_keys() const;

        using index_ts_type::size;
        // Retrieves valid items
        [[nodiscard]] key_value_collection_type items();

        [[nodiscard]] key_value_collection_type items() const;

        [[nodiscard]] key_value_collection_type valid_items();

        [[nodiscard]] key_value_collection_type valid_items() const;

        [[nodiscard]] key_value_collection_type modified_items();
        [[nodiscard]] key_value_collection_type modified_items() const;

        [[nodiscard]] bool has_reference() const override;

      protected:
        using T_TS::index_with_constraint;
        using T_TS::ts_values;

        template <bool is_delta> nb::object py_value_with_constraint(const std::function<bool(const ts_type &)> &constraint) const;

        // Retrieves keys that match the constraint
        [[nodiscard]] key_collection_type keys_with_constraint(const std::function<bool(const ts_type &)> &constraint) const;

        // Retrieves the items that match the constraint
        [[nodiscard]] key_value_collection_type
        key_value_with_constraint(const std::function<bool(const ts_type &)> &constraint) const;

      private:
        TimeSeriesSchema::ptr _schema;
    };

    struct TimeSeriesBundleOutput : TimeSeriesBundle<IndexedTimeSeriesOutput>
    {
        using ptr = nb::ref<TimeSeriesBundleOutput>;
        using bundle_type::TimeSeriesBundle;

        void py_set_value(nb::object value) override;
        void mark_invalid() override;
        void apply_result(nb::object value) override;
        [[nodiscard]] bool can_apply_result(nb::object value) override;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override;

        static void register_with_nanobind(nb::module_ &m);

      protected:
        using bundle_type::set_ts_values;
        friend TimeSeriesBundleOutputBuilder;
    };

    struct TimeSeriesBundleInput : TimeSeriesBundle<IndexedTimeSeriesInput>
    {
        using ptr = nb::ref<TimeSeriesBundleInput>;
        using bundle_type::TimeSeriesBundle;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override;

        // Static method for nanobind registration
        static void register_with_nanobind(nb::module_ &m);

        // This is used by the nested graph infra to rewrite the stub inputs when we build the nested graphs.
        // The general pattern in python was copy_with(node, ts=...)
        // To keep the code in sync for now, will keep this, but there is probably a better way to implement this going forward.
        ptr copy_with(const node_ptr &parent, collection_type ts_values);

      protected:
        using bundle_type::set_ts_values;
        friend TimeSeriesBundleInputBuilder;
    };

}  // namespace hgraph

#endif  // TSB_H
