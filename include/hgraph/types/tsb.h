
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

        explicit TimeSeriesBundle(const node_ptr &parent, TimeSeriesSchema::ptr schema)
            : T_TS(parent), _schema{std::move(schema)} {}
        explicit TimeSeriesBundle(const TimeSeriesType::ptr &parent, TimeSeriesSchema::ptr schema)
            : T_TS(parent), _schema{std::move(schema)} {}
        TimeSeriesBundle(const TimeSeriesBundle &)            = default;
        TimeSeriesBundle(TimeSeriesBundle &&)                 = default;
        TimeSeriesBundle &operator=(const TimeSeriesBundle &) = default;
        TimeSeriesBundle &operator=(TimeSeriesBundle &&)      = default;
        ~TimeSeriesBundle() override                          = default;

        [[nodiscard]] nb::object py_value() const override {
            return py_value_with_constraint<false>([](const ts_type &ts) { return ts.valid(); });
        }

        [[nodiscard]] nb::object py_delta_value() const override {
            return py_value_with_constraint<true>([](const ts_type &ts) { return ts.modified(); });
        }

        // Default iterator iterates over keys to keep this more consistent with Python (c.f. dict)
        [[nodiscard]] raw_key_const_iterator begin() const { return _schema->keys().begin(); }

        [[nodiscard]] raw_key_const_iterator end() const { return _schema->keys().end(); }

        using index_ts_type::operator[];

        [[nodiscard]] ts_type::ptr &operator[](const std::string &key) {
            // Return the value of the ts_bundle for the schema key instance.
            auto it{std::ranges::find(_schema->keys(), key)};
            if (it != _schema->keys().end()) {
                size_t index{static_cast<size_t>(std::distance(_schema->keys().begin(), it))};
                return this->operator[](index);
            }
            throw std::out_of_range("Key not found in TimeSeriesSchema");
        }

        [[nodiscard]] const ts_type::ptr &operator[](const std::string &key) const {
            return const_cast<bundle_type *>(this)->operator[](key);
        }

        [[nodiscard]] bool contains(const std::string &key) const {
            return std::ranges::find(_schema->keys(), key) != _schema->keys().end();
        }

        [[nodiscard]] const TimeSeriesSchema &schema() const { return *_schema; }

        [[nodiscard]]  TimeSeriesSchema &schema()  { return *_schema; }

        // Retrieves valid keys
        [[nodiscard]] key_collection_type keys() const { return {_schema->keys().begin(), _schema->keys().end()}; }

        [[nodiscard]] key_collection_type valid_keys() const {
            return keys_with_constraint([](const ts_type &ts) -> bool { return ts.valid(); });
        }

        [[nodiscard]] key_collection_type modified_keys() const {
            return keys_with_constraint([](const ts_type &ts) -> bool { return ts.modified(); });
        }

        using index_ts_type::size;
        // Retrieves valid items
        [[nodiscard]] key_value_collection_type items() {
            key_value_collection_type result;
            result.reserve(this->size());
            for (size_t i = 0; i < this->size(); ++i) { result.emplace_back(schema().keys()[i], operator[](i)); }
            return result;
        }

        [[nodiscard]] key_value_collection_type items() const { return const_cast<bundle_type *>(this)->items(); }

        [[nodiscard]] key_value_collection_type valid_items() {
            auto index_result{this->items_with_constraint([](const ts_type &ts) -> bool { return ts.valid(); })};
            key_value_collection_type result;
            result.reserve(index_result.size());
            for (auto &[ndx, ts] : index_result) { result.emplace_back(schema().keys()[ndx], ts); }
            return result;
        }

        [[nodiscard]] key_value_collection_type valid_items() const { return const_cast<bundle_type *>(this)->valid_items(); }

        [[nodiscard]] key_value_collection_type modified_items() {
            auto index_result{this->items_with_constraint([](const ts_type &ts) -> bool { return ts.modified(); })};
            key_value_collection_type result;
            result.reserve(index_result.size());
            for (auto &[ndx, ts] : index_result) { result.emplace_back(schema().keys()[ndx], ts); }
            return result;
        }
        [[nodiscard]] key_value_collection_type modified_items() const { return const_cast<bundle_type *>(this)->modified_items(); }

      protected:
        using T_TS::index_with_constraint;
        using T_TS::ts_values;

        template <bool is_delta> nb::object py_value_with_constraint(const std::function<bool(const ts_type &)> &constraint) const {
            nb::dict out;
            auto     include_nones{!_schema->scalar_type().is_none()};
            for (size_t i = 0, l = ts_values().size(); i < l; ++i) {
                if (auto ts{ts_values()[i]}; constraint(*ts)) {
                    if constexpr (is_delta) {
                        out[_schema->keys()[i].c_str()] = ts->py_delta_value();
                    } else {
                        out[_schema->keys()[i].c_str()] =  ts->py_value();
                    }
                } else if (include_nones) {
                    out[_schema->keys()[i].c_str()] = nb::none();
                }
            }

            if (!include_nones) { return out; }
            return nb::cast<nb::object>(_schema->scalar_type()(**out));
        }

        // Retrieves keys that match the constraint
        [[nodiscard]] key_collection_type keys_with_constraint(const std::function<bool(const ts_type &)> &constraint) const {
            auto                      index_results = index_with_constraint(constraint);
            std::vector<c_string_ref> result;
            result.reserve(index_results.size());
            for (auto i : index_results) { result.emplace_back(_schema->keys()[i]); }
            return result;
        }

        // Retrieves the items that match the constraint
        [[nodiscard]] key_value_collection_type
        key_value_with_constraint(const std::function<bool(const ts_type &)> &constraint) const {
            auto                      index_results = items_with_constraint(constraint);
            key_value_collection_type result;
            result.reserve(index_results.size());
            for (auto &[ndx, ts] : index_results) { result.emplace_back(_schema->keys()[ndx], ts); }
            return result;
        }

      private:
        TimeSeriesSchema::ptr _schema;
    };

    struct TimeSeriesBundleOutput : TimeSeriesBundle<IndexedTimeSeriesOutput>
    {
        using ptr = nb::ref<TimeSeriesBundleOutput>;
        using bundle_type::TimeSeriesBundle;

        void apply_result(nb::object value) override;

        static void register_with_nanobind(nb::module_ &m);

      protected:
        using bundle_type::set_ts_values;
        friend TimeSeriesBundleOutputBuilder;
    };

    struct TimeSeriesBundleInput : TimeSeriesBundle<IndexedTimeSeriesInput>
    {
        using ptr = nb::ref<TimeSeriesBundleInput>;
        using bundle_type::TimeSeriesBundle;

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
