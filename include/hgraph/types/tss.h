//
// Created by Howard Henson on 04/05/2025.
//

#ifndef TSS_H
#define TSS_H

#include <hgraph/python/pyb_wiring.h>
#include <hgraph/types/time_series_type.h>
#include <unordered_set>

namespace hgraph
{

    struct SetDelta : nanobind::intrusive_base
    {
        using ptr = nb::ref<SetDelta>;

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
        using ptr = nb::ref<SetDelta_Object>;

        SetDelta_Object(nb::object added_elements, nb::object removed_elements);

        [[nodiscard]] virtual nb::object py_added_elements() const;
        [[nodiscard]] virtual nb::object py_removed_elements() const;

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
    class TimeSeriesSet : T_TS
    {
      public:
        [[nodiscard]] virtual bool              py_contains(const nb::object &item) const    = 0;
        [[nodiscard]] virtual size_t            size() const                                 = 0;
        [[nodiscard]] virtual const nb::object &py_values() const                            = 0;
        [[nodiscard]] virtual const nb::object &py_added() const                             = 0;
        [[nodiscard]] virtual bool              py_was_added(const nb::object &item) const   = 0;
        [[nodiscard]] virtual const nb::object &py_removed() const                           = 0;
        [[nodiscard]] virtual bool              py_was_removed(const nb::object &item) const = 0;
    };

    void tss_register_with_nanobind(nb::module_ &m);

}  // namespace hgraph

#endif  // TSS_H
