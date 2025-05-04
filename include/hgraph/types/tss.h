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
}  // namespace hgraph

#endif  // TSS_H
