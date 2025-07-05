#include <hgraph/types/constants.h>

namespace hgraph
{

    static nb::object REMOVE;
    static nb::object REMOVE_IF_EXISTS;

    nb::object get_remove() {
        if (!REMOVE.is_valid()) {
            REMOVE = nb::module_::import_("hgraph").attr("REMOVE");
        }
        return REMOVE;
    }

    nb::object get_remove_if_exists() {
        if (!REMOVE_IF_EXISTS.is_valid()) { REMOVE_IF_EXISTS = nb::module_::import_("hgraph").attr("REMOVE_IF_EXISTS"); }
        return REMOVE_IF_EXISTS;
    }

    static nb::object REMOVED;
    nb::object        get_removed() {
        if (!REMOVED.is_valid()) { REMOVED = nb::module_::import_("hgraph").attr("Removed"); }
        return REMOVED;
    }

    static nb::object FROZENSET;
    nb::object        get_frozenset() {
        if (!FROZENSET.is_valid()) { FROZENSET = nb::module_::import_("builtins").attr("frozenset"); }
        return FROZENSET;
    }

    static nb::object FROZENDICT;
    nb::object get_frozendict() {
        if (!FROZENDICT.is_valid()) { FROZENDICT = nb::module_::import_("frozendict").attr("frozendict"); }
        return FROZENDICT;
    }

}  // namespace hgraph