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
        if (!REMOVE_IF_EXISTS.is_valid()) {
            REMOVE_IF_EXISTS = nb::module_::import_("hgraph").attr("REMOVE_IF_EXISTS");
        }
        return REMOVE_IF_EXISTS;
    }

}  // namespace hgraph