#include <hgraph/runtime/record_replay.h>

namespace hgraph
{

    std::string get_fq_recordable_id(const Traits &traits, const std::string &recordable_id) {
        auto parent_id{traits.get_trait_or(RECORDABLE_ID_TRAIT, nb::none())};
        if (parent_id.is_none()) {
            if (recordable_id.empty()) { throw std::runtime_error("No recordable id provided and no parent order id found"); }
            return recordable_id;
        } else {
            auto parent_id_str{nb::cast<std::string>(parent_id)};
            if (recordable_id.size() == 0) {
                return parent_id_str;
            } else {
                return parent_id_str + "." + recordable_id;
            }
        }
    }
}  // namespace hgraph
