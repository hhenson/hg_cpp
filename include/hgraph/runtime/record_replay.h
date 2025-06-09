
#ifndef RECORD_REPLAY_H
#define RECORD_REPLAY_H

#include <stdexcept>
#include <string>
#include <hgraph/types/traits.h>

namespace hgraph
{
    static const std::string RECORDABLE_ID_TRAIT = "recordable_id";

    /**
     * Resolves the recordable id by collecting the full path or recordable id's from this recordable_id to the
     * outer component graph.
     */
    std::string get_fq_recordable_id(const Traits &traits, const std::string &recordable_id = "");
}  // namespace hgraph

#endif //RECORD_REPLAY_H
