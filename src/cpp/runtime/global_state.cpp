//
// Created for hg_cpp
//

#include <hgraph/runtime/global_state.h>

namespace hgraph {

    GlobalState& GlobalState::instance() {
        static GlobalState instance;
        return instance;
    }

    GlobalState::value_type GlobalState::get(const std::string& key, const value_type& default_value) const {
        auto it = state_.find(key);
        if (it != state_.end()) {
            return it->second;
        }
        return default_value;
    }

    bool GlobalState::contains(const std::string& key) const {
        return state_.find(key) != state_.end();
    }

    void GlobalState::set(const std::string& key, const value_type& value) {
        state_[key] = value;
    }

    GlobalState::value_type GlobalState::pop(const std::string& key, const value_type& default_value) {
        auto it = state_.find(key);
        if (it != state_.end()) {
            value_type value = it->second;
            state_.erase(it);
            return value;
        }
        return default_value;
    }

    void GlobalState::erase(const std::string& key) {
        state_.erase(key);
    }

    GlobalState::value_type GlobalState::operator[](const std::string& key) const {
        return get(key);
    }

}  // namespace hgraph
