//
// Created for hg_cpp
//

#ifndef GLOBAL_STATE_H
#define GLOBAL_STATE_H

#include <hgraph/hgraph_base.h>
#include <unordered_map>
#include <string>

namespace hgraph {

    /**
     * GlobalState is a singleton dictionary for storing global state across the application.
     * Used by components like ComponentNode and MeshNode to track instance registration.
     */
    class HGRAPH_EXPORT GlobalState {
    public:
        using value_type = nb::object;

        static GlobalState& instance();

        // Dictionary-like interface
        value_type get(const std::string& key, const value_type& default_value = nb::none()) const;
        bool contains(const std::string& key) const;
        void set(const std::string& key, const value_type& value);
        value_type pop(const std::string& key, const value_type& default_value = nb::none());
        void erase(const std::string& key);

        // Operator overloads for dict-like syntax
        value_type operator[](const std::string& key) const;

    private:
        GlobalState() = default;
        ~GlobalState() = default;
        GlobalState(const GlobalState&) = delete;
        GlobalState& operator=(const GlobalState&) = delete;

        std::unordered_map<std::string, value_type> state_;
    };

}  // namespace hgraph

#endif  // GLOBAL_STATE_H
