//
// Created for hg_cpp
//

#ifndef GLOBAL_STATE_H
#define GLOBAL_STATE_H

#include <hgraph/hgraph_base.h>
#include <unordered_map>
#include <string>
#include <memory>

namespace hgraph {

    /**
     * GlobalState provides a global state dictionary that can be accessed by all components of the graph.
     * Supports nested contexts via context manager protocol (__enter__/__exit__).
     * Matches the Python GlobalState implementation.
     */
    class HGRAPH_EXPORT GlobalState : public nb::intrusive_base {
    public:
        using value_type = nb::object;
        using ptr = nb::ref<GlobalState>;

        // Singleton access
        static ptr instance();
        static void set_instance(const ptr& instance);
        static bool has_instance();
        static void reset();

        // Constructor
        GlobalState();
        explicit GlobalState(const nb::dict& kwargs);

        // Context manager protocol
        ptr __enter__();
        void __exit__(const nb::object& exc_type, const nb::object& exc_val, const nb::object& exc_tb);

        // Dictionary-like interface
        value_type get(const std::string& key, const value_type& default_value = nb::none()) const;
        value_type setdefault(const std::string& key, const value_type& default_value);
        bool contains(const std::string& key) const;
        value_type pop(const std::string& key, const value_type& default_value = nb::none());

        // Dict methods
        nb::list keys() const;
        nb::list values() const;
        nb::list items() const;
        size_t len() const;
        nb::dict get_combined_dict() const;

        // Operator overloads for dict-like syntax
        value_type operator[](const std::string& key) const;
        void __setitem__(const std::string& key, const value_type& value);
        void __delitem__(const std::string& key);
        bool __contains__(const std::string& key) const;

        // Attribute access
        value_type __getattr__(const std::string& name) const;
        void __setattr__(const std::string& name, const value_type& value);
        void __delattr__(const std::string& name);

        // String representation
        std::string __repr__() const;
        std::string __str__() const;
        bool __bool__() const;

        static void register_with_nanobind(nb::module_& m);

    private:
        std::unordered_map<std::string, value_type> _state;
        ptr _previous;

        static ptr _instance;
    };

}  // namespace hgraph

#endif  // GLOBAL_STATE_H
