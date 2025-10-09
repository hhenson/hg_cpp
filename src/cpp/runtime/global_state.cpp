//
// Created for hg_cpp
//

#include <hgraph/runtime/global_state.h>

namespace hgraph {

    GlobalState::GlobalState() : _previous(nullptr) {}

    GlobalState::GlobalState(const nb::dict& kwargs) : _previous(nullptr) {
        for (auto [key, value] : kwargs) {
            _state[nb::cast<std::string>(key)] = nb::cast<value_type>(value);
        }
    }

    GlobalState* GlobalState::__enter__() {
        // Replicate Python's GlobalState.__enter__() behavior
        // This allows C++ GlobalState to be used directly in with statements

        // For now, simplified implementation - just return self
        // The caller (Python's with statement via set_implementation_class) should
        // ensure GlobalState.set_instance() is called appropriately
        _previous = nullptr;
        return this;
    }

    void GlobalState::__exit__(const nb::object& exc_type, const nb::object& exc_val, const nb::object& exc_tb) {
        // Simplified implementation - just clear _previous
        // The caller (Python's with statement) should handle singleton restoration
        _previous = nullptr;
    }

    void GlobalState::set_previous(const ptr& previous) {
        _previous = previous;
    }

    GlobalState::ptr GlobalState::get_previous() const {
        return _previous;
    }

    GlobalState::value_type GlobalState::get(const std::string& key, const value_type& default_value) const {
        auto it = _state.find(key);
        if (it != _state.end()) {
            return it->second;
        }
        if (_previous) {
            return _previous->get(key, default_value);
        }
        return default_value;
    }

    GlobalState::value_type GlobalState::setdefault(const std::string& key, const value_type& default_value) {
        auto it = _state.find(key);
        if (it != _state.end()) {
            return it->second;
        }
        if (_previous) {
            return _previous->setdefault(key, default_value);
        }
        _state[key] = default_value;
        return default_value;
    }

    bool GlobalState::contains(const std::string& key) const {
        return __contains__(key);
    }

    GlobalState::value_type GlobalState::pop(const std::string& key, const value_type& default_value) {
        auto it = _state.find(key);
        if (it != _state.end()) {
            value_type value = it->second;
            _state.erase(it);
            return value;
        }
        return default_value;
    }

    nb::list GlobalState::keys() const {
        nb::list result;
        for (const auto& [key, _] : _state) {
            result.append(key);
        }
        return result;
    }

    nb::list GlobalState::values() const {
        nb::list result;
        for (const auto& [_, value] : _state) {
            result.append(value);
        }
        return result;
    }

    nb::list GlobalState::items() const {
        nb::list result;
        for (const auto& [key, value] : _state) {
            result.append(nb::make_tuple(key, value));
        }
        return result;
    }

    size_t GlobalState::len() const {
        if (!_previous) {
            return _state.size();
        }
        return _state.size() + _previous->len();
    }

    nb::dict GlobalState::get_combined_dict() const {
        nb::dict result;
        if (_previous) {
            result = _previous->get_combined_dict();
        }
        for (const auto& [key, value] : _state) {
            result[key.c_str()] = value;
        }
        return result;
    }

    GlobalState::value_type GlobalState::operator[](const std::string& key) const {
        auto it = _state.find(key);
        if (it == _state.end()) {
            if (_previous) {
                return (*_previous)[key];
            }
            throw nb::key_error(("Key not found: " + key).c_str());
        }
        return it->second;
    }

    void GlobalState::__setitem__(const std::string& key, const value_type& value) {
        _state[key] = value;
    }

    void GlobalState::__delitem__(const std::string& key) {
        auto it = _state.find(key);
        if (it == _state.end()) {
            if (_previous) {
                _previous->__delitem__(key);
            } else {
                throw nb::key_error(("Key not found: " + key).c_str());
            }
        } else {
            _state.erase(it);
        }
    }

    bool GlobalState::__contains__(const std::string& key) const {
        if (_state.find(key) != _state.end()) {
            return true;
        }
        return _previous ? _previous->__contains__(key) : false;
    }

    GlobalState::value_type GlobalState::__getattr__(const std::string& name) const {
        auto it = _state.find(name);
        if (it == _state.end()) {
            if (_previous) {
                return _previous->__getattr__(name);
            }
            throw nb::attribute_error(("Attribute not found: " + name).c_str());
        }
        return it->second;
    }

    void GlobalState::__setattr__(const std::string& name, const value_type& value) {
        if (name == "_state" || name == "_previous") {
            throw std::runtime_error("Cannot set internal attribute: " + name);
        }
        _state[name] = value;
    }

    void GlobalState::__delattr__(const std::string& name) {
        auto it = _state.find(name);
        if (it == _state.end()) {
            if (_previous) {
                _previous->__delattr__(name);
            } else {
                throw nb::attribute_error(("Attribute not found: " + name).c_str());
            }
        } else {
            _state.erase(it);
        }
    }

    std::string GlobalState::__repr__() const {
        std::string result = "GlobalState({";
        bool first = true;
        for (const auto& [key, value] : _state) {
            if (!first) result += ", ";
            first = false;
            result += "'" + key + "': " + nb::cast<std::string>(nb::repr(value));
        }
        result += "}";
        if (_previous) {
            result += ", previous=" + _previous->__repr__();
        }
        result += ")";
        return result;
    }

    std::string GlobalState::__str__() const {
        return nb::cast<std::string>(nb::str(get_combined_dict()));
    }

    bool GlobalState::__bool__() const {
        return !_state.empty() || (_previous && _previous->__bool__());
    }

    void GlobalState::register_with_nanobind(nb::module_& m) {
        auto cls = nb::class_<GlobalState>(m, "GlobalState")
            // Support zero-arg construction
            .def(nb::init<>())
            // Support dict construction
            .def(nb::init<const nb::dict&>())
            // Support **kwargs construction - custom __init__ that accepts kwargs
            .def("__init__", [](GlobalState* self, nb::kwargs kwargs) {
                // First construct the object in-place
                new (self) GlobalState();
                // Then populate with kwargs
                if (kwargs.size() > 0) {
                    for (auto [key, value] : kwargs) {
                        self->__setitem__(nb::cast<std::string>(key), nb::cast<nb::object>(value));
                    }
                }
            })
            // Context manager protocol - minimal implementation
            // Python's GlobalState class wraps these to add singleton management
            .def("__enter__", &GlobalState::__enter__)
            .def("__exit__", &GlobalState::__exit__,
                 "exc_type"_a.none(), "exc_val"_a.none(), "exc_tb"_a.none())
            // Helper methods for Python to manage nested contexts
            .def("set_previous", &GlobalState::set_previous, "previous"_a.none())
            .def("get_previous", &GlobalState::get_previous)
            .def("get", &GlobalState::get, "key"_a, "default"_a = nb::none())
            .def("setdefault", &GlobalState::setdefault, "key"_a, "default"_a)
            .def("__contains__", &GlobalState::__contains__, "key"_a)
            .def("pop", &GlobalState::pop, "key"_a, "default"_a = nb::none())
            .def("keys", &GlobalState::keys)
            .def("values", &GlobalState::values)
            .def("items", &GlobalState::items)
            .def("__len__", &GlobalState::len)
            .def("__getitem__", &GlobalState::operator[], "key"_a)
            .def("__setitem__", &GlobalState::__setitem__, "key"_a, "value"_a)
            .def("__delitem__", &GlobalState::__delitem__, "key"_a)
            .def("__getattr__", &GlobalState::__getattr__, "name"_a)
            .def("__setattr__", &GlobalState::__setattr__, "name"_a, "value"_a)
            .def("__delattr__", &GlobalState::__delattr__, "name"_a)
            .def("__repr__", &GlobalState::__repr__)
            .def("__str__", &GlobalState::__str__)
            .def("__bool__", &GlobalState::__bool__);
    }

}  // namespace hgraph
