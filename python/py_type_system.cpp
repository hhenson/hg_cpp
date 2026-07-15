/**
 * Type-system bindings: type handles and constructors, scalar/size/type
 * patterns, ResolutionScope (the python DSL's wiring-time resolution window
 * onto the C++ type machinery), generic-target resolution, and
 * register_python_overload.
 */
#include "py_wiring.h"
#include "py_bindings.h"

namespace nb = nanobind;
using namespace hgraph;
using namespace hgraph::python_bridge;

namespace hgraph::python_bridge
{
    void bind_type_system(nb::module_ &m)
    {
    nb::class_<PyTsType>(m, "TsType")
        .def("__eq__", [](const PyTsType &self, nb::handle other) {
            return nb::isinstance<PyTsType>(other) && nb::cast<PyTsType &>(other).meta == self.meta;
        })
        .def("__hash__", [](const PyTsType &self) { return std::hash<const void *>{}(self.meta); })
        .def_prop_ro("kind", [](const PyTsType &self) { return static_cast<int>(self.meta->kind); })
        .def_prop_ro("value_kind",
                     [](const PyTsType &self) {
                         return self.meta->value_schema != nullptr
                                    ? static_cast<int>(self.meta->value_schema->value_kind())
                                    : -1;
                     })
        .def_prop_ro("fixed_size", [](const PyTsType &self) {
            return self.meta->kind == TSTypeKind::TSL ? self.meta->fixed_size() : 0;
        })
        .def_prop_ro("is_ts", [](const PyTsType &self) {
            return self.meta != nullptr && self.meta->kind == TSTypeKind::TS;
        })
        .def_prop_ro("is_tsd", [](const PyTsType &self) {
            return self.meta != nullptr && self.meta->kind == TSTypeKind::TSD;
        })
        .def_prop_ro("is_tsl", [](const PyTsType &self) {
            return self.meta != nullptr && self.meta->kind == TSTypeKind::TSL;
        })
        .def_prop_ro("is_tsb", [](const PyTsType &self) {
            return self.meta != nullptr && self.meta->kind == TSTypeKind::TSB;
        })
        .def_prop_ro("is_ref", [](const PyTsType &self) {
            return self.meta != nullptr && self.meta->kind == TSTypeKind::REF;
        })
        .def_prop_ro("is_fixed_tsl", [](const PyTsType &self) {
            return self.meta != nullptr && self.meta->kind == TSTypeKind::TSL && self.meta->fixed_size() > 0;
        })
        .def_prop_ro("is_ts_bundle", [](const PyTsType &self) {
            return self.meta != nullptr && self.meta->kind == TSTypeKind::TS &&
                   self.meta->value_schema != nullptr &&
                   self.meta->value_schema->value_kind() == ValueTypeKind::Bundle;
        })
        .def_prop_ro("is_ts_mapping", [](const PyTsType &self) {
            return self.meta != nullptr && self.meta->kind == TSTypeKind::TS &&
                   self.meta->value_schema != nullptr &&
                   self.meta->value_schema->value_kind() == ValueTypeKind::Map;
        })
        .def_prop_ro("is_tss", [](const PyTsType &self) {
            return self.meta != nullptr && self.meta->kind == TSTypeKind::TSS;
        })
        .def_prop_ro("is_ts_sequence", [](const PyTsType &self) {
            return self.meta != nullptr && self.meta->kind == TSTypeKind::TS &&
                   self.meta->value_schema != nullptr &&
                   (self.meta->value_schema->value_kind() == ValueTypeKind::Tuple ||
                    self.meta->value_schema->value_kind() == ValueTypeKind::List);
        })
        .def_prop_ro("is_ts_json", [](const PyTsType &self) {
            return stdlib::json_tree::is_json_ts(self.meta);
        })
        .def("__repr__", [](const PyTsType &self) {
            return self.meta != nullptr && !self.meta->name().empty() ? std::string{self.meta->name()}
                                                                      : std::string{"<ts?>"};
        });
    nb::class_<PyScalarPattern>(m, "ScalarPattern")
        .def("__repr__", [](const PyScalarPattern &self) {
            return scalar_pattern_to_string(self.pattern);
        });
    nb::class_<PySizePattern>(m, "SizePattern")
        .def("__repr__", [](const PySizePattern &self) {
            return self.variable ? "~" + self.name : std::to_string(self.value);
        });
    nb::class_<PyTypePattern>(m, "TypePattern")
        .def("__repr__", [](const PyTypePattern &self) {
            return ts_pattern_to_string(self.pattern);
        })
        .def_prop_ro("ts_kind", [](const PyTypePattern &self) -> int {
            // The TS KIND the pattern describes (structural introspection -
            // the python DSL never classifies by rendered labels). -1 = a
            // whole-time-series variable (kind unconstrained).
            switch (self.pattern.kind)
            {
                case TypePattern::Kind::Var: return -1;
                case TypePattern::Kind::Concrete:
                    return self.pattern.meta != nullptr ? static_cast<int>(self.pattern.meta->kind) : -1;
                case TypePattern::Kind::TS: return static_cast<int>(TSTypeKind::TS);
                case TypePattern::Kind::TSS: return static_cast<int>(TSTypeKind::TSS);
                case TypePattern::Kind::TSL: return static_cast<int>(TSTypeKind::TSL);
                case TypePattern::Kind::TSD: return static_cast<int>(TSTypeKind::TSD);
                case TypePattern::Kind::TSW: return static_cast<int>(TSTypeKind::TSW);
                case TypePattern::Kind::TSB: return static_cast<int>(TSTypeKind::TSB);
                case TypePattern::Kind::REF: return static_cast<int>(TSTypeKind::REF);
                case TypePattern::Kind::Signal: return static_cast<int>(TSTypeKind::SIGNAL);
            }
            return -1;
        });
    m.attr("TS_KIND_TS")  = static_cast<int>(TSTypeKind::TS);
    m.attr("TS_KIND_TSS") = static_cast<int>(TSTypeKind::TSS);
    m.attr("TS_KIND_TSL") = static_cast<int>(TSTypeKind::TSL);
    m.attr("TS_KIND_TSD") = static_cast<int>(TSTypeKind::TSD);
    m.attr("TS_KIND_TSB") = static_cast<int>(TSTypeKind::TSB);
    m.attr("TS_KIND_TSW") = static_cast<int>(TSTypeKind::TSW);

    m.def("ts_type", [](const std::string &name) {
        const auto *meta = TypeRegistry::instance().time_series_type(name);
        if (meta == nullptr) { throw nb::value_error(("unknown time-series type: " + name).c_str()); }
        return PyTsType{meta};
    });

    // --- time-series type CONSTRUCTION (the Python type layer builds
    // TS[...]/TSS[...]/TSD[...]/TSL[...]/TSB[...] through these) ---
    nb::class_<PyValueType>(m, "ValueType")
        .def("__eq__",
             [](const PyValueType &self, nb::handle other) {
                 if (!nb::isinstance<PyValueType>(other)) { return false; }
                 return self.meta == nb::cast<PyValueType &>(other).meta;   // metas are interned
             })
        .def("__hash__", [](const PyValueType &self) { return std::hash<const void *>{}(self.meta); })
        .def_prop_ro("name", [](const PyValueType &self) {
            return self.meta != nullptr ? std::string{self.meta->name()} : std::string{};
        })
        .def_prop_ro("namespace", [](const PyValueType &self) {
            return self.meta != nullptr ? std::string{self.meta->bundle_namespace()} : std::string{};
        })
        .def_prop_ro("local_name", [](const PyValueType &self) {
            return self.meta != nullptr ? std::string{self.meta->bundle_local_name()} : std::string{};
        })
        .def_prop_ro("fields", [](const PyValueType &self) {
            nb::list result;
            if (self.meta == nullptr) { return result; }
            for (std::size_t index = 0; index < self.meta->field_count; ++index)
            {
                const auto &field = self.meta->fields[index];
                result.append(nb::make_tuple(
                    std::string{field.name != nullptr ? field.name : ""},
                    PyValueType{field.type}));
            }
            return result;
        });
    m.def("value_type", [](const std::string &name) {
        const auto *meta = TypeRegistry::instance().value_type(name);
        if (meta == nullptr) { throw nb::value_error(("unknown value type: " + name).c_str()); }
        return PyValueType{meta};
    });
    m.def("ts", [](PyValueType v) { return PyTsType{TypeRegistry::instance().ts(v.meta)}; });
    m.def("ref_ts", [](PyTsType target) { return PyTsType{TypeRegistry::instance().ref(target.meta)}; });
    m.def("ref_target", [](PyTsType ref) { return PyTsType{TypeRegistry::instance().dereference(ref.meta)}; });
    m.def("set_vt", [](PyValueType e) { return PyValueType{TypeRegistry::instance().set(e.meta)}; });
    m.def("map_vt", [](PyValueType k, PyValueType v) {
        return PyValueType{TypeRegistry::instance().map(k.meta, v.meta)};
    });
    m.def("tuple_vt", [](PyValueType e) {
        // A homogeneous variadic tuple (python's tuple[X, ...]).
        return PyValueType{TypeRegistry::instance().list(e.meta, 0, true)};
    });
    m.def("nullable_tuple_vt", [](PyValueType e) {
        // A NULLABLE variadic tuple (elements may be None holes).
        return PyValueType{TypeRegistry::instance().nullable_tuple(e.meta)};
    });
    m.def("series_vt", [](PyValueType e) {
        return PyValueType{TypeRegistry::instance().series(e.meta)};
    });
    m.def("un_named_bundle_vt", [](nb::list fields) {
        // The structural (un-named) bundle - python's compound_scalar()
        // anonymous compounds (nominal-vs-structural rule, scalar.rst).
        std::vector<std::pair<std::string, const ValueTypeMetaData *>> field_metas;
        field_metas.reserve(nb::len(fields));
        for (nb::handle field : fields)
        {
            auto pair = nb::cast<nb::tuple>(field);
            field_metas.emplace_back(nb::cast<std::string>(pair[0]),
                                     nb::cast<PyValueType &>(pair[1]).meta);
        }
        return PyValueType{TypeRegistry::instance().un_named_bundle(field_metas)};
    });
    m.def("frame_vt", [](PyValueType schema) {
        // Frame[Schema]: the typed frame meta carrying its column bundle.
        return PyValueType{TypeRegistry::instance().frame(schema.meta)};
    });
    m.def("table_schema_info", [](PyTsType ts, const std::string &date_key, const std::string &as_of_key) {
        // TABLE layout introspection (design record step 6): the C++ layout
        // is the single source; python's TableSchema maps it declaratively.
        const auto &layout =
            hgraph::stdlib::table_ts_detail::ts_table_layout(ts.meta, date_key, as_of_key);
        nb::dict info;
        nb::list keys, types, partition_keys, removed_keys;
        for (std::size_t i = 0; i < layout.keys.size(); ++i)
        {
            keys.append(nb::str(layout.keys[i].c_str()));
            const auto *meta = layout.col_metas[i];
            types.append(nb::str(meta != nullptr && meta->header.label != nullptr ? meta->header.label : "?"));
        }
        for (const auto &name : layout.partition_keys) { partition_keys.append(nb::str(name.c_str())); }
        for (const auto &name : layout.removed_keys) { removed_keys.append(nb::str(name.c_str())); }
        info["keys"]           = keys;
        info["types"]          = types;
        info["partition_keys"] = partition_keys;
        info["removed_keys"]   = removed_keys;
        info["date_key"]       = nb::str(layout.date_key.c_str());
        info["as_of_key"]      = nb::str(layout.as_of_key.c_str());
        info["is_multi_row"]   = layout.is_multi_row;
        return info;
    });
    m.def("fixed_tuple_vt", [](nb::list elements) {
        std::vector<const ValueTypeMetaData *> metas;
        metas.reserve(nb::len(elements));
        for (nb::handle element : elements) { metas.push_back(nb::cast<PyValueType &>(element).meta); }
        return PyValueType{TypeRegistry::instance().tuple(metas)};
    });
    // Type INTROSPECTION for wiring-time target inference (py convert etc.).
    m.def("ts_value_vt", [](PyTsType t) { return PyValueType{t.meta->value_schema}; });
    m.def("vt_kind", [](PyValueType v) { return static_cast<int>(v.meta->value_kind()); });
    m.def("vt_element", [](PyValueType v) { return PyValueType{v.meta->element_type}; });
    m.def("vt_key", [](PyValueType v) { return PyValueType{v.meta->key_type}; });
    m.def("tsd_key_vt", [](PyTsType t) { return PyValueType{t.meta->key_type()}; });
    m.def("tsb_value_vt", [](PyTsType t) { return PyValueType{t.meta->value_schema}; });
    m.def("tsl_element_ts", [](PyTsType t) { return PyTsType{t.meta->element_ts()}; });
    m.def("tss", [](PyValueType v) { return PyTsType{TypeRegistry::instance().tss(v.meta)}; });
    m.def("tsd", [](PyValueType k, PyTsType v) { return PyTsType{TypeRegistry::instance().tsd(k.meta, v.meta)}; });
    m.def("tsw", [](PyValueType v, std::size_t period, std::size_t min_period) {
        return PyTsType{TypeRegistry::instance().tsw(v.meta, period, min_period)};
    }, nb::arg("value"), nb::arg("period"), nb::arg("min_period") = 0);
    // The scalar-level JSON builders (hgraph's to_json_builder /
    // from_json_builder): serialize/parse a plain value by its schema.
    m.def("value_to_json", [](PyValueType meta, nb::handle value) {
        const Value owned = python_bridge::py_to_value_as(value, meta.meta);
        return to_json_string(owned.view());
    });
    m.def("value_from_json", [](PyValueType meta, const std::string &text) {
        const auto realization = TypeRealizationSnapshot::capture(TypeRegistry::instance());
        TypeRealizationScope scope{realization.get()};
        const Value parsed = from_json_string(meta.meta, text);
        return python_bridge::value_to_py(parsed.view());
    });
    m.def("enum_vt", [](const std::string &name, nb::list members, nb::object cls) {
        std::vector<std::pair<std::string, long long>> table;
        table.reserve(nb::len(members));
        for (nb::handle item : members)
        {
            auto pair = nb::cast<nb::tuple>(item);
            table.emplace_back(nb::cast<std::string>(pair[0]), nb::cast<long long>(pair[1]));
        }
        const auto *meta = TypeRegistry::instance().enum_type(name, table);
        python_bridge::enum_class_registry()[meta] = std::move(cls);
        return PyValueType{meta};
    });
    m.def("tsw_duration", [](PyValueType v, TimeDelta time_range, TimeDelta min_time_range) {
        return PyTsType{TypeRegistry::instance().tsw_duration(v.meta, time_range, min_time_range)};
    }, nb::arg("value"), nb::arg("time_range"), nb::arg("min_time_range") = TimeDelta{0});
    m.def("tsl", [](PyTsType e, std::size_t size) { return PyTsType{TypeRegistry::instance().tsl(e.meta, size)}; },
          nb::arg("element"), nb::arg("size") = 0);
    m.def("tsb", [](const std::string &name, nb::list fields) {
        std::vector<std::pair<std::string, const TSValueTypeMetaData *>> entries;
        entries.reserve(nb::len(fields));
        for (nb::handle field : fields)
        {
            auto pair = nb::cast<nb::tuple>(field);
            entries.emplace_back(nb::cast<std::string>(pair[0]), nb::cast<PyTsType &>(pair[1]).meta);
        }
        return PyTsType{TypeRegistry::instance().tsb(name, entries)};
    });

    m.def("scalar_pattern_var", [](const std::string &name) {
        return PyScalarPattern{ScalarPattern::var(name)};
    });
    m.def("scalar_pattern_var", [](const std::string &name, nb::list constraints) {
        std::vector<const ValueTypeMetaData *> metas;
        metas.reserve(nb::len(constraints));
        for (nb::handle constraint : constraints)
        {
            metas.push_back(nb::cast<PyValueType &>(constraint).meta);
        }
        return PyScalarPattern{ScalarPattern::var(name, std::move(metas))};
    });
    m.def("scalar_pattern_value", [](PyValueType value) {
        return PyScalarPattern{ScalarPattern::concrete(value.meta)};
    });
    m.def("scalar_pattern_unknown_tuple", [] {
        return PyScalarPattern{ScalarPattern::unknown_tuple()};
    });
    m.def("scalar_pattern_unknown_tuple", [](PyScalarPattern element) {
        return PyScalarPattern{ScalarPattern::unknown_tuple(std::move(element.pattern))};
    });
    m.def("scalar_pattern_homogeneous_tuple", [](PyScalarPattern element) {
        return PyScalarPattern{ScalarPattern::homogeneous_tuple(std::move(element.pattern))};
    });
    m.def("scalar_pattern_fixed_tuple", [](nb::list elements) {
        std::vector<ScalarPattern> patterns;
        patterns.reserve(nb::len(elements));
        for (nb::handle element : elements) { patterns.push_back(nb::cast<PyScalarPattern>(element).pattern); }
        return PyScalarPattern{ScalarPattern::fixed_tuple(std::move(patterns))};
    });
    m.def("scalar_pattern_set", [](PyScalarPattern element) {
        return PyScalarPattern{ScalarPattern::set(std::move(element.pattern))};
    });
    m.def("scalar_pattern_map", [](PyScalarPattern key, PyScalarPattern value) {
        return PyScalarPattern{ScalarPattern::map(std::move(key.pattern), std::move(value.pattern))};
    });
    m.def("scalar_pattern_bundle", [] {
        return PyScalarPattern{ScalarPattern::bundle()};
    });
    m.def("scalar_pattern_bundle", [](const std::string &schema_variable) {
        return PyScalarPattern{ScalarPattern::bundle_var(schema_variable)};
    });

    m.def("size_pattern_var", [](const std::string &name) {
        return PySizePattern{true, name, 0};
    });
    m.def("size_pattern_value", [](std::size_t value) {
        return PySizePattern{false, {}, value};
    });

    // ------------------------------------------------------------------
    // ResolutionScope: the python DSL's wiring-time resolution window onto
    // the C++ type/pattern machinery (type_resolution.h). Input patterns
    // match against wired port schemas accumulating type-variable bindings;
    // outputs resolve from the same map. Python adds NO parallel classifier
    // (ruling 2026-07-11: identify the intent, serve it from the C++ type
    // system).
    // ------------------------------------------------------------------
    {
        nb::class_<PyResolutionScope>(m, "ResolutionScope")
            .def(nb::init<>())
            .def("match",
                 [](PyResolutionScope &self, PyTypePattern pattern, PyTsType actual) {
                     return input_ts_pattern_match(pattern.pattern, actual.meta, self.map);
                 },
                 nb::arg("pattern"), nb::arg("actual"))
            .def("resolve_ts",
                 [](PyResolutionScope &self, PyTypePattern pattern) -> std::optional<PyTsType> {
                     try
                     {
                         const auto *meta = ts_pattern_resolve(pattern.pattern, self.map);
                         if (meta == nullptr) { return std::nullopt; }
                         return PyTsType{meta};
                     }
                     catch (const std::exception &)
                     {
                         return std::nullopt;   // unresolved variables remain
                     }
                 },
                 nb::arg("pattern"))
            .def("bind_ts", [](PyResolutionScope &self, const std::string &name, PyTsType meta) {
                self.map.bind_ts(name, meta.meta);
            })
            .def("bind_scalar", [](PyResolutionScope &self, const std::string &name, PyValueType meta) {
                self.map.bind_scalar(name, meta.meta);
            })
            .def("bind_size", [](PyResolutionScope &self, const std::string &name, std::size_t size) {
                self.map.bind_size(name, size);
            })
            .def("find_ts",
                 [](const PyResolutionScope &self, const std::string &name) -> std::optional<PyTsType> {
                     const auto *meta = self.map.find_ts(name);
                     if (meta == nullptr) { return std::nullopt; }
                     return PyTsType{meta};
                 })
            .def("find_scalar",
                 [](const PyResolutionScope &self, const std::string &name) -> std::optional<PyValueType> {
                     const auto *meta = self.map.find_scalar(name);
                     if (meta == nullptr) { return std::nullopt; }
                     return PyValueType{meta};
                 })
            .def("find_size",
                 [](const PyResolutionScope &self, const std::string &name) -> std::optional<std::size_t> {
                     return self.map.find_size(name);
                 })
            .def_prop_ro("bindings", [](const PyResolutionScope &self) {
                // The resolver-lambda ``mapping`` argument: every bound
                // variable by name (ts handles, scalar metas, sizes).
                nb::dict out;
                for (const auto &[name, meta] : self.map.ts_vars) { out[nb::str(name.c_str())] = PyTsType{meta}; }
                for (const auto &[name, meta] : self.map.scalar_vars)
                {
                    out[nb::str(name.c_str())] = PyValueType{meta};
                }
                for (const auto &[name, size] : self.map.size_vars) { out[nb::str(name.c_str())] = size; }
                return out;
            });
    }

    m.def("type_pattern_var", [](const std::string &name) {
        return PyTypePattern{TypePattern::var(name)};
    });
    m.def("type_pattern_var", [](const std::string &name, nb::list constraints) {
        std::vector<const TSValueTypeMetaData *> metas;
        metas.reserve(nb::len(constraints));
        for (nb::handle constraint : constraints)
        {
            metas.push_back(nb::cast<PyTsType &>(constraint).meta);
        }
        return PyTypePattern{TypePattern::var(name, std::move(metas))};
    });
    m.def("type_pattern_concrete", [](PyTsType type) {
        return PyTypePattern{TypePattern::concrete(type.meta)};
    });
    m.def("type_pattern_ts", [](PyScalarPattern value) {
        return PyTypePattern{TypePattern::ts(std::move(value.pattern))};
    });
    m.def("type_pattern_tss", [] {
        return PyTypePattern{TypePattern::tss(ScalarPattern::var("T"))};
    });
    m.def("type_pattern_tss", [](PyScalarPattern element) {
        return PyTypePattern{TypePattern::tss(std::move(element.pattern))};
    });
    m.def("type_pattern_tsd", [] {
        return PyTypePattern{TypePattern::tsd(ScalarPattern::var("K"), TypePattern::var("V"))};
    });
    m.def("type_pattern_tsd", [](PyScalarPattern key, PyTypePattern value) {
        return PyTypePattern{TypePattern::tsd(std::move(key.pattern), std::move(value.pattern))};
    });
    m.def("type_pattern_tsl", [] {
        return PyTypePattern{TypePattern::tsl_var(TypePattern::var("V"), "SIZE")};
    });
    m.def("type_pattern_tsl", [](PyTypePattern element, PySizePattern size) {
        TypePattern pattern = size.variable
                                  ? TypePattern::tsl_var(std::move(element.pattern), size.name)
                                  : TypePattern::tsl(std::move(element.pattern), size.value);
        return PyTypePattern{std::move(pattern)};
    });
    m.def("type_pattern_tsw", [] {
        return PyTypePattern{TypePattern::tsw_any(ScalarPattern::var("T"))};
    });
    m.def("type_pattern_tsw", [](PyScalarPattern element) {
        return PyTypePattern{TypePattern::tsw_any(std::move(element.pattern))};
    });
    m.def("type_pattern_tsw", [](PyScalarPattern element, std::size_t period, std::size_t min_period) {
        return PyTypePattern{TypePattern::tsw(std::move(element.pattern), period, min_period)};
    }, nb::arg("element"), nb::arg("period"), nb::arg("min_period") = 0);
    m.def("type_pattern_tsb", [] {
        return PyTypePattern{TypePattern::tsb_var("SCHEMA")};
    });
    m.def("type_pattern_tsb", [](const std::string &schema_variable) {
        return PyTypePattern{TypePattern::tsb_var(schema_variable)};
    });
    m.def("type_pattern_tsb_fields", [](nb::list field_names, nb::list children) {
        if (nb::len(field_names) != nb::len(children))
        {
            throw nb::value_error("TSB pattern field names and child patterns must have the same size");
        }
        std::vector<std::string> names;
        std::vector<TypePattern> patterns;
        names.reserve(nb::len(field_names));
        patterns.reserve(nb::len(children));
        for (nb::handle name : field_names) { names.push_back(nb::cast<std::string>(name)); }
        for (nb::handle child : children)
        {
            patterns.push_back(nb::cast<PyTypePattern &>(child).pattern);
        }
        return PyTypePattern{TypePattern::tsb(std::move(names), std::move(patterns))};
    });
    m.def("type_pattern_substitute_scalars", [](PyTypePattern pattern, nb::dict replacements) {
        std::unordered_map<std::string, ScalarPattern> values;
        values.reserve(nb::len(replacements));
        for (auto [name, replacement] : replacements)
        {
            values.emplace(nb::cast<std::string>(name),
                           nb::cast<PyScalarPattern &>(replacement).pattern);
        }
        return PyTypePattern{substitute_scalar_patterns(std::move(pattern.pattern), values)};
    });
    m.def("type_pattern_ref", [](PyTypePattern target) {
        return PyTypePattern{TypePattern::ref(std::move(target.pattern))};
    });
    m.def("type_pattern_signal", [] {
        return PyTypePattern{TypePattern::signal()};
    });
    const auto target_input_schemas = [](nb::tuple inputs) {
        std::vector<const TSValueTypeMetaData *> schemas;
        schemas.reserve(nb::len(inputs));
        for (nb::handle input : inputs)
        {
            if (nb::isinstance<PyPort>(input))
            {
                schemas.push_back(nb::cast<PyPort>(input).ref.schema);
            }
            else if (nb::isinstance<PyTsType>(input))
            {
                schemas.push_back(nb::cast<PyTsType>(input).meta);
            }
            else
            {
                throw nb::type_error("type target resolution inputs must be Port or TsType objects");
            }
        }
        return schemas;
    };

    const auto selected_key_names = [](nb::object keys) {
        std::vector<std::string> selected;
        if (keys.is_none()) { return selected; }
        for (nb::handle key : keys) { selected.push_back(nb::cast<std::string>(key)); }
        return selected;
    };

    m.def("resolve_convert_target",
          [target_input_schemas, selected_key_names](PyTypePattern pattern, nb::tuple inputs, nb::object keys) {
              std::vector<const TSValueTypeMetaData *> schemas = target_input_schemas(inputs);
              std::vector<std::string> selected = selected_key_names(std::move(keys));
              return PyTsType{stdlib::resolve_convert_target(
                  pattern.pattern,
                  std::span<const TSValueTypeMetaData *const>{schemas.data(), schemas.size()},
                  std::span<const std::string>{selected.data(), selected.size()})};
          },
          nb::arg("pattern"), nb::arg("inputs"), nb::arg("keys") = nb::none());

    m.def("resolve_collect_target",
          [target_input_schemas](PyTypePattern pattern, nb::tuple inputs) {
              std::vector<const TSValueTypeMetaData *> schemas = target_input_schemas(inputs);
              return PyTsType{stdlib::resolve_collect_target(
                  pattern.pattern,
                  std::span<const TSValueTypeMetaData *const>{schemas.data(), schemas.size()})};
          },
          nb::arg("pattern"), nb::arg("inputs"));

    m.def("resolve_combine_target",
          [target_input_schemas](PyTypePattern pattern, nb::tuple inputs) {
              std::vector<const TSValueTypeMetaData *> schemas = target_input_schemas(inputs);
              return PyTsType{stdlib::resolve_combine_target(
                  pattern.pattern,
                  std::span<const TSValueTypeMetaData *const>{schemas.data(), schemas.size()})};
          },
          nb::arg("pattern"), nb::arg("inputs"));

    m.def("resolve_emit_target",
          [target_input_schemas](PyTsType value_ts, nb::tuple inputs) {
              std::vector<const TSValueTypeMetaData *> schemas = target_input_schemas(inputs);
              return PyTsType{stdlib::resolve_emit_target(
                  value_ts.meta,
                  std::span<const TSValueTypeMetaData *const>{schemas.data(), schemas.size()})};
          },
          nb::arg("value_ts"), nb::arg("inputs"));

    m.def("operator_output_is_selective", [](const std::string &name) {
        return OperatorRegistry::instance().output_is_selective(name);
    });

    // --- python operator overloads (end-game A2): a python @compute_node/
    // @graph registers as an ordinary OperatorImpl{Source::Python} candidate.
    // Matching/ranking/normalisation are ENTIRELY the C++ registry's (the
    // standing ruling); the wire closure calls back into the python wiring
    // function under a borrowed Wiring (the WiredFn trampoline pattern).
    m.def(
        "register_python_overload",
        [](const std::string &name, nb::list params, nb::object output, nb::object wire_fn,
           nb::object requires_fn, bool variadic, bool has_kwargs,
           std::optional<std::size_t> positional_params) {
            OperatorImpl impl;
            impl.name       = name;
            impl.source     = OperatorImpl::Source::Python;
            impl.variadic   = variadic;
            impl.has_kwargs = has_kwargs;
            for (nb::handle item : params)
            {
                auto         entry = nb::cast<nb::tuple>(item);
                ParamPattern pp;
                pp.name = nb::cast<std::string>(entry[0]);
                if (nb::isinstance<PyTypePattern>(entry[1]))
                {
                    pp.kind = ParamPattern::Kind::Input;
                    pp.ts   = nb::cast<PyTypePattern &>(entry[1]).pattern;
                }
                else
                {
                    pp.kind   = ParamPattern::Kind::Scalar;
                    pp.scalar = nb::cast<PyScalarPattern &>(entry[1]).pattern;
                }
                if (nb::len(entry) > 2)
                {
                    // The third slot is the DEFAULT: python None on a ts
                    // param = the null source (an EMPTY Value); scalars
                    // convert by inference.
                    nb::handle default_value = entry[2];
                    if (default_value.is_none()) { pp.default_value = Value{}; }
                    else { pp.default_value = py_to_value(default_value); }
                }
                impl.params.push_back(std::move(pp));
            }
            if (!output.is_none())
            {
                impl.has_output = true;
                impl.output     = nb::cast<PyTypePattern &>(output).pattern;
            }
            if (positional_params.has_value()) { impl.positional_params = *positional_params; }
            impl.rank  = operator_dispatch_detail::operator_rank(impl.params);
            impl.label = [&] {
                std::string out = name + "(";
                for (std::size_t i = 0; i < impl.params.size(); ++i)
                {
                    if (i != 0) { out += ", "; }
                    if (impl.variadic && i + 1 == impl.params.size()) { out += "*"; }
                    out += impl.params[i].kind == ParamPattern::Kind::Input
                               ? ts_pattern_to_string(impl.params[i].ts)
                               : scalar_pattern_to_string(impl.params[i].scalar);
                    if (impl.params[i].default_value.has_value()) { out += "=…"; }
                }
                if (impl.has_kwargs) { out += impl.params.empty() ? "**kwargs" : ", **kwargs"; }
                out += ") [py]";
                if (impl.has_output) { out += " -> " + ts_pattern_to_string(impl.output); }
                return out;
            }();
            if (!requires_fn.is_none())
            {
                impl.requires_predicate = [requires_fn](const ResolutionMap &map,
                                                        OperatorCallContext context) -> bool {
                    nb::gil_scoped_acquire gil;
                    PyResolutionScope      scope;
                    scope.map = map;
                    nb::dict scalars;
                    for (std::size_t i = 0; i < context.args.size() && i < context.params.size(); ++i)
                    {
                        if (context.params[i].kind == ParamPattern::Kind::Scalar &&
                            context.args[i].kind == WiringArg::Kind::Scalar &&
                            context.args[i].scalar_value.has_value())
                        {
                            scalars[nb::str(context.params[i].name.c_str())] =
                                value_to_py(context.args[i].scalar_value.view());
                        }
                    }
                    return nb::cast<bool>(requires_fn(nb::cast(scope), scalars));
                };
            }
            impl.wire = [wire_fn](Wiring &w, const ResolutionMap &, std::span<const WiringArg> args,
                                  std::span<const std::pair<std::string, WiringPortRef>> kwargs)
                -> OperatorWireResult {
                nb::gil_scoped_acquire gil;
                nb::list               py_args;
                for (const WiringArg &arg : args)
                {
                    if (arg.kind == WiringArg::Kind::TimeSeries)
                    {
                        // An unwired ts default (null source) crosses as None;
                        // the python node wires its own nothing-source.
                        if (arg.port.schema == nullptr) { py_args.append(nb::none()); }
                        else { py_args.append(nb::cast(PyPort{arg.port})); }
                    }
                    else if (!arg.scalar_value.has_value()) { py_args.append(nb::none()); }
                    else { py_args.append(value_to_py(arg.scalar_value.view())); }
                }
                nb::dict py_kwargs;
                for (const auto &[kw_name, port] : kwargs)
                {
                    py_kwargs[nb::str(kw_name.c_str())] = nb::cast(PyPort{port});
                }
                nb::object borrowed = nb::cast(PyWiring::borrow(w));
                nb::object result   = wire_fn(borrowed, nb::tuple(py_args), py_kwargs);
                if (result.is_none()) { return OperatorWireResult{}; }
                return OperatorWireResult{true, Port<void>{w, nb::cast<PyPort &>(result).ref}};
            };
            OperatorRegistry::instance().register_overload(std::move(impl));
        },
        nb::arg("name"), nb::arg("params"), nb::arg("output").none(), nb::arg("wire_fn"),
        nb::arg("requires_fn").none() = nb::none(), nb::arg("variadic") = false,
        nb::arg("has_kwargs") = false, nb::arg("positional_params") = nb::none());
    }
}  // namespace hgraph::python_bridge
