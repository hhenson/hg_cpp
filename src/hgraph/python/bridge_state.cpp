#include <hgraph/python/bridge_state.h>
#include <hgraph/python/native_scalar_registration.h>

#if HGRAPH_ENABLE_PYTHON_USER_NODES

#include <hgraph/types/frame.h>
#include <hgraph/types/series.h>
#include <hgraph/types/time_series_reference.h>
#include <hgraph/types/value_callable.h>
#include <hgraph/types/wired_fn.h>

#include <stdexcept>
#include <unordered_map>

namespace hgraph::python_bridge {
namespace {
struct NativeScalarRegistrations {
  std::unordered_map<PyObject *,
                     std::pair<nb::object, const ValueTypeMetaData *>>
      by_python_type;
  std::unordered_map<const ValueTypeMetaData *, nb::object> by_native_type;
};

NativeScalarRegistrations &native_scalar_registrations() {
  static auto *registrations = new NativeScalarRegistrations{};
  return *registrations;
}
} // namespace

nb::object &cmp_result_enum_slot() {
  static auto *slot = new nb::object{};
  return *slot;
}

nb::object &divide_by_zero_enum_slot() {
  static auto *slot = new nb::object{};
  return *slot;
}

nb::object &removed_sentinel_slot() {
  static auto *slot = new nb::object{};
  return *slot;
}

nb::object &remove_if_exists_sentinel_slot() {
  static auto *slot = new nb::object{};
  return *slot;
}

nb::object &removed_class_slot() {
  static auto *slot = new nb::object{};
  return *slot;
}

nb::object &set_delta_class_slot() {
  static auto *slot = new nb::object{};
  return *slot;
}

nb::object &delta_shaper_slot() {
  static auto *slot = new nb::object{};
  return *slot;
}

PyInferValueFn &py_infer_value_slot() {
  static PyInferValueFn slot = nullptr;
  return slot;
}

nb::dict &bundle_class_registry() {
  static auto *registry = new nb::dict{};
  return *registry;
}

std::unordered_map<const void *, PyBundleClassInfo> &
bundle_class_info_registry() {
  static auto *registry =
      new std::unordered_map<const void *, PyBundleClassInfo>{};
  return *registry;
}

std::unordered_map<const void *, const void *> &tsb_compound_value_registry() {
  static auto *registry = new std::unordered_map<const void *, const void *>{};
  return *registry;
}

void register_native_scalar_type(nb::handle python_type,
                                 const ValueTypeMetaData *native_value_type) {
  if (PyType_Check(python_type.ptr()) == 0) {
    throw nb::type_error("python_type must be a Python class");
  }
  if (native_value_type == nullptr ||
      native_value_type->value_kind() != ValueTypeKind::Atomic) {
    throw nb::type_error("native_value_type must be an atomic scalar schema");
  }

  auto &registrations = native_scalar_registrations();
  const auto python_entry =
      registrations.by_python_type.find(python_type.ptr());
  if (python_entry != registrations.by_python_type.end() &&
      python_entry->second.second != native_value_type) {
    throw std::invalid_argument("Python class is already registered to a "
                                "different native scalar schema");
  }
  const auto native_entry =
      registrations.by_native_type.find(native_value_type);
  if (native_entry != registrations.by_native_type.end() &&
      !native_entry->second.is(python_type)) {
    throw std::invalid_argument("native scalar schema is already registered to "
                                "a different Python class");
  }
  if (python_entry != registrations.by_python_type.end()) {
    return;
  }

  nb::object retained = nb::borrow<nb::object>(python_type);
  registrations.by_python_type.emplace(python_type.ptr(),
                                       std::pair{retained, native_value_type});
  registrations.by_native_type.emplace(native_value_type, std::move(retained));
}

const ValueTypeMetaData *
native_scalar_type_for_python(nb::handle python_type) {
  const auto &registrations = native_scalar_registrations();
  const auto found = registrations.by_python_type.find(python_type.ptr());
  return found == registrations.by_python_type.end() ? nullptr
                                                     : found->second.second;
}

nb::object
python_type_for_native_scalar(const ValueTypeMetaData *native_value_type) {
  const auto &registrations = native_scalar_registrations();
  const auto found = registrations.by_native_type.find(native_value_type);
  return found == registrations.by_native_type.end()
             ? nb::none()
             : nb::borrow<nb::object>(found->second);
}

const ValueTypeMetaData *native_scalar_type_for_value(nb::handle value) {
  if (!value.is_valid()) {
    return nullptr;
  }
  if (const auto *exact =
          native_scalar_type_for_python(nb::handle(Py_TYPE(value.ptr())))) {
    return exact;
  }
  const auto &registrations = native_scalar_registrations();
  for (const auto &[python_type, entry] : registrations.by_python_type) {
    static_cast<void>(python_type);
    if (nb::isinstance(value, entry.first)) {
      return entry.second;
    }
  }
  return nullptr;
}

void clear_native_scalar_types() noexcept {
  auto &registrations = native_scalar_registrations();
  registrations.by_native_type.clear();
  registrations.by_python_type.clear();
}
} // namespace hgraph::python_bridge

namespace hgraph {
#define HGRAPH_DEFINE_PYTHON_CONVERSION_TRAIT(Type, Label)                     \
  python_conversion_traits<Type>::ToPythonHook &                               \
  python_conversion_traits<Type>::to_python_hook() noexcept {                  \
    static ToPythonHook hook = nullptr;                                        \
    return hook;                                                               \
  }                                                                            \
  python_conversion_traits<Type>::FromPythonHook &                             \
  python_conversion_traits<Type>::from_python_hook() noexcept {                \
    static FromPythonHook hook = nullptr;                                      \
    return hook;                                                               \
  }                                                                            \
  nanobind::object python_conversion_traits<Type>::to_python(                  \
      const Type &value) {                                                     \
    const auto hook = to_python_hook();                                        \
    if (hook == nullptr) {                                                     \
      throw std::logic_error(                                                  \
          Label " python conversion hook not installed (import the module)");  \
    }                                                                          \
    return hook(value);                                                        \
  }                                                                            \
  Type python_conversion_traits<Type>::from_python(nanobind::handle source) {  \
    const auto hook = from_python_hook();                                      \
    if (hook == nullptr) {                                                     \
      throw std::logic_error(                                                  \
          Label " python conversion hook not installed (import the module)");  \
    }                                                                          \
    return hook(source);                                                       \
  }

HGRAPH_DEFINE_PYTHON_CONVERSION_TRAIT(Frame, "Frame")
HGRAPH_DEFINE_PYTHON_CONVERSION_TRAIT(Series, "Series")
HGRAPH_DEFINE_PYTHON_CONVERSION_TRAIT(TimeSeriesReference,
                                      "TimeSeriesReference")
HGRAPH_DEFINE_PYTHON_CONVERSION_TRAIT(ValueCallable, "ValueCallable")
HGRAPH_DEFINE_PYTHON_CONVERSION_TRAIT(WiredFn, "WiredFn")

#undef HGRAPH_DEFINE_PYTHON_CONVERSION_TRAIT
} // namespace hgraph

#endif
