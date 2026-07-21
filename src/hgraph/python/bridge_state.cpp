#include <hgraph/python/bridge_state.h>

#if HGRAPH_ENABLE_PYTHON_USER_NODES

#include <hgraph/types/frame.h>
#include <hgraph/types/series.h>
#include <hgraph/types/time_series_reference.h>
#include <hgraph/types/value_callable.h>
#include <hgraph/types/wired_fn.h>

#include <stdexcept>

namespace hgraph::python_bridge {
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
