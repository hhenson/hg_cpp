/*
 * The entry point into the python _hgraph module exposing the C++ types to python.
 *
 * Note that as a pattern, we will return alias shared pointers for objects that have their life-times managed by an outer object
 * such as ExecutionGraph, where the life-time of the objects contained within are all directly managed by the outer object.
 * This reduces the number of shared pointers that need to be constructed inside the graph and thus provides a small improvement
 * on memory and general performance.
 *
 */

#include <hgraph/python/pyb_wiring.h>
#include <nanobind/intrusive/counter.h>
#include <nanobind/intrusive/counter.inl>

void export_runtime(nb::module_ &);
void export_builders(nb::module_ &);
void export_types(nb::module_ &);
void export_utils(nb::module_ &);

NB_MODULE(_hgraph, m) {
    nb::set_leak_warnings(false);
    m.doc() = "The HGraph C++ runtime engine";
    nb::intrusive_init(
        [](PyObject *o) noexcept {
            nb::gil_scoped_acquire guard;
            Py_INCREF(o);
        },
        [](PyObject *o) noexcept {
            nb::gil_scoped_acquire guard;
            Py_DECREF(o);
        });

    nb::class_<nb::intrusive_base>(
        m, "intrusive_base",
        nb::intrusive_ptr<nb::intrusive_base>(
            [](nb::intrusive_base *o, PyObject *po) noexcept { o->set_self_py(po); }));

    export_utils(m);
    export_types(m);
    export_builders(m);
    export_runtime(m);
}

/*
 nb::class_<Object>(
   m, "Object",
   nb::intrusive_ptr<Object>(
       [](Object *o, PyObject *po) noexcept { o->set_self_py(po); }));
*/