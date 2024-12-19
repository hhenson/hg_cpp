#include <hgraph/python/pyb_wiring.h>
#include <hgraph/util/lifecycle.h>

namespace hgraph
{
    bool ComponentLifeCycle::is_started() const { return _started; }

    bool ComponentLifeCycle::is_starting() const { return _transitioning && !_started; }

    bool ComponentLifeCycle::is_stopping() const { return _transitioning && _started; }

    void ComponentLifeCycle::register_with_nanobind(nb::module_ &m) {
        nb::class_<ComponentLifeCycle>(
            m, "ComponentLifeCycle",
            nb::intrusive_ptr<ComponentLifeCycle>([](ComponentLifeCycle *o, PyObject *po) noexcept { o->set_self_py(po); }))
            .def_prop_ro("is_started", &ComponentLifeCycle::is_started)
            .def_prop_ro("is_starting", &ComponentLifeCycle::is_starting)
            .def_prop_ro("is_stopping", &ComponentLifeCycle::is_stopping);

        m.def("initialise_component", &initialise_component, "component"_a);
        m.def("start_component", &start_component, "component"_a);
        m.def("stop_component", &stop_component, "component"_a);
        m.def("dispose_component", &dispose_component, "component"_a);

        nb::class_<InitialiseDisposeContext>(m, "initialise_dispose_context").def(nb::init<ComponentLifeCycle &>(), "component"_a);

        nb::class_<StartStopContext>(m, "start_stop_context").def(nb::init<StartStopContext &>(), "component"_a);
    }

    struct TransitionGuard
    {
        TransitionGuard(ComponentLifeCycle &component) : _component{component} { _component._transitioning = true; }
        ~TransitionGuard() { _component._transitioning = false; }

      private:
        ComponentLifeCycle &_component;
    };

    void initialise_component(ComponentLifeCycle &component) { component.initialise(); }

    /*
     * NOTE the LifeCycle methods are expected to be called on a single thread, so the simple gaurd clauses
     * used here are sufficient to ensure we don't accidentally start/stop more than once.
     */

    void start_component(ComponentLifeCycle &component) {
        if (component.is_started() || component.is_starting()) { return; }
        TransitionGuard guard{component};
        component.start();
        // If start fails (throws an exception), we will not land up setting the started flag to true.
        // But in either case (success or failure) the TransitionGuard destructor will be called and the
        // transitioning flag will be set to false.
        component._started = true;
    }

    void stop_component(ComponentLifeCycle &component) {
        if (!component.is_started() || component.is_stopping()) { return; }
        TransitionGuard guard{component};
        component.stop();
        component._started = false;
    }

    void dispose_component(ComponentLifeCycle &component) { component.dispose(); }

    InitialiseDisposeContext::InitialiseDisposeContext(ComponentLifeCycle &component) : _component{component} {
        initialise_component(component);
    }

    InitialiseDisposeContext::~InitialiseDisposeContext() { dispose_component(_component); }

    StartStopContext::StartStopContext(ComponentLifeCycle &component) : _component{component} { start_component(_component); }

    StartStopContext::~StartStopContext() { stop_component(_component); }
}  // namespace hgraph
