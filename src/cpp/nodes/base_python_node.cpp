#include <hgraph/nodes/base_python_node.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/tsb.h>

namespace hgraph
{
    BasePythonNode::BasePythonNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::ptr signature,
                                   nb::dict scalars, nb::callable eval_fn, nb::callable start_fn, nb::callable stop_fn)
        : Node(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars)), _eval_fn{std::move(eval_fn)},
          _start_fn{std::move(start_fn)}, _stop_fn{std::move(stop_fn)} {}

    void BasePythonNode::_initialise_kwargs() {
        // Assuming Injector and related types are properly defined, and scalars is a map-like container
        auto &signature_args = signature().args;
        _kwargs              = {};

        bool has_injectables{signature().injectables != 0};
        for (const auto &[key_, value] : scalars()) {
            std::string key{nb::cast<std::string>(key_)};
            if (has_injectables && signature().injectable_inputs->contains(key)) {
                // TODO: This may be better extracted directly, but for now use the python function calls.
                nb::object node{nb::cast(this)};
                nb::object key_handle{value(node)};
                _kwargs[key_] = key_handle;  // Assuming this call applies the Injector properly
            } else {
                _kwargs[key_] = value;
            }
        }
        for (size_t i = 0, l = signature().time_series_inputs.has_value() ? signature().time_series_inputs->size() : 0; i < l;
             ++i) {
            // Apple does not yet support ranges::contains :(
            auto key{input().schema().keys()[i]};
            if (std::ranges::find(signature_args, key) != std::ranges::end(signature_args)) {
                // Force exposure of inputs as base TimeSeriesInput to avoid double-wrapping as derived classes
                // This fixes a strange bug, but is potentially risky if the user holds a reference to this (which should
                // technically never actually happen)
                _kwargs[key.c_str()] = nb::cast(static_cast<TimeSeriesInput *>(input()[i].get()), nb::rv_policy::reference);
            }
        }
    }

    void BasePythonNode::_initialise_state() {
        if (has_recordable_state()) {
            // TODO: Implement this once a bit more infra is in place
            throw std::runtime_error("Recordable state not yet implemented");
            // auto &record_context = RecordReplayContext::instance();
            // auto  mode           = record_context.mode();
            //
            // if (mode.contains(RecordReplayEnum::RECOVER)) {
            //     // TODO: make recordable_id unique by using parent node context information
            //     auto recordable_id   = get_fq_recordable_id(this->graph().traits(), this->signature().record_replay_id());
            //     auto clock           = this->graph().evaluation_clock();
            //     auto evaluation_time = clock.evaluation_time();
            //     auto as_of_time      = get_as_of(clock);
            //
            //     this->recordable_state().value() =
            //         replay_const("__state__", this->signature().recordable_state().tsb_type().py_type(), recordable_id,
            //                      evaluation_time - MIN_TD,  // We want the state just before now
            //                      as_of_time)
            //             .value();
            // }
        }
    }

    void BasePythonNode::do_eval() {
        // Handle context inputs - enter all valid context managers
        std::vector<nb::object> active_contexts;

        if (signature().context_inputs.has_value() && !signature().context_inputs->empty()) {
            // Enter all valid context inputs
            active_contexts.reserve(signature().context_inputs->size());
            for (const auto &context_key : *signature().context_inputs) {
                if (input()[context_key]->valid()) {
                    nb::object context_value = input()[context_key]->py_value();
                    // ReSharper disable once CppExpressionWithoutSideEffects
                    context_value.attr("__enter__")();  // MOVE TO PYTHON NODE
                    active_contexts.push_back(context_value);
                }
            }
        }
        try {
            try {
                auto out{_eval_fn(**_kwargs)};
                if (!out.is_none()) { output().apply_result(out); }
            } catch (nb::python_error &e) {
                // Convert Python error into enriched NodeException immediately to ensure readable propagation
                throw NodeException::capture_error(e, *this, "During Python node evaluation");
            }
            // Exit contexts in reverse order (success case)
            for (auto it = active_contexts.rbegin(); it != active_contexts.rend(); ++it) {
                it->attr("__exit__")(nb::none(), nb::none(), nb::none());
            }
        } catch (...) {
            // Exit contexts in reverse order (exception case)
            for (auto it = active_contexts.rbegin(); it != active_contexts.rend(); ++it) {
                try {
                    it->attr("__exit__")(nb::none(), nb::none(), nb::none());
                } catch (...) {
                    // Suppress exceptions during cleanup to preserve original exception
                }
            }

            throw;  // Re-throw the original exception
        }
    }

    void BasePythonNode::do_start() {
        if (_start_fn.is_valid() && !_start_fn.is_none()) {
            // Get the callable signature parameters using inspect.signature
            // This matches Python's approach: signature(self.start_fn).parameters.keys()
            // Using __code__.co_varnames includes local variables, not just parameters
            auto inspect = nb::module_::import_("inspect");
            auto sig     = inspect.attr("signature")(_start_fn);
            auto params  = sig.attr("parameters").attr("keys")();

            // Filter kwargs to only include parameters in start_fn signature
            nb::dict filtered_kwargs;
            for (auto k : params) {
                if (_kwargs.contains(k)) { filtered_kwargs[k] = _kwargs[k]; }
            }
            // Call start_fn with filtered kwargs
            _start_fn(**filtered_kwargs);
        }
    }

    void BasePythonNode::do_stop() {
        if (_stop_fn.is_valid() and !_stop_fn.is_none()) {
            // Get the callable signature parameters using inspect.signature
            // This matches Python's approach: signature(self.stop_fn).parameters.keys()
            // Using __code__.co_varnames includes local variables, not just parameters
            auto inspect = nb::module_::import_("inspect");
            auto sig     = inspect.attr("signature")(_stop_fn);
            auto params  = sig.attr("parameters").attr("keys")();

            // Filter kwargs to only include parameters in stop_fn signature
            nb::dict filtered_kwargs;
            for (auto k : params) {
                if (_kwargs.contains(k)) { filtered_kwargs[k] = _kwargs[k]; }
            }

            // Call stop_fn with filtered kwargs
            _stop_fn(**filtered_kwargs);
        }
    }

    void BasePythonNode::initialise() {}

    void BasePythonNode::start() {
        _initialise_kwargs();
        _initialise_inputs();
        _initialise_state();
        // Now call parent class
        Node::start();
    }

    void BasePythonNode::dispose() { _kwargs.clear(); }

}  // namespace hgraph
