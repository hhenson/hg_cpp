#include <fmt/format.h>
#include <hgraph/nodes/context_node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/tsb.h>
#include <nanobind/nanobind.h>

namespace hgraph
{

    void ContextStubSourceNode::do_start() {
        _subscribed_output = nullptr;
        notify();
    }

    void ContextStubSourceNode::do_stop() {
        if (_subscribed_output.get() != nullptr) {
            _subscribed_output->un_subscribe(this);
            _subscribed_output.reset();
        }
    }

    void ContextStubSourceNode::do_eval() {
        // Build the global state key: "context-<owning_graph_id prefix>-<path>"
        // depth indicates how many elements of owning_graph_id to include
        int depth = 0;
        try {
            nb::object d = scalars()["depth"];  // may be None
            if (d.is_valid() && !d.is_none()) { depth = nb::cast<int>(d); }
        } catch (...) { depth = 0; }

        std::string path_str;
        try {
            path_str = nb::cast<std::string>(scalars()["path"]);
        } catch (...) { throw std::runtime_error("ContextStubSourceNode: missing 'path' scalar"); }

        // Slice owning_graph_id by depth and build a Python tuple string to exactly match Python formatting
        const auto &og  = owning_graph_id();
        int         use = std::max(0, std::min<int>(depth, static_cast<int>(og.size())));

        // Build Python list of prefix ids
        nb::list py_list;
        for (int i = 0; i < use; ++i) { py_list.append(og[static_cast<size_t>(i)]); }
        // Python format of owning_graph_id as exposed by nanobind is a list; use list string form to match capture key
        std::string og_prefix = nb::cast<std::string>(nb::str(py_list));

        auto key = fmt::format("context-{}-{}", og_prefix, path_str);

        // Lookup in GlobalState
        nb::object shared = GlobalState::get(key, nb::none());
        if (!shared.is_valid() || shared.is_none()) {
            std::string diag;
            try {
                nb::object gs       = GlobalState::instance();
                nb::object keys_obj = gs.attr("keys")();
                std::vector<std::string> ctx_keys;
                for (auto item : nb::iter(keys_obj)) {
                    std::string s = nb::cast<std::string>(nb::str(item));
                    if (s.rfind("context-", 0) == 0) { ctx_keys.push_back(s); }
                }
                if (!ctx_keys.empty()) { diag = fmt::format(" Available context keys: [{}]", fmt::join(ctx_keys, ", ")); }
            } catch (...) {
                // ignore diagnostics failures
            }
            throw std::runtime_error(fmt::format("Missing shared output for path: {}{}", key, diag));
        }

        time_series_reference_output_ptr output_ts = nullptr;

        // Case 1: direct TimeSeriesReferenceOutput stored in GlobalState
        if (shared.type().is(nb::type<TimeSeriesReferenceOutput>())) {
            output_ts = nb::cast<time_series_reference_output_ptr>(shared);
            // Ensure current value is propagated (no-op if already set)
            output_ts->set_value(output_ts->value());
        }
        // Case 2: TimeSeriesReferenceInput stored in GlobalState
        else if (shared.type().is(nb::type<TimeSeriesReferenceInput>())) {
            auto ref = nb::cast<time_series_reference_input_ptr>(shared);
            if (ref->has_peer()) {
                // Use the bound peer output
                output_ts = dynamic_cast<TimeSeriesReferenceOutput *>(ref->output().get());
                output_ts->set_value(output_ts->value());
            } else {
                // No peer bound: use this node's own reference output and seed it with the input's value
                output_ts = dynamic_cast<TimeSeriesReferenceOutput *>(output_ptr().get());
                output_ts->set_value(ref->value());
            }
        } else {
            throw std::runtime_error(
                fmt::format("Context found an unknown output type bound to {}: {}", key, nb::str(shared.type()).c_str()));
        }

        if (output_ts.get() != nullptr) {
            bool is_same{_subscribed_output.get() == output_ts.get()};
            if (!is_same) {
                output_ts->subscribe(this);
                if (_subscribed_output != nullptr) { _subscribed_output->un_subscribe(this); }
                _subscribed_output = output_ts;
            }
        }

    }

    void register_context_node_with_nanobind(nb::module_ &m) {
        nb::class_<ContextStubSourceNode, Node>(m, "ContextStubSourceNode");
    }

}  // namespace hgraph
