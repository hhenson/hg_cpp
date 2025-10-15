#include <hgraph/nodes/context_node.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/tsb.h>
#include <fmt/format.h>

namespace hgraph {

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
        nb::object d = scalars()["depth"]; // may be None
        if (d.is_valid() && !d.is_none()) {
            depth = nb::cast<int>(d);
        }
    } catch (...) {
        depth = 0;
    }

    std::string path_str;
    try {
        path_str = nb::cast<std::string>(scalars()["path"]);
    } catch (...) {
        throw std::runtime_error("ContextStubSourceNode: missing 'path' scalar");
    }

    // Slice owning_graph_id by depth
    const auto &og = owning_graph_id();
    int use = std::max(0, std::min<int>(depth, static_cast<int>(og.size())));
    std::string og_prefix;
    if (use > 0) {
        og_prefix = fmt::format("{}", fmt::join(og.begin(), og.begin() + use, ", "));
        og_prefix = fmt::format("({})", og_prefix);
    } else {
        og_prefix = "()";
    }

    auto key = fmt::format("context-{}-{}", og_prefix, path_str);

    // Lookup in GlobalState
    nb::object shared = GlobalState::get(key, nb::none());
    if (!shared.is_valid() || shared.is_none()) {
        throw std::runtime_error(fmt::format("Missing shared output for path: {}", key));
    }

    time_series_output_ptr output_ts = nullptr;

    // Case 1: direct TimeSeriesOutput
    if (shared.type().is(nb::type<TimeSeriesOutput>())) {
        // Borrowed reference to underlying C++ object
        TimeSeriesOutput &out_ref = nb::cast<TimeSeriesOutput &>(shared);
        output_ts = time_series_output_ptr(&out_ref);
    } else {
        // If object has a peer, then try to extract .output
        try {
            nb::object has_peer_attr = shared.attr("has_peer");
            bool       hp            = nb::cast<bool>(has_peer_attr);
            if (hp) {
                nb::object out_obj = shared.attr("output");
                if (out_obj.type().is(nb::type<TimeSeriesOutput>())) {
                    TimeSeriesOutput &out_ref2 = nb::cast<TimeSeriesOutput &>(out_obj);
                    output_ts = time_series_output_ptr(&out_ref2);
                }
            }
        } catch (...) {
            // ignore, we'll treat as no output
        }
    }

    if (output_ts.get() != nullptr) {
        output_ts->subscribe(this);
        if (_subscribed_output.get() != nullptr && _subscribed_output.get() != output_ts.get()) {
            _subscribed_output->un_subscribe(this);
        }
        _subscribed_output = output_ts;
    }

    // Set the reference value on our output (if available)
    try {
        nb::object value = shared.attr("value");
        output().apply_result(value);
    } catch (...) {
        // If no value attribute, clear?
        output().apply_result(nb::none());
    }
}

void register_context_node_with_nanobind(nb::module_ &m) {
    nb::class_<ContextStubSourceNode, Node>(m, "ContextStubSourceNode");
}

} // namespace hgraph
