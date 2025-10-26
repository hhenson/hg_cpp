#include <hgraph/nodes/push_queue_node.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/time_series_type.h>

namespace hgraph
{
    void PushQueueNode::do_eval() {}

    void PushQueueNode::enqueue_message(nb::object message) {
        ++_messages_queued;
        _receiver->enqueue({node_ndx(), std::move(message)});
    }

    bool PushQueueNode::apply_message(nb::object message) {
        if (_elide || output()->can_apply_result(message)) {
            output()->apply_result(std::move(message));
            return true;
        }
        return false;
    }

    int64_t PushQueueNode::messages_in_queue() const { return _messages_queued - _messages_dequeued; }

    void PushQueueNode::set_receiver(sender_receiver_state_ptr value) { _receiver = value; }

    void PushQueueNode::start() {
        _receiver = &graph()->receiver();
        _elide    = scalars().contains("elide") ? nb::cast<bool>(scalars()["elide"]) : false;
        _batch    = scalars().contains("batch") ? nb::cast<bool>(scalars()["batch"]) : false;

        // If an eval function was provided (from push_queue decorator), call it with a sender and scalar kwargs
        if (_eval_fn.is_valid() && !_eval_fn.is_none()) {
            // Create a Python-callable sender that enqueues messages into this node
            nb::object sender = nb::cpp_function([this](nb::object m) { this->enqueue_message(std::move(m)); });
            // Call eval_fn(sender, **scalars)
            try {
                _eval_fn(sender, **scalars());
            } catch (nb::python_error &e) { throw NodeException::capture_error(e, *this, "During push-queue start"); }
        }
    }

}  // namespace hgraph
