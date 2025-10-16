import hg_cpp
import pytest
import _hgraph  # Direct access to C++ NodeSignature and enums


class TestCppNodeSignature:
    """Test suite for C++ NodeSignature implementation (via _hgraph module)"""

    def test_basic_construction(self):
        """Test basic C++ NodeSignature construction"""
        sig = _hgraph.NodeSignature(
            name="test_node",
            node_type=_hgraph.NodeTypeEnum.COMPUTE_NODE,
            args=["a", "b"],
            time_series_inputs=None,
            time_series_output=None,
            scalars=None,
            src_location=None,
            active_inputs=None,
            valid_inputs=None,
            all_valid_inputs=None,
            context_inputs=None,
            injectable_inputs=None,
            injectables=0,
            capture_exception=False,
            trace_back_depth=0,
            wiring_path_name="test.path",
            label=None,
            capture_values=False,
            record_replay_id=None,
        )

        assert sig.name == "test_node"
        assert sig.node_type == _hgraph.NodeTypeEnum.COMPUTE_NODE
        assert sig.args == ["a", "b"]
        assert sig.wiring_path_name == "test.path"
        assert sig.capture_exception == False
        assert sig.capture_values == False

    def test_node_type_preservation(self):
        """Test that node_type is preserved correctly in C++"""
        for node_type in [
            _hgraph.NodeTypeEnum.PUSH_SOURCE_NODE,
            _hgraph.NodeTypeEnum.PULL_SOURCE_NODE,
            _hgraph.NodeTypeEnum.COMPUTE_NODE,
            _hgraph.NodeTypeEnum.SINK_NODE,
        ]:
            sig = _hgraph.NodeSignature(
                name="test",
                node_type=node_type,
                args=[],
                time_series_inputs=None,
                time_series_output=None,
                scalars=None,
                src_location=None,
                active_inputs=None,
                valid_inputs=None,
                all_valid_inputs=None,
                context_inputs=None,
                injectable_inputs=None,
                injectables=0,
                capture_exception=False,
                trace_back_depth=0,
                wiring_path_name="test",
                label=None,
                capture_values=False,
                record_replay_id=None,
            )
            assert sig.node_type == node_type, f"Expected {node_type}, got {sig.node_type}"

    def test_to_dict_includes_all_fields(self):
        """Test that C++ to_dict includes all fields including context_inputs"""
        sig = _hgraph.NodeSignature(
            name="test_node",
            node_type=_hgraph.NodeTypeEnum.SINK_NODE,
            args=["x", "y", "z"],
            time_series_inputs=None,
            time_series_output=None,
            scalars=None,
            src_location=None,
            active_inputs={"x"},
            valid_inputs={"x"},
            all_valid_inputs=None,
            context_inputs={"ctx1", "ctx2"},  # This is the key field to test
            injectable_inputs=None,
            injectables=1,
            capture_exception=True,
            trace_back_depth=10,
            wiring_path_name="my.test.path",
            label="my_label",
            capture_values=True,
            record_replay_id="replay_123",
        )

        d = sig.to_dict()

        # Verify all fields are present
        assert d["name"] == "test_node"
        assert d["node_type"] == _hgraph.NodeTypeEnum.SINK_NODE
        assert d["args"] == ["x", "y", "z"]
        assert d["wiring_path_name"] == "my.test.path"
        assert d["label"] == "my_label"
        assert d["capture_exception"] == True
        assert d["capture_values"] == True
        assert d["trace_back_depth"] == 10
        assert d["record_replay_id"] == "replay_123"
        assert d["active_inputs"] == {"x"}
        assert d["valid_inputs"] == {"x"}

        # The critical test: context_inputs must be in the dict
        assert "context_inputs" in d, "context_inputs missing from to_dict()!"
        assert d["context_inputs"] == {"ctx1", "ctx2"}

    def test_copy_with_no_changes(self):
        """Test C++ copy_with with no arguments preserves everything"""
        sig = _hgraph.NodeSignature(
            name="original",
            node_type=_hgraph.NodeTypeEnum.PULL_SOURCE_NODE,
            args=["a"],
            time_series_inputs=None,
            time_series_output=None,
            scalars=None,
            src_location=None,
            active_inputs=None,
            valid_inputs=None,
            all_valid_inputs=None,
            context_inputs={"ctx"},
            injectable_inputs=None,
            injectables=0,
            capture_exception=False,
            trace_back_depth=5,
            wiring_path_name="original.path",
            label="original_label",
            capture_values=False,
            record_replay_id=None,
        )

        copied = sig.copy_with()

        assert copied.name == sig.name
        assert copied.node_type == sig.node_type
        assert copied.args == sig.args
        assert copied.wiring_path_name == sig.wiring_path_name
        assert copied.label == sig.label
        assert copied.trace_back_depth == sig.trace_back_depth
        assert copied.capture_exception == sig.capture_exception
        assert copied.capture_values == sig.capture_values
        # Critical: context_inputs must be preserved
        assert copied.context_inputs == sig.context_inputs

    def test_copy_with_name_change_preserves_other_fields(self):
        """Test C++ copy_with changing only name preserves all other fields"""
        sig = _hgraph.NodeSignature(
            name="original",
            node_type=_hgraph.NodeTypeEnum.COMPUTE_NODE,
            args=["a", "b"],
            time_series_inputs=None,
            time_series_output=None,
            scalars=None,
            src_location=None,
            active_inputs={"a"},
            valid_inputs=None,
            all_valid_inputs=None,
            context_inputs={"ctx1", "ctx2"},
            injectable_inputs=None,
            injectables=0,
            capture_exception=False,
            trace_back_depth=0,
            wiring_path_name="test",
            label=None,
            capture_values=False,
            record_replay_id=None,
        )

        copied = sig.copy_with(name="modified")

        assert copied.name == "modified"
        assert copied.node_type == sig.node_type  # Must be preserved
        assert copied.args == sig.args
        assert copied.wiring_path_name == sig.wiring_path_name
        assert copied.active_inputs == sig.active_inputs
        # Critical: context_inputs must be preserved!
        assert copied.context_inputs == {"ctx1", "ctx2"}

    def test_copy_with_node_type_preserved(self):
        """Test that C++ copy_with preserves node_type when not explicitly changed"""
        for node_type in [
            _hgraph.NodeTypeEnum.PUSH_SOURCE_NODE,
            _hgraph.NodeTypeEnum.PULL_SOURCE_NODE,
            _hgraph.NodeTypeEnum.COMPUTE_NODE,
            _hgraph.NodeTypeEnum.SINK_NODE,
        ]:
            sig = _hgraph.NodeSignature(
                name="test",
                node_type=node_type,
                args=[],
                time_series_inputs=None,
                time_series_output=None,
                scalars=None,
                src_location=None,
                active_inputs=None,
                valid_inputs=None,
                all_valid_inputs=None,
                context_inputs=None,
                injectable_inputs=None,
                injectables=0,
                capture_exception=False,
                trace_back_depth=0,
                wiring_path_name="test",
                label=None,
                capture_values=False,
                record_replay_id=None,
            )

            # Change something else, node_type should be preserved
            copied = sig.copy_with(name="changed")
            assert copied.node_type == node_type, f"node_type changed from {node_type} to {copied.node_type}"

    def test_copy_with_context_inputs_preserved(self):
        """Test that C++ copy_with specifically preserves context_inputs"""
        sig = _hgraph.NodeSignature(
            name="test",
            node_type=_hgraph.NodeTypeEnum.COMPUTE_NODE,
            args=["a", "b", "c"],
            time_series_inputs=None,
            time_series_output=None,
            scalars=None,
            src_location=None,
            active_inputs={"a", "b"},
            valid_inputs={"a", "b", "c"},
            all_valid_inputs={"a"},
            context_inputs={"ctx1", "ctx2"},
            injectable_inputs=None,
            injectables=0,
            capture_exception=False,
            trace_back_depth=0,
            wiring_path_name="test",
            label=None,
            capture_values=False,
            record_replay_id=None,
        )

        # Change name, but context_inputs should remain
        copied = sig.copy_with(name="changed")

        assert copied.active_inputs == {"a", "b"}
        assert copied.valid_inputs == {"a", "b", "c"}
        assert copied.all_valid_inputs == {"a"}
        # This is the critical assertion
        assert copied.context_inputs == {"ctx1", "ctx2"}, f"context_inputs was {copied.context_inputs}, expected {{'ctx1', 'ctx2'}}"

    def test_copy_with_multiple_changes(self):
        """Test C++ copy_with changing multiple fields at once"""
        sig = _hgraph.NodeSignature(
            name="original",
            node_type=_hgraph.NodeTypeEnum.COMPUTE_NODE,
            args=["a"],
            time_series_inputs=None,
            time_series_output=None,
            scalars=None,
            src_location=None,
            active_inputs=None,
            valid_inputs=None,
            all_valid_inputs=None,
            context_inputs={"original_ctx"},
            injectable_inputs=None,
            injectables=0,
            capture_exception=False,
            trace_back_depth=0,
            wiring_path_name="original.path",
            label=None,
            capture_values=False,
            record_replay_id=None,
        )

        copied = sig.copy_with(
            name="modified",
            label="new_label",
            capture_exception=True,
            trace_back_depth=15,
        )

        assert copied.name == "modified"
        assert copied.label == "new_label"
        assert copied.capture_exception == True
        assert copied.trace_back_depth == 15
        # These must be preserved
        assert copied.node_type == sig.node_type
        assert copied.args == sig.args
        assert copied.wiring_path_name == sig.wiring_path_name
        assert copied.context_inputs == {"original_ctx"}

    def test_uses_properties(self):
        """Test the C++ uses_* properties"""
        sig = _hgraph.NodeSignature(
            name="test",
            node_type=_hgraph.NodeTypeEnum.COMPUTE_NODE,
            args=[],
            time_series_inputs=None,
            time_series_output=None,
            scalars=None,
            src_location=None,
            active_inputs=None,
            valid_inputs=None,
            all_valid_inputs=None,
            context_inputs=None,
            injectable_inputs=None,
            injectables=(
                4
                | 1
                | 16
            ),
            capture_exception=False,
            trace_back_depth=0,
            wiring_path_name="test",
            label=None,
            capture_values=False,
            record_replay_id=None,
        )

        assert sig.uses_scheduler == True
        assert sig.uses_state == True
        assert sig.uses_clock == True
        assert sig.uses_engine == False
        assert sig.uses_output_feedback == False

    def test_node_type_queries(self):
        """Test C++ is_*_node properties"""
        compute_sig = _hgraph.NodeSignature(
            name="test",
            node_type=_hgraph.NodeTypeEnum.COMPUTE_NODE,
            args=[],
            time_series_inputs=None,
            time_series_output=None,
            scalars=None,
            src_location=None,
            active_inputs=None,
            valid_inputs=None,
            all_valid_inputs=None,
            context_inputs=None,
            injectable_inputs=None,
            injectables=0,
            capture_exception=False,
            trace_back_depth=0,
            wiring_path_name="test",
            label=None,
            capture_values=False,
            record_replay_id=None,
        )

        assert compute_sig.is_compute_node == True
        assert compute_sig.is_source_node == False
        assert compute_sig.is_sink_node == False

        source_sig = compute_sig.copy_with(node_type=_hgraph.NodeTypeEnum.PULL_SOURCE_NODE)
        assert source_sig.is_pull_source_node == True
        assert source_sig.is_source_node == True
