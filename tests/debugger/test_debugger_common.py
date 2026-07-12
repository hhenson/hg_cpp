"""Tests for debugger-independent common ABI formatting."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2] / "tools" / "debugger"))

import hgraph_debug_common as common


def node_schema(**overrides):
    snapshot = {
        "magic": common.SCHEMA_HEADER_MAGIC,
        "abi_version": common.SCHEMA_HEADER_ABI_VERSION,
        "family": 3,
        "kind": 0,
        "label": "compute[int]",
        "introspection": 0,
    }
    snapshot.update(overrides)
    return snapshot


def node_record(**overrides):
    snapshot = {
        "magic": common.TYPE_RECORD_MAGIC,
        "abi_version": common.TYPE_RECORD_ABI_VERSION,
        "role": 3,
        "reserved0": 0,
        "ops_abi_version": 1,
        "reserved1": 0,
        "capabilities": (1 << 4) | (1 << 9),
        "implementation_label": "hgraph.node.native",
        "schema": node_schema(),
        "plan": 0x1000,
        "ops": 0x2000,
        "debug": 0,
    }
    snapshot.update(overrides)
    return snapshot


class CommonDebuggerFormattingTest(unittest.TestCase):
    def test_schema_summary_uses_common_classification(self):
        self.assertTrue(common.schema_valid(node_schema()))
        self.assertEqual(
            common.schema_summary(node_schema()),
            'SchemaHeader{valid family=Node kind=Compute(0) abi=1 label="compute[int]"}',
        )

    def test_schema_validation_rejects_unknown_abi_and_family(self):
        self.assertFalse(common.schema_valid(node_schema(abi_version=2)))
        self.assertFalse(common.schema_valid(node_schema(family=9)))

    def test_record_summary_shows_semantic_and_implementation_identity(self):
        snapshot = node_record()
        self.assertTrue(common.record_valid(snapshot))
        summary = common.record_summary(snapshot)
        self.assertIn("valid Node/Runtime", summary)
        self.assertIn('semantic="compute[int]"', summary)
        self.assertIn('implementation="hgraph.node.native"', summary)
        self.assertIn("capabilities=mutable|viewable", summary)
        self.assertIn("plan=0x1000 ops=0x2000 debug=null", summary)

    def test_record_validation_rejects_role_and_required_pointer_mismatches(self):
        self.assertFalse(common.record_valid(node_record(role=1)))
        self.assertFalse(common.record_valid(node_record(plan=0)))
        self.assertFalse(common.record_valid(node_record(ops_abi_version=0)))
        self.assertFalse(common.record_valid(node_record(capabilities=1 << 20)))

    def test_pointer_summaries_cover_unbound_typed_null_live_and_malformed(self):
        record = node_record()
        self.assertEqual(common.pointer_summary("AnyPtr", 0, 0, 0), "AnyPtr{unbound}")

        typed_null = common.pointer_summary("TypedPtr", 0x3000, 0, 0, record)
        self.assertIn("TypedPtr{typed-null Node/Runtime", typed_null)
        self.assertIn("access=read-only", typed_null)

        live = common.pointer_summary("TypedPtr", 0x3000, 0x4000, 1, record)
        self.assertIn("TypedPtr{live Node/Runtime", live)
        self.assertIn("access=writable", live)
        self.assertIn("record=0x3000 data=0x4000", live)

        malformed = common.pointer_summary("AnyPtr", 0, 0x4000, 3)
        self.assertIn("AnyPtr{malformed", malformed)
        self.assertIn("access=invalid(3)", malformed)

    def test_family_specific_kinds_do_not_overlap_in_output(self):
        self.assertEqual(common.kind_text(1, 0), "Atomic(0)")
        self.assertEqual(common.kind_text(2, 0), "TS(0)")
        self.assertEqual(common.kind_text(3, 0), "Compute(0)")
        self.assertEqual(common.kind_text(5, 0), "Simulation(0)")
        self.assertEqual(common.kind_text(4, common.TYPE_KIND_NONE), "none")


if __name__ == "__main__":
    unittest.main()
