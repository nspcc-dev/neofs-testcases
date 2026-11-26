import importlib
from pathlib import Path
from unittest import TestCase

from neofs_testlib.protobuf.generated.refs import types_pb2 as refs_types_pb2
from neofs_testlib.protobuf.generated.session import types_pb2 as session_types_pb2


class TestProtobufBindings(TestCase):
    def test_all_generated_modules_import(self):
        generated_dir = Path(__file__).resolve().parents[2] / "neofs_testlib" / "protobuf" / "generated"
        pb2_files = sorted(generated_dir.rglob("*_pb2.py"))
        self.assertGreater(len(pb2_files), 0)

        for pb2_file in pb2_files:
            rel_parts = pb2_file.relative_to(generated_dir).with_suffix("").parts
            module_name = "neofs_testlib.protobuf.generated." + ".".join(rel_parts)
            module = importlib.import_module(module_name)
            self.assertTrue(hasattr(module, "DESCRIPTOR"), f"{module_name} missing DESCRIPTOR")

    def test_session_v2_28_fields_serialize(self):
        meta_header = session_types_pb2.RequestMetaHeader(valid_until_time=1_800_000_000)
        decoded_meta = session_types_pb2.RequestMetaHeader.FromString(meta_header.SerializeToString(deterministic=True))
        self.assertEqual(decoded_meta.valid_until_time, 1_800_000_000)

        sig = refs_types_pb2.Signature(
            key=b"\x01\x02",
            sign=b"\x03\x04",
            scheme=refs_types_pb2.SignatureScheme.N3,
        )
        verify_header = session_types_pb2.RequestVerificationHeader(request_signature=sig)
        decoded_verify = session_types_pb2.RequestVerificationHeader.FromString(
            verify_header.SerializeToString(deterministic=True)
        )
        self.assertEqual(decoded_verify.request_signature.key, b"\x01\x02")
        self.assertEqual(decoded_verify.request_signature.sign, b"\x03\x04")
        self.assertEqual(decoded_verify.request_signature.scheme, refs_types_pb2.SignatureScheme.N3)
