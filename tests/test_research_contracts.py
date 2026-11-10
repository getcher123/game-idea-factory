"""Research contract regression tests using a disposable synthetic run only.

These fixtures deliberately contain no observations from the real market and
cannot be used for a pilot. The command-line interface never enables synthetic
mode; that switch is available to this test's Pipeline instance alone.
"""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "tools"))

from pipeline import Pipeline, PipelineError, validate_schema  # noqa: E402


RUN_ID = "synthetic_research_contracts"
FIXTURES = {
    "evidence.jsonl": "evidence-synthetic.jsonl",
    "signals.jsonl": "signals-synthetic.jsonl",
    "clusters.json": "clusters-synthetic.json",
    "opportunities.json": "opportunities-synthetic.json",
}
SUPPORTED_SCHEMA_KEYWORDS = {
    "$schema", "$id", "title", "description", "type", "required",
    "properties", "additionalProperties", "items", "enum", "const",
    "pattern", "minLength", "maxLength", "minItems", "maxItems",
    "minimum", "maximum", "anyOf", "uniqueItems",
}


class ResearchContractsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="game-idea-factory-research-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for directory in ("config", "schemas"):
            shutil.copytree(PROJECT_ROOT / directory, self.root / directory)
        self.pipeline = Pipeline(self.root, allow_synthetic=True)
        self.pipeline.init(RUN_ID, through="G2")
        self.run_dir = self.root / "runs" / RUN_ID
        for target, fixture in FIXTURES.items():
            shutil.copyfile(PROJECT_ROOT / "examples" / fixture, self.run_dir / target)

    def read_file(self, name: str):
        content = (self.run_dir / name).read_text(encoding="utf-8")
        if name.endswith(".jsonl"):
            return [json.loads(line) for line in content.splitlines() if line.strip()]
        return json.loads(content)

    def write_file(self, name: str, value) -> None:
        if name.endswith(".jsonl"):
            content = "\n".join(json.dumps(row, ensure_ascii=False) for row in value) + "\n"
        else:
            content = json.dumps(value, ensure_ascii=False, indent=2) + "\n"
        (self.run_dir / name).write_text(content, encoding="utf-8")

    def test_synthetic_fixture_validates_and_unknowns_survive_unchanged(self) -> None:
        before = {name: (self.run_dir / name).read_bytes() for name in FIXTURES}
        result = self.pipeline.validate_research(RUN_ID)
        self.assertEqual(result["evidence"], 4)
        self.assertEqual(result["signals"], 3)
        self.assertEqual(result["clusters"], 3)
        self.assertEqual(result["territories"], 3)
        evidence = self.read_file("evidence.jsonl")
        self.assertEqual(evidence[0]["traffic_origin"], "unknown")
        self.assertEqual(evidence[0]["region"], "unknown")
        self.assertIsNone(evidence[0]["metrics"][0]["baseline_value"])
        self.assertEqual(evidence[0]["source_family"], evidence[1]["source_family"])
        self.assertEqual(before, {name: (self.run_dir / name).read_bytes() for name in FIXTURES})

    def test_real_mode_does_not_accept_synthetic_research(self) -> None:
        with self.assertRaises(PipelineError):
            Pipeline(self.root, allow_synthetic=False).validate_research(RUN_ID)

    def test_broken_evidence_links_and_source_family_are_blocked(self) -> None:
        original_signals = self.read_file("signals.jsonl")
        signals = deepcopy(original_signals)
        signals[0]["evidence_ids"] = ["E-SYN-NOT-FOUND"]
        self.write_file("signals.jsonl", signals)
        with self.assertRaises(PipelineError):
            self.pipeline.validate_research(RUN_ID)
        self.write_file("signals.jsonl", original_signals)

        original_evidence = self.read_file("evidence.jsonl")
        evidence = deepcopy(original_evidence)
        evidence[1]["source_family"] = "synthetic-false-independent-repost"
        self.write_file("evidence.jsonl", evidence)
        with self.assertRaises(PipelineError):
            self.pipeline.validate_research(RUN_ID)
        self.write_file("evidence.jsonl", original_evidence)

        evidence = deepcopy(original_evidence)
        evidence[0]["repost_of"] = "E-SYN-002"
        self.write_file("evidence.jsonl", evidence)
        with self.assertRaises(PipelineError):
            self.pipeline.validate_research(RUN_ID)

    def test_territory_cannot_claim_independent_families_it_does_not_cite(self) -> None:
        opportunities = self.read_file("opportunities.json")
        first = opportunities["territories"][0]
        self.assertEqual(first["signal_ids"], ["S-SYN-001"])
        self.assertEqual(first["source_independence"], "single_source")
        first["source_independence"] = "multiple_independent"
        self.write_file("opportunities.json", opportunities)
        with self.assertRaises(PipelineError):
            self.pipeline.validate_research(RUN_ID)

    def test_metric_window_compares_utc_instants(self) -> None:
        evidence = self.read_file("evidence.jsonl")
        metric = evidence[0]["metrics"][0]

        # 02:00 UTC is later than 09:00 at UTC+09 (00:00 UTC).
        metric["window_start"] = "2026-10-01T02:00:00Z"
        metric["window_end"] = "2026-10-01T09:00:00+09:00"
        self.write_file("evidence.jsonl", evidence)
        with self.assertRaises(PipelineError):
            self.pipeline.validate_research(RUN_ID)

        # Same local wall-clock strings reversed: 00:00 UTC precedes 02:00 UTC.
        metric["window_start"] = "2026-10-01T09:00:00+09:00"
        metric["window_end"] = "2026-10-01T02:00:00Z"
        self.write_file("evidence.jsonl", evidence)
        self.pipeline.validate_research(RUN_ID)

    def test_nonfinite_metric_numbers_are_rejected_as_invalid_json(self) -> None:
        original = (self.run_dir / "evidence.jsonl").read_text(encoding="utf-8")
        self.assertIn('"value":1800', original)
        for token in ("Infinity", "-Infinity", "NaN"):
            with self.subTest(token=token):
                altered = original.replace('"value":1800', f'"value":{token}', 1)
                (self.run_dir / "evidence.jsonl").write_text(altered, encoding="utf-8")
                with self.assertRaises(PipelineError):
                    self.pipeline.validate_research(RUN_ID)

    def test_schemas_use_supported_validator_vocabulary(self) -> None:
        def scan(schema, address):
            self.assertIsInstance(schema, dict, address)
            for keyword in schema:
                self.assertIn(keyword, SUPPORTED_SCHEMA_KEYWORDS, f"{address}.{keyword}")
            for name, prop in schema.get("properties", {}).items():
                scan(prop, f"{address}.properties.{name}")
            for branch_index, branch in enumerate(schema.get("anyOf", [])):
                scan(branch, f"{address}.anyOf[{branch_index}]")
            for field in ("items", "additionalProperties"):
                if isinstance(schema.get(field), dict):
                    scan(schema[field], f"{address}.{field}")

        for path in sorted((self.root / "schemas").glob("*.schema.json")):
            with self.subTest(schema=path.name):
                schema = json.loads(path.read_text(encoding="utf-8"))
                self.assertEqual(schema.get("$schema"), "https://json-schema.org/draft/2020-12/schema")
                scan(schema, path.name)

    def test_every_example_matches_internal_and_optional_external_contract(self) -> None:
        try:
            from jsonschema import Draft202012Validator
        except ImportError:
            Draft202012Validator = None
        cases = {
            "evidence": "evidence-synthetic.jsonl",
            "signal": "signals-synthetic.jsonl",
            "clusters": "clusters-synthetic.json",
            "opportunities": "opportunities-synthetic.json",
            "review": "review-synthetic.json",
            "proposal": "proposal-synthetic.json",
        }
        for schema_name, fixture in cases.items():
            schema = json.loads((self.root / "schemas" / f"{schema_name}.schema.json").read_text())
            if Draft202012Validator is not None:
                Draft202012Validator.check_schema(schema)
            source = (PROJECT_ROOT / "examples" / fixture).read_text(encoding="utf-8")
            rows = [json.loads(line) for line in source.splitlines() if line.strip()] \
                if fixture.endswith(".jsonl") else [json.loads(source)]
            for index, row in enumerate(rows):
                with self.subTest(schema=schema_name, row=index):
                    self.assertTrue(row["is_synthetic"])
                    validate_schema(row, schema)
                    if Draft202012Validator is not None:
                        Draft202012Validator(schema).validate(row)


if __name__ == "__main__":
    unittest.main()
