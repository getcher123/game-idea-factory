"""Behavioral tests for the local gate registry.

Everything runs in a disposable repository copy. In particular, fixture approvals,
reviews and evidence cannot be mistaken for a live pilot or real user decisions.
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

from pipeline import Pipeline, PipelineError  # noqa: E402


RUN_ID = "synthetic_trial_01"
RESEARCH_FILES = ("evidence.jsonl", "signals.jsonl", "clusters.json", "opportunities.json")


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


class PipelineBehaviorTests(unittest.TestCase):
    """Checks failure-closed transitions, provenance, and preserved revisions."""

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="game-idea-factory-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for directory in ("config", "schemas"):
            shutil.copytree(PROJECT_ROOT / directory, self.root / directory)
        self.pipeline = Pipeline(self.root, allow_synthetic=True)
        self.run_id = RUN_ID
        self.review_sequence = 0

    @property
    def run_dir(self) -> Path:
        return self.root / "runs" / self.run_id

    def init(self, through: str = "G2") -> None:
        self.pipeline.init(self.run_id, through=through)

    def proposal(self, gate: str, version: str = "v1", *, author: str | None = None,
                 options: list[dict] | None = None, extra_files: list[str] | None = None) -> dict:
        """Create and register a versioned synthetic gate manifest and its material."""
        material = f"artifacts/{gate}-{version}.md"
        (self.run_dir / material).write_text(
            f"Synthetic, non-pilot material for {gate}, revision {version}.\n", encoding="utf-8"
        )
        if options is None:
            if gate == "G1":
                territory_doc = read_json(self.run_dir / "opportunities.json")
                options = [{"id": t["id"], "label": t["title"]}
                           for t in territory_doc["territories"]]
            elif gate in ("G2", "G6"):
                options = [{"id": f"variant-{number}", "label": f"Variant {number}"}
                           for number in (1, 2, 3)]
            else:
                options = [{"id": "browser", "label": "Browser"},
                           {"id": "mobile", "label": "Mobile"}]
        files = [material, *(extra_files or [])]
        if gate == "G1":
            files += list(RESEARCH_FILES)
        manifest = {
            "gate": gate,
            "author_ids": [author or f"author-{gate.lower()}"],
            "files": files,
            "summary": f"Synthetic proposal {gate} {version}",
            "options": options,
            "is_synthetic": True,
        }
        manifest_path = f"artifacts/{gate}-{version}.manifest.json"
        write_json(self.run_dir / manifest_path, manifest)
        result = self.pipeline.propose(self.run_id, manifest_path)
        self.assertEqual(result["gate"], gate)
        return result

    def approve(self, proposal: dict, *, choice: str | None = None) -> dict:
        if choice is None:
            choice = proposal["options"][0]["id"]
        return self.pipeline.approve(
            self.run_id,
            proposal["sha256"],
            choice,
            f"I explicitly approve {choice} in the disposable test fixture.",
            f"synthetic-conversation/{proposal['gate']}/{choice}",
        )

    def reach_g1(self) -> dict:
        self.init()
        self.approve(self.proposal("G0"))
        self.assertEqual(self.pipeline.status(self.run_id)["gate"], "G1")
        self.copy_research_fixtures()
        return self.proposal("G1")

    def copy_research_fixtures(self) -> None:
        sources = {
            "evidence.jsonl": "evidence-synthetic.jsonl",
            "signals.jsonl": "signals-synthetic.jsonl",
            "clusters.json": "clusters-synthetic.json",
            "opportunities.json": "opportunities-synthetic.json",
        }
        for target, source in sources.items():
            shutil.copyfile(PROJECT_ROOT / "examples" / source, self.run_dir / target)

    def review(self, proposal: dict, lens: str, reviewer_id: str, *, verdict: str = "PASS",
               findings: list[dict] | None = None) -> Path:
        self.review_sequence += 1
        report = {
            "run_id": self.run_id,
            "gate": proposal["gate"],
            "lens": lens,
            "reviewer_id": reviewer_id,
            "artifact_sha256": proposal["sha256"],
            "verdict": verdict,
            "findings": findings if findings is not None else [],
            "evidence_refs": [],
            "reviewed_at": "2026-11-11T11:00:00Z",
            "is_synthetic": True,
        }
        path = self.run_dir / "reviews" / f"{proposal['gate']}-{lens}-{self.review_sequence:02d}.json"
        write_json(path, report)
        self.pipeline.review(self.run_id, str(path.relative_to(self.run_dir)))
        return path

    def pass_g1(self, proposal: dict) -> None:
        self.review(proposal, "evidence", "independent-evidence-reviewer")
        self.review(proposal, "market_skeptic", "independent-market-reviewer")
        self.assertEqual(self.pipeline.status(self.run_id)["status"], "awaiting_user")

    def reach_g2(self) -> dict:
        g1 = self.reach_g1()
        self.pass_g1(g1)
        self.approve(g1)
        self.assertEqual(self.pipeline.status(self.run_id)["gate"], "G2")
        return self.proposal("G2")

    @staticmethod
    def finding(finding_id: str = "finding-1", *, severity: str = "critical",
                resolution: str = "open", risk_acceptance: dict | None = None) -> dict:
        return {
            "id": finding_id,
            "severity": severity,
            "observation": "Synthetic finding with a test-only observation.",
            "source": "Synthetic artifact G1",
            "action": "Fix or explicitly resolve the finding.",
            "resolution": resolution,
            "risk_acceptance": risk_acceptance,
        }

    def test_init_safe_ids_duplicate_run_and_initial_audit(self) -> None:
        with self.assertRaises(PipelineError):
            self.pipeline.init("../escape", through="G2")
        with self.assertRaises(PipelineError):
            self.pipeline.init("A-B", through="G2")
        self.init()
        self.assertEqual(self.pipeline.audit(self.run_id)["status"], "draft")
        with self.assertRaises(PipelineError):
            self.init()

    def test_real_mode_rejects_synthetic_manifest(self) -> None:
        real_pipeline = Pipeline(self.root, allow_synthetic=False)
        real_pipeline.init("real_mode_guard", through="G2")
        directory = self.root / "runs" / "real_mode_guard"
        (directory / "artifacts" / "fake.md").write_text("Synthetic example only\n")
        write_json(directory / "artifacts" / "fake.manifest.json", {
            "gate": "G0", "author_ids": ["author"],
            "files": ["artifacts/fake.md"], "summary": "Synthetic fixture",
            "options": [{"id": "test", "label": "Synthetic choice"}],
            "is_synthetic": True,
        })
        with self.assertRaises(PipelineError):
            real_pipeline.propose("real_mode_guard", "artifacts/fake.manifest.json")
        self.assertEqual(real_pipeline.status("real_mode_guard")["status"], "draft")

    def test_unapproved_and_ambiguous_choices_do_not_advance(self) -> None:
        self.init()
        g0 = self.proposal("G0")
        with self.assertRaises(PipelineError):
            self.pipeline.approve(self.run_id, g0["sha256"], "unknown-choice", "yes", "message-1")
        with self.assertRaises(PipelineError):
            self.pipeline.approve(self.run_id, g0["sha256"], "browser", "  ", "message-1")
        with self.assertRaises(PipelineError):
            self.pipeline.approve(self.run_id, g0["sha256"], "browser", "yes", "  ")
        self.assertEqual(self.pipeline.status(self.run_id)["gate"], "G0")
        self.assertEqual(self.pipeline.audit(self.run_id)["status"], "awaiting_user")

    def test_manifest_and_content_mutation_invalidate_approval(self) -> None:
        self.init()
        g0 = self.proposal("G0")
        content = self.run_dir / g0["files"][0]["path"]
        original = content.read_bytes()
        content.write_bytes(original + b"unreviewed change")
        with self.assertRaises(PipelineError):
            self.pipeline.audit(self.run_id)
        with self.assertRaises(PipelineError):
            self.approve(g0)
        content.write_bytes(original)
        self.pipeline.audit(self.run_id)
        manifest = self.run_dir / g0["manifest"]
        original_manifest = manifest.read_bytes()
        data = read_json(manifest)
        data["summary"] += " unreviewed"
        write_json(manifest, data)
        with self.assertRaises(PipelineError):
            self.pipeline.audit(self.run_id)
        manifest.write_bytes(original_manifest)
        self.pipeline.audit(self.run_id)

    def test_superseded_proposal_requires_its_own_approval_sha(self) -> None:
        self.init()
        first = self.proposal("G0", "v1")
        replacement = self.proposal("G0", "v2")
        self.assertNotEqual(first["sha256"], replacement["sha256"])
        with self.assertRaises(PipelineError):
            self.approve(first)
        self.approve(replacement)
        self.assertEqual(self.pipeline.audit(self.run_id)["gate"], "G1")
        self.assertEqual(len([x for x in read_json(self.run_dir / "run.json")["history"]
                              if x["event"] == "proposed"]), 2)

    def test_required_lenses_and_distinct_reviewers(self) -> None:
        g1 = self.reach_g1()
        with self.assertRaises(PipelineError):
            self.approve(g1)
        with self.assertRaises(PipelineError):
            self.review(g1, "evidence", "author-g1")
        self.review(g1, "evidence", "reviewer-one")
        with self.assertRaises(PipelineError):
            self.approve(g1)
        with self.assertRaises(PipelineError):
            self.review(g1, "market_skeptic", "reviewer-one")
        self.assertEqual(self.pipeline.status(self.run_id)["status"], "awaiting_review")
        self.review(g1, "market_skeptic", "reviewer-two")
        self.approve(g1)
        self.assertEqual(self.pipeline.audit(self.run_id)["gate"], "G2")

    def test_blocked_and_unresolved_findings_stop_progress(self) -> None:
        g1 = self.reach_g1()
        self.review(g1, "evidence", "reviewer-one", verdict="CHANGES_REQUIRED",
                    findings=[self.finding()])
        self.review(g1, "market_skeptic", "reviewer-two")
        self.assertEqual(self.pipeline.status(self.run_id)["status"], "blocked")
        with self.assertRaises(PipelineError):
            self.approve(g1)
        with self.assertRaises(PipelineError):
            self.review(g1, "evidence", "reviewer-one", findings=[self.finding()])
        with self.assertRaises(PipelineError):
            self.review(g1, "evidence", "reviewer-one", findings=[self.finding(
                severity="critical", resolution="accepted_risk",
                risk_acceptance={"quote": "Accept", "message_ref": "synthetic-ref"},
            )])

    def test_prior_major_finding_cannot_disappear_in_same_version(self) -> None:
        g1 = self.reach_g1()
        self.review(g1, "evidence", "reviewer-one", verdict="CHANGES_REQUIRED",
                    findings=[self.finding("prior-major", severity="major")])
        self.review(g1, "market_skeptic", "reviewer-two")
        # A second PASS on identical artifacts must not silently erase the open finding.
        with self.assertRaises(PipelineError):
            self.review(g1, "evidence", "reviewer-one", verdict="PASS", findings=[])
        with self.assertRaises(PipelineError):
            self.approve(g1)

    def test_reproposing_identical_package_cannot_erase_reviews(self) -> None:
        g1 = self.reach_g1()
        self.review(g1, "evidence", "reviewer-one", verdict="CHANGES_REQUIRED",
                    findings=[self.finding("still-open")])
        before = (self.run_dir / "run.json").read_bytes()
        with self.assertRaises(PipelineError):
            self.pipeline.propose(self.run_id, g1["manifest"])
        self.assertEqual((self.run_dir / "run.json").read_bytes(), before)

    def test_unresolved_critical_severity_cannot_be_downgraded(self) -> None:
        g1 = self.reach_g1()
        self.review(g1, "evidence", "reviewer-one", verdict="CHANGES_REQUIRED",
                    findings=[self.finding("critical-issue", severity="critical")])
        # Interposing an open major report must not make a critical risk acceptable.
        with self.assertRaises(PipelineError):
            self.review(g1, "evidence", "reviewer-one", verdict="CHANGES_REQUIRED",
                        findings=[self.finding("critical-issue", severity="major")])
        self.assertEqual(self.pipeline.status(self.run_id)["status"], "blocked")

    def test_review_source_is_immutable_and_cannot_be_reused(self) -> None:
        g1 = self.reach_g1()
        path = self.review(g1, "evidence", "reviewer-one")
        self.assertTrue(self.pipeline.audit(self.run_id)["ok"])
        with self.assertRaises(PipelineError):
            self.pipeline.review(self.run_id, str(path.relative_to(self.run_dir)))
        data = read_json(path)
        data["verdict"] = "NOT_RUN"
        write_json(path, data)
        with self.assertRaises(PipelineError):
            self.pipeline.audit(self.run_id)

    def test_user_approval_file_and_history_are_audited(self) -> None:
        self.init()
        g0 = self.proposal("G0")
        self.approve(g0)
        run = read_json(self.run_dir / "run.json")
        decision_file = self.run_dir / run["records"]["G0"]["decision"]["record_path"]
        decision = read_json(decision_file)
        decision["quote"] = "Altered approval text"
        write_json(decision_file, decision)
        with self.assertRaises(PipelineError):
            self.pipeline.audit(self.run_id)

    def test_history_replay_rejects_manual_stage_or_event_edit(self) -> None:
        self.init()
        g0 = self.proposal("G0")
        self.approve(g0)
        run_file = self.run_dir / "run.json"
        original = read_json(run_file)
        corrupted = deepcopy(original)
        corrupted["current_gate"] = "G2"
        write_json(run_file, corrupted)
        with self.assertRaises(PipelineError):
            self.pipeline.audit(self.run_id)
        corrupted = deepcopy(original)
        corrupted["history"].pop()
        write_json(run_file, corrupted)
        with self.assertRaises(PipelineError):
            self.pipeline.audit(self.run_id)

    def test_back_invalidates_active_records_but_keeps_history_and_snapshots(self) -> None:
        g2 = self.reach_g2()
        self.assertEqual(self.pipeline.status(self.run_id)["gate"], "G2")
        before = read_json(self.run_dir / "run.json")
        g1_sha = before["records"]["G1"]["proposal"]["sha256"]
        g2_sha = g2["sha256"]
        self.pipeline.decide(self.run_id, "back", "Please rework the research",
                             "synthetic-conversation/back-to-research", target="G1")
        after = read_json(self.run_dir / "run.json")
        self.assertEqual(after["current_gate"], "G1")
        self.assertEqual(after["status"], "needs_revalidation")
        self.assertEqual(set(after["records"]), {"G0"})
        invalidated = after["history"][-1]["user_decision"]["invalidated_records"]
        self.assertEqual(set(invalidated), {"G1", "G2"})
        self.assertTrue((self.run_dir / "snapshots" / g1_sha).is_dir())
        self.assertTrue((self.run_dir / "snapshots" / g2_sha).is_dir())
        self.assertTrue(self.pipeline.audit(self.run_id)["ok"])

    def test_invalidated_snapshot_must_remain_verifiable(self) -> None:
        self.init()
        g0 = self.proposal("G0")
        self.approve(g0)
        self.pipeline.decide(self.run_id, "back", "Reopen brief", "synthetic-conversation/back-G0",
                             target="G0")
        self.assertTrue(self.pipeline.audit(self.run_id)["ok"])
        archived_material = self.run_dir / "snapshots" / g0["sha256"] / g0["files"][0]["path"]
        archived_material.unlink()
        with self.assertRaises(PipelineError):
            self.pipeline.audit(self.run_id)

    def test_g2_scope_is_hard_stop_until_explicit_extension(self) -> None:
        g2 = self.reach_g2()
        result = self.approve(g2)
        self.assertEqual(result, {"gate": "G2", "status": "complete"})
        self.assertEqual(self.pipeline.audit(self.run_id)["status"], "complete")
        with self.assertRaises(PipelineError):
            self.proposal("G3")
        with self.assertRaises(PipelineError):
            self.pipeline.extend(self.run_id, "G2", "Continue", "synthetic-ref")
        self.pipeline.extend(self.run_id, "G9", "Please continue through design G9",
                             "synthetic-conversation/extend-G9")
        self.assertEqual(self.pipeline.audit(self.run_id)["gate"], "G3")
        self.assertEqual(self.pipeline.status(self.run_id)["end_gate"], "G9")

    def test_hold_stop_and_user_resume_are_explicit(self) -> None:
        self.init()
        g0 = self.proposal("G0")
        self.pipeline.decide(self.run_id, "hold", "Pause this project", "synthetic-conversation/hold")
        with self.assertRaises(PipelineError):
            self.approve(g0)
        self.pipeline.decide(self.run_id, "resume", "Resume this project", "synthetic-conversation/resume")
        self.approve(g0)
        self.pipeline.decide(self.run_id, "stop", "Close this project", "synthetic-conversation/stop")
        self.assertEqual(self.pipeline.audit(self.run_id)["status"], "stopped")
        with self.assertRaises(PipelineError):
            self.proposal("G1")

    def test_artifact_paths_cannot_escape_run(self) -> None:
        self.init()
        for invalid in ("../outside.json", "/etc/passwd", "artifacts/../../escape.json"):
            with self.subTest(manifest=invalid):
                with self.assertRaises(PipelineError):
                    self.pipeline.propose(self.run_id, invalid)
        source = self.root / "outside-run.txt"
        source.write_text("This is outside the run boundary")
        (self.run_dir / "artifacts" / "external-link.txt").symlink_to(source)
        write_json(self.run_dir / "artifacts" / "escape.manifest.json", {
            "gate": "G0", "author_ids": ["orchestrator"],
            "files": ["artifacts/external-link.txt"], "summary": "Forbidden external symlink",
            "options": [{"id": "x", "label": "X"}], "is_synthetic": True,
        })
        with self.assertRaises(PipelineError):
            self.pipeline.propose(self.run_id, "artifacts/escape.manifest.json")

    def test_g1_research_validates_evidence_links_and_repost_families(self) -> None:
        self.init()
        self.copy_research_fixtures()
        result = self.pipeline.validate_research(self.run_id)
        self.assertEqual(result["territories"], 3)
        self.assertEqual(result["source_families_total"], 3)

        signals_path = self.run_dir / "signals.jsonl"
        original_signals = signals_path.read_bytes()
        signals = [json.loads(line) for line in signals_path.read_text().splitlines()]
        signals[0]["evidence_ids"] = ["unknown-evidence"]
        signals_path.write_text("\n".join(json.dumps(item) for item in signals) + "\n")
        with self.assertRaises(PipelineError):
            self.pipeline.validate_research(self.run_id)
        signals_path.write_bytes(original_signals)

        evidence_path = self.run_dir / "evidence.jsonl"
        original_evidence = evidence_path.read_bytes()
        evidence = [json.loads(line) for line in evidence_path.read_text().splitlines()]
        evidence[1]["source_family"] = "fake-independent-source"
        evidence_path.write_text("\n".join(json.dumps(item) for item in evidence) + "\n")
        with self.assertRaises(PipelineError):
            self.pipeline.validate_research(self.run_id)
        evidence_path.write_bytes(original_evidence)

        cluster_path = self.run_dir / "clusters.json"
        original_clusters = read_json(cluster_path)
        corrupted = deepcopy(original_clusters)
        corrupted["clusters"][0]["source_families"] = ["fake-independent-source"]
        write_json(cluster_path, corrupted)
        with self.assertRaises(PipelineError):
            self.pipeline.validate_research(self.run_id)
        write_json(cluster_path, original_clusters)
        self.assertEqual(self.pipeline.validate_research(self.run_id)["source_families_used"], 3)


if __name__ == "__main__":
    unittest.main()
