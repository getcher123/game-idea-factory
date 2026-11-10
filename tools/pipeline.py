#!/usr/bin/env python3
"""Local, dependency-free gate registry. Agents perform research and design.

This tool checks recorded evidence and integrity, not the identity of a human
or agent. It never calls a model, publishes a game, or invents user consent.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import date, datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import sys


ROOT = Path(__file__).resolve().parents[1]
GATES = tuple(f"G{i}" for i in range(12))
RESEARCH_FILES = ("evidence.jsonl", "signals.jsonl", "clusters.json", "opportunities.json")
ID_PATTERN = r"[a-z0-9][a-z0-9_-]{2,63}"


class PipelineError(ValueError):
    """An actionable validation or transition error."""


def require(condition, message):
    if not condition:
        raise PipelineError(message)


def now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def digest(data):
    return hashlib.sha256(data).hexdigest()


def canonical(data):
    return json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def strict_json(text):
    def reject_constant(value):
        raise PipelineError(f"Недопустимое JSON-число: {value}")
    return json.loads(text, parse_constant=reject_constant)


def read_json(path):
    try:
        return strict_json(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise PipelineError(f"Не удалось прочитать JSON {path}: {exc}") from exc


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temp, path)


def safe_file(directory, relative, exists=True):
    require(isinstance(relative, str) and bool(relative), "Нужен относительный путь файла")
    path = Path(relative)
    require(not path.is_absolute() and ".." not in path.parts and "\\" not in relative,
            f"Недопустимый путь: {relative}")
    require(directory.absolute() == directory.resolve(), "Симлинки в корне пакета запрещены")
    cursor = directory
    for part in path.parts:
        cursor = cursor / part
        require(not cursor.is_symlink(), f"Симлинки в пакете запрещены: {relative}")
    target = (directory / path).resolve()
    require(target.is_relative_to(directory.resolve()), f"Путь вышел за границы run: {relative}")
    if exists:
        require(target.is_file(), f"Файл отсутствует: {relative}")
    return target


def validate_schema(value, schema, at="$"):
    """Validate the documented JSON Schema subset used by this repository."""
    if "anyOf" in schema:
        for candidate in schema["anyOf"]:
            try:
                validate_schema(value, candidate, at)
                break
            except PipelineError:
                pass
        else:
            raise PipelineError(f"{at}: не подходит ни один anyOf")
    types = schema.get("type", [])
    if isinstance(types, str):
        types = [types]
    matches = {
        "object": isinstance(value, dict), "array": isinstance(value, list),
        "string": isinstance(value, str), "boolean": isinstance(value, bool),
        "integer": isinstance(value, int) and not isinstance(value, bool),
        "number": isinstance(value, (int, float)) and not isinstance(value, bool),
        "null": value is None,
    }
    require(not types or any(matches.get(t, False) for t in types), f"{at}: ожидается {types}")
    if "const" in schema:
        require(type(value) is type(schema["const"]) and value == schema["const"], f"{at}: неверная константа")
    if "enum" in schema:
        require(value in schema["enum"], f"{at}: неизвестное значение {value!r}")
    if isinstance(value, dict):
        for key in schema.get("required", []):
            require(key in value, f"{at}: отсутствует {key}")
        props = schema.get("properties", {})
        for key, item in value.items():
            if key in props:
                validate_schema(item, props[key], f"{at}.{key}")
            elif schema.get("additionalProperties") is False:
                raise PipelineError(f"{at}: неизвестное поле {key}")
            elif isinstance(schema.get("additionalProperties"), dict):
                validate_schema(item, schema["additionalProperties"], f"{at}.{key}")
    if isinstance(value, list):
        require(len(value) >= schema.get("minItems", 0), f"{at}: слишком мало элементов")
        require(len(value) <= schema.get("maxItems", sys.maxsize), f"{at}: слишком много элементов")
        if schema.get("uniqueItems"):
            require(len({canonical(x) for x in value}) == len(value), f"{at}: повторяющиеся элементы")
        for i, item in enumerate(value):
            validate_schema(item, schema.get("items", {}), f"{at}[{i}]")
    if isinstance(value, str):
        require(len(value.strip()) >= schema.get("minLength", 0), f"{at}: пустое/короткое значение")
        require(len(value) <= schema.get("maxLength", sys.maxsize), f"{at}: слишком длинное значение")
        if "pattern" in schema:
            require(re.search(schema["pattern"], value) is not None, f"{at}: неверный формат")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        require(not isinstance(value, float) or math.isfinite(value), f"{at}: число должно быть конечным")
        require(value >= schema.get("minimum", float("-inf")), f"{at}: значение ниже minimum")
        require(value <= schema.get("maximum", float("inf")), f"{at}: значение выше maximum")


def check_date(value, label, timestamp=False):
    if value is None:
        return
    try:
        if timestamp or "T" in value:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            require(parsed.tzinfo is not None, f"{label}: нужен часовой пояс")
            return parsed.astimezone(timezone.utc)
        else:
            return datetime.combine(date.fromisoformat(value), datetime.min.time(), timezone.utc)
    except (ValueError, TypeError) as exc:
        raise PipelineError(f"{label}: нужна дата ISO 8601") from exc


class Pipeline:
    def __init__(self, root=ROOT, *, allow_synthetic=False):
        self.root = Path(root).resolve()
        self.routes = read_json(self.root / "config/routes.json")["routes"]
        # Tests use a temporary repository. The CLI never enables this option.
        self.allow_synthetic = allow_synthetic

    def contract(self, name, value):
        validate_schema(value, read_json(self.root / "schemas" / f"{name}.schema.json"))
        if isinstance(value, dict) and value.get("is_synthetic"):
            require(self.allow_synthetic, f"{name}: синтетические данные запрещены в настоящем run")

    def directory(self, run_id):
        require(re.fullmatch(ID_PATTERN, run_id) is not None, "run_id: 3–64 строчных букв, цифр, _ или -")
        directory = self.root / "runs" / run_id
        require(directory.resolve().is_relative_to(self.root / "runs"), "Путь run вышел за границы runs")
        return directory

    @contextmanager
    def lock(self, run_id):
        directory = self.directory(run_id)
        require(directory.is_dir(), "Run не создан; сначала init")
        lock = directory / ".lock"
        try:
            fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError as exc:
            raise PipelineError("Run занят другим писателем; проверьте процесс, не удаляйте активный .lock") from exc
        try:
            os.close(fd)
            yield
        finally:
            lock.unlink()

    def load(self, run_id):
        run = read_json(self.directory(run_id) / "run.json")
        self.contract("run", run)
        require(run["run_id"] == run_id, "run_id не совпадает с папкой")
        return run

    def save(self, run, event, **details):
        run["updated_at"] = now()
        run["history"].append({"event": event, "at": run["updated_at"], **details})
        self.contract("run", run)
        write_json(self.directory(run["run_id"]) / "run.json", run)

    def init(self, run_id, through="G9"):
        require(through in ("G2", "G9", "G11"), "Граница run: G2, G9 или G11")
        directory = self.directory(run_id)
        require(not directory.exists(), "Run уже существует; используй status и audit")
        directory.mkdir(parents=True)
        for name in ("artifacts", "reviews", "decisions", "snapshots"):
            (directory / name).mkdir()
        for name in ("evidence.jsonl", "signals.jsonl"):
            (directory / name).write_text("", encoding="utf-8")
        write_json(directory / "clusters.json", {"clusters": [], "is_synthetic": self.allow_synthetic})
        write_json(directory / "opportunities.json", {"gate": "G1", "territories": [], "is_synthetic": self.allow_synthetic})
        run = {"schema_version": "1.0", "run_id": run_id, "created_at": now(), "updated_at": now(),
               "current_gate": "G0", "end_gate": through, "status": "draft", "records": {},
               "history": [], "is_synthetic": self.allow_synthetic}
        self.save(run, "initialized", end_gate=through)
        return run

    def snapshot(self, run_id, manifest_path):
        directory = self.directory(run_id)
        manifest_file = safe_file(directory, manifest_path)
        manifest = read_json(manifest_file)
        self.contract("proposal", manifest)
        require(len(set(manifest["files"])) == len(manifest["files"]), "Manifest содержит повторные пути")
        require(len(set(manifest["author_ids"])) == len(manifest["author_ids"]), "Повторный author_id")
        choices = [x["id"] for x in manifest["options"]]
        require(len(set(choices)) == len(choices), "Повторный ID выбора")
        files = []
        resolved = set()
        for relative in sorted(manifest["files"]):
            path = safe_file(directory, relative)
            require(path not in resolved and path != manifest_file, "Повтор файла через алиас или сам manifest")
            require(path != directory / "run.json" and ".lock" not in path.parts
                    and not path.is_relative_to(directory / "snapshots"),
                    "Рабочее состояние нельзя включать в proposal")
            resolved.add(path)
            files.append({"path": relative, "sha256": digest(path.read_bytes())})
        content = {"manifest_sha256": digest(manifest_file.read_bytes()), "files": files}
        return {"manifest": manifest_path, **content, "sha256": digest(canonical(content)),
                "author_ids": manifest["author_ids"], "options": manifest["options"],
                "summary": manifest["summary"], "gate": manifest["gate"]}

    def freeze(self, run_id, proposal):
        directory = self.directory(run_id)
        destination = directory / "snapshots" / proposal["sha256"]
        for relative in [proposal["manifest"]] + [f["path"] for f in proposal["files"]]:
            source = safe_file(directory, relative)
            target = safe_file(destination, relative, exists=False)
            if target.exists():
                require(target.read_bytes() == source.read_bytes(), "Архив proposal повреждён")
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, target)

    def ensure_snapshot(self, run_id, proposal):
        actual = self.snapshot(run_id, proposal["manifest"])
        require(actual == proposal, "Материалы изменились после propose; создай новую версию и повтори ревью")
        self.ensure_archive(run_id, proposal)

    def ensure_archive(self, run_id, proposal):
        archive = self.directory(run_id) / "snapshots" / proposal["sha256"]
        require(digest(safe_file(archive, proposal["manifest"]).read_bytes()) == proposal["manifest_sha256"],
                "Архив manifest повреждён")
        for file in proposal["files"]:
            require(digest(safe_file(archive, file["path"]).read_bytes()) == file["sha256"], "Архив материала повреждён")

    def validate_research(self, run_id):
        directory = self.directory(run_id)

        def jsonl(name, contract):
            rows = []
            for index, line in enumerate(safe_file(directory, name).read_text(encoding="utf-8").splitlines(), 1):
                if not line.strip():
                    continue
                try:
                    value = strict_json(line)
                except ValueError as exc:
                    raise PipelineError(f"{name}:{index}: неверный JSON") from exc
                self.contract(contract, value)
                rows.append(value)
            require(rows, f"{name}: нет наблюдений")
            ids = [row["id"] for row in rows]
            require(len(set(ids)) == len(ids), f"{name}: повторные ID")
            return {row["id"]: row for row in rows}

        evidence = jsonl("evidence.jsonl", "evidence")
        signals = jsonl("signals.jsonl", "signal")
        clusters_doc = read_json(safe_file(directory, "clusters.json"))
        opportunity_doc = read_json(safe_file(directory, "opportunities.json"))
        self.contract("clusters", clusters_doc)
        self.contract("opportunities", opportunity_doc)
        for item in evidence.values():
            for field in ("observed_at", "event_at", "published_at"):
                check_date(item[field], f"{item['id']}.{field}", timestamp=field == "observed_at")
            parent = item["repost_of"]
            require(parent is None or parent in evidence, f"{item['id']}: неизвестный repost_of")
            if parent:
                require(item["source_family"] == evidence[parent]["source_family"],
                        "Перепост обязан иметь тот же source_family")
            seen, cursor = set(), item["id"]
            while cursor is not None:
                require(cursor not in seen, "Цикл repost_of")
                seen.add(cursor)
                cursor = evidence[cursor]["repost_of"]
                require(cursor is None or cursor in evidence, "Неизвестный repost_of")
            for metric in item["metrics"]:
                for field in ("window_start", "window_end"):
                    check_date(metric[field], f"{item['id']}.{field}")
                start, end = metric["window_start"], metric["window_end"]
                if start and end:
                    require(check_date(start, "window_start") <= check_date(end, "window_end"),
                            "Окно метрики заканчивается раньше начала")
        for item in signals.values():
            require(all(e in evidence for e in item["evidence_ids"]), f"{item['id']}: неизвестный evidence_id")
            require(len(set(item["evidence_ids"])) == len(item["evidence_ids"]), "Повтор evidence_id")
            check_date(item["observed_at"], f"{item['id']}.observed_at", timestamp=True)
        clusters = clusters_doc["clusters"]
        require(len({c["id"] for c in clusters}) == len(clusters), "Повтор cluster_id")
        assigned = set()
        for cluster in clusters:
            ids = cluster["signal_ids"]
            require(all(s in signals for s in ids), "Cluster с неизвестным signal_id")
            require(len(set(ids)) == len(ids), "Повтор сигнала в cluster")
            require(not (assigned & set(ids)), "Сигнал входит в несколько clusters")
            assigned.update(ids)
            require(all(signals[s]["cluster_id"] == cluster["id"] for s in ids), "cluster_id не согласован")
            refs = {e for s in ids for e in signals[s]["evidence_ids"]}
            families = {evidence[e]["source_family"] for e in refs}
            require(set(cluster["evidence_ids"]) == refs, "Cluster содержит неверные evidence_ids")
            require(set(cluster["source_families"]) == families, "Неверное число независимых source_families")
            require(len(cluster["source_families"]) == len(families), "Повтор source_family")
        require(assigned == set(signals), "Не все сигналы включены в clusters")
        territories = opportunity_doc["territories"]
        require(len(territories) == 3 and len({t["id"] for t in territories}) == 3,
                "G1 требует три разные территории")
        for territory in territories:
            require(all(s in signals for s in territory["signal_ids"]), "Территория с неизвестным сигналом")
            require(len(set(territory["signal_ids"])) == len(territory["signal_ids"]), "Повтор signal_id территории")
            refs = {e for s in territory["signal_ids"] for e in signals[s]["evidence_ids"]}
            families = {evidence[e]["source_family"] for e in refs}
            declared = territory["source_independence"]
            if declared == "multiple_independent":
                require(len(families) >= 2, "Территория заявляет независимость при одной source_family")
            elif declared == "single_source":
                require(len(families) == 1, "single_source расходится с реестром source_family")
        used_refs = {e for signal in signals.values() for e in signal["evidence_ids"]}
        return {"evidence": len(evidence), "signals": len(signals), "clusters": len(clusters),
                "territories": len(territories),
                "source_families_total": len({e["source_family"] for e in evidence.values()}),
                "source_families_used": len({evidence[e]["source_family"] for e in used_refs}),
                "scope": "Форматы и связи; достоверность проверяется независимым evidence-review"}

    def validate_review(self, run, proposal, review):
        self.contract("review", review)
        require(review["run_id"] == run["run_id"], "Ревью принадлежит другому run")
        require(review["gate"] == proposal["gate"], "Ревью принадлежит другому gate")
        require(review["artifact_sha256"] == proposal["sha256"], "Ревью проверяло другую версию")
        require(review["reviewer_id"] not in proposal["author_ids"], "Автор не может быть независимым ревьюером")
        require(review["lens"] in self.routes[proposal["gate"]]["required_reviews"], "Линза не входит в обязательный маршрут")
        check_date(review["reviewed_at"], "reviewed_at", timestamp=True)
        ids = [f["id"] for f in review["findings"]]
        require(len(set(ids)) == len(ids), "Повторный finding_id")
        for finding in review["findings"]:
            if finding["resolution"] == "accepted_risk":
                require(finding["severity"] != "critical", "Критический риск нельзя принять вместо исправления")
                acceptance = finding["risk_acceptance"]
                require(acceptance and acceptance["quote"].strip() and acceptance["message_ref"].strip(),
                        "Принятие риска требует настоящей цитаты и message_ref")
            if review["verdict"] == "PASS" and finding["severity"] in ("critical", "major"):
                require(finding["resolution"] in ("fixed", "accepted_risk"), "PASS содержит открытое блокирующее замечание")

    def review_status(self, run, record):
        required = self.routes[record["proposal"]["gate"]]["required_reviews"]
        latest, unresolved = {}, {}
        for review in record["reviews"]:
            self.validate_review(run, record["proposal"], review)
            lens = review["lens"]
            findings = {f["id"]: f for f in review["findings"]}
            for (old_lens, finding_id), severity in list(unresolved.items()):
                if old_lens != lens:
                    continue
                replacement = findings.get(finding_id)
                if replacement:
                    require(replacement["severity"] == severity, "Нельзя менять серьёзность незакрытого finding")
                closed = replacement and replacement["resolution"] in ("fixed", "accepted_risk")
                if closed:
                    require(replacement["severity"] == severity, "Нельзя понизить серьёзность незакрытого finding")
                    require(severity != "critical" or replacement["resolution"] == "fixed", "Critical требует исправления")
                    del unresolved[(old_lens, finding_id)]
                elif review["verdict"] == "PASS":
                    raise PipelineError(f"PASS потерял незакрытый finding {finding_id}; нужен явный результат исправления")
            for finding in review["findings"]:
                if finding["severity"] in ("critical", "major") and finding["resolution"] == "open":
                    unresolved[(lens, finding["id"])] = finding["severity"]
            latest[review["lens"]] = review
        ids = [r["reviewer_id"] for r in latest.values()]
        require(len(set(ids)) == len(ids), "Обязательные линзы должны принадлежать разным субагентам")
        if any(r["verdict"] != "PASS" for r in latest.values()):
            return "blocked"
        return "awaiting_user" if set(latest) == set(required) else "awaiting_review"

    def verify_history(self, run):
        """Rebuild the active projection; an edited pointer alone cannot skip a gate."""
        records, cursor, end, status = {}, "G0", None, "draft"
        for index, event in enumerate(run["history"]):
            check_date(event["at"], "history.at", timestamp=True)
            kind = event["event"]
            if kind == "initialized":
                require(index == 0 and end is None, "Некорректная инициализация истории")
                end = event["end_gate"]
                require(end in ("G2", "G9", "G11"), "Неверная граница в истории")
                continue
            require(end is not None, "История не начинается с initialized")
            if kind == "proposed":
                require(event["gate"] == cursor and status not in ("hold", "stopped", "complete"), "Неверная последовательность proposal")
                require(event["superseded_record"] == records.get(cursor), "История предыдущей версии утрачена")
                proposal = event["proposal"]
                require(proposal["sha256"] == event["proposal_sha256"], "Proposal hash расходится с историей")
                records[cursor] = {"proposal": proposal, "reviews": [], "decision": None}
                status = self.review_status(run, records[cursor])
            elif kind == "review_recorded":
                require(event["gate"] == cursor and cursor in records and records[cursor]["decision"] is None,
                        "Ревью вне активного gate")
                require(status not in ("hold", "stopped", "complete"), "Ревью в закрытом run")
                records[cursor]["reviews"].append(event["review"])
                status = self.review_status(run, records[cursor])
            elif kind == "approved":
                decision = event["decision"]
                self.contract("decision", decision)
                require(status == "awaiting_user" and decision["gate"] == cursor, "Утверждение перескочило gate")
                require(decision["proposal_sha256"] == records[cursor]["proposal"]["sha256"], "Утверждение другой версии в истории")
                records[cursor]["decision"] = decision
                if cursor == end:
                    status = "complete"
                else:
                    cursor, status = GATES[GATES.index(cursor) + 1], "draft"
            elif kind in ("user_decision", "scope_extended"):
                user = event["user_decision"]
                require(user["actor"] == "user" and user["run_id"] == run["run_id"] and user["gate"] == cursor,
                        "Потеряна привязка пользовательского решения")
                require(user["quote"].strip() and user["message_ref"].strip(), "Нет источника пользовательского решения")
                action = user["decision"]
                if kind == "scope_extended":
                    target = user["through"]
                    require(action == "extend" and status == "complete" and target in ("G9", "G11")
                            and GATES.index(target) > GATES.index(end), "Неверное расширение объёма")
                    cursor, end, status = GATES[GATES.index(end) + 1], target, "draft"
                elif action in ("back", "revise"):
                    target = user["target"]
                    require(target in GATES and GATES.index(target) <= GATES.index(cursor), "Неверный возврат в истории")
                    removed = {g: r for g, r in records.items() if GATES.index(g) >= GATES.index(target)}
                    require(removed == user["invalidated_records"], "История инвалидированных решений утрачена")
                    records = {g: r for g, r in records.items() if g not in removed}
                    cursor, status = target, "needs_revalidation"
                elif action == "hold":
                    require(status != "stopped", "Нельзя отложить закрытый run")
                    status = "hold"
                elif action == "stop":
                    require(status != "stopped", "Двойное закрытие run")
                    status = "stopped"
                elif action == "resume":
                    require(status == "hold", "Resume без hold")
                    record = records.get(cursor)
                    status = "complete" if record and record["decision"] else self.review_status(run, record) if record else "draft"
                else:
                    raise PipelineError("Неизвестное пользовательское решение в истории")
            else:
                raise PipelineError(f"Неизвестное событие истории: {kind}")
        require((records, cursor, end, status) == (run["records"], run["current_gate"], run["end_gate"], run["status"]),
                "run.json расходится с последовательностью событий; ручной сдвиг gate запрещён")

    def audit(self, run_id, *, skip_current=False):
        run = self.load(run_id)
        self.verify_history(run)
        for event in run["history"]:
            if event["event"] == "proposed":
                self.ensure_archive(run_id, event["proposal"])
            elif event["event"] == "review_recorded":
                source = safe_file(self.directory(run_id), event["file"])
                checksum = event["review_source_sha256"]
                require(digest(source.read_bytes()) == checksum and read_json(source) == event["review"],
                        "Исходный файл ревью изменён; восстанавливай его или добавляй отдельную версию")
                frozen = safe_file(self.directory(run_id), f"snapshots/reviews/{checksum}.json")
                require(digest(frozen.read_bytes()) == checksum, "Архив ревью повреждён")
        current = GATES.index(run["current_gate"])
        require(current <= GATES.index(run["end_gate"]), "Gate вышел за границу run")
        expected_prior = set(GATES[:current])
        require(expected_prior <= set(run["records"]), "Нарушена последовательность утверждений")
        require(set(run["records"]) <= set(GATES[:current + 1]), "Есть активные записи будущих гейтов")
        for gate, record in run["records"].items():
            require(record["proposal"]["gate"] == gate, "Запись содержит предложение другого gate")
            if not (skip_current and gate == run["current_gate"]):
                self.ensure_snapshot(run_id, record["proposal"])
            status = self.review_status(run, record)
            decision = record["decision"]
            if gate in expected_prior or (run["status"] == "complete" and gate == run["current_gate"]):
                require(decision is not None, f"Нет одобрения {gate}")
            if decision is not None:
                self.contract("decision", decision)
                check_date(decision["recorded_at"], "decision.recorded_at", timestamp=True)
                require(decision["gate"] == gate and decision["run_id"] == run_id, "Неверная привязка решения")
                require(decision["decision"] == "approve" and decision["actor"] == "user", "Это не пользовательское утверждение")
                require(decision["proposal_sha256"] == record["proposal"]["sha256"], "Решение о другой версии")
                require(decision["choice"] in {x["id"] for x in record["proposal"]["options"]}, "Выбран неизвестный вариант")
                require(status == "awaiting_user", f"{gate}: обязательные ревью не пройдены")
                path = safe_file(self.directory(run_id), decision["record_path"])
                require(read_json(path) == decision, "Журнал решения расходится с run.json")
        if "G1" in run["records"] and not (skip_current and run["current_gate"] == "G1"):
            self.validate_research(run_id)
        record = run["records"].get(run["current_gate"])
        if run["status"] == "complete":
            require(run["current_gate"] == run["end_gate"] and record and record["decision"], "Ложный complete")
        elif run["status"] not in ("hold", "stopped"):
            if record:
                require(record["decision"] is None, "Текущий незавершённый gate уже утверждён")
                require(run["status"] == self.review_status(run, record), "Статус не согласован с ревью")
            else:
                require(run["status"] in ("draft", "needs_revalidation"), "Нет proposal для данного статуса")
        return {"ok": True, "run_id": run_id, "gate": run["current_gate"], "status": run["status"],
                "scope": "Целостность файлов, связей и записей; подлинность сообщений проверяет оркестратор"}

    def propose(self, run_id, manifest):
        with self.lock(run_id):
            self.audit(run_id, skip_current=True)
            run = self.load(run_id)
            require(run["status"] not in ("complete", "hold", "stopped"), "Сначала возобнови/пересмотри run")
            gate = run["current_gate"]
            # Prior approved gates must remain intact even when replacing a draft.
            old = run["records"].pop(gate, None)
            original_status = run["status"]
            run["status"] = "draft"
            for prior in run["records"].values():
                self.ensure_snapshot(run_id, prior["proposal"])
            proposal = self.snapshot(run_id, manifest)
            require(proposal["gate"] == gate, "Manifest относится к другому gate")
            require(old is None or proposal["sha256"] != old["proposal"]["sha256"],
                    "Эта версия уже зарегистрирована; используй status/review, не сбрасывай отзывы")
            if gate == "G1":
                self.validate_research(run_id)
                require(set(RESEARCH_FILES) <= {x["path"] for x in proposal["files"]},
                        "G1 manifest должен включать все четыре файла исследования")
                territories = read_json(self.directory(run_id) / "opportunities.json")["territories"]
                require({o["id"] for o in proposal["options"]} == {t["id"] for t in territories},
                        "Варианты G1 должны совпадать с ID трёх территорий")
            if gate in ("G2", "G6"):
                require(len(proposal["options"]) == 3, f"{gate} требует три сравнимых варианта")
            record = {"proposal": proposal, "reviews": [], "decision": None}
            run["records"][gate] = record
            run["status"] = self.review_status(run, record)
            self.freeze(run_id, proposal)
            self.save(run, "proposed", gate=gate, proposal_sha256=proposal["sha256"],
                      proposal=proposal, superseded_record=old, previous_status=original_status)
            return proposal

    def review(self, run_id, relative):
        with self.lock(run_id):
            self.audit(run_id)
            run = self.load(run_id)
            require(run["status"] in ("awaiting_review", "blocked", "awaiting_user"), "Нет доступного gate для ревью")
            record = run["records"][run["current_gate"]]
            source = safe_file(self.directory(run_id), relative)
            require(not any(e.get("file") == relative for e in run["history"] if e["event"] == "review_recorded"),
                    "Файл ревью уже зарегистрирован; используй новый путь версии")
            review = read_json(source)
            self.validate_review(run, record["proposal"], review)
            record["reviews"].append(review)
            run["status"] = self.review_status(run, record)
            checksum = digest(source.read_bytes())
            frozen = safe_file(self.directory(run_id), f"snapshots/reviews/{checksum}.json", exists=False)
            frozen.parent.mkdir(parents=True, exist_ok=True)
            if frozen.exists():
                require(frozen.read_bytes() == source.read_bytes(), "Архив ревью повреждён")
            else:
                shutil.copyfile(source, frozen)
            self.save(run, "review_recorded", gate=run["current_gate"], file=relative, review=review,
                      review_source_sha256=checksum)
            return {"status": run["status"], "lens": review["lens"], "verdict": review["verdict"]}

    def user_record(self, run, decision, quote, message_ref, **extra):
        require(bool(quote.strip()) and bool(message_ref.strip()), "Нужны настоящая цитата и message_ref")
        return {"actor": "user", "decision": decision, "quote": quote, "message_ref": message_ref,
                "recorded_at": now(), "gate": run["current_gate"], "run_id": run["run_id"], **extra}

    def approve(self, run_id, sha256, choice, quote, message_ref):
        with self.lock(run_id):
            self.audit(run_id)
            run = self.load(run_id)
            require(run["status"] == "awaiting_user", "Переход закрыт: нужен proposal и все независимые PASS")
            gate = run["current_gate"]
            record = run["records"][gate]
            require(record["proposal"]["sha256"] == sha256, "Ответ относится к другой версии proposal")
            require(choice in {x["id"] for x in record["proposal"]["options"]}, "Выбор не входит в показанные варианты")
            path = f"decisions/{len(run['history']):04d}-{gate}-approve.json"
            decision = self.user_record(run, "approve", quote, message_ref, proposal_sha256=sha256,
                                        choice=choice, record_path=path, is_synthetic=self.allow_synthetic)
            self.contract("decision", decision)
            write_json(safe_file(self.directory(run_id), path, exists=False), decision)
            record["decision"] = decision
            if gate == run["end_gate"]:
                run["status"] = "complete"
            else:
                run["current_gate"] = GATES[GATES.index(gate) + 1]
                run["status"] = "draft"
            self.save(run, "approved", decision=decision)
            return {"gate": run["current_gate"], "status": run["status"]}

    def decide(self, run_id, decision, quote, message_ref, target=None):
        with self.lock(run_id):
            run = self.load(run_id)
            self.verify_history(run)
            require(decision in ("revise", "back", "hold", "stop", "resume"), "Неизвестное решение")
            event = self.user_record(run, decision, quote, message_ref)
            if decision in ("revise", "back"):
                target = target if decision == "back" else run["current_gate"]
                require(target in GATES and GATES.index(target) <= GATES.index(run["current_gate"]),
                        "Возврат возможен только к текущему или раннему gate")
                invalidated = {g: r for g, r in run["records"].items() if GATES.index(g) >= GATES.index(target)}
                run["records"] = {g: r for g, r in run["records"].items() if g not in invalidated}
                run["current_gate"], run["status"] = target, "needs_revalidation"
                event.update(target=target, invalidated_records=invalidated)
            elif decision == "resume":
                require(run["status"] == "hold", "Resume доступен только после hold")
                record = run["records"].get(run["current_gate"])
                if record and record["decision"]:
                    run["status"] = "complete"
                else:
                    run["status"] = self.review_status(run, record) if record else "draft"
            else:
                require(run["status"] != "stopped", "Run закрыт; пересмотр требует revise или back")
                run["status"] = "hold" if decision == "hold" else "stopped"
            self.save(run, "user_decision", user_decision=event)
            return {"gate": run["current_gate"], "status": run["status"]}

    def extend(self, run_id, through, quote, message_ref):
        with self.lock(run_id):
            self.audit(run_id)
            run = self.load(run_id)
            require(run["status"] == "complete", "Сначала заверши согласованный объём run")
            require(through in ("G9", "G11") and GATES.index(through) > GATES.index(run["end_gate"]),
                    "Новая граница должна быть позже прежней")
            event = self.user_record(run, "extend", quote, message_ref, through=through)
            run["current_gate"] = GATES[GATES.index(run["end_gate"]) + 1]
            run["end_gate"], run["status"] = through, "draft"
            self.save(run, "scope_extended", user_decision=event)
            return {"gate": run["current_gate"], "end_gate": through, "status": run["status"]}

    def status(self, run_id):
        run = self.load(run_id)
        gate = run["current_gate"]
        record = run["records"].get(gate)
        return {"run_id": run_id, "gate": gate, "end_gate": run["end_gate"], "status": run["status"],
                "route": self.routes[gate], "proposal_sha256": record["proposal"]["sha256"] if record else None,
                "review_verdicts": {r["lens"]: r["verdict"] for r in record["reviews"]} if record else {},
                "warning": "status показывает реестр; audit отдельно проверяет файлы и связи"}

    def check_repo(self):
        require(set(self.routes) == set(GATES), "Маршруты должны покрывать G0–G11")
        for route in self.routes.values():
            for path in route["docs"] + [route["agent_file"]]:
                safe_file(self.root, path)
            require(len(set(route["required_reviews"])) == len(route["required_reviews"]), "Дубли review lenses")
        for name in ("evidence", "signal", "clusters", "opportunities", "review", "proposal", "run", "decision"):
            read_json(self.root / "schemas" / f"{name}.schema.json")
        for path in ("AGENTS.md", "state.md", "agents/reviewers.md", "docs/PILOT_RUNBOOK.md",
                     "docs/CLI.md", "docs/research/2026-11-discovery.md"):
            safe_file(self.root, path)
        return {"ok": True, "routes": len(self.routes), "schemas": 8}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check-repo")
    for command in ("init", "status", "audit", "validate-research", "propose", "review", "approve", "decide", "extend"):
        p = sub.add_parser(command)
        p.add_argument("--run", required=True)
        if command in ("init", "extend"):
            p.add_argument("--through", choices=("G2", "G9", "G11"), default="G9", required=command == "extend")
        if command == "propose":
            p.add_argument("--manifest", required=True)
        if command == "review":
            p.add_argument("--file", required=True)
        if command in ("approve", "decide", "extend"):
            p.add_argument("--quote", required=True)
            p.add_argument("--message-ref", required=True)
        if command == "approve":
            p.add_argument("--sha256", required=True)
            p.add_argument("--choice", required=True)
        if command == "decide":
            p.add_argument("--decision", choices=("revise", "back", "hold", "stop", "resume"), required=True)
            p.add_argument("--target", choices=GATES)
    args = vars(parser.parse_args(argv))
    command = args.pop("command").replace("-", "_")
    if "run" in args:
        args["run_id"] = args.pop("run")
    if "file" in args:
        args["relative"] = args.pop("file")
    try:
        result = getattr(Pipeline(), command)(**args)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except (PipelineError, OSError, KeyError, TypeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
