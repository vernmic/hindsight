"""Preview or reverse a journaled gardener run or native source repair."""

import argparse
import json
from pathlib import Path

import gardener_repair as repair
import gardener_writer as writer

ROOT = Path("/mnt/i/hermes")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("kind", choices=["run", "repair"])
    p.add_argument("reference")
    p.add_argument("--memory-id", action="append")
    p.add_argument("--apply", action="store_true")
    a = p.parse_args()
    if a.kind == "run":
        # The writer also rejects unsafe identifiers before composing SQL.
        sql = writer.rollback_sql(a.reference, a.memory_id)
        if a.apply:
            print(json.dumps(writer.query(sql)))
        else:
            print(
                json.dumps(
                    {
                        "preview": True,
                        "run_id": a.reference,
                        "apply_command": "gardener_rollback.py run " + a.reference + " --apply",
                    }
                )
            )
    else:
        path = Path(a.reference).resolve()
        allowed = (ROOT / "reports/gardener-repairs-20261007").resolve()
        if not path.is_relative_to(allowed):
            raise ValueError("Repair journal outside authorized directory")
        record = json.loads(path.read_text())
        key = record["memory_id"]
        bank = record["bank"]
        if record.get("stage") != "applied":
            raise ValueError("Repair not applied")
        current = repair.api(bank, "/memories/" + key)
        if current["text"] != record["after"]["text"] or current.get("document_id") != record["before"].get(
            "document_id"
        ):
            raise ValueError("Later changes prevent rollback")
        if a.apply:
            result = repair.api(
                bank,
                "/memories/" + key,
                "PATCH",
                {
                    "text": record["before"]["text"],
                    "entities": record["before"].get("entities", []),
                    "resolve_entities": False,
                },
            )
            record.update(stage="reverted", rollback=result)
            path.write_text(json.dumps(record, indent=2))
            print(json.dumps({"reverted": key}))
        else:
            print(
                json.dumps(
                    {"preview": True, "memory_id": key, "before": record["before"]["text"], "after": current["text"]}
                )
            )


if __name__ == "__main__":
    main()
