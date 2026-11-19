"""skill_roster resolves every skill path in ONE pass (F2 root cause).

Fixture tree covers: a dir-name/display-name divergence, an ambiguous display name
claimed by two distinct skills, a category-nested skill, and an external skills dir.
The single-pass index must agree with the host's _find_skill on every query form,
and must parse each SKILL.md's frontmatter at most once per build.
"""

from pathlib import Path


def _build_tree(tmp_path: Path):
    local = tmp_path / "skills"
    external = tmp_path / "external-skills"
    # Divergent names: directory axolotl, frontmatter display name "Axolotl Care".
    (local / "axolotl").mkdir(parents=True)
    (local / "axolotl" / "SKILL.md").write_text("---\nname: Axolotl Care\ndescription: a\n---\nbody", encoding="utf-8")
    # Category-nested skill: local/mlops/axolotl2, no display divergence.
    (local / "mlops" / "axolotl2").mkdir(parents=True)
    (local / "mlops" / "axolotl2" / "SKILL.md").write_text("---\nname: axolotl2\ndescription: b\n---\nbody2", encoding="utf-8")
    # Ambiguous display name: two distinct skills claim "Shared Name".
    (local / "one").mkdir()
    (local / "one" / "SKILL.md").write_text("---\nname: Shared Name\ndescription: c\n---\nx", encoding="utf-8")
    (external / "two").mkdir(parents=True)
    (external / "two" / "SKILL.md").write_text("---\nname: Shared Name\ndescription: d\n---\ny", encoding="utf-8")
    # Plain external skill found only by directory name.
    (external / "outsider").mkdir()
    (external / "outsider" / "SKILL.md").write_text("---\nname: Outsider Display\ndescription: e\n---\nz", encoding="utf-8")
    return local, external


def _patch_host(monkeypatch, local: Path, external: Path):
    import tools.skill_manager_tool as smt

    monkeypatch.setattr(smt, "_skills_dir", lambda: local, raising=True)
    try:
        import agent.skill_utils as su

        monkeypatch.setattr(su, "get_all_skills_dirs", lambda: [local, external], raising=True)
    except ImportError:
        monkeypatch.setattr(smt, "get_all_skills_dirs", lambda: [local, external], raising=True)
    return smt


QUERIES = [
    "axolotl",
    "Axolotl Care",
    "mlops/axolotl2",
    "axolotl2",
    "Shared Name",
    "outsider",
    "Outsider Display",
    "two",
    "nope",
    "axolotl/missing",
]


def test_index_agrees_with_find_skill_on_every_query_form(tmp_path, monkeypatch):
    local, external = _build_tree(tmp_path)
    smt = _patch_host(monkeypatch, local, external)
    import jev_gate.selector as selector

    resolve = selector._skill_path_index()
    for name in QUERIES:
        expected = smt._find_skill(name)
        actual = resolve(name)
        assert (None if actual is None else actual["path"]) == (
            None if expected is None else expected["path"]
        ), name


def test_frontmatter_parsed_at_most_once_per_skill(tmp_path, monkeypatch):
    local, external = _build_tree(tmp_path)
    smt = _patch_host(monkeypatch, local, external)
    real = smt._read_frontmatter_name
    calls = {"n": 0}

    def counting(path):
        calls["n"] += 1
        return real(path)

    monkeypatch.setattr(smt, "_read_frontmatter_name", counting, raising=True)
    import jev_gate.selector as selector

    resolve = selector._skill_path_index()
    for name in QUERIES:
        resolve(name)
    # Five SKILL.md files exist; the old per-row _find_skill loop parsed ~5 x rows,
    # the single-pass index at most once per file.
    assert calls["n"] <= 5


def test_roster_features_identical_through_both_resolvers(tmp_path, monkeypatch):
    local, external = _build_tree(tmp_path)
    smt = _patch_host(monkeypatch, local, external)
    import jev_gate.selector as selector

    resolve = selector._skill_path_index()
    rows = ["axolotl", "Axolotl Care", "mlops/axolotl2", "one", "two", "outsider", "Shared Name", "nope"]
    for name in rows:
        old = smt._find_skill(name)
        new = resolve(name)
        old_size = (
            (old["path"] / "SKILL.md").stat().st_size
            if old and (old["path"] / "SKILL.md").is_file()
            else None
        )
        new_size = (
            (new["path"] / "SKILL.md").stat().st_size
            if new and (new["path"] / "SKILL.md").is_file()
            else None
        )
        assert new_size == old_size, name