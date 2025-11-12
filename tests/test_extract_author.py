import subprocess
import sys
from pathlib import Path

import scripts.extract_author as ea


def run_script(path: Path):
    """Helper to run the CLI on a path and return (stdout, stderr, code)."""
    result = subprocess.run(
        [sys.executable, "scripts/extract_author.py", str(path)],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout, result.stderr, result.returncode


def test_extract_dynasty_and_author_handles_prefixed_names():
    assert ea.extract_dynasty_and_author("西晋释法炬") == ("西晋", "释法炬")
    assert ea.extract_dynasty_and_author("晋释道安") == ("晋", "释道安")
    assert ea.extract_dynasty_and_author("唐·慧净") == ("唐", "慧净")
    assert ea.extract_dynasty_and_author("唐代慧净") == ("唐", "慧净")


def test_derive_from_existing_author_formats_plain_entries():
    formatted = ea.derive_from_existing_author("西晋释法炬,晋释道安")
    assert formatted == ["[西晋]释法炬", "[晋]释道安"]

    formatted_brackets = ea.derive_from_existing_author("[唐]慧净")
    assert formatted_brackets == ["[唐]慧净"]


def test_strip_polite_suffix_is_applied():
    assert ea.strip_polite_suffix("陈函辉顿首") == "陈函辉"
    assert ea.strip_polite_suffix("元晓师") == "元晓"
    formatted = ea.derive_from_existing_author("陈函辉顿首")
    assert formatted == ["陈函辉"]


def test_cli_updates_frontmatter_and_is_idempotent(tmp_path):
    md_file = tmp_path / "sample.md"
    md_file.write_text(
        "---\n"
        "title: 测试\n"
        "category: /test\n"
        "lastmod: 2024-01-01T00:00:00Z\n"
        "author: ''\n"
        "---\n"
        "晋释道安撰...\n",
        encoding="utf-8",
    )

    stdout, stderr, code = run_script(md_file)
    assert code == 0
    assert f"{md_file}: (none) -> [晋]释道安" in stdout
    assert stderr == ""
    updated = md_file.read_text(encoding="utf-8")
    assert "author: '[晋]释道安'" in updated

    # Running again should produce no output and leave file untouched.
    stdout2, stderr2, code2 = run_script(md_file)
    assert (code2, stdout2.strip(), stderr2) == (0, "", "")


def test_cli_strips_polite_suffix(tmp_path):
    md_file = tmp_path / "polite.md"
    md_file.write_text(
        "---\n"
        "author: ''\n"
        "---\n"
        "陈函辉顿首撰\n",
        encoding="utf-8",
    )

    stdout, stderr, code = run_script(md_file)
    assert code == 0
    assert "陈函辉" in stdout
    updated = md_file.read_text(encoding="utf-8")
    assert "author: '陈函辉'" in updated


def test_cli_skips_when_author_already_present(tmp_path):
    md_file = tmp_path / "existing.md"
    original_content = (
        "---\n"
        "author: '[唐]李白'\n"
        "---\n"
        "唐代李白撰...\n"
    )
    md_file.write_text(original_content, encoding="utf-8")

    stdout, stderr, code = run_script(md_file)
    assert (code, stdout.strip(), stderr.strip()) == (0, "", "")
    assert md_file.read_text(encoding="utf-8") == original_content
