"""Guard rails for a public repo: fail if a secret or an environment-specific id is committed."""

import re
import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
SELF = Path(__file__).resolve()

# Credential-shaped strings. Obvious placeholders (XXXX, user:pass@) are allowed.
PATTERNS = {
    "AWS access key": r"AKIA[0-9A-Z]{16}",
    "Databricks token": r"dapi[0-9a-f]{20,}",
    "private key": r"BEGIN (RSA |EC |OPENSSH )?PRIVATE KEY",
    "GitHub token": r"gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}",
    "JWT": r"eyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{10,}",
    "URL with password": r"[a-z+]+://[^\s:@/'\"<]+:(?!pass@|XXXX|<)[^\s@/'\"<]{4,}@[^\s'\"]+",
}
# Config keys whose value must be an environment placeholder, never a literal.
ENV_ONLY_KEYS = {"project", "bucket", "http_path", "host", "password", "database", "url"}


def tracked_files() -> list[Path]:
    try:
        out = subprocess.run(
            ["git", "ls-files"], cwd=ROOT, capture_output=True, text=True, check=True
        ).stdout.split()
    except (OSError, subprocess.CalledProcessError):
        out = [str(p.relative_to(ROOT)) for p in ROOT.rglob("*") if p.is_file()]
    skip = {".venv", ".git", "logs", "secrets", "__pycache__"}
    return [
        ROOT / f
        for f in out
        if not skip & set(Path(f).parts) and (ROOT / f).is_file() and (ROOT / f) != SELF
    ]


def test_no_credential_shaped_strings_in_tracked_files():
    hits = []
    for path in tracked_files():
        try:
            text = path.read_text()
        except UnicodeDecodeError:
            continue  # binary (e.g. the seed Parquet file)
        hits += [
            f"{path.relative_to(ROOT)}: {label}"
            for label, pattern in PATTERNS.items()
            if re.search(pattern, text)
        ]
    assert not hits, hits


def test_real_env_and_keys_are_git_ignored():
    ignore = (ROOT / ".gitignore").read_text().splitlines()
    assert ".env" in ignore and "secrets/*" in ignore
    names = {p.name for p in tracked_files()}
    assert ".env" not in names and "gcp-sa.json" not in names


def test_sources_yaml_has_no_literal_environment_values():
    def walk(node, path=""):
        if isinstance(node, dict):
            for k, v in node.items():
                yield from walk(v, f"{path}.{k}")
        elif isinstance(node, list):
            for i, v in enumerate(node):
                yield from walk(v, f"{path}[{i}]")
        else:
            yield path, node

    data = yaml.safe_load((ROOT / "config" / "sources.yaml").read_text())
    bad = [
        f"{path}={value!r}"
        for path, value in walk(data)
        if path.rsplit(".", 1)[-1] in ENV_ONLY_KEYS and isinstance(value, str) and "${" not in value
    ]
    assert not bad, bad
