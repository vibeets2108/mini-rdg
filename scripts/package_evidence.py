"""Create a local review bundle; does not publish or include private handoff notes."""

import argparse
import hashlib
import json
import zipfile
from pathlib import Path


def package(root, study):
    root = root.resolve()
    study = study.resolve()
    if not study.is_relative_to(root / "results"):
        raise ValueError("Select a study inside this project's results directory")
    manifest = json.loads((study / "manifest.json").read_text())
    if manifest["status"] != "complete" or not (study / "summary.json").exists():
        raise ValueError("Study must be complete and analyzed first")
    destination = root / "results" / f"{study.name}-review.zip"
    if destination.exists():
        raise FileExistsError(destination)
    paths = [
        root / name for name in (
            "README.md", "pom.xml", "Dockerfile", "compose.yaml", ".gitignore",
            ".gitattributes", ".dockerignore", "docs/methodology.md",
            "docs/results.md", "docs/article-draft.md",
        )
    ]
    for folder in ("src", "scripts", ".github/workflows"):
        paths.extend(p for p in (root / folder).rglob("*")
                     if p.is_file() and "__pycache__" not in p.parts)
    raw = []
    for pattern in ("*/queries.csv", "*/operations.csv", "*/state.csv", "*/run.json"):
        raw.extend(study.glob(pattern))
    for name in (
        "manifest.json", "summary.json", "summary.csv", "phases.json", "report.md",
        "latency.svg", "images.json", "image-identities.txt", "container-stats.jsonl",
        "local-jar.sha256",
    ):
        raw.append(study / name)
    paths.extend(raw)
    checksums = {}
    with zipfile.ZipFile(destination, "x", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(set(paths)):
            relative = path.relative_to(root).as_posix()
            content = path.read_bytes()
            checksums[relative] = hashlib.sha256(content).hexdigest()
            archive.writestr(relative, content)
        archive.writestr("SHA256SUMS.json", json.dumps(checksums, indent=2))
    print(destination)
    return destination


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("study", type=Path)
    args = parser.parse_args()
    package(Path(__file__).resolve().parents[1], args.study)
