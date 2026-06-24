#!/usr/bin/env python3
"""register_schemas.py — register proto schemas with the Confluent Schema Registry.

This is the ONLY writer of schemas to the SR. It runs inside the contracts repo
(where proto/*.proto actually lives), reads the real .proto text, and registers
each topic's schema under "<topic>-value" (TopicNameStrategy). The language SDKs
NEVER register — they only resolve the assigned schema_id at runtime.

scripts/schemas.json declares:
  - libraries: imported .proto files that are not bound to a topic (e.g.
    proto/shared.proto, buf/validate/validate.proto). `subject` is the import
    path; `proto` (relative to proto/) or `path` (relative to repo) says where
    to read its text.
  - topics: topic -> .proto + the library subjects it imports (references).

A library's own imports are resolved transitively (e.g. validate.proto imports
buf/validate/expression.proto), so only the direct imports need to be listed.
Google well-known types (google/protobuf/*.proto) resolve in the SR and need no
reference.

protovalidate's buf/validate/*.proto are not in this repo; in CI, vendor them:
    buf dep update && buf export . -o "$PROTO_EXPORT_DIR"
and run with PROTO_EXPORT_DIR set so those imports are found.

Usage:
    SCHEMA_REGISTRY_URL=http://localhost:8081 \
    PROTO_EXPORT_DIR=/tmp/protoexport \
    python scripts/register_schemas.py

Optional auth (for a secured SR):
    SCHEMA_REGISTRY_API_KEY / SCHEMA_REGISTRY_API_SECRET
"""
import base64
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PROTO_DIR = REPO / "proto"
MANIFEST = REPO / "scripts" / "schemas.json"
SR_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081").rstrip("/")
EXPORT_DIR = os.environ.get("PROTO_EXPORT_DIR")

WKT_PREFIX = "google/"  # well-known types: the SR resolves these itself
IMPORT_RE = re.compile(r'^\s*import\s+(?:public\s+|weak\s+)?"([^"]+)"\s*;', re.MULTILINE)


def _auth_header(req: urllib.request.Request) -> None:
    key = os.environ.get("SCHEMA_REGISTRY_API_KEY")
    if not key:
        return
    secret = os.environ.get("SCHEMA_REGISTRY_API_SECRET", "")
    token = base64.b64encode(f"{key}:{secret}".encode()).decode()
    req.add_header("Authorization", f"Basic {token}")


def _request(url: str, *, data: bytes | None = None, method: str = "GET"):
    headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"} if data else {}
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    _auth_header(req)
    with urllib.request.urlopen(req) as resp:
        return json.load(resp)


def _subject_url(subject: str, suffix: str) -> str:
    # Subject names (e.g. "buf/validate/validate.proto") contain slashes — encode them.
    return f"{SR_URL}/subjects/{urllib.parse.quote(subject, safe='')}{suffix}"


def register(subject: str, schema_text: str, references: list[dict]) -> int:
    """POST a PROTOBUF schema under <subject>/versions; returns the schema id.

    SR is idempotent: re-registering an identical schema returns the same id.
    """
    payload = {"schemaType": "PROTOBUF", "schema": schema_text}
    if references:
        payload["references"] = references
    body = json.dumps(payload).encode()
    return _request(_subject_url(subject, "/versions"), data=body, method="POST")["id"]


def latest_version(subject: str) -> int:
    return _request(_subject_url(subject, "/versions/latest"))["version"]


def _resolve(import_path: str, declared: dict[str, dict]) -> Path | None:
    """Find an imported .proto: declared location, then buf export tree, then repo."""
    candidates: list[Path] = []
    lib = declared.get(import_path)
    if lib:
        if "proto" in lib:
            candidates.append(PROTO_DIR / lib["proto"])
        if "path" in lib:
            candidates.append(REPO / lib["path"])
    if EXPORT_DIR:
        candidates.append(Path(EXPORT_DIR) / import_path)
    candidates.append(REPO / import_path)        # e.g. proto/shared.proto
    candidates.append(PROTO_DIR / import_path)   # e.g. shared.proto (bare)
    return next((c for c in candidates if c.is_file()), None)


def _custom_imports(text: str) -> list[str]:
    """Imports that need a registered reference (everything but Google WKTs)."""
    return [imp for imp in IMPORT_RE.findall(text) if not imp.startswith(WKT_PREFIX)]


def _register_library(import_path: str, declared: dict[str, dict],
                      registered: dict[str, int]) -> None:
    """Register an imported .proto (its own imports first) as a library subject."""
    if import_path in registered:
        return
    path = _resolve(import_path, declared)
    if path is None:
        print(f"  cannot resolve import {import_path!r} — declare it in schemas.json "
              f"'libraries' or set PROTO_EXPORT_DIR (buf export . -o <dir>)",
              file=sys.stderr)
        sys.exit(1)
    text = path.read_text(encoding="utf-8")
    refs = _references(_custom_imports(text), declared, registered)
    register(import_path, text, refs)
    registered[import_path] = latest_version(import_path)
    print(f"  [lib] {import_path}: v{registered[import_path]}")


def _references(import_paths: list[str], declared: dict[str, dict],
                registered: dict[str, int]) -> list[dict]:
    """Register each import first, then return the SR references list."""
    refs = []
    for imp in import_paths:
        _register_library(imp, declared, registered)
        refs.append({"name": imp, "subject": imp, "version": registered[imp]})
    return refs


def _normalize(manifest) -> dict:
    """Accept the legacy list form ([{topic, proto}, ...]) or the object form."""
    if isinstance(manifest, list):
        return {"libraries": [], "topics": manifest}
    return {"libraries": manifest.get("libraries", []), "topics": manifest.get("topics", [])}


def main() -> None:
    manifest = _normalize(json.loads(MANIFEST.read_text(encoding="utf-8")))
    declared = {lib["subject"]: lib for lib in manifest["libraries"]}
    registered: dict[str, int] = {}  # import path -> registered version
    print(f"Registering schemas at {SR_URL}")
    try:
        for entry in manifest["topics"]:
            topic, proto_file = entry["topic"], entry["proto"]
            subject = f"{topic}-value"
            schema_text = (PROTO_DIR / proto_file).read_text(encoding="utf-8")
            # Use the topic's declared references; fall back to its actual imports.
            ref_paths = entry.get("references") or _custom_imports(schema_text)
            refs = _references(ref_paths, declared, registered)
            schema_id = register(subject, schema_text, refs)
            print(f"  {subject}: id={schema_id}  ({proto_file})")
    except urllib.error.HTTPError as e:
        print(f"  HTTP {e.code}: {e.read().decode()}", file=sys.stderr)
        sys.exit(1)
    except urllib.error.URLError as e:
        print(f"  cannot reach SR at {SR_URL}: {e}", file=sys.stderr)
        sys.exit(1)
    print("OK — schemas registered.")


if __name__ == "__main__":
    main()
