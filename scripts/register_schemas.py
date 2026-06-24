#!/usr/bin/env python3
"""register_schemas.py — register proto schemas with the Confluent Schema Registry.

This is the ONLY writer of schemas to the SR. It runs inside the contracts repo
(where proto/*.proto actually lives), reads the real .proto text, and registers
each topic's schema under "<topic>-value" (TopicNameStrategy). The language SDKs
NEVER register — they only resolve the assigned schema_id at runtime.

scripts/schemas.json declares:
  - libraries: shared .proto files that are imported by contracts but not bound
    to any topic (e.g. proto/shared.proto, buf/validate/validate.proto). Each is
    registered under its own subject so contracts can reference it.
  - topics:    topic -> .proto + the library subjects it imports (references).

Custom imports (proto/shared.proto, buf/validate/validate.proto) must be
registered as referenced libraries. Google well-known types
(google/protobuf/*.proto) resolve automatically in the SR and need no reference.

The protovalidate library (buf/validate/validate.proto) is not in this repo;
vendor it once with `buf export buf.build/bufbuild/protovalidate -o vendor/protovalidate`
(buf runs on CI/Linux, not the Windows host). The path is declared per library.

Usage:
    SCHEMA_REGISTRY_URL=http://localhost:8081 python scripts/register_schemas.py

Optional auth (for a secured SR):
    SCHEMA_REGISTRY_API_KEY / SCHEMA_REGISTRY_API_SECRET
"""
import base64
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PROTO_DIR = REPO / "proto"
MANIFEST = REPO / "scripts" / "schemas.json"
SR_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081").rstrip("/")


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


def register(subject: str, schema_text: str, references: list[dict] | None = None) -> int:
    """POST a PROTOBUF schema under <subject>/versions; returns the schema id.

    SR is idempotent: re-registering an identical schema returns the same id.
    `references` is a list of {name, subject, version} linking imports to their
    registered subjects.
    """
    payload = {"schemaType": "PROTOBUF", "schema": schema_text}
    if references:
        payload["references"] = references
    body = json.dumps(payload).encode()
    return _request(f"{SR_URL}/subjects/{subject}/versions", data=body, method="POST")["id"]


def latest_version(subject: str) -> int:
    return _request(f"{SR_URL}/subjects/{subject}/versions/latest")["version"]


def _read_library_text(lib: dict) -> str:
    """Resolve a library's .proto text: `proto` is relative to proto/, `path` to the repo root."""
    if "proto" in lib:
        return (PROTO_DIR / lib["proto"]).read_text(encoding="utf-8")
    if "path" in lib:
        return (REPO / lib["path"]).read_text(encoding="utf-8")
    raise KeyError(f"library {lib.get('subject')!r} needs a 'proto' or 'path' field")


def _normalize(manifest) -> dict:
    """Accept the legacy list form ([{topic, proto}, ...]) or the object form."""
    if isinstance(manifest, list):
        return {"libraries": [], "topics": manifest}
    return {"libraries": manifest.get("libraries", []), "topics": manifest.get("topics", [])}


def main() -> None:
    manifest = _normalize(json.loads(MANIFEST.read_text(encoding="utf-8")))
    print(f"Registering schemas at {SR_URL}")

    # 1) Libraries first (referenced by contracts). Track subject -> version.
    versions: dict[str, int] = {}
    for lib in manifest["libraries"]:
        subject = lib["subject"]
        try:
            schema_text = _read_library_text(lib)
        except FileNotFoundError:
            hint = lib.get("note", "")
            print(f"  library {subject}: file missing. {hint}", file=sys.stderr)
            sys.exit(1)
        refs = [
            {"name": r, "subject": r, "version": versions[r]}
            for r in lib.get("references", [])
        ]
        try:
            schema_id = register(subject, schema_text, refs)
            versions[subject] = latest_version(subject)
            print(f"  [lib] {subject}: id={schema_id} v{versions[subject]}")
        except urllib.error.HTTPError as e:
            print(f"  [lib] {subject}: HTTP {e.code}: {e.read().decode()}", file=sys.stderr)
            sys.exit(1)
        except urllib.error.URLError as e:
            print(f"  cannot reach SR at {SR_URL}: {e}", file=sys.stderr)
            sys.exit(1)

    # 2) Topic schemas, linking their imports to the registered library subjects.
    for entry in manifest["topics"]:
        topic, proto_file = entry["topic"], entry["proto"]
        subject = f"{topic}-value"
        schema_text = (PROTO_DIR / proto_file).read_text(encoding="utf-8")
        missing = [r for r in entry.get("references", []) if r not in versions]
        if missing:
            print(f"  {subject}: undeclared reference(s) {missing} — add them to 'libraries'", file=sys.stderr)
            sys.exit(1)
        refs = [
            {"name": r, "subject": r, "version": versions[r]}
            for r in entry.get("references", [])
        ]
        try:
            schema_id = register(subject, schema_text, refs)
            print(f"  {subject}: id={schema_id}  ({proto_file})")
        except urllib.error.HTTPError as e:
            print(f"  {subject}: HTTP {e.code}: {e.read().decode()}", file=sys.stderr)
            sys.exit(1)
        except urllib.error.URLError as e:
            print(f"  cannot reach SR at {SR_URL}: {e}", file=sys.stderr)
            sys.exit(1)

    print("OK — schemas registered.")


if __name__ == "__main__":
    main()
