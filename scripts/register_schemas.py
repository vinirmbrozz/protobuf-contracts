#!/usr/bin/env python3
"""register_schemas.py — register proto schemas with the Confluent Schema Registry.

This is the ONLY writer of schemas to the SR. It runs inside the contracts repo
(where proto/*.proto actually lives), reads the real .proto text, and registers
each topic's schema under "<topic>-value" (TopicNameStrategy). The language SDKs
NEVER register — they only resolve the assigned schema_id at runtime.

Mapping of topic -> .proto file is declared in scripts/schemas.json.

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


def register(subject: str, schema_text: str) -> int:
    """POST the .proto schema under <subject>/versions; returns the schema id.

    SR is idempotent: re-registering an identical schema returns the same id.
    """
    body = json.dumps({"schemaType": "PROTOBUF", "schema": schema_text}).encode()
    req = urllib.request.Request(
        f"{SR_URL}/subjects/{subject}/versions",
        data=body,
        method="POST",
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    )
    _auth_header(req)
    with urllib.request.urlopen(req) as resp:
        return json.load(resp)["id"]


def main() -> None:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    print(f"Registering schemas at {SR_URL}")
    for entry in manifest:
        topic, proto_file = entry["topic"], entry["proto"]
        subject = f"{topic}-value"
        schema_text = (PROTO_DIR / proto_file).read_text(encoding="utf-8")
        try:
            schema_id = register(subject, schema_text)
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
