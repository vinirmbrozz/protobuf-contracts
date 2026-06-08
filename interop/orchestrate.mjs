/**
 * Cross-language interop orchestrator.
 *
 * Against a REAL Schema Registry, each language produces a frame and ALL three
 * languages consume + verify it — proving the SDKs interoperate on the wire.
 * The schema_id is resolved at runtime from SR (no brittle hard-coded fixtures).
 *
 *   docker-compose up -d
 *   SCHEMA_REGISTRY_URL=http://localhost:8081 python scripts/register_schemas.py
 *   SCHEMA_REGISTRY_URL=http://localhost:8081 node interop/orchestrate.mjs
 */
import { execFileSync } from 'node:child_process';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const repo = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const tmp = mkdtempSync(join(tmpdir(), 'interop-'));
const TOPIC = 'transactions';
const PYTHON = process.env.PYTHON ?? 'python';

function run(cmd, args) {
  execFileSync(cmd, args, { cwd: repo, env: process.env, stdio: 'inherit' });
}

// Build the Go CLI once (interop/go is its own module).
const goBin = join(tmp, process.platform === 'win32' ? 'go-cli.exe' : 'go-cli');
run('go', ['build', '-C', 'interop/go', '-o', goBin, '.']);

const langs = {
  node: { produce: (f) => ['node', ['interop/cli.js', 'produce', TOPIC, f]], consume: (f) => ['node', ['interop/cli.js', 'consume', TOPIC, f]] },
  go: { produce: (f) => [goBin, ['produce', TOPIC, f]], consume: (f) => [goBin, ['consume', TOPIC, f]] },
  python: { produce: (f) => [PYTHON, ['interop/python/cli.py', 'produce', TOPIC, f]], consume: (f) => [PYTHON, ['interop/python/cli.py', 'consume', TOPIC, f]] },
};

const names = Object.keys(langs);
let checks = 0;
for (const producer of names) {
  const file = join(tmp, `${producer}.bin`);
  run(...langs[producer].produce(file));
  for (const consumer of names) {
    run(...langs[consumer].consume(file));
    checks++;
  }
}

console.log(`\n✅ cross-language interop OK — ${checks} produce→consume combinations (Go/Node/Python)`);
