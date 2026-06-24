#!/usr/bin/env node
// Auditoria da esteira SDD (protobuf-contracts) — ESCOPADA na camada SDD.
// Cobre: CLAUDE.md, docs/, specs/, .claude/skills/. Os READMEs de biblioteca
// (gen/, sdk/, interop, raiz) e os docs de REFERÊNCIA legados ficam exemptos —
// são docs do projeto, não artefatos da esteira (evita firula de frontmatter).
// Uso: node scripts/audit-esteira.mjs [dir]   (default ".") — exit 1 se violar (gate de CI).

import { readFileSync, readdirSync, statSync, existsSync } from "node:fs";
import { join, dirname, relative, resolve, extname } from "node:path";

const ROOT = resolve(process.argv[2] || ".");
const SDD_ROOTS = ["CLAUDE.md", "docs", "specs", ".claude/skills"];
// Docs de referência que predam o SDD — exemptos de frontmatter e link-check.
const EXEMPT = new Set([
  "docs/visao-geral.md",
  "docs/confluent-sr-serde-spec.md",
  "docs/packaging.md",
  "docs/versioning-policy.md",
]);

const rel = (f) => relative(ROOT, f).replace(/\\/g, "/");
const errors = [];
const err = (file, msg) => errors.push(`${rel(file) || file}: ${msg}`);

function walk(dir) {
  const out = [];
  for (const name of readdirSync(dir)) {
    if (name === "node_modules" || name === ".git" || name.startsWith(".tmp")) continue;
    const full = join(dir, name);
    if (statSync(full).isDirectory()) out.push(...walk(full));
    else if (extname(full) === ".md") out.push(full);
  }
  return out;
}

function parseFrontmatter(text) {
  if (!text.startsWith("---")) return null;
  const end = text.indexOf("\n---", 3);
  if (end === -1) return null;
  const keys = {};
  for (const line of text.slice(3, end).trim().split("\n")) {
    const m = line.match(/^([A-Za-z_][\w-]*)\s*:/);
    if (m) keys[m[1]] = line.slice(m[0].length).trim();
  }
  return keys;
}

const isSkillDialect = (f) => rel(f).includes("/.claude/skills/") || rel(f).startsWith(".claude/skills/");

// Coleta os .md só dentro do escopo SDD.
let files = [];
for (const r of SDD_ROOTS) {
  const p = join(ROOT, r);
  if (!existsSync(p)) continue;
  if (statSync(p).isDirectory()) files.push(...walk(p));
  else if (extname(p) === ".md") files.push(p);
}
files = files.filter((f) => !EXEMPT.has(rel(f)));

// 1) Frontmatter + dialeto
for (const f of files) {
  const fm = parseFrontmatter(readFileSync(f, "utf8"));
  if (!fm) { err(f, "sem frontmatter"); continue; }
  if (!fm.name) err(f, "frontmatter sem `name`");
  if (!fm.description) err(f, "frontmatter sem `description`");
  if (isSkillDialect(f)) {
    if ("alwaysApply" in fm) err(f, "dialeto skill não deve ter `alwaysApply`");
  } else if (!("alwaysApply" in fm)) {
    err(f, "doc sem `alwaysApply`");
  } else if (!/^(true|false)$/.test(fm.alwaysApply)) {
    err(f, `alwaysApply inválido: ${fm.alwaysApply}`);
  }
}

// 2) Links relativos quebrados
const linkRe = /\]\(([^)]+)\)/g;
for (const f of files) {
  const text = readFileSync(f, "utf8");
  let m;
  while ((m = linkRe.exec(text))) {
    let target = m[1].trim();
    if (/^(https?:|mailto:|#)/.test(target)) continue;
    if (/[<>]|XXXX|NNNN|\s/.test(target)) continue;
    target = target.split("#")[0];
    if (!target) continue;
    if (!existsSync(resolve(dirname(f), target))) err(f, `link quebrado → ${target}`);
  }
}

// 3) Toda pasta specs/NNNN-* precisa de spec.md
const specsDir = join(ROOT, "specs");
if (existsSync(specsDir)) {
  for (const name of readdirSync(specsDir)) {
    if (/^\d{4}-/.test(name) && !existsSync(join(specsDir, name, "spec.md")))
      err(join(specsDir, name), "feature sem `spec.md`");
  }
}

if (errors.length) {
  console.error(`\n✗ Auditoria da esteira: ${errors.length} problema(s)\n`);
  for (const e of errors) console.error(`  • ${e}`);
  console.error("");
  process.exit(1);
} else {
  console.log(`✓ Auditoria da esteira: ${files.length} docs SDD OK (frontmatter, links, specs).`);
}
