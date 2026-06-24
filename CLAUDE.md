---
name: CLAUDE
description: Convenções SDD do protobuf-contracts (invariantes, skills, gates). Sempre ativo.
alwaysApply: true
---

# CLAUDE.md — convenções (protobuf-contracts)

Projeto de **contratos protobuf → SDK por linguagem + Confluent Schema Registry**, operado em
**Spec-Driven Development (SDD)** enxuto. Leia antes de mexer.

## Contexto base (leia sempre)
`docs/STATE.md` (onde paramos) · a `spec.md` da feature ativa em `specs/` · este arquivo.
O resto é **sob demanda** (glossário, ADRs, specs antigas, docs de referência) — puxe pelo `description`.

## Invariantes do projeto (não-negociáveis)
- O `.proto` é tocado só pela **geração**: `buf generate` faz os SDKs; o **registrador** registra. Ninguém lê `.proto` em runtime.
- `gen/` e `sdk/**/generated/` são **gerados** — nunca editar à mão. A geração roda no **CI/Linux**, não no host Windows.
- Compatibilidade **BACKWARD**: campo novo em `snake_case` com número novo; nunca remover/renumerar.
- O SDK só **resolve** o `schema_id` no SR; segurança é aplicada **no consumidor** (rejeita → DLQ); o `msg_index` sai do **descriptor**.
- **Nunca dar push.** O merge é decisão humana.

## Como trabalhar (skills)
- Contrato novo → `/novo-contrato`. Campo novo em contrato existente → `/adicionar-campo`.
- SDK de linguagem nova → `/nova-linguagem`. Provar cross-language → `/interop` (read-only).

## A spec é a fonte da verdade
Implemente a partir de `specs/NNNN-*/spec.md` — os critérios de aceite (`AC-N`) são o contrato e o
oráculo de teste. Spec ambígua → **pare e pergunte**. Não implemente fora do escopo (`Fora de escopo` é vinculante).

## Onde escrever
- Decisão difícil de reverter → **ADR** em `docs/architecture/adr/` (imutável; crie um novo, não edite o antigo).
- Estado do trabalho / próximo passo → `docs/STATE.md`.
- Termo de domínio → `docs/glossary.md`.

## Gates (rodam no CI — workflow `esteira`)
- `node scripts/audit-esteira.mjs .` — estrutura, frontmatter, links, toda `specs/NNNN-*/` com `spec.md`.
- `node scripts/eval-spec-fidelity.mjs .` — cada `AC-N` coberto por task (rastreabilidade).
