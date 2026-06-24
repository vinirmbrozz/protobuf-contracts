---
name: adr-0001-record-architecture-decisions
description: ADR-0001 — registrar decisões de arquitetura em ADRs curtos e imutáveis. Puxe ao precisar entender por que registramos decisões assim.
alwaysApply: false
---

# ADR-0001 — Registrar decisões de arquitetura

## Status
Aceito.

## Contexto
Decisões difíceis de reverter precisam de **memória durável e rastreável** — diferente do `STATE.md`,
que é estado volátil do trabalho.

## Decisão
Registrar cada decisão estrutural num **ADR curto e imutável** em `docs/architecture/adr/`. Mudou de
ideia? **Crie um ADR novo que substitui** o anterior — nunca edite um ADR já aceito.

## Consequências
Histórico auditável do "porquê"; o raciocínio não se perde entre sessões/pessoas.
