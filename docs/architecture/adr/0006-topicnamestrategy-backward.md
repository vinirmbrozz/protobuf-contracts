---
name: adr-0006-topicnamestrategy-backward
description: ADR-0006 — subject por TopicNameStrategy (<topic>-value) e compatibilidade BACKWARD. Puxe ao nomear subjects ou evoluir um schema.
alwaysApply: false
---

# ADR-0006 — TopicNameStrategy + compatibilidade BACKWARD

## Status
Aceito (jun/2026).

## Contexto
Precisamos de uma convenção de **subject** no Schema Registry e de uma regra de **evolução** de schema.

## Decisão
- **Subject = `<topic>-value`** (TopicNameStrategy) — atrela o schema ao tópico Kafka.
- Compatibilidade **BACKWARD** (global no SR): campo só se **adiciona** (`snake_case`, número novo);
  **nunca** remover/renumerar. Uma quebra exige decisão consciente (novo ADR) + bump de versão.

## Consequências
- Evolução previsível; mudança incompatível é barrada pelo SR **e** pelo `buf breaking` no CI.
- Ver `docs/versioning-policy.md` para o detalhamento da política.
