---
name: adr-0004-security-at-consumer
description: ADR-0004 — conformidade/segurança aplicada no consumidor (não no broker); limites honestos. Puxe ao discutir segurança/garantias do sistema.
alwaysApply: false
---

# ADR-0004 — Enforcement no consumidor

## Status
Aceito (jun/2026).

## Contexto
Kafka puro aceita qualquer byte. Barrar a mensagem **na escrita** exigiria broker-side schema
validation (Confluent Platform) ou um proxy — que não temos.

## Decisão
A conformidade é aplicada no **consumidor** (`consume`), que rejeita com **erro tipado → DLQ**:
magic byte errado, frame curto, `schema_id` que **não é versão do subject** daquele tópico,
message-index divergente, ou payload que não desserializa. O envelope **não** autentica o remetente.

## Consequências
- Garante integridade **estrutural e de schema** no consumo.
- **Não** garante admissão no broker nem autenticação de remetente — camadas futuras (broker-side
  validation, Kafka ACLs/TLS, assinatura).
