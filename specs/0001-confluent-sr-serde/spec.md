---
name: spec-0001-confluent-sr-serde
description: Spec do serde Confluent SR (bind/produce/consume + envelope + validação estrita no consumidor). Status implementado — puxe ao mexer no serde.
alwaysApply: false
---

# Spec — serde Confluent Schema Registry

> **Status: implementado.** Os critérios de aceite são o contrato e o oráculo de teste.

## Resumo
O SDK de cada linguagem serializa/deserializa mensagens Kafka no envelope Confluent SR, resolvendo o
`schema_id` no Registry e validando estritamente no consumo (rejeição tipada → DLQ).

## Critérios de aceite

### AC-1: bind resolve o schema_id
- **Dado** um subject `<topic>-value` registrado no SR
- **Quando** o serviço faz `bind(topic, Tipo)`
- **Então** o SDK resolve e cacheia o `schema_id` (GET `/subjects/<t>-value/versions/latest`), sem registrar nada.

### AC-2: produce carimba o envelope
- **Dado** um tópico bound
- **Quando** `produce(topic, msg)`
- **Então** retorna `[0x00][schema_id BE4][message-index][payload proto3]`, com o `message-index` derivado do descriptor do tipo.

### AC-3: round-trip
- **Dado** um frame produzido para o tópico
- **Quando** `consume(topic, frame)`
- **Então** devolve a mensagem original (campos iguais).

### AC-4: rejeita magic byte inválido
- **Dado** bytes cujo 1º byte ≠ `0x00`
- **Quando** `consume`
- **Então** erro tipado (magic) → DLQ; não desserializa.

### AC-5: rejeita frame curto
- **Dado** um frame com menos que o header mínimo
- **Quando** `consume`
- **Então** erro tipado (frame curto).

### AC-6: rejeita schema_id de outro subject
- **Dado** um frame com `schema_id` que não é versão do subject do tópico (ou desconhecido no SR)
- **Quando** `consume`
- **Então** erro tipado (schema foreign) → DLQ.

### AC-7: rejeita message-index divergente
- **Dado** um frame cujo `message-index` ≠ o do tipo bound
- **Quando** `consume`
- **Então** erro tipado (message-index).

### AC-8: rejeita payload inválido
- **Dado** um frame com payload protobuf inválido
- **Quando** `consume`
- **Então** erro tipado (deserialize) → DLQ.

### AC-9: bind falha se o subject não está registrado
- **Dado** um subject ausente no SR
- **Quando** `bind`
- **Então** falha-rápido (erro) — o SDK não registra.

### AC-10: interoperam cross-language
- **Dado** um frame produzido por uma linguagem (Go/Node/Python)
- **Quando** consumido por outra
- **Então** devolve a mensagem (frames **byte-idênticos** para a mesma mensagem + schema).

## Casos de borda
- A validação de `schema_id` é **cacheada** (uma chamada ao SR por id).

## Fora de escopo
> Vinculante.
- Admissão no broker (broker-side validation) e autenticação de remetente — ver ADR-0004.
- Publicação dos SDKs nos registries.

## Rastreabilidade
- Wire format (design): [`confluent-sr-serde-spec.md`](../../docs/confluent-sr-serde-spec.md)
- ADRs: [thin SDK](../../docs/architecture/adr/0002-thin-sdk.md) · [message-index](../../docs/architecture/adr/0003-message-index-from-descriptor.md) · [segurança](../../docs/architecture/adr/0004-security-at-consumer.md)
