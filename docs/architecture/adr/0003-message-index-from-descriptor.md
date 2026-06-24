---
name: adr-0003-message-index-from-descriptor
description: ADR-0003 — message-index do envelope derivado do descriptor (variável), nunca hardcoded. Puxe ao implementar framing num SDK.
alwaysApply: false
---

# ADR-0003 — message-index derivado do descriptor

## Status
Aceito (jun/2026).

## Contexto
O envelope Confluent inclui o índice da mensagem dentro do `.proto`. Hardcodar `0x00` quebra quando há
mais de uma mensagem no arquivo (ex.: `Transaction` é índice 1, não 0) e quebra a interop com
consumidores Confluent reais.

## Decisão
O `message-index` é **derivado do descriptor** (tamanho variável; `0x00` é só a otimização do índice 0).
Go e Python têm o descriptor nativo no código gerado; o Node embute um `FileDescriptorSet`.

## Consequências
- Genérico para qualquer mensagem — sem código por-contrato.
- O Node depende de um `FileDescriptorSet` embutido (o ts-proto não traz descriptor).
