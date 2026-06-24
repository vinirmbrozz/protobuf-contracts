---
name: adr-0002-thin-sdk
description: ADR-0002 — SDK fino que só resolve o schema_id; o registro fica fora da lib. Puxe ao mexer no serde ou no fluxo de registro.
alwaysApply: false
---

# ADR-0002 — SDK fino (resolve, não registra)

## Status
Aceito (jun/2026).

## Contexto
Uma abordagem anterior embutia o `.proto` como **string dentro do SDK** e fazia a lib **registrar** o
schema no SR. Isso duplicava a fonte da verdade (drift) e era frágil.

## Decisão
O SDK é **fino**: tipos gerados + serde + descriptor embutido. Ele só **resolve** o `schema_id` no SR
(read-only). O **registro** é um passo externo — o registrador (`scripts/register_schemas.py`), único
writer do SR, que lê o `.proto` real.

## Consequências
- A lib não carrega cópia do `.proto`; um serviço nunca lê `.proto` em runtime.
- Registro vira passo de build/ops; o `bind` falha-rápido se o subject ainda não estiver no SR.
