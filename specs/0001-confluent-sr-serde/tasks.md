---
name: tasks-0001-confluent-sr-serde
description: Tasks do serde Confluent SR, ligadas aos AC (rastreabilidade spec→task). Status: implementado.
alwaysApply: false
---

# Tasks — serde Confluent SR

> Cada task referencia o(s) `AC-N` que cobre. Implementado nos 3 SDKs + harness de interop.

| Task | Cobre AC | Status |
|---|---|---|
| `bind` resolve o `schema_id` no SR; falha-rápido se ausente (Go/Node/Python) | AC-1, AC-9 | ✅ |
| Framing do envelope + `message-index` derivado do descriptor | AC-2 | ✅ |
| Round-trip `produce` → `consume` | AC-3 | ✅ |
| Consumidor estrito: magic byte, frame curto, schema_id do subject, message-index, deserialize | AC-4, AC-5, AC-6, AC-7, AC-8 | ✅ |
| Harness de interop cross-language (matriz 3×3 contra SR real) | AC-10 | ✅ |

**Onde estão os testes:** `sdk/{go,node,python}` (unit com SR mockado + integração com SR real) e
`interop/orchestrate.mjs`.

> Para subir a fidelidade de "aviso" → "coberto" no eval, marcar os testes com o token `AC-N`
> (opcional; é melhoria de rastreabilidade, não bloqueia).
