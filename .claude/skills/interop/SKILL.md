---
name: interop
description: Provar interoperabilidade cross-language — sobe Kafka+SR, registra os schemas e roda a matriz 3×3 (Go/Node/Python produzem e consomem entre si). Read-only (não altera o projeto). Rode quando quiser confirmar que os SDKs concordam.
---

# /interop — provar o wire format cross-language (contra SR real)

Verificação **read-only**: usa os SDKs **já gerados** pra provar que falam o mesmo wire format. Não
gera SDK, não muda o repo, não publica nada — só produz/consome mensagens de teste contra um SR local.

## Passos
1. `docker compose up -d` — Kafka + Schema Registry; espere o SR responder em `:8081`.
2. Registre os schemas: `SCHEMA_REGISTRY_URL=http://localhost:8081 python scripts/register_schemas.py`.
3. Prepare os SDKs que os CLIs importam: `sdk/node` (`npm i && npm run build`),
   `sdk/python` (`pip install sdk/python/`), `interop` (`npm i`).
4. Rode: `SCHEMA_REGISTRY_URL=http://localhost:8081 node interop/orchestrate.mjs`.

## Resultado esperado
Matriz **3×3 verde** — cada linguagem produz um frame e as três consomem+verificam; frames byte-idênticos.

## Definition of Done
- [ ] 9/9 produce→consume verdes contra SR real.
