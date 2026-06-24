---
name: adr-0008-protovalidate-field-rules
description: ADR-0008 — validação de campo no .proto via protovalidate; deps por linguagem e referência no SR. Puxe ao adicionar regras de validação ou mexer no codegen/registro.
alwaysApply: false
---

# ADR-0008 — Validação de campo via protovalidate

## Status
Aceito (jun/2026).

## Contexto
As regras de validação (campo obrigatório, `amount_total > 0`, `pix_key_type` num conjunto) viviam no
`Valid()` do provider Go (`data-rudder-provider`). O **contrato** (fonte da verdade) não as expressava —
quem só tem o `.proto`/SDK não conhecia as regras.

## Decisão
Portar as regras para os `.proto` via **protovalidate** (`(buf.validate.field)`), com a dep
`buf.build/bufbuild/protovalidate` no `buf.yaml`, resolvida por **`buf dep update` no CI** (gera o
`buf.lock`). As regras viajam no **descriptor**; o enforcement em runtime fica a cargo do consumidor
(coerente com a ADR-0004) — rodar o protovalidate no `consume` é um passo futuro.

## Consequências (integração por linguagem — não é de graça)
- **Go**: o código gerado faz blank-import do **módulo Go do protovalidate** (BSR
  `buf.build/gen/go/bufbuild/protovalidate/...`) → entra no `go.mod` (resolvido via `go mod tidy`).
- **Node**: o ts-proto **gera os tipos do `buf/validate` inline** → self-contained.
- **Python**: o `protovalidate` (pip) **não** expõe um pacote `buf.validate` importável. Solução:
  gerar `buf/validate/*_pb2.py` **dentro do SDK** via `buf generate --include-imports`
  (`buf.gen.pyimports.yaml`), que inclui imports não-WKT e **exclui** os WKT do google (vêm do runtime).
- **Registro no SR**: schemas com protovalidate importam `buf/validate/validate.proto` (e transitivos,
  ex. `expression.proto`) — que **não** são WKT, logo exigem *schema references*. O registrador
  (`scripts/register_schemas.py`) **auto-descobre** as referências pelos `import` (resolve transitivo) e
  o CI vendoriza via `buf export` (`PROTO_EXPORT_DIR`).
