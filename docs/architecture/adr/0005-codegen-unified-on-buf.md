---
name: adr-0005-codegen-unified-on-buf
description: ADR-0005 — codegen unificado no buf (Strategy A); gen/ e sdk/ idênticos, gerados no CI. Puxe ao mexer em buf.gen.yaml / generate.yml.
alwaysApply: false
---

# ADR-0005 — Codegen unificado no buf

## Status
Aceito (jun/2026).

## Contexto
Havia **dois codegens conflitantes**: um workflow que rodava `protoc` "na mão" (layout plano,
recriava pacote legado) e o `buf.gen.yaml`. Eles divergiam no layout e ressuscitavam código stale a
cada push.

## Decisão
`buf generate` é a **fonte única** de codegen — **Strategy A**: emite `gen/<lang>` (registro canônico)
e `sdk/<lang>` (publicável), **idênticos** no código gerado. Roda no CI/Linux (`generate.yml`); `gen/`
e `sdk/**/generated/` **nunca** são editados à mão. *(A geração não roda no host Windows — buf + ts-proto
quebram lá; é tarefa de CI/container.)*

## Consequências
- Um só mecanismo; `gen/` == `sdk/` garantido.
- A geração é responsabilidade do CI/container, não do dev local.
