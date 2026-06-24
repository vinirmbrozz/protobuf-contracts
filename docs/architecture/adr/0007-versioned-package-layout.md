---
name: adr-0007-versioned-package-layout
description: ADR-0007 — layout buf-idiomático versionado (raiz do módulo em proto/, dir = package, sufixo vN). Puxe ao criar/mover protos ou mexer no buf.yaml.
alwaysApply: false
---

# ADR-0007 — Layout de pacote versionado (buf-idiomático)

## Status
Aceito (jun/2026). Estende a ADR-0005 (codegen no buf) e ajusta a "Strategy A" para Go.

## Contexto
Os protos eram **flat** em `proto/` (`proto/transaction.proto`), com package sem versão
(`protobuf.transaction`) e exceções de lint carregadas (`PACKAGE_DIRECTORY_MATCH`,
`PACKAGE_VERSION_SUFFIX`). Ao introduzir contratos reais multi-domínio (`transaction`, `onboarding`,
`type` compartilhado), ter **vários packages no mesmo diretório** quebra `PACKAGE_SAME_DIRECTORY`, e a
falta de versão impede evoluir um domínio sem mexer nos outros.

## Decisão
Adotar a convenção do Style Guide do buf (e do googleapis): **`<root>/<org>/<domínio>/<versão>/<arquivo>.proto`**,
com **package = caminho do diretório** e **sufixo de versão**:
- `proto/protobuf/transaction/v1/transaction.proto` → `package protobuf.transaction.v1`
- `proto/protobuf/onboarding/v1/onboarding.proto` → `package protobuf.onboarding.v1`
- `proto/protobuf/type/v1/{address,registration,banking,pix}.proto` → `package protobuf.type.v1` (à la `google.type`)

A **raiz do módulo buf passa a ser `proto/`** via `buf.yaml` **v2** (`modules: [path: proto]`), então
`dir == package`. Com isso o conjunto `STANDARD` de lint passa **sem nenhuma exceção**. `go_package`
recebe alias `;<domínio>v1` para evitar colisão do nome de pacote Go "v1".

## Consequências
- Versionamento por domínio: um breaking change vira `v2` ao lado de `v1`, sem tocar os demais.
- FQNs versionados (`protobuf.transaction.v1.Transaction`) e caminhos de geração mais profundos
  (`gen/<lang>/protobuf/<domínio>/v1/…`).
- **Go**: o `go_package` aponta para `gen/go`, então `sdk/go` e `interop/go` fazem `require`+`replace`
  do módulo `gen/go` (os tipos `type/v1` vivem lá). Ou seja, a premissa da ADR-0005 de "gen/ e sdk/
  idênticos e independentes" **não vale para Go multi-pacote** — `sdk/go` depende de `gen/go`.
- Ver `docs/versioning-policy.md` para a política de evolução.
