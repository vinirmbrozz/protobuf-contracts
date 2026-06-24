---
name: STATE
description: Memória de trabalho (continuidade) do protobuf-contracts — estado atual, próximo passo, decisões e jornada. Contexto base, leia ao retomar.
alwaysApply: true
---

# STATE — protobuf-contracts (continuidade)

> **Memória volátil** (estado/próximo passo). Decisão durável vai pra ADR (`docs/architecture/adr/`).
> Apresentação: [`README.md`](../README.md) · wire format: [`docs/confluent-sr-serde-spec.md`](confluent-sr-serde-spec.md).

## 1. O que é
Repositório central de **contratos de dados**. O `.proto` é a **fonte única**: dele geramos uma
**SDK por linguagem** (Go, Node, Python) via `buf`, e registramos os schemas no **Confluent Schema
Registry**. Serviços **importam o SDK** para produzir/consumir no Kafka; o SR é o porteiro.

## 2. Modelo atual — "thin SDK" (Decisão A)
Os princípios (ver README §"O contrato do projeto"):
1. O `.proto` é tocado por **uma coisa só: a geração** (`buf generate` faz os SDKs; o **registrador**
   lê o `.proto` para registrar o schema). Ninguém lê `.proto` em runtime.
2. O **SDK é a única interface**: tipos gerados + **descriptor embutido** + serde (envelope Confluent)
   + integração de leitura com o SR.
3. O serviço faz `bind(topic, Tipo)` no startup, depois `produce`/`consume`. Nunca vê schema, `.proto`,
   descriptor, codec, magic byte, `schema_id` ou payload cru.
4. O **Schema Registry é a autoridade**. O SDK **resolve** o `schema_id` (só lê); quem **registra** é o
   registrador (build/ops). Segurança é aplicada **no consumidor** (validação por subject + rejeição
   tipada → DLQ); o broker não barra escrita (limite honesto — ver spec §11).

## 3. Layout
- `proto/protobuf/<domínio>/v1/*.proto` — fontes versionadas (raiz do módulo = `proto/` via `buf.yaml`
  **v2**; dir = package). Domínios: `transaction`, `onboarding`, `type` (compartilhado). Ver ADR-0007.
- `gen/{go,node,typescript,python}` — código gerado canônico (registro; não usar pra serde).
- `sdk/{go,node,python}` — **SDKs publicáveis** (tipos + serde). É o que os serviços importam.
  Go: `sdk/go`/`interop/go` `require`+`replace` `gen/go` (o `go_package` aponta pra lá).
  Python: árvore gerada top-level `sdk/python/{protobuf,buf}/` + `protobuf_contracts/` (serde).
- `scripts/register_schemas.py` + `scripts/schemas.json` — **registrador** (único que escreve no SR);
  topic→proto + **auto-descoberta** de schema references pelos `import` (ADR-0008).
- `interop/` — 3 CLIs (`cli.js` ESM, `go/main.go`, `python/cli.py`) + `orchestrate.mjs` (matriz 3×3).
- `.github/workflows/` — `buf-ci.yml` (lint, breaking, 3 testes de SDK, interop real com Kafka+SR) e
  `generate.yml` (`buf dep update` + `buf generate` + passo Python `--include-imports`
  `buf.gen.pyimports.yaml` + embute o `descriptor-set.ts` do Node; auto-commit `[skip ci]`).
- `buf.yaml` (v2, `modules: [path: proto]`, lint `STANDARD` sem exceções) · `buf.gen.yaml` (gen/+sdk/) ·
  `buf.gen.pyimports.yaml` (SDK Python com imports).

## 4. A jornada (refactor thin SDK → contratos reais)
**De onde viemos:** uma entrega de agente (rod-30) embutia o **`.proto` como string hardcoded** dentro
da lib e fazia a lib **registrar** o schema. O fundador rejeitou: duplicava a fonte da verdade e era
frágil. Discussão de design levou à **Decisão A** (registro fora da lib; SDK só resolve `schema_id`).

**Refactor thin SDK (chats anteriores, fundador + Claude como dupla):**
- **3 SDKs reescritos** no modelo thin (Go/Node/Python): `bind`/`produce`/`consume`, resolve-only,
  consumidor estrito, **`msg_index` variável derivado do descriptor** (corrigiu bug do `0x00` fixo —
  no mock, `Transaction` era índice 1). Unit (mock SR) + **integração contra SR real** — todos verdes.
- **Registrador** (`scripts/register_schemas.py`) — lê o `.proto` real e registra no SR (id atribuído
  pelo SR). Validado contra SR real.
- **Interop cross-language**: orquestrador roda **9/9** (cada lib produz, as 3 consomem) contra SR real,
  frames byte-idênticos.
- **CI** reescrito (Kafka+SR via docker-compose no runner) + `generate.yml` gera o descriptor.
- **README** de apresentação (3 fluxogramas mermaid) + refresh de `sdk/node/README`, `gen/*/README`,
  `interop/README`, spec e `visao-geral`.
- **Limpeza**: destrackeado `sdk/node/dist/` + `__pycache__`/`.egg-info` (build artifacts), `.gitignore`
  ajustado, `prepare` no `sdk/node`.

**Neste chat (rename + contratos reais, tudo na main):**
- **Rename de marca** Truther → Protobuf (repo, pacotes, módulo Go, package proto). Pendência humana:
  renomear o repo no GitHub + a pasta local.
- **Mock descartado → contratos reais** espelhando o `data-rudder-provider` (ADR-0009): `transaction`,
  `onboarding`, `type` (compartilhado). Só evento de criação; dinheiro=string, enums, Timestamp, Struct.
- **Layout versionado buf-idiomático** (ADR-0007): `proto/protobuf/<domínio>/v1/`, `buf.yaml` v2, lint
  `STANDARD` sem exceções.
- **protovalidate** (ADR-0008): regras portadas do `Valid()` Go; deps por linguagem (Go via módulo BSR;
  Python self-contained via `--include-imports`; Node inline no ts-proto); refs no SR auto-descobertas.
- **3 SDKs re-fiados** p/ os tipos versionados — Go/Node/Python verificados local (build+test) e verdes na main.
- **Docs alinhadas** (README, spec, visao-geral, versioning-policy, packaging) à API real e ao layout novo.

**Lições (registradas):**
- O `msg_index` precisa ser **derivado do descriptor** (genérico); hardcode quebra interop.
- **protovalidate não é de graça**: cada linguagem resolve `buf.validate` diferente (Go=módulo BSR,
  Node=inline ts-proto, Python=gerar via `--include-imports`); e o SR precisa das referências.
- **Branch do agente herda `origin/main` como upstream** se criada de `origin/main` → o Sync do VSCode
  empurra pra main. Criar branch e dar `git branch --unset-upstream` (vira "Publish Branch").
- A **geração (`buf generate`) roda no CI/Linux**, não no host Windows (shim npm do ts-proto quebra sob
  o buf no Windows). O host consome os artefatos.
- Node (ts-proto) não embute descriptor → embutimos um **`FileDescriptorSet`** (base64) + `$type`
  (outputTypeRegistry) para resolver o índice genericamente, em paridade com Go/Python.
- "mock não basta": tudo foi provado contra **SR real**.

## 5. Decisões-chave
- **Decisão A** (registro fora da lib; SDK resolve) — escolhida pelo fundador. Ver ADR-0002.
- `TopicNameStrategy` (`<topic>-value`), compat **BACKWARD** — ADR-0006.
- Segurança = conformidade aplicada no **consumidor** (não admissão no broker, não autenticação de
  remetente — camadas futuras: broker-side validation, ACL/TLS) — ADR-0004.
- **Layout versionado** `proto/protobuf/<domínio>/v1/` (buf v2, dir=package, sufixo de versão) — ADR-0007.
- **protovalidate** para regras de campo no `.proto` (deps por linguagem; refs no SR) — ADR-0008.
- **Contratos espelham o `data-rudder-provider`** (rotas de criação) + convenções de mapeamento
  (dinheiro=string, enums, Timestamp, Struct) — ADR-0009.

## 6. Pendências
**Concluído neste chat (na main):** rename Truther→Protobuf · contratos reais versionados
(`transaction`/`onboarding`/`type`) · protovalidate · 3 SDKs re-fiados (verdes) · docs alinhadas.

**Em aberto:**
- **Humano**: renomear o repo no GitHub e a pasta local (o agente não faz). O código já diz `protobuf-contracts`.
- **Enforcement de runtime do protovalidate** no `consume` (hoje as regras viajam no descriptor, mas o
  SDK ainda não roda o protovalidate) — ver ADR-0008.
- **Outras operações** do `data-rudder-provider` (update/status/batch) se/quando precisar — hoje só o
  evento de criação (ADR-0009).
- **Público vs privado dos registries** (npm/PyPI/Go) — decidir antes de publicar.
- **Broker-side schema validation** (Confluent Platform/proxy) se um dia quiser barrar na escrita.

## 7. Como rodar / retomar
```bash
docker compose up -d                                              # Kafka + SR (localhost:8081)
SCHEMA_REGISTRY_URL=http://localhost:8081 python scripts/register_schemas.py
SCHEMA_REGISTRY_URL=http://localhost:8081 node interop/orchestrate.mjs   # matriz 3×3
```
Testes por SDK: `sdk/go` `go test ./...` · `sdk/node` `npm install && npm test` ·
`sdk/python` `pip install sdk/python/ pytest && python -m pytest sdk/python/tests/test_serde_unit.py`.
A geração roda no CI; localmente exige `buf` + plugins (preferir o container/CI).
