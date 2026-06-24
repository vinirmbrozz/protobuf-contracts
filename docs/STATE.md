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
- `proto/` — fontes (fonte única).
- `gen/{go,node,typescript,python}` — código gerado canônico (registro; não usar pra serde).
- `sdk/{go,node,python}` — **SDKs publicáveis** (tipos + serde). É o que os serviços importam.
- `scripts/register_schemas.py` + `scripts/schemas.json` — **registrador** (único que escreve no SR) +
  mapa topic→proto.
- `interop/` — 3 CLIs (`cli.js` ESM, `go/main.go`, `python/cli.py`) + `orchestrate.mjs` (matriz 3×3).
- `.github/workflows/` — `buf-ci.yml` (lint, breaking, 3 testes de SDK, interop real com Kafka+SR) e
  `generate.yml` (buf generate + embute o `descriptor-set.ts` do Node; auto-commit `[skip ci]`).

## 4. A jornada deste chat (o refactor para thin SDK)
**De onde viemos:** uma entrega de agente (rod-30) embutia o **`.proto` como string hardcoded** dentro
da lib e fazia a lib **registrar** o schema. O fundador rejeitou: duplicava a fonte da verdade e era
frágil. Discussão de design levou à **Decisão A** (registro fora da lib; SDK só resolve `schema_id`).

**O que foi feito (fundador + Claude como dupla, implementando direto numa branch):**
- **3 SDKs reescritos** no modelo thin (Go/Node/Python): `bind`/`produce`/`consume`, resolve-only,
  consumidor estrito, **`msg_index` variável derivado do descriptor** (corrigiu bug do `0x00` fixo —
  `Transaction` é índice 1, não 0). Unit (mock SR) + **integração contra SR real** — todos verdes.
- **Registrador** (`scripts/register_schemas.py`) — lê o `.proto` real e registra no SR (id atribuído
  pelo SR). Validado contra SR real.
- **Interop cross-language**: orquestrador roda **9/9** (cada lib produz, as 3 consomem) contra SR real,
  frames byte-idênticos.
- **CI** reescrito (Kafka+SR via docker-compose no runner) + `generate.yml` gera o descriptor.
- **README** de apresentação (3 fluxogramas mermaid) + refresh de `sdk/node/README`, `gen/*/README`,
  `interop/README`, spec e `visao-geral`.
- **Limpeza**: destrackeado `sdk/node/dist/` + `__pycache__`/`.egg-info` (build artifacts), `.gitignore`
  ajustado, `prepare` no `sdk/node`.

**Lições (registradas):**
- O `msg_index` precisa ser **derivado do descriptor** (genérico); hardcode quebra interop.
- A **geração (`buf generate`) roda no CI/Linux**, não no host Windows (shim npm do ts-proto quebra sob
  o buf no Windows). O host consome os artefatos.
- Node (ts-proto) não embute descriptor → embutimos um **`FileDescriptorSet`** (base64) + `$type`
  (outputTypeRegistry) para resolver o índice genericamente, em paridade com Go/Python.
- "mock não basta": tudo foi provado contra **SR real**.

## 5. Decisões-chave
- **Decisão A** (registro fora da lib; SDK resolve) — escolhida pelo fundador.
- `TopicNameStrategy` (`<topic>-value`), compat **BACKWARD**.
- Segurança = conformidade aplicada no **consumidor** (não admissão no broker, não autenticação de
  remetente — camadas futuras: broker-side validation, ACL/TLS).

## 6. Pendências
- 🟡 **EM ANDAMENTO — protos reais a partir do `data-rudder-provider`.** Mock descartado. **Feito:**
  `proto/shared.proto` (PixKeyType, Address, RegistrationData, BankingData), `proto/transaction.proto`
  (Transaction = `CreateTransactionReq`: device/transaction/credit_card/boleto/order/pos/customer/extra_data)
  e `proto/onboarding.proto` (`CreateOnboardingReq`). Decisões: só evento de criação; idiomático
  (dinheiro=string decimal, enums `_UNSPECIFIED=0`, `Timestamp`, `Struct`); `shared.proto` importado por
  ambos. **Validações via protovalidate** portando os `Valid()` do Go (required, `amount_total>0` via CEL,
  enum `pix_key_type` defined_only). `scripts/schemas.json` reestruturado (libraries+topics+references) e
  `register_schemas.py` agora é **reference-aware**. **Falta:**
  1. **Vendorizar `buf/validate/validate.proto`** (`buf export buf.build/bufbuild/protovalidate -o vendor/protovalidate`,
     roda no CI) — o registrador precisa dele p/ registrar os schemas com protovalidate (referência no SR).
  2. **Regerar no CI** (`generate.yml`) — ⚠️ `buf breaking` vai acusar (substituição do mock é breaking
     intencional; sem consumidores reais ainda). Validar a sintaxe protovalidate só roda no CI (buf não roda no Windows).
  3. **Re-fiação do SDK** (depende do código regerado): re-exports (`__init__.py`, `index.ts`), **testes**
     (referenciam `PredictiveAnalyzer`/campos do mock), **interop** CLIs, exemplos do README.
- **Renomear a marca "Truther" → "Protobuf"** — ✅ **feito** (este passo). Repo agora `protobuf-contracts`;
  pacotes `@protobuf/contracts` / `protobuf-contracts` / `protobuf_contracts`; package proto `protobuf.transaction`;
  módulo Go `github.com/vinirmbrozz/protobuf-contracts`. Código **gerado** (`gen/**`, `sdk/**/generated`,
  `*_pb2.py`/`*.pb.go`/`*_pb.js`, descriptor binário) **regenera no CI** (`generate.yml`) — não editado à mão.
  Falta (humano): renomear o repo no GitHub e a pasta local.
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
