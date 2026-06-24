# Visão geral — como o protobuf-contracts funciona

> Arquivo para leitura e aprendizado de quem não tem familiaridade com o projeto.
> Pode ser lido por IA para referência, mas o foco é ensinar alguém a caminhar sozinho pelo projeto.
> Mapa mental do projeto, destilado dos 6 tópicos de entendimento (ROD-14).
> Para o detalhe formal, ver [`confluent-sr-serde-spec.md`](confluent-sr-serde-spec.md) (o contrato)
> e [`versioning-policy.md`](versioning-policy.md) (a política).

## Objetivo
O **Protobuf é a fonte única** das estruturas de dados que trafegam no Kafka. Só estruturas
**registradas e válidas** cruzam o Kafka (producer e consumer), e as mesmas estruturas são usadas
em **Go, Python e Node** — padronização total. O Schema Registry (Confluent) é o "cartório" dos contratos.

Fluxo macro: `proto/*.proto` → **buf** gera SDKs (Go/TS/Node/Python) → cada serviço usa uma **lib
tipada** de produce/consume que embrulha a mensagem no **envelope Confluent** e valida contra o
**Schema Registry**.

---

## 1. Formato de fio (o coração)
Toda mensagem no Kafka vai dentro do **envelope Confluent**:
```
[ Magic Byte 0x00 ][ Schema ID (4 bytes, big-endian) ][ Message Index (0x00) ][ Payload protobuf ]
```
- **Magic byte** sempre `0x00` — consumidor rejeita qualquer outro 1º byte.
- **Schema ID** — número que o Schema Registry deu ao schema; diz "sigo o contrato nº N".
- **Message index** — qual mensagem no `.proto`; convenção Protobuf = 1 msg por subject → sempre `0x00`.
- **Payload** — proto3 binário (sem prefixo de tamanho).

Cabeçalho fixo de 6 bytes + payload. **Isto é o portão**: produtor embrulha com o id; consumidor
checa magic → lê id → confere no registry → decodifica → valida (protovalidate) → erro vai pra **DLQ**.
Só o que é registrado + válido chega no handler.

## 2. Governança com `buf`
- `buf.yaml` — regras de **lint** (estilo/nomes) e **breaking** (detecta mudança incompatível).
- `buf.gen.yaml` — **codegen**: 1 proto → SDKs Go, TypeScript, Node(JS), Python.
- `.github/workflows/buf-ci.yml` — CI roda lint + breaking (vs `main`) + os testes de interop a cada PR.
- O CI **roda em qualquer mudança no proto**, mas **só falha** em lint quebrado ou mudança **incompatível**;
  mudança **aditiva passa**.

## 3. Versionamento & compatibilidade
Princípio: **evolução aditiva + BACKWARD** (schema novo lê dado escrito pelo schema velho).
- ✅ Seguro (sem bump): adicionar campo opcional novo, valor de enum novo, mensagem nova, comentário.
- ❌ Breaking: renomear/remover campo, mudar tipo/número, remover mensagem, `optional`→`required`.
- 🔑 **Nunca reusar número de campo** (o fio identifica por número, não por nome) → ao remover, use `reserved`.
- "Faça no lugar": renomear → **adiciona campo novo + deprecia o velho**; reserva o número só na remoção (depois, em migração coordenada / `v2`).

## 4. Ambiente local (Kafka + Schema Registry)
`docker-compose.yml` sobe **zookeeper + kafka + schema-registry** (imagens Confluent), só pra dev:
- Kafka em **`localhost:9092`** (host) / `kafka:29092` (interno aos containers).
- Schema Registry em **`localhost:8081`** (`SCHEMA_REGISTRY_URL`).
- `BACKWARD` ligado globalmente. **`PLAINTEXT`, sem auth** — é dev; segurança é assunto de prod (ver Segurança).

## 5. Harness de interop
Prova que Go/Node/Python falam **o mesmo contrato byte a byte** — contra um **Schema Registry real**.
- Um **CLI por linguagem** (`interop/cli.js`, `interop/go/main.go`, `interop/python/cli.py`) usa o
  SDK da própria linguagem (`bind`/`produce`/`consume`).
- O **orquestrador** (`interop/orchestrate.mjs`) roda a matriz **3×3**: cada linguagem **produz** um
  frame e as três **consomem+verificam** — o `schema_id` é resolvido em runtime (sem fixture fixa).
- Hoje: **9/9 verde** (Go↔Node↔Python), frames byte-idênticos. Roda no CI (`buf-ci.yml`, job
  `interop`) subindo Kafka+SR via `docker compose`.

## 6. O `proto` em si
`proto/transaction.proto` é um **exemplo/seed** (não a estrutura final). Anatomia: `message` = struct;
campo = `tipo nome = número;`; mensagens compõem (Transaction contém PredictiveAnalyzer).
Convenções para os protos reais: snake_case, numerar de 1 sem reusar, **enum** (com `_UNSPECIFIED=0`)
em vez de string livre, validação (protovalidate) já no desenho, dinheiro como string/inteiro (nunca float).

## Segurança (modelo)
O envelope é **governança/correção, não autenticação**. O formato é público (magic `0x00` é conhecido) —
não protege contra quem não deveria escrever. Segurança real é camada separada: **Kafka SASL/mTLS + ACLs +
Schema Registry privado/autenticado + rede**. Não trocar o formato por um "secreto" achando que vira segurança.
(Detalhe em §11 do `confluent-sr-serde-spec.md`.)

---

## Mapa de arquivos
| Caminho | O quê |
|---|---|
| `proto/` | os contratos (`.proto`) — fonte única |
| `buf.yaml` / `buf.gen.yaml` | governança (lint/breaking) e codegen |
| `gen/{go,node,typescript,python}` | SDKs gerados (nunca editar à mão) |
| `docs/confluent-sr-serde-spec.md` | o contrato formal (wire format, registro, validação, segurança) |
| `docs/versioning-policy.md` | política de versionamento/compatibilidade |
| `interop/` | harness de interop cross-language |
| `docker-compose.yml` | Kafka + Schema Registry locais (dev) |
| `.github/workflows/buf-ci.yml` | CI (lint, breaking, interop) |

## Status (ROD-13 épico → ROD-14..17)
- **ROD-14** (Platform Engineer): a fundação — SPEC + buf + versionamento + ambiente + interop (referência Node). ✅
- **ROD-15** (Go), **ROD-16** (Node/TS), **ROD-17** (Python): as **libs por linguagem** que implementam
  o produce/consume tipado conforme o SPEC. (Onde o padrão vira código de produção.)
