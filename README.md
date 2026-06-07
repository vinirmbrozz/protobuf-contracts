# truther-contracts

[![buf CI](https://github.com/vinirmbrozz/truther-contracts/actions/workflows/buf-ci.yml/badge.svg)](https://github.com/vinirmbrozz/truther-contracts/actions/workflows/buf-ci.yml)

## O que é / por quê

`truther-contracts` é o repositório central de contratos de dados da Truther. **Protobuf é a fonte única**: toda estrutura que trafega pelo Kafka é definida em `proto/`, gerada automaticamente para Go, Node e Python via **buf**, e registrada no **Confluent Schema Registry**. Nenhuma mensagem inválida ou não registrada cruza o Kafka — producers e consumers são tipados contra os mesmos schemas compilados e o mesmo envelope de wire format, garantindo interoperabilidade total entre linguagens.

---

## Layout do repositório

| Caminho | Conteúdo |
|---------|----------|
| `proto/` | Fontes `.proto` — única fonte de verdade; nunca editar arquivos gerados |
| `gen/` | Codegen canônico gerado por `buf generate`; nunca editar à mão |
| `sdk/go/` | Pacote Go publicável: tipos gerados + serde Confluent SR |
| `sdk/node/` | Pacote Node/TS publicável: `@truther/contracts` — tipos + serde |
| `sdk/python/` | Pacote Python publicável: `truther-contracts` — tipos + serde |
| `interop/` | Harness cross-language: Go / Node / Python produzem e consomem entre si |
| `docs/` | Especificações e políticas aprofundadas |
| `buf.yaml` | Configuração do buf (lint, breaking, protovalidate) |
| `buf.gen.yaml` | Pipeline de codegen multi-linguagem |
| `.github/` | Workflows CI (`buf-ci.yml`, `generate.yml`) |

---

## Como consumir (por linguagem)

### Go

```bash
go get github.com/vinirmbrozz/truther-contracts/sdk/go@latest
```

```go
import (
    "os"
    serde "github.com/vinirmbrozz/truther-contracts/sdk/go"
    txpb "github.com/vinirmbrozz/truther-contracts/sdk/go/proto"
)

// Cria o serde — lê SCHEMA_REGISTRY_URL do ambiente
s, err := serde.New()

// Registra o tipo na inicialização do serviço (idempotente)
schema, _ := os.ReadFile("proto/transaction.proto")
s.RegisterType("transactions", &txpb.Transaction{}, string(schema))

// Producer
frame, err := s.Produce("transactions", &txpb.Transaction{
    TransactionAmount: "9.99",
    FinalDecision:    "APPROVED",
})

// Consumer
msg, err := s.Consume("transactions", kafkaRawBytes)
tx := msg.(*txpb.Transaction)
```

### Node / TypeScript

```bash
npm install file:./sdk/node   # ou o pacote publicado @truther/contracts
```

```typescript
import { TrutherSerde, Transaction } from '@truther/contracts';
import type { MessageCodec } from '@truther/contracts';
import { readFileSync } from 'fs';

const serde = new TrutherSerde(); // lê SCHEMA_REGISTRY_URL do ambiente

// Codec adapter ts-proto
const TransactionCodec: MessageCodec<Transaction> = {
  encode: (msg) => Buffer.from(Transaction.encode(msg).finish()),
  decode: (bytes) => Transaction.decode(bytes),
};

// Registra na inicialização
const protoContent = readFileSync('./proto/transaction.proto', 'utf8');
await serde.registerSchema('transactions', protoContent);

// Producer
const frame = serde.produce('transactions', {
  transactionAmount: '9.99',
  finalDecision: 'APPROVED',
}, TransactionCodec);

// Consumer — lança SerdeError se o payload for inválido (roteie para DLQ)
const tx = await serde.consume('transactions', kafkaRawBytes, TransactionCodec);
```

### Python

```bash
pip install sdk/python/     # ou o pacote publicado truther-contracts
```

```python
from truther_contracts import Transaction
from truther_contracts.serde import KafkaSerde

serde = KafkaSerde()  # lê SCHEMA_REGISTRY_URL do ambiente

# Registra na inicialização (síncrono, idempotente)
serde.startup({"transactions": Transaction})

# Producer
frame = serde.produce("transactions", Transaction(
    transaction_amount="9.99",
    final_decision="APPROVED",
))

# Consumer — lança SerdeError se o payload for inválido (roteie para DLQ)
tx = serde.consume("transactions", kafka_raw_bytes, Transaction)
```

> **Variáveis de ambiente** (todas as linguagens): `SCHEMA_REGISTRY_URL` (obrigatória), `SCHEMA_REGISTRY_API_KEY`, `SCHEMA_REGISTRY_API_SECRET` (opcionais, para ambientes autenticados).

---

## Como contribuir / mexer no proto

1. Edite `proto/<domain>.proto`
2. Rode as verificações:
   ```bash
   buf lint          # estilo e nomenclatura
   buf breaking      # regressão de compatibilidade vs. baseline
   buf generate      # regera gen/ e sdk/ (ambos ficam em sincronia)
   ```
3. **Novos campos devem usar `snake_case`.** Tipos existentes nunca são removidos — regra de compatibilidade **BACKWARD**.
4. Consulte [`docs/versioning-policy.md`](docs/versioning-policy.md) antes de qualquer mudança estrutural ou remoção.

---

## Wire format (resumo)

Todo valor de mensagem Kafka usa o envelope Confluent:

```
[0x00 magic] [schema_id: 4 bytes BE] [0x00 msg-index] [payload proto3]
```

Producers registram o schema no SR e serializam com o `schema_id`; consumers validam o `schema_id` contra o SR antes de deserializar, rejeitando payloads inválidos ou desconhecidos (roteiam para DLQ). Especificação completa: [`docs/confluent-sr-serde-spec.md`](docs/confluent-sr-serde-spec.md).

---

## Toolchain / pré-requisitos

| Ferramenta | Como instalar |
|------------|---------------|
| `buf` | `brew install bufbuild/buf/buf` — [instruções completas](https://buf.build/docs/installation) |
| Plugin Go (`protoc-gen-go`) | `go install google.golang.org/protobuf/cmd/protoc-gen-go@latest` |
| Plugins Node/TS (`ts-proto`) | `npm install` na raiz do repo (instalado via `node_modules`) |
| Schema Registry local | `docker compose up -d` → SR disponível em `http://localhost:8081` |

Para detalhes de configuração de plugins e ambientes de CI, veja [`docs/packaging.md`](docs/packaging.md).

---

## Links

- [Visão geral do projeto](docs/visao-geral.md) — como o truther-contracts funciona, para quem está chegando agora
- [SPEC serde Confluent SR](docs/confluent-sr-serde-spec.md) — contrato autoritativo do wire format e SR
- [Política de versionamento](docs/versioning-policy.md) — compatibilidade BACKWARD, layout proto, snake_case
- [Packaging e layout dos SDKs](docs/packaging.md) — estrutura canônica `sdk/`, estratégia de codegen
