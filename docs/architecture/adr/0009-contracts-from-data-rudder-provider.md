---
name: adr-0009-contracts-from-data-rudder-provider
description: ADR-0009 — contratos reais espelham as rotas de criação do data-rudder-provider + convenções de mapeamento (dinheiro=string, enums, Timestamp, Struct). Puxe ao criar/evoluir um contrato.
alwaysApply: false
---

# ADR-0009 — Contratos reais a partir do data-rudder-provider

## Status
Aceito (jun/2026). Descarta o `transaction.proto` mock (seed).

## Contexto
O `proto/transaction.proto` original era um **mock seed** (PredictiveAnalyzer + Transaction simplória),
sem relação com um domínio real. Decidimos apontar para uma situação real usando o projeto
`data-rudder-provider` (provider HTTP de antifraude) como fonte.

## Decisão
Espelhar as **rotas de criação** do `data-rudder-provider`:
- `POST /api/v1/fraud/transactions/` (`CreateTransactionReq`) → `protobuf.transaction.v1.Transaction`.
- `POST /api/v1/fraud/onboarding/` (`CreateOnboardingReq`) → `protobuf.onboarding.v1.Onboarding`.

**Escopo**: só o **evento de criação** (não update/status/batch). **Convenções de mapeamento**
(idiomáticas, mesmo divergindo do Go-fonte):
- **Dinheiro = string decimal** (nunca float, embora o source use `float64`) — `amount_total`, `unit_price`, etc.
- **Enums** proto com `_UNSPECIFIED = 0` para valores fixos (`pix_key_type`), em vez de string livre.
- **`google.protobuf.Timestamp`** para instantes; string para datas soltas/parciais (`birthdate`, validade de cartão).
- **`google.protobuf.Struct`** para `extra_data` (payload livre).
- **Tipos compartilhados** (`Address`, `RegistrationData`, `BankingData`, `PixKeyType`) em `protobuf.type.v1`.
- Validação portada via protovalidate (ver ADR-0008).

## Consequências
- O mock foi removido — **breaking intencional** (sem consumidores reais ainda; `buf breaking` acusa no PR).
- `Transaction` passou a ser **índice 0** no seu arquivo (corrige a antiga lição "Transaction é índice 1").
- Novos contratos/rotas seguem o mesmo mapeamento e o layout versionado (ADR-0007).
