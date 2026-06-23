---
name: novo-contrato
description: Criar um contrato NOVO (novo .proto / mensagem / tópico) de ponta a ponta — spec+AC, compatibilidade, regenerar os SDKs, registrar o schema e fechar ADR/STATE. Para evoluir um contrato que já existe, use /adicionar-campo.
---

# /novo-contrato — criar um contrato novo

Leva um **contrato novo** (novo `.proto`, mensagem ou tópico) do `.proto` até os SDKs regenerados e o
schema registrável no Schema Registry, respeitando os invariantes do projeto. *(Só adicionar um campo
a um contrato existente? Use `/adicionar-campo`.)*

## Invariantes (não-negociáveis)
- **O `.proto` é tocado só pela geração.** `gen/` e `sdk/**/generated/` são **gerados** — nunca editar à mão.
- **Compatibilidade BACKWARD:** campo novo em `snake_case` com número novo; **nunca** remover/renumerar campo existente.
- **A geração (`buf generate`) roda no CI/Linux (container)** — não no host Windows.
- **O SDK só resolve** o `schema_id` no SR; quem **registra** é o registrador (passo 5).
- Segurança é aplicada **no consumidor**; o `msg_index` sai do **descriptor** (genérico, sem código por-contrato).
- **Nunca dar push.** O merge é decisão humana.

## Passos

### 1. Spec
Abra `specs/NNNN-<nome>/spec.md` com os **AC** (Given/When/Then) do contrato novo, incluindo um
**AC de compatibilidade** (consumidor sem o tipo o ignora). Novo tópico / bounded context →
`design.md` aprovado **antes** de codar; decisão difícil de reverter → **ADR**.

> Só **adicionar um campo** a um contrato que já existe? Não é aqui — use **`/adicionar-campo`**.

### 2. Editar o `.proto`
Em `proto/`. Campo novo: `snake_case`, número inédito, nunca reusar/remover. Termo de domínio novo →
adicione a `docs/glossary.md` no mesmo PR.

### 3. Gate de compatibilidade
```bash
buf lint
buf breaking --against '.git#branch=main'
```
Quebrou BACKWARD → **PARE**. Só prossiga se a quebra for intencional, registrada em **ADR** e com o
bump de versão consciente (ver `docs/versioning-policy.md`).

### 4. Regenerar (no container/CI — nunca à mão)
`buf generate` regenera `gen/` + `sdk/{go,node,python}` e reembute o `descriptor-set` do Node.
Confirme o **Strategy A**: `gen/` == `sdk/` byte-a-byte. No host Windows não roda — rode no container/CI
(é o `generate.yml` que regenera; nunca edite gerado à mão).

### 5. Registrar o schema (se tópico novo)
Adicione `{ "topic": "<t>", "proto": "<arquivo>.proto" }` em `scripts/schemas.json` e rode:
```bash
SCHEMA_REGISTRY_URL=http://localhost:8081 python scripts/register_schemas.py
```
Confirme o `schema_id` atribuído. O registrador é o **único** que escreve no SR.

### 6. Bind + interop
Tipo novo → adicione aos CLIs de interop e à matriz cross-language; rode `/interop`
(sobe Kafka+SR, registra, roda o orquestrador Go↔Node↔Python).

### 7. Verificar
- Testes dos SDKs verdes (`go test ./...` · `npm test` · `pytest`).
- Interop verde (9/9).
- `node scripts/audit-esteira.mjs .` e `node scripts/eval-spec-fidelity.mjs .` verdes — cada `AC-N`
  coberto por task; idealmente marque os testes com o token `AC-N`.

### 8. Fechar
- Decisão durável → **ADR** em `docs/architecture/adr/` (imutável; nunca edite ADR antigo).
- Atualize `docs/STATE.md` (o que foi feito, próximo passo, bloqueios).
- A `spec.md` reflete o construído — ou registre `// SPEC_DEVIATION: <motivo>` e resolva.
- Abra o **PR**. **Não** dê push/merge.

## Definition of Done
- [ ] Contrato BACKWARD-compatível (`buf breaking` ok) — ou ADR + bump consciente.
- [ ] `gen/` e `sdk/` regenerados pelo buf (não à mão) e em sincronia (Strategy A).
- [ ] Schema registrável no SR; `scripts/schemas.json` atualizado se tópico novo.
- [ ] AC cobertos por task/teste; `audit` + `eval` verdes.
- [ ] ADR / glossário / `STATE.md` atualizados; spec reflete o construído.
- [ ] PR aberto, **sem push**.
