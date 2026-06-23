---
name: adicionar-campo
description: Adicionar/alterar um campo num contrato .proto EXISTENTE, de forma compatível (BACKWARD). Pouca cerimônia. Use pra evoluir um contrato que já existe — não pra criar um novo (veja /novo-contrato).
---

# /adicionar-campo — evoluir um contrato existente (compatível)

Evolução leve de um `.proto` que **já existe**. Pra contrato/mensagem/tópico novo, use `/novo-contrato`.

## Invariantes
- Campo novo em `snake_case`, número **inédito**. **Nunca** remover/renumerar campo existente (quebra BACKWARD).
- `gen/` e `sdk/**/generated/` são **gerados** — nunca editar à mão; geração roda no **CI/container**.
- **Sem push** (merge é decisão humana).

## Passos
1. Edite o `.proto` em `proto/`: adicione o campo (`snake_case`, número novo). Termo de domínio novo → `docs/glossary.md`.
2. Compatibilidade: `buf lint` + `buf breaking --against '.git#branch=main'` — **deve passar** (é compatível).
   Se **quebrar**, não é "adicionar campo": pare e trate como decisão (ADR + bump, ver `docs/versioning-policy.md`).
3. Regenere via `buf generate` no **CI/container** (nunca à mão); confirme `gen/` == `sdk/` (Strategy A).
4. *(Opcional, recomendado)* `/interop` — confirma que os 3 SDKs continuam concordando.
5. Abra o **PR** (sem push). Uma linha em `docs/STATE.md` se valer registrar.

## Definition of Done
- [ ] Campo compatível — `buf breaking` verde.
- [ ] `gen/` + `sdk/` regenerados pelo buf, em sincronia; nada editado à mão.
- [ ] PR aberto, **sem push**.
