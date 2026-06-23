---
name: nova-linguagem
description: Adicionar um SDK de uma linguagem nova (ex.: Rust, Java) conformando ao contrato "thin SDK". Use ao expandir o suporte multi-linguagem do projeto.
---

# /nova-linguagem — adicionar um SDK

Um SDK novo precisa implementar **o mesmo contrato** dos existentes (Go/Node/Python). É raro, mas
fácil de divergir — siga o contrato thin à risca, senão quebra a interoperabilidade.

## O contrato thin (o que o SDK precisa entregar)
- **Tipos gerados pelo buf** — adicione o plugin da linguagem no `buf.gen.yaml` (saída em `gen/<lang>` + `sdk/<lang>`).
- **Envelope Confluent:** `[0x00][schema_id BE4][message-index][payload proto3]`. O `message-index` é
  **derivado do descriptor** (variável; `0x00` só pro índice 0). Se o gerador não embute descriptor
  (como o ts-proto), embuta um `FileDescriptorSet`.
- **`bind(topic, Tipo)`** — resolve o `schema_id` no SR (`GET /subjects/<t>-value/versions/latest`). **Só lê, não registra.**
- **`produce(topic, msg)`** — carimba o envelope (id + msg-index). **`consume(topic, bytes)`** — valida
  magic + `schema_id` é versão do subject + msg-index + decode; erro **tipado** → DLQ.

## Passos
1. Plugin no `buf.gen.yaml` (`gen/<lang>` + `sdk/<lang>`); regenere via `buf generate` no CI/container.
2. Implemente o serde: framing variável + resolve + **consumidor estrito** (com os negativos de segurança).
3. Testes: unit (mock SR) + integração (SR real), espelhando os dos outros SDKs.
4. CLI de interop pra a linguagem + adicione à matriz; rode `/interop` (deve fechar **com** a nova língua).
5. CI: testes da linguagem no `buf-ci`; o job de interop cobre o round-trip.
6. ADR se a inclusão trouxe decisão durável; atualize `docs/STATE.md`. **PR sem push.**

## Definition of Done
- [ ] Tipos gerados pelo buf; serde implementa o wire format (msg-index do descriptor).
- [ ] `bind` resolve; consumidor estrito com negativos cobertos; DLQ tipado.
- [ ] interop **verde incluindo** a linguagem nova.
