---
name: glossary
description: Linguagem ubíqua do protobuf-contracts. Puxe quando precisar do significado exato de um termo do domínio (serde, descriptor, subject, message-index…).
alwaysApply: false
---

# Glossário

Use **exatamente** estes termos no código, na spec e na conversa. Termo novo → adicione aqui no mesmo PR.

- **Contrato** — uma mensagem definida em `proto/*.proto`; a fonte única da estrutura de dados.
- **serde** — *serializer + deserializer*: transforma objeto ↔ bytes, embrulhando/desembrulhando o envelope Confluent. Entregue pronto pelo SDK (`produce`/`consume`).
- **descriptor** — descrição da estrutura da mensagem (campos, tipos, ordem de declaração) embutida no código gerado; é dela que o SDK tira o `message-index` sem ler `.proto` em runtime.
- **SDK** — biblioteca por linguagem gerada do `.proto` (`sdk/{go,node,python}`): tipos + serde + descriptor.
- **envelope (Confluent)** — o formato de wire: `[0x00 magic][schema_id BE4][message-index][payload proto3]`.
- **schema_id** — identificador que o Schema Registry atribui a um schema registrado; viaja no envelope.
- **subject** — chave do schema no SR. Aqui: `<topic>-value` (**TopicNameStrategy**).
- **message-index** — índice da mensagem no `.proto` (ordem de declaração); **variável**, derivado do descriptor. `0x00` é a otimização do índice 0.
- **registrador** — `scripts/register_schemas.py`; o **único** que escreve schema no SR (lê o `.proto` real). O SDK só resolve.
- **BACKWARD** — regra de compatibilidade: o schema novo consegue ler dados do antigo. Campo só se **adiciona** (nunca remove/renumera).
- **DLQ** — *dead-letter queue*: para onde o consumidor roteia uma mensagem rejeitada (erro tipado).
- **frame** — os bytes do envelope (header + payload).
- **Strategy A** — `buf generate` emite `gen/<lang>` (registro canônico) e `sdk/<lang>` (publicável) **idênticos** no código gerado.
