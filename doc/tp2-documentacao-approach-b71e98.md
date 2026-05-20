# TP2 — Abordagem de Documentação e Teoria

Ajuste ao plano de desenvolvimento: a documentação teórica e explicação do código é escrita num único ficheiro `DEFESA_TP2.md` no projeto, atualizado no fim de cada fase, em vez de comentários extensos nos ficheiros de código.

---

## O que muda

### Código (ficheiros .cs e .py)
- Comentários **mínimos e técnicos** — apenas o necessário para perceber o código no momento
- Sem blocos teóricos extensos dentro dos ficheiros de código
- Estilo normal de indústria: comentários curtos, nomes de função auto-descritivos

### Ficheiro DEFESA_TP2.md (criado e atualizado ao longo do projeto)
Localização: `c:\Users\User\Desktop\SD-2025-2026-G30\DEFESA_TP2.md`

Estrutura do ficheiro:

```
# DEFESA TP2 — Documentação Técnica e Teoria SD

## Fase 1 — RPC
### Teoria: O que é RPC
### O que implementámos
### PreProcessingService — o que faz e porquê
### AnalysisService — o que faz e porquê
### Integração no GatewayNode — como e porquê
### Integração no ServerNode — como e porquê
### Código relevante comentado (excertos)

## Fase 2 — Pub/Sub com RabbitMQ
### Teoria: O que é Pub/Sub e Message Broker
### ...

## Fase 3 — Base de Dados e Interface Web
### Teoria: Persistência em Sistemas Distribuídos
### ...

## Glossário de Conceitos SD
```

---

## Quando é atualizado

| Momento | Ação |
|---------|------|
| Fim da Etapa 1.1 + 1.2 | Adiciona secção Fase 1 RPC (teoria + PreProcessing + Gateway) |
| Fim da Etapa 1.3 + 1.4 | Completa secção Fase 1 (Analysis + Server) |
| Fim da Fase 2 | Adiciona secção Fase 2 Pub/Sub |
| Fim da Fase 3 | Adiciona secção Fase 3 BD + Interface |

---

## Conteúdo tipo de cada secção

Para cada componente desenvolvido, o `DEFESA_TP2.md` vai conter:

1. **Teoria SD relevante** — definição, conceito, onde se enquadra na literatura
2. **Decisão de design** — porquê escolhemos esta abordagem e não outra
3. **O que o código faz** — explicação funcional, sem ser o código em si
4. **Excertos de código anotados** — trechos-chave com anotação linha a linha
5. **Referência ao enunciado** — ligação explícita ao §2.1, §2.2, etc. do protocolo2.md
6. **Possíveis perguntas da defesa** — o que o júri pode perguntar e a resposta esperada

---

## Progresso

- [ ] Criar DEFESA_TP2.md com estrutura base (feito na Fase 1.1)
- [ ] Completar secção Fase 1 após Fase 1 concluída
- [ ] Completar secção Fase 2 após Fase 2 concluída
- [ ] Completar secção Fase 3 após Fase 3 concluída
