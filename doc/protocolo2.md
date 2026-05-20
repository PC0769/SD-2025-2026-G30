# TP2 - Serviços de Análise e Monitorização Urbana para One Health
**Sistemas Distribuídos 2025/2026**
**Docentes:** Hugo Paredes | Tiago Pinto | Cristiano Pendão
**Instituição:** UTAD | ECT | DE

---

## 1. Contexto e Objetivos
[cite_start]O sistema simula a infraestrutura de monitorização ambiental urbana composta por sensores, gateways e servidores, usando comunicação baseada em sockets e protocolos definidos manualmente. [cite: 17, 18]

[cite_start]Pretende-se agora dar continuidade ao sistema desenvolvido, evoluindo-o para uma arquitetura distribuída mais robusta, mantendo as funcionalidades anteriores e introduzindo novos mecanismos de comunicação e processamento. [cite: 19] [cite_start]Esta nova fase deverá contemplar: [cite: 20]
* [cite_start]A utilização de RPC (Remote Procedure Call) para ligação entre componentes. [cite: 21]
* [cite_start]A substituição da comunicação direta entre Sensores e Gateways por um sistema Publish/Subscribe. [cite: 23]

---

## 2. Requisitos Tecnológicos e Componentes

### 2.1. Comunicação RPC
[cite_start]Devem ser implementados procedimentos remotos para processamento em dois pontos críticos: [cite: 57, 58]

* [cite_start]**Gateway $\rightarrow$ Serviço de Pré-processamento:** O Gateway utiliza RPC para invocar um serviço externo que assegura a uniformização dos dados (ex: conversão de escalas, tratamento de formatos JSON/XML/CSV) antes da agregação. [cite: 59, 60]
* [cite_start]**Servidor $\rightarrow$ Serviço de Análise e Previsão:** O Servidor invoca, via RPC, um serviço especializado para realizar análises estatísticas, deteção de padrões de poluição ou previsão de riscos para a saúde pública. [cite: 61]

### 2.2. Comunicação baseada em Publicação/Subscrição (Pub/Sub)
* [cite_start]A ligação entre os Sensores e os Gateways deve utilizar um sistema de publicação/subscrição (Pub/Sub), baseado em **RabbitMQ**. [cite: 71]
* [cite_start]Cada **SENSOR** publica os dados recolhidos (temperatura, humidade, PM2.5, etc.) em tópicos específicos. [cite: 72] [cite_start]Os tópicos podem ser organizados por tipo de dado ou por zona. [cite: 73]
* [cite_start]Os **GATEWAYS** subscrevem os tópicos de interesse das zonas que gerem e/ou tipos de dados que devem gerir e encaminhar, permitindo uma receção de dados desacoplada e escalável. [cite: 74]

### 2.3. Funcionalidades Adicionais e Persistência
* [cite_start]O **Servidor** deverá armazenar os dados recebidos e os resultados das análises realizadas numa base de dados (relacional ou NoSQL), permitindo a persistência da informação para posterior consulta. [cite: 76]
* [cite_start]O sistema deverá disponibilizar uma **interface simples** (acessível via linha de comandos ou interface web), permitindo a visualização dos resultados das análises realizadas. [cite: 77]
* [cite_start]O pedido de novas análises pode também ser despoletado por esta interface, suportando a parametrização dos dados a analisar (ex: intervalo temporal, tipo de sensor, identificação do sensor). [cite: 78]

> [cite_start]**Fator de Valorização:** Além do processamento de dados em diferentes formatos, é considerado fator de valorização a inclusão de componentes do sistema implementados com recurso a diferentes tecnologias e linguagens de programação, e.g. na simulação dos dados recolhidos pelos sensores e/ou na implementação dos serviços RPC. [cite: 79, 80]

---

## 3. Planeamento e Faseamento do Trabalho
O trabalho deve ser desenvolvido de forma incremental por fases. [cite_start]São propostas as seguintes fases: [cite: 100, 101]

1. [cite_start]**Fase 1:** Implementação das chamadas RPC para pré-processamento e análise de dados. [cite: 102]
2. [cite_start]**Fase 2:** Implementação da comunicação Pub-Sub entre Sensores e Gateways. [cite: 103]
3. [cite_start]**Fase 3:** Desenvolvimento das funcionalidades adicionais: BD, interface de visualização. [cite: 104]

---

## 4. Entregáveis e Regras de Avaliação

### 4.1. Relatório Técnico
[cite_start]O trabalho deve ser acompanhado de um relatório técnico, com um **máximo de 4 páginas** (excluindo anexos), descrevendo as opções de implementação e os principais componentes desenvolvidos. [cite: 81, 82] [cite_start]Deve incluir: [cite: 83]

* [cite_start]**Protocolo de comunicação:** Descrição do protocolo utilizado entre os vários componentes (Sensores, Gateways, Servidor e serviços RPC), incluindo a definição das chamadas remotas (RPC), mensagens de publicação/subscrição (Pub-Sub), estrutura de dados, e gestão dos fluxos de informação. [cite: 84, 93]
* [cite_start]**Implementação:** Identificação e descrição das principais partes do código, incluindo a justificação das tecnologias e linguagens utilizadas, bem como de outras decisões relevantes. [cite: 94]
* **Anexo - Código Fonte:** Deve ser incluído o código desenvolvido devidamente comentado. [cite_start]**Nota:** Este anexo pode ser substituído pela indicação do repositório onde se encontra alojado o código (ex: GitLab ou GitHub), incluindo as *issues* criadas para o planeamento e execução das tarefas. [cite: 95, 96]

### 4.2. Prazos e Apresentação
* [cite_start]**Data Limite de Entrega:** Submissão através do Moodle até ao final do dia **29 de maio**. [cite: 98]
* [cite_start]**Defesa:** É obrigatória a apresentação do trabalho submetido na aula PL seguinte à data da entrega. [cite: 99]