## Pipeline Híbrido para Análise da Alfabetização no Brasil


Tech Challenge – Fase 2 | Arquitetura de Dados (Batch + Streaming) utilizando Databricks e Google Cloud 

---

## 1. Contexto do Problema & Desafio Educacional

A não alfabetização infantil é uma das principais barreiras ao desenvolvimento educacional, social e econômico do Brasil. O **Compromisso Nacional Criança Alfabetizada** mobiliza União, estados e municípios para garantir que todas as crianças estejam alfabetizadas até o final do 2º ano do Ensino Fundamental, com meta nacional de universalização até 2030.

Para tornar esse objetivo mensurável, o INEP realizou em 2023 a **Pesquisa Alfabetiza Brasil**, que definiu que **743 pontos** na escala de proficiência do SAEB seria o corte a partir do qual uma criança é considerada alfabetizada. Esse parâmetro deu origem ao **Indicador Criança Alfabetizada**: o percentual de estudantes que atingem esse patamar.

Medir o indicador é o primeiro passo. O segundo, e o que este pipeline endereça diretamente, é permitir a **comparação entre meta e resultado**, por município, estado e ano, para identificar:

- **Onde** as metas estão sendo atingidas e onde não estão
- **Quais fatores regionais** (rede de ensino, série) se associam a sucesso ou fracasso na alfabetização
- **Como a tendência evolui no tempo**, para antecipar risco de não cumprimento da meta de 2030.
- **E dessa forma, poder criar planos para atingimento das metas em todos os niveis**
  
Este projeto constrói a infraestrutura de dados que torna essas perguntas respondíveis de forma confiável, escalável e a baixo custo.

---

## 2. Arquitetura da Solução

A solução integra duas nuvens: **Google Cloud Platform** (fonte de dados, via BigQuery/Base dos Dados) e **Databricks** (processamento, armazenamento analítico e orquestração), seguindo a **Arquitetura Medalhão** (Bronze > Silver > Gold) com ingestão híbrida **batch + streaming**.

<img width="2400" height="1725" alt="arquitetura_pipeline" src="https://github.com/user-attachments/assets/aecd4602-f887-41f0-a713-c579417e145f" />


**Evidência de execução real**: o Job orquestrado rodando de ponta a ponta no Databricks Workflows, com as 4 tasks sendo concluídas com sucesso:

<img width="1436" height="880" alt="job_run_databricks" src="https://github.com/user-attachments/assets/1ee8792e-472a-4094-bc8a-db986a08c9f2" />




**Escolha das Ferramentas Cloud:** A Base dos Dados publica seus datasets exclusivamente no BigQuery e não faria sentido replicar essa infraestrutura. O Google Cloud aqui atua só como **uma ponte para fonte de dados fiel à original**; todo o processamento, curadoria, orquestração e disponibilização analítica acontece no Databricks, que foi escolhido por implementar arquitetura Medalhão de forma facil e eficaz e por rodar Spark de forma gerenciada, sem a necessidade de operar cluster Hadoop manualmente.

---

## 3. Fluxo de Dados

**Batch:**
1. Um projeto GCP próprio (`crianca-alfabetizada-pipeline`) foi criado como **projeto de billing**, com uma Service Account (`databricks-federation`) autorizada a ler o projeto público `basedosdados` (roles `BigQuery User` + `BigQuery Data Viewer`).
2. Dentro do Databricks, o **Spark BigQuery Connector** (`spark.read.format("bigquery")`) lê cada uma das 8 tabelas de origem diretamente, usando a credencial da Service Account, e grava como tabela Delta na camada **Bronze**.
3. A camada **Silver** codifica colunas de código (utilizando a tabela **dicionario** como base), e joining com nomes de UF/Município (pelos diretórios), e valida qualidade (duplicidade, nulos, chaves órfãs).
4. A camada **Gold** consolida os dados tratados em 3 tabelas analíticas prontas para consumo.

**Streaming (simulado):**
1. Um notebook simulador gera eventos JSON representando novas medições do indicador, gravando-os em uma **Unity Catalog Volume** (landing zone). Importante clarificar, que foi utilizado um simulador para teste de que o pipeline inteiro rodaria sem problemas, caso fosse um caso real, o set up teria que ser diferente. 
2. O **Auto Loader** (Spark Structured Streaming) lê esses arquivos incrementalmente (só os novos, sem reprocessar os arquivos ja lidos) e grava na Bronze em modo **append**.
3. O mesmo pipeline de codificação/enriquecimento da Silver batch é aplicado ao streaming.
4. Na Gold, os dados de streaming são unidos pela **UNION** aos dados batch na tabela de indicador por município.

**Orquestração (Workflows/Jobs):**
1. Todo o fluxo acima é encapsulado em um **Job do Databricks Workflows**, com 4 tasks organizadas em DAG: **ingestao_bronze** e **streaming_indicador** rodam em paralelo (ja que sao fontes independentes); **transformacao_silver** depende de ambas para comecar a rodar e a  **transformacao_gold** depende da Silver acontecer sem erros.
2. Cada task tem **retries automáticos** (2 tentativas, 15 min de intervalo) e **notificação por e-mail** em caso de sucesso ou falha.
3. O Job roda em um **schedule recorrente a cada 2 semanas**.
4. Cada execução registra **linhas processadas e tempo de execução** em uma tabela de log (**log_execucoes**), consultável para auditoria e análise de tendência operacional.

---

## 4. Tecnologias Utilizadas

| Tecnologia | Papel no pipeline | Justificativa |
|---|---|---|
| **Google BigQuery** | Fonte de dados (Base dos Dados) | Única plataforma onde a Base dos Dados publica os datasets educacionais |
| **Google Cloud IAM (Service Account)** | Autenticação segura Databricks ↔ BigQuery | Acesso de leitura (least privilege: só `User` + `Data Viewer`), sem expor credenciais de usuário |
| **Databricks Free Edition** | Processamento, armazenamento (Delta Lake), orquestração | Serverless (zero gestão de infraestrutura), gratuito, com Unity Catalog, Workflows e Structured Streaming nativos |
| **Spark BigQuery Connector** | Leitura das tabelas de origem | Nativo do runtime Databricks; leitura tabela-a-tabela evita o limite de quota de schemas do Unity Catalog  |
| **Delta Lake** | Formato de armazenamento das 3 camadas | ACID transactions, schema enforcement, time travel — resolve grande parte do requisito de qualidade de dados "de graça" |
| **Unity Catalog** | Governança (catálogo por camada) | Isolamento de permissões entre Bronze/Silver/Gold/Monitoramento |
| **Auto Loader (Structured Streaming)** | Ingestão streaming simulada | Processamento incremental nativo, sem necessidade de infraestrutura de mensageria externa |
| **Databricks Workflows** | Orquestração, retries, notificações, schedule | Nativo da plataforma, sem custo de ferramenta externa |

---

## 5. Decisões de Arquitetura e Trade-offs

| Decisão | Alternativa considerada | Motivo da escolha |
|---|---|---|
| **Databricks (Spark gerenciado) em vez de Hadoop/EMR operado manualmente** | Cluster Hadoop/YARN próprio (ex: AWS EMR) | Menor esforço operacional ja  que a arquitetura Medalhão é nativa do ecossistema Delta Lake.Porem abrindo mão de controle direto sobre HDFS/YARN em favor de produtividade |
| **Leitura tabela-a-tabela via Spark BigQuery Connector, não Lakehouse Federation (foreign catalog)** | Federar o projeto `basedosdados` inteiro como catálogo espelhado | O projeto `basedosdados` tem 186 datasets e espelhar todos como schemas excedeu a quota de 50 schemas do Free Edition. A leitura somente dos datasets escolhidos evita esse limite (`UC_RESOURCE_QUOTA_EXCEEDED`) |
| **Bronze sem joins com toda integração na Silver** | Já aplicar os `LEFT JOIN` (como nos SQLs originais da Base dos Dados) direto na ingestão | Preserva os dado brutos na Bronze (que facilita auditoria, e caso algo errado acontece em outras camadas) e centraliza toda lógica de transformação em um único lugar (Silver), facilitando manutenção |
| **Colunas codificadas mantêm código bruto + descrição (`rede_codigo` + `rede`), por exemplo** | Sobrescrever o código pela descrição, como no SQL original da fonte | Aumenta a Rastreabilidade: permite auditar a codificação a qualquer momento, sem depender de recontatar a fonte e facilita processamento futuros para aplicacao em AI, com base em numeros e nao textos |
| **Gold Table 2 (metas vs. resultado) filtrada para `rede = "Municipal"` e `ano >= 2024`** | Comparar todas as redes e anos disponíveis | Metas só existem para a rede Municipal a partir de 2024 (confirmado com testes dentro das camada silver: 78% de nulos fora desse filtro, caindo a ~4% dentro dele) |
| **Gold Table 3 (evolução temporal) na granularidade UF, não por aluno** | Agregar direto de **alunos** (3,87M linhas) | Performance e custo: a granularidade UF é suficiente para visualizar tendência nacional, sem processar volume desnecessário, tambem imaginando o futuro onde os dados porem ser muito maiores |
| **Streaming com `trigger(availableNow=True)`, não streaming contínuo** (always-on) | Cluster streaming rodando 24/7 aguardando novos eventos | FinOps: processa o disponível e encerra, em vez de manter compute alocado esperando; mais barato em ambiente serverless com quota ja que a meta nao tende a necessitar mudancas e atualizacoes frequentes |
| **Cadência do Job a cada 2 semanas (batch e streaming juntos)** | Jobs separados com frequências distintas (batch quinzenal, streaming a cada poucas horas) | Simplicidade operacional nesta fase do projeto; a fonte real (avaliação INEP/SAEB) nao é atualizada com alta frequência, então a cadência batch é apropriada. Evolução natural seria separar as cadências para reforçar a narrativa de streaming "quase tempo real" |
| **Catálogo por camada (`_bronze`, `_silver`, `_gold`, `_monitoramento`)**, não schemas dentro de um catálogo único | Um catálogo único com schemas por camada | Isolamento de governança: permite políticas de acesso diferentes por camada (ex: só o pipeline escreve na Bronze; analistas só leem a Gold) |
| **`nivel_alfabetizacao` (escala 0–5) mantido sem codificação de faixas** | Inferir e documentar limites de corte para cada nível | Validado no codigo que é uma escala ordinal com correlação à taxa de alfabetização (0 = 30,8% médio → 5 = 88,6% médio), mas a fonte não documenta os cortes exatos. Fabricar limites seria dado inventado entao optou-se pela transparência porem com limitação |

---

## 6. Qualidade de Dados

| Verificação | Resultado |
|---|---|
| Duplicidade — todas as 8 tabelas Silver | Chave primária única confirmada em cada uma (ex: `alunos` → `id_aluno + ano`, não `id_aluno` apenas) |
| Chaves órfãs — joins geográfico (UF, Município) | 0 nulos em `sigla_uf_nome` e `id_municipio_nome` nas tabelas que dependem desse join |
| Nulos investigados — Gold Table 2 (metas vs. resultado) | 3,96% de nulos remanescentes mesmo após filtro correto, decompostos em: 96 casos de chave ausente (município sem meta definida) + 120 casos de meta nula na própria fonte (lacuna do dado bruto do INEP). Importante mencionar que esses dados foram alterando durante a fase de teste do streaming afetrando numeros absolutos, porem nao afetando o numero de nulos |
| Contaminação cruzada streaming → batch | Confirmado que 0 dos nulos da Gold Table 2 vêm de dados de streaming — 100% originários do batch |

---

## 7. Monitoramento e FinOps

**Monitoramento:**
- **Falhas de ingestão**: Job com retries automáticos (2x, 15 min de intervalo) por task.
- **Alertas de erro**: notificação por e-mail em sucesso e falha, testada em cenário real (uma falha de path de credencial foi capturada e notificada corretamente durante o desenvolvimento).
- **Volume processado e latência**: tabela `bd_alfabetizacao_monitoramento.default.log_execucoes` registra, por execução, camada, tabela, linhas processadas, tempo de execução e status sendo consultáveis para análise de tendência de performance ao longo do tempo.
- **Latência de streaming**: métricas nativas do Structured Streaming (`query.recentProgress`) capturam linhas processadas por micro-batch.

<img width="1436" height="880" alt="Screenshot 2026-07-12 at 11 37 37 AM" src="https://github.com/user-attachments/assets/ae416ae4-a0b5-441b-aab3-217a935483f6" />


<img width="1436" height="880" alt="Screenshot 2026-07-12 at 11 38 05 AM" src="https://github.com/user-attachments/assets/2cb25727-b044-4bc8-8650-bc9f075d7d4e" />


**FinOps:**
- **Databricks Free Edition (serverless)**: sem custo de infraestrutura ociosa — compute é alocado sob demanda por execução.
- **`trigger(availableNow=True)`** em vez de streaming contínuo: evita manter compute reservado 24/7 esperando eventos.
- **Cadência de execução (2 semanas)** alinhada à frequência real de atualização da fonte (avaliação INEP), evitando processamento redundante.
- **Granularidade UF** (não aluno) na tabela de evolução temporal: reduz volume processado na camada Gold sem perda de valor analítico para o caso de uso.
- **Formato Delta/Parquet** em todas as camadas: compressão e leitura colunar eficientes.

---

## 8. Aplicação em IA

A camada Gold foi desenhada para alimentar diretamente iniciativas de IA e análise avançada:

- **`indicador_alfabetizacao_municipio`**: base para modelos preditivos de risco de não-alfabetização por município, usando features como rede de ensino, série e histórico de proficiência.
- **`metas_vs_resultado_municipio`**: a coluna `gap_meta` permite treinar modelos de classificação/regressão para identificar municípios com maior risco de não atingir a meta de 2030, priorizando intervenção de política pública.
- **`evolucao_temporal_indicador`**: séries temporais por UF podem alimentar modelos de forecasting para projetar trajetória até 2030 por estado.
- **`nivel_alfabetizacao`** (escala 0–5): apesar de não ter cortes documentados, sua correlação monotônica com a taxa de alfabetização o torna um candidato a feature ordinal em modelos de clusterização de vulnerabilidade educacional.

---

## 10. Vídeo Executivo

<p align="center">
  <a href="httpsbe/Y9Z_vlcWKUs
    <img src="https://img.youtube.com/vi/Y9Z_vlcWKUs/ult.jpg
  </a>
</p>

Apresentação executiva (até 5 min) cobrindo problema de negócio, arquitetura da solução, valor da pipeline e potencial de aplicação em IA.


