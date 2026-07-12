<img width="1600" height="1150" alt="arquitetura_pipeline" src="https://github.com/user-attachments/assets/a589184b-ec07-40c5-a8e8-50b4d113080a" /># Pipeline Híbrido para Análise da Alfabetização no Brasil

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

![Uploading arqui<svg viewBox="0 0 1600 1150" xmlns="http://www.w3.org/2000/svg" font-family="Arial, Helvetica, sans-serif">
  <defs>
    <marker id="arrow" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto" markerUnits="strokeWidth">
      <path d="M0,0 L0,6 L9,3 z" fill="#FF0066"/>
    </marker>
  </defs>

  <rect x="0" y="0" width="1600" height="1150" fill="#FFFFFF"/>

  <!-- Title -->
  <text x="800" y="50" text-anchor="middle" font-size="30" font-weight="bold" fill="#0D0D0D">
    Arquitetura da Solução — Pipeline Híbrido de Alfabetização
  </text>

  <!-- ===================== GOOGLE CLOUD PLATFORM ===================== -->
  <rect x="60" y="100" width="380" height="230" rx="14" fill="#0D0D0D" stroke="#FF0066" stroke-width="3"/>
  <text x="250" y="135" text-anchor="middle" font-size="20" font-weight="bold" fill="#FF0066">GOOGLE CLOUD PLATFORM</text>

  <rect x="90" y="160" width="320" height="70" rx="10" fill="#FFFFFF" stroke="#FF0066" stroke-width="2"/>
  <text x="250" y="188" text-anchor="middle" font-size="17" font-weight="bold" fill="#0D0D0D">BigQuery</text>
  <text x="250" y="212" text-anchor="middle" font-size="14" fill="#333333">Base dos Dados (basedosdados)</text>

  <rect x="90" y="250" width="320" height="60" rx="10" fill="#FFFFFF" stroke="#FF0066" stroke-width="2"/>
  <text x="250" y="278" text-anchor="middle" font-size="16" font-weight="bold" fill="#0D0D0D">Service Account</text>
  <text x="250" y="298" text-anchor="middle" font-size="13" fill="#333333">BigQuery User + Data Viewer</text>

  <!-- Arrow GCP -> Bronze -->
  <line x1="440" y1="215" x2="560" y2="215" stroke="#FF0066" stroke-width="3" marker-end="url(#arrow)"/>
  <text x="500" y="200" text-anchor="middle" font-size="13" fill="#0D0D0D" font-style="italic">Spark BigQuery</text>
  <text x="500" y="235" text-anchor="middle" font-size="13" fill="#0D0D0D" font-style="italic">Connector</text>

  <!-- ===================== DATABRICKS CONTAINER ===================== -->
  <rect x="560" y="100" width="980" height="380" rx="14" fill="#FFFFFF" stroke="#0D0D0D" stroke-width="3"/>
  <text x="1050" y="135" text-anchor="middle" font-size="20" font-weight="bold" fill="#0D0D0D">DATABRICKS FREE EDITION</text>

  <!-- Bronze -->
  <rect x="590" y="160" width="280" height="90" rx="10" fill="#0D0D0D" stroke="#FF0066" stroke-width="2"/>
  <text x="730" y="192" text-anchor="middle" font-size="18" font-weight="bold" fill="#FF0066">🥉 BRONZE</text>
  <text x="730" y="215" text-anchor="middle" font-size="13" fill="#FFFFFF">8 tabelas batch</text>
  <text x="730" y="235" text-anchor="middle" font-size="13" fill="#FFFFFF">+ indicador_streaming</text>

  <!-- arrow bronze -> silver -->
  <line x1="870" y1="205" x2="960" y2="205" stroke="#FF0066" stroke-width="3" marker-end="url(#arrow)"/>

  <!-- Silver -->
  <rect x="960" y="160" width="280" height="90" rx="10" fill="#0D0D0D" stroke="#FF0066" stroke-width="2"/>
  <text x="1100" y="192" text-anchor="middle" font-size="18" font-weight="bold" fill="#FF0066">🥈 SILVER</text>
  <text x="1100" y="215" text-anchor="middle" font-size="13" fill="#FFFFFF">Decodificação +</text>
  <text x="1100" y="235" text-anchor="middle" font-size="13" fill="#FFFFFF">Enriquecimento + Qualidade</text>

  <!-- arrow silver -> gold -->
  <line x1="1240" y1="205" x2="1300" y2="205" stroke="#FF0066" stroke-width="3" marker-end="url(#arrow)"/>

  <!-- Gold -->
  <rect x="1300" y="160" width="220" height="90" rx="10" fill="#0D0D0D" stroke="#FF0066" stroke-width="2"/>
  <text x="1410" y="192" text-anchor="middle" font-size="18" font-weight="bold" fill="#FF0066">🥇 GOLD</text>
  <text x="1410" y="215" text-anchor="middle" font-size="13" fill="#FFFFFF">3 tabelas</text>
  <text x="1410" y="235" text-anchor="middle" font-size="13" fill="#FFFFFF">analíticas</text>

  <!-- ===================== STREAMING (below, inside Databricks box) ===================== -->
  <rect x="590" y="290" width="180" height="70" rx="10" fill="#FFFFFF" stroke="#0D0D0D" stroke-width="2"/>
  <text x="680" y="320" text-anchor="middle" font-size="14" font-weight="bold" fill="#0D0D0D">Simulador de</text>
  <text x="680" y="340" text-anchor="middle" font-size="14" font-weight="bold" fill="#0D0D0D">Eventos</text>

  <line x1="770" y1="325" x2="830" y2="325" stroke="#FF0066" stroke-width="3" marker-end="url(#arrow)"/>

  <rect x="830" y="290" width="180" height="70" rx="10" fill="#FFFFFF" stroke="#0D0D0D" stroke-width="2"/>
  <text x="920" y="320" text-anchor="middle" font-size="14" font-weight="bold" fill="#0D0D0D">UC Volume</text>
  <text x="920" y="340" text-anchor="middle" font-size="14" font-weight="bold" fill="#0D0D0D">(landing zone)</text>

  <line x1="1010" y1="325" x2="1070" y2="325" stroke="#FF0066" stroke-width="3" marker-end="url(#arrow)"/>

  <rect x="1070" y="290" width="180" height="70" rx="10" fill="#FFFFFF" stroke="#0D0D0D" stroke-width="2"/>
  <text x="1160" y="320" text-anchor="middle" font-size="14" font-weight="bold" fill="#0D0D0D">Auto Loader</text>
  <text x="1160" y="340" text-anchor="middle" font-size="14" font-weight="bold" fill="#0D0D0D">(Structured Streaming)</text>

  <!-- arrow autoloader up to bronze -->
  <polyline points="1160,290 1160,252 870,252" fill="none" stroke="#FF0066" stroke-width="3" marker-end="url(#arrow)"/>

  <!-- ===================== ORCHESTRATION (Job) ===================== -->
  <rect x="560" y="530" width="980" height="260" rx="14" fill="#0D0D0D" stroke="#FF0066" stroke-width="3"/>
  <text x="1050" y="565" text-anchor="middle" font-size="20" font-weight="bold" fill="#FF0066">
    ⚙️ DATABRICKS WORKFLOWS — JOB ORQUESTRADO
  </text>

  <!-- Task boxes -->
  <rect x="600" y="600" width="220" height="70" rx="10" fill="#FFFFFF" stroke="#FF0066" stroke-width="2"/>
  <text x="710" y="640" text-anchor="middle" font-size="15" font-weight="bold" fill="#0D0D0D">ingestao_bronze</text>

  <rect x="600" y="690" width="220" height="70" rx="10" fill="#FFFFFF" stroke="#FF0066" stroke-width="2"/>
  <text x="710" y="720" text-anchor="middle" font-size="14" font-weight="bold" fill="#0D0D0D">streaming</text>
  <text x="710" y="740" text-anchor="middle" font-size="14" font-weight="bold" fill="#0D0D0D">_indicador</text>

  <line x1="820" y1="635" x2="895" y2="670" stroke="#FF0066" stroke-width="3" marker-end="url(#arrow)"/>
  <line x1="820" y1="725" x2="895" y2="680" stroke="#FF0066" stroke-width="3" marker-end="url(#arrow)"/>

  <rect x="900" y="635" width="230" height="70" rx="10" fill="#FFFFFF" stroke="#FF0066" stroke-width="2"/>
  <text x="1015" y="665" text-anchor="middle" font-size="14" font-weight="bold" fill="#0D0D0D">transformacao</text>
  <text x="1015" y="685" text-anchor="middle" font-size="14" font-weight="bold" fill="#0D0D0D">_silver</text>

  <line x1="1130" y1="670" x2="1200" y2="670" stroke="#FF0066" stroke-width="3" marker-end="url(#arrow)"/>

  <rect x="1200" y="635" width="230" height="70" rx="10" fill="#FFFFFF" stroke="#FF0066" stroke-width="2"/>
  <text x="1315" y="665" text-anchor="middle" font-size="14" font-weight="bold" fill="#0D0D0D">transformacao</text>
  <text x="1315" y="685" text-anchor="middle" font-size="14" font-weight="bold" fill="#0D0D0D">_gold</text>

  <text x="1050" y="775" text-anchor="middle" font-size="14" fill="#FFFFFF" font-style="italic">
    Retries: 2x (10min) · Schedule: a cada 2 semanas
  </text>

  <!-- Arrow job -> notification/log -->
  <line x1="1050" y1="790" x2="1050" y2="840" stroke="#FF0066" stroke-width="3" marker-end="url(#arrow)"/>

  <!-- Notification + Log -->
  <rect x="770" y="850" width="260" height="80" rx="10" fill="#FFFFFF" stroke="#0D0D0D" stroke-width="2"/>
  <text x="900" y="885" text-anchor="middle" font-size="16" font-weight="bold" fill="#0D0D0D">📧 Notificação e-mail</text>
  <text x="900" y="910" text-anchor="middle" font-size="13" fill="#333333">sucesso e falha</text>

  <rect x="1070" y="850" width="260" height="80" rx="10" fill="#FFFFFF" stroke="#0D0D0D" stroke-width="2"/>
  <text x="1200" y="885" text-anchor="middle" font-size="16" font-weight="bold" fill="#0D0D0D">📊 log_execucoes</text>
  <text x="1200" y="910" text-anchor="middle" font-size="13" fill="#333333">volume + latência (Delta)</text>

  <!-- Arrow databricks -> orchestration connecting label -->
  <line x1="1050" y1="480" x2="1050" y2="525" stroke="#0D0D0D" stroke-width="2" stroke-dasharray="6,4"/>

  <!-- Legend -->
  <rect x="60" y="1000" width="20" height="20" fill="#0D0D0D" stroke="#FF0066" stroke-width="2"/>
  <text x="90" y="1016" font-size="14" fill="#0D0D0D">Camada / Etapa de processamento</text>

  <rect x="60" y="1035" width="20" height="20" fill="#FFFFFF" stroke="#0D0D0D" stroke-width="2"/>
  <text x="90" y="1051" font-size="14" fill="#0D0D0D">Componente / Task individual</text>

  <line x1="60" y1="1080" x2="90" y2="1080" stroke="#FF0066" stroke-width="3" marker-end="url(#arrow)"/>
  <text x="100" y="1085" font-size="14" fill="#0D0D0D">Fluxo de dados</text>
</svg>
tetura_pipeline.svg…]()


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
1. Um notebook simulador gera eventos JSON representando novas medições do indicador, gravando-os em uma **Unity Catalog Volume** (landing zone).
2. O **Auto Loader** (Spark Structured Streaming) lê esses arquivos incrementalmente — só os novos, sem reprocessar — e grava na Bronze em modo `append`.
3. O mesmo pipeline de decodificação/enriquecimento da Silver batch é aplicado ao streaming.
4. Na Gold, os dados de streaming são unidos (`UNION`) aos dados batch na tabela de indicador por município.

**Orquestração (Workflows/Jobs):**
1. Todo o fluxo acima é encapsulado em um **Job do Databricks Workflows**, com 4 tasks organizadas em DAG: `ingestao_bronze` e `streaming_indicador` rodam em paralelo (fontes independentes); `transformacao_silver` depende de ambas; `transformacao_gold` depende da Silver.
2. Cada task tem **retries automáticos** (2 tentativas, 10 min de intervalo) e **notificação por e-mail** em caso de sucesso ou falha.
3. O Job roda em um **schedule recorrente a cada 2 semanas**.
4. Cada execução registra **linhas processadas e tempo de execução** em uma tabela de log (`log_execucoes`), consultável para auditoria e análise de tendência operacional.

---

## 4. Tecnologias Utilizadas

| Tecnologia | Papel no pipeline | Justificativa |
|---|---|---|
| **Google BigQuery** | Fonte de dados (Base dos Dados) | Única plataforma onde a Base dos Dados publica os datasets educacionais |
| **Google Cloud IAM (Service Account)** | Autenticação segura Databricks ↔ BigQuery | Acesso de leitura escopado (least privilege: só `User` + `Data Viewer`), sem expor credenciais de usuário |
| **Databricks Free Edition** | Processamento, armazenamento (Delta Lake), orquestração | Serverless (zero gestão de infraestrutura), gratuito, com Unity Catalog, Workflows e Structured Streaming nativos |
| **Spark BigQuery Connector** | Leitura das tabelas de origem | Nativo do runtime Databricks; leitura tabela-a-tabela evita o limite de quota de schemas do Unity Catalog (ver seção 5) |
| **Delta Lake** | Formato de armazenamento das 3 camadas | ACID transactions, schema enforcement, time travel — resolve grande parte do requisito de qualidade de dados "de graça" |
| **Unity Catalog** | Governança (catálogo por camada) | Isolamento de permissões entre Bronze/Silver/Gold/Monitoramento |
| **Auto Loader (Structured Streaming)** | Ingestão streaming simulada | Processamento incremental nativo, sem necessidade de infraestrutura de mensageria externa |
| **Databricks Workflows** | Orquestração, retries, notificações, schedule | Nativo da plataforma, sem custo de ferramenta externa (ex: Airflow) |

---

## 5. Decisões Arquiteturais e Trade-offs

| Decisão | Alternativa considerada | Motivo da escolha |
|---|---|---|
| **Databricks (Spark gerenciado) em vez de Hadoop/EMR operado manualmente** | Cluster Hadoop/YARN próprio (ex: AWS EMR) | Menor esforço operacional; a arquitetura Medalhão é nativa do ecossistema Delta Lake. Trade-off consciente: abre mão de controle direto sobre HDFS/YARN em favor de produtividade |
| **Leitura tabela-a-tabela via Spark BigQuery Connector, não Lakehouse Federation (foreign catalog)** | Federar o projeto `basedosdados` inteiro como catálogo espelhado | O projeto `basedosdados` tem 186 datasets; espelhar todos como schemas excedeu a quota de 50 schemas do Free Edition (`UC_RESOURCE_QUOTA_EXCEEDED`). A leitura direcionada evita esse limite estrutural |
| **Bronze sem joins/decodificação; toda integração na Silver** | Já aplicar os `LEFT JOIN` de enriquecimento (como nos SQLs originais da Base dos Dados) direto na ingestão | Preserva fidelidade ao dado bruto na Bronze (histórico auditável); centraliza toda lógica de transformação em um único lugar (Silver), facilitando manutenção |
| **Colunas decodificadas mantêm código bruto + descrição (`rede_codigo` + `rede`)** | Sobrescrever o código pela descrição, como no SQL original da fonte | Rastreabilidade: permite auditar a decodificação a qualquer momento, sem depender de recontatar a fonte |
| **Gold Table 2 (metas vs. resultado) filtrada para `rede = "Municipal"` e `ano >= 2024`** | Comparar todas as redes e anos disponíveis | Metas só existem para a rede Municipal a partir de 2024 (confirmado empiricamente: 78% de nulos fora desse filtro, caindo a ~4% dentro dele) |
| **Gold Table 3 (evolução temporal) na granularidade UF, não microdado de aluno** | Agregar direto de `alunos` (3,87M linhas) | Performance e custo: a granularidade UF é suficiente para visualizar tendência nacional, sem processar volume desnecessário |
| **Streaming com `trigger(availableNow=True)`, não streaming contínuo (always-on)** | Cluster streaming rodando 24/7 aguardando novos eventos | FinOps: processa o disponível e encerra, em vez de manter compute alocado esperando; mais barato em ambiente serverless com quota |
| **Cadência do Job a cada 2 semanas (batch e streaming juntos)** | Jobs separados com frequências distintas (batch quinzenal, streaming a cada poucas horas) | Simplicidade operacional nesta fase do projeto; a fonte real (avaliação INEP/SAEB) tampouco é atualizada com alta frequência, então a cadência batch é defensável. Evolução natural seria separar as cadências para reforçar a narrativa de streaming "quase tempo real" |
| **Catálogo por camada (`_bronze`, `_silver`, `_gold`, `_monitoramento`)**, não schemas dentro de um catálogo único | Um catálogo único com schemas por camada | Isolamento de governança: permite políticas de acesso diferentes por camada (ex: só o pipeline escreve na Bronze; analistas só leem a Gold) |
| **`nivel_alfabetizacao` (escala 0–5) mantido sem decodificação de faixas** | Inferir e documentar limites de corte para cada nível | Validado empiricamente que é uma escala ordinal com correlação monotônica à taxa de alfabetização (0 = 30,8% médio → 5 = 88,6% médio), mas a fonte não documenta os cortes exatos. Fabricar limites seria dado inventado — optou-se pela transparência sobre a limitação |

---

## 6. Qualidade de Dados

| Verificação | Resultado |
|---|---|
| Duplicidade — todas as 8 tabelas Silver | ✅ Chave primária única confirmada em cada uma (ex: `alunos` → `id_aluno + ano`, não `id_aluno` isolado) |
| Chaves órfãs — enriquecimento geográfico (UF, Município) | ✅ 0 nulos em `sigla_uf_nome` e `id_municipio_nome` nas tabelas que dependem desse join |
| Nulos investigados — Gold Table 2 (metas vs. resultado) | ⚠️ 3,96% de nulos remanescentes mesmo após filtro correto, decompostos em: 96 casos de chave ausente (município sem meta definida) + 120 casos de meta nula na própria fonte (lacuna do dado bruto do INEP) |
| Contaminação cruzada streaming → batch | ✅ Confirmado que 0 dos nulos da Gold Table 2 vêm de dados de streaming — 100% originários do batch |

---

## 7. Monitoramento e FinOps

**Monitoramento:**
- **Falhas de ingestão**: Job com retries automáticos (2x, 10 min de intervalo) por task.
- **Alertas de erro**: notificação por e-mail em sucesso e falha, testada em cenário real (uma falha de path de credencial foi capturada e notificada corretamente durante o desenvolvimento).
- **Volume processado e latência**: tabela `bd_alfabetizacao_monitoramento.default.log_execucoes` registra, por execução, camada, tabela, linhas processadas, tempo de execução e status — consultável para análise de tendência de performance ao longo do tempo.
- **Latência de streaming**: métricas nativas do Structured Streaming (`query.recentProgress`) capturam linhas processadas por micro-batch.

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
- **`evolucao_temporal_indicador`**: séries temporais por UF podem alimentar modelos de forecasting (ex: ARIMA, Prophet) para projetar trajetória até 2030 por estado.
- **`nivel_alfabetizacao`** (escala ordinal 0–5): apesar de não ter cortes documentados, sua correlação monotônica com a taxa de alfabetização o torna um candidato a feature ordinal em modelos de clusterização de vulnerabilidade educacional.

---

## 9. Estrutura do Repositório

```
├── README.md
└── Tech/
    ├── Tech_Challenge_02_Camada_Bronze_Ingestion       # Ingestão batch (BigQuery → Bronze) + streaming (simulador + Auto Loader → Bronze)
    ├── Tech_Challenge_02_Streaming_Ingestion            # Simulador de eventos + Auto Loader (Bronze streaming)
    ├── Tech_Challenge_02_Camada_Silver_Transformation   # Decodificação, enriquecimento geográfico, validação de qualidade
    └── Tech_Challenge_02_Gold_Tran...                   # 3 tabelas analíticas (indicador, metas vs. resultado, evolução temporal)
```

---

## 10. Vídeo Executivo

📹 *[Link do vídeo a ser adicionado]* — apresentação executiva (até 5 min) cobrindo problema de negócio, arquitetura da solução, valor da pipeline e potencial de aplicação em IA.

---

## 11. Como Reproduzir

**Pré-requisitos:**
- Conta [Databricks Free Edition](https://www.databricks.com/) (gratuita, sem cartão de crédito)
- Projeto no Google Cloud com a BigQuery API habilitada
- Service Account do GCP com roles `BigQuery User` + `BigQuery Data Viewer`, chave JSON exportada

**Passos:**
1. Faça upload da chave JSON da Service Account para o Workspace do Databricks.
2. Execute os notebooks na ordem: `Bronze_Ingestion` → `Streaming_Ingestion` → `Silver_Transformation` → `Gold_Transformations`.
3. (Opcional) Configure o Job de orquestração no Databricks Workflows replicando o DAG descrito na Seção 2.
