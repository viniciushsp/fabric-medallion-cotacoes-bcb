# Data Pipeline Brazilian Central Bank's

### pt-br
Neste repositório é possível verificar os notebooks utilizados no projeto de medallion em um workspace Fabric F2.

### en-us
In this repository, you can find the notebooks used in the Medallion architecture project within a Fabric F2 workspace.

---

## 1. [notebook-ingest-moedas.py](https://github.com/viniciushsp/fabric-medallion-cotacoes-bcb/blob/main/notebook_ingest_moedas.py)

### pt-br
Neste notebook importa-se os dados através do endpoint do Banco Central do Brasil (`Moedas`) das moedas disponíveis e cria-se uma tabela através do SparkPy. Toda vez que o notebook for executado, irá sobrescrever as moedas que foram importadas, fazendo com que quando houverem novos registros, esses sejam criados sem afetar as demais.

### en-us
This notebook imports data from the Brazilian Central Bank's endpoint (`Moedas`) for the available currencies and creates a table using SparkPy. Each time the notebook is run, it overwrites the previously imported currencies, ensuring that any new records are added without affecting the existing ones.

---

## 2. [notebook-ingest-cotacoes.py](https://github.com/viniciushsp/fabric-medallion-cotacoes-bcb/blob/main/notebook_ingest_cotacoes.py)

### pt-br
Neste notebook importa-se os dados através do endpoint do Banco Central do Brasil (`CotacaoMoedaPeriodo`), define-se as variáveis de busca para que o pipeline fique dinâmico, e através deste método obtemos as cotações de todas as moedas contidas em `notebook-ingest-moedas.py`. Após obter os dados via JSON, cria-se um arquivo `.parquet` na pasta "Novos" para cada uma das moedas a fim de utilizarmos esses arquivos para carregar os dados rapidamente nas etapas posteriores.

### en-us
This notebook imports exchange rate data from the Brazilian Central Bank's endpoint (`CotacaoMoedaPeriodo`) and sets the search variables to make the pipeline dynamic. It retrieves the exchange rates for all currencies listed in `notebook-ingest-moedas.py`. After retrieving the data in JSON format, a `.parquet` file is created in the "Novos" folder for each currency, which will be used for quick data loading in the subsequent steps.

---

## 3. [notebook-bronze-to-silver.py](https://github.com/viniciushsp/fabric-medallion-cotacoes-bcb/blob/main/notebook_bronze_to_silver)

### pt-br
Inicia-se o notebook buscando os dados nos arquivos `.parquet`, e com SparkSQL é criada uma tabela que unifica todos os arquivos a fim de utilizarmos na camada gold. As intercorrências iniciais, como duplicações e erros são tratadas nesta etapa, e após isso utilizando a biblioteca `mssparkutils`, manipula-se os arquivos retirando-os da pasta "Novos" e enviando-os para a pasta "Carregados", para que esses não influenciem nos próximos carregamentos.

### en-us
This notebook starts by reading the `.parquet` files and uses SparkSQL to create a unified table that will be used in the gold layer. Initial issues such as duplicates and errors are handled during this stage. After the data is processed, the `mssparkutils` library is used to move the files from the "Novos" folder to the "Carregados" folder to prevent them from affecting future data loads.

---

## 4. [notebook-silver-to-gold.py](https://github.com/viniciushsp/fabric-medallion-cotacoes-bcb/blob/main/notebook_silver_to_gold.sql)

### pt-br
Neste notebook, é utilizado puramente SparkSQL para que se manipule os dados já tabulados na etapa anterior. É realizado os tratamentos finais de dias que não contenham cotações, criando um plano cartesiano e buscando as datas mínimas e máximas.

### en-us
This notebook uses pure SparkSQL to process the data previously organized in the silver layer. Final transformations are applied, including handling days without exchange rate data by generating a Cartesian plan and retrieving the minimum and maximum dates.
