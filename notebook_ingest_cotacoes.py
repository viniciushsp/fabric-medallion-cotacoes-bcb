#importa todas as bibliotecas que podem vir a ser utilizadas no projeto

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import *
import requests 
import json

#Cria a função para transformar a tabela de moedas em uma lista de itens únicos

df_moedas = spark.sql("SELECT moeda FROM dim_moedas")
lista_moedas = [row['moeda']for row in df_moedas.collect()]
print(lista_moedas)

# Define as variáveis a ser utilizada no projeto para pipeline de dados

data_inicial = '01-01-2025'
# data_final = '12-01-2024'
data_final = data_final = datetime.now().strftime('%m-%d-%Y')
top = 100
skip = 0

#inicia o loop com "for" para cada moeda única que constar na "lista_moedas" e "while parea durar enquanto a condição for atendida"

for moeda in lista_moedas:

    skip = 0

    todos_dados = []


    while  True:
        url = (
            f"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1"
            f"/odata/CotacaoMoedaPeriodo(moeda=@moeda,dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?"
            f"@moeda='{moeda}'&@dataInicial='{data_inicial}'&@dataFinalCotacao='{data_final}'"
            f"&$top={top}&$skip={skip}&$filter=tipoBoletim%20eq%20'Fechamento'&$format=json&$select=cotacaoCompra,dataHoraCotacao"
        )


        response = requests.get(url)

        dados = response.json()['value']

        if not dados:
                break

        todos_dados.extend(dados)
        skip += top

# Adicionando a coluna de moeda no arquivo parquet.

    if todos_dados:
        df = spark.createDataFrame(todos_dados) \
            .withColumn('moeda',lit(moeda))

    data_inicial_path = datetime.strptime(data_inicial, "%m-%d-%Y").strftime("%Y%m%d")
    data_final_path = datetime.strptime(data_final, "%m-%d-%Y").strftime("%Y%m%d")

    path = (
        f"Files/Cotacoes/Novos/"
        f"{moeda}-"
        f"{data_inicial_path}_"
        f"{data_final_path}"
        f".parquet"
    )

    df.write.mode('overwrite').parquet(path)
