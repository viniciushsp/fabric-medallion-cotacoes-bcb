import requests

url = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/Moedas?$top=100&$format=json&$select=simbolo,nomeFormatado"

response = requests.get(url)

dados = response.json()['value']

df = spark.createDataFrame(dados)

df = df.selectExpr(
    "nomeFormatado as moeda_nome",
    "simbolo as moeda"
)

df.write.mode('overwrite').saveAsTable('dim_moedas')