%%sql

-- Cria ou substitui a tabela Delta
CREATE OR REPLACE TABLE Lakehouse_Gold.fact_cotacoes
USING DELTA
AS

-- Extrai mínimas e máximas datas
WITH min_max AS (
  SELECT MIN(Data) AS min_data, 
         MAX(Data) AS max_data
  FROM Lakehouse_Silver.cotacoes
),

-- Gera sequência de datas
datas_periodo AS (
  SELECT explode(
    sequence(
      d.min_data,
      d.max_data,
      interval 1 day
    )
  ) AS Data
  FROM min_max d
),

-- Moedas distintas
moedas AS (
  SELECT DISTINCT Moeda
  FROM Lakehouse_Gold.dim_moedas
),

-- Faz o plano cartesiano entre moedas e datas
cross_tb AS (
  SELECT m.Moeda, d.Data
  FROM moedas m
  CROSS JOIN datas_periodo d
),

-- Dias sem cotações
datas_faltantes AS (
  SELECT 
    cr.Moeda,
    cr.Data
  FROM cross_tb cr
  LEFT JOIN (
    SELECT DISTINCT 
        Moeda, 
        Data
    FROM Lakehouse_Silver.cotacoes
  ) c
  ON cr.Moeda = c.Moeda AND cr.Data = c.Data
  WHERE c.Data IS NULL
),

-- Adiciona as linhas faltantes
linhas_adicionadas AS (
  SELECT
    dt_falt.Moeda,
    dt_falt.Data,   -- Corrigido para "Data"
    NULL AS Cotacao -- Valores nulos para cotações adicionadas
  FROM datas_faltantes dt_falt
),

-- Combina as cotações existentes com as linhas adicionadas
append AS (
  SELECT
    Moeda,
    Data,
    Cotacao
  FROM Lakehouse_Silver.cotacoes

  UNION ALL

  SELECT
    Moeda,
    Data,
    Cotacao
  FROM linhas_adicionadas
)

-- Forward Fill: Preenche os valores de cotação para as novas linhas
SELECT
  Moeda,
  Data,
  last_value(Cotacao, true) 
    OVER (
      PARTITION BY Moeda 
      ORDER BY Data
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS Cotacao
FROM append
ORDER BY Data, Moeda;