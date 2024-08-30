# Tarefa 2

**Nome do Estagiário:** Guilherme Canarini Kaneda
**Data:** 30/08/2024

## Descrição

Você possui dois datasets em formato CSV, um contendo informações sobre campanhas (Campaign Dataset) e outro sobre compras (Purchase Dataset). O objetivo desta tarefa é criar um script SQL que integre esses datasets e retorne uma tabela consolidada com as seguintes informações:

1. client_id: Identificação do cliente.
2. total_price: Total gasto pelo cliente, calculado como (price * amount * discount_applied).
3. most_purchase_location: Local mais utilizado pelo cliente para realizar compras (website, app, store).
4. first_purchase: Data da primeira compra realizada pelo cliente.
5. last_purchase: Data da última compra realizada pelo cliente.
6. most_campaign: Campanha mais recebida pelo cliente.
7. quantity_error: Quantidade de campanhas que retornaram o status "error" para o cliente.
8. date_today: Data atual formatada como YYYY-MM-DD.
9. anomes_today: Data atual formatada como MMYYYY (tipo int).

## Script

Criação das tabelas "purchases" e "campaigns".

```
CREATE TABLE IF NOT EXISTS campaigns (
    id INT,
    id_campaign INT,
    type_campaign STRING,
    days_valid INT,
    data_campaign TIMESTAMP,
    channel STRING,
    return_status STRING,
    return_date TIMESTAMP,
    client_id STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
-- Pula a primeira linha do arquivo, o cabeçalho
STORED AS TEXTFILE TBLPROPERTIES ("skip.header.line.count"="1")

CREATE TABLE IF NOT EXISTS purchases (
    purchase_id STRING,
    product_name STRING,
    product_id STRING,
    amount INT,
    price DOUBLE,
    discount_applied DOUBLE,
    payment_method STRING,
    purchase_datetime TIMESTAMP,
    purchase_location STRING,
    client_id STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
-- Pula a primeira linha do arquivo, o cabeçalho
STORED AS TEXTFILE TBLPROPERTIES ("skip.header.line.count"="1")
```

Inserção dos dados dos arquivos CSV para as tabelas criadas, a partir de um endereço no MinIO.

```
LOAD DATA INPATH 's3a://raw/datasets/purchases_2023.csv' INTO TABLE purchases

LOAD DATA INPATH 's3a://raw/datasets/campaigns_2023_hist.csv' INTO TABLE campaigns
```

Para integrar as duas tabelas com as informações necessárias utilizei o CTE `with`, que permite a nomeação de um bloco de subconsulta, que pode ser referenciado em vários lugares dentro da consulta SQL principal.

### total_price

Subconsulta para selecionar o total gasto por cliente.

```
WITH total_price AS (
    SELECT client_id, ROUND(SUM(price * amount * discount_applied),2) as total_price FROM purchases GROUP BY client_id
)
```

### most_purchase_location

Subconsulta para identificar o local mais utilizado por cliente para realizar compras.

Utilizei a função `ROW_NUMBER`, que gera um número sequencial para as linhas dentro de cada partição de um conjunto de dados. No caso, a partição foi feita por cliente, e as linhas foram numeradas de forma decrescente com base na contagem de ocorrências de cada local. Dessa forma, na consulta principal, é possível selecionar apenas a primeira linha de cada partição, correspondendo ao local mais frequente para cada cliente.

```
WITH locations AS (
    SELECT client_id, purchase_location, ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY COUNT(*) DESC) AS most_used FROM purchases GROUP BY client_id, purchase_location
)
```

### first_purchase e last_purchase

Subconsultas para selecionar a primeira e última data de compra por cliente.

De forma semelhante a subconsulta passada, usei a função `ROW_NUMBER` para ordenar a contagem da partição de forma descrescente e selecionar a última data por cliente e, de forma crescente para selecionar a primeira data por cliente.

```
WITH datas_asc AS (
    SELECT client_id, purchase_datetime, ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY purchase_datetime ASC) AS data_asc FROM purchases
),

datas_desc AS (
    SELECT client_id, purchase_datetime, ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY purchase_datetime DESC) AS data_desc FROM purchases
)
```

### most_campaign

Subconsulta para selecionar a campanha mais recebida por cliente.

```
WITH campaigns_received AS (
    SELECT client_id, id_campaign, ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY COUNT(*) DESC) AS most_campaign FROM campaigns WHERE return_status = "received" GROUP BY client_id, id_campaign
)
```

### quantity_error

Subconsulta para selecionar a quantidade de recebimento de erros das campanhas por cliente.

```
WITH count_error AS (
    SELECT client_id, COUNT(return_status) as count_error FROM campaigns WHERE return_status = "error" GROUP BY client_id
)
```

### date_today

A função `CURRENT_DATE` retorna a data e hora atual, já formatada como YYYY-MM-DD.

```
CURRENT_DATE AS data_today
```

### anomes_today

Para isso, além de usar a função `CURRENT_DATE`, formatei a data como MMYYYY com a função `DATE_FORMAT`, para, por fim, converter para `int` com a função `CAST`.

```
CAST(DATE_FORMAT(CURRENT_DATE, "MMYYYY") AS int) AS anomes_today
```

### Consulta principal

Após definir as subconsultas utilizando o `WITH`, combinei-as em uma única consulta principal através de um `JOIN`. O `ON` é utilizado para especificar as condições que determinam como as subconsultas devem ser relacionadas entre si.

Finalmente, a consulta resultante é agrupada com base nas colunas obtidas das subconsultas.

```
SELECT t.client_id AS client_id, 
    t.total_price AS total_price,
    l.purchase_location AS most_purchase_location,  
    a.purchase_datetime AS first_purchase, 
    d.purchase_datetime AS last_purchase, 
    r.id_campaign AS most_campaign,
    e.count_error as quantity_error,
    CURRENT_DATE AS data_today, 
    CAST(DATE_FORMAT(CURRENT_DATE, "MMYYYY") AS int) AS anomes_today
    FROM total_price t JOIN locations l ON t.client_id = l.client_id AND l.most_used = 1
    JOIN datas_asc a ON l.client_id = a.client_id AND a.data_asc = 1
    JOIN datas_desc d ON a.client_id = d.client_id AND d.data_desc = 1
    JOIN campaigns_received r ON d.client_id = r.client_id AND r.most_campaign = 1
    JOIN count_error e ON r.client_id = e.client_id
    GROUP BY t.client_id, l.purchase_location, a.purchase_datetime, d.purchase_datetime, r.id_campaign, t.total_price, e.count_error
```

### [Script](/tarefa2_script.sql)