%hive

-- Criação da tabela com as colunas do csv
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

----------------------------------------------------------------------

-- Criação da tabela com as colunas do csv
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

--------------------------------------------------------------------------------------

-- Importa um arquivo e insere seus dados na tabela criada
LOAD DATA INPATH 's3a://raw/datasets/campaigns_2023_hist.csv' INTO TABLE campaigns

--------------------------------------------------------------------------------------

-- Importa um arquivo e insere seus dados na tabela criada
LOAD DATA INPATH 's3a://raw/datasets/purchases_2023.csv' INTO TABLE purchases

--------------------------------------------------------------------------------------

WITH locations AS (
    SELECT client_id, purchase_location, ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY COUNT(*) DESC) AS most_used FROM purchases GROUP BY client_id, purchase_location
),
datas_asc AS (
    SELECT client_id, purchase_datetime, ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY purchase_datetime ASC) AS data_asc FROM purchases
),
datas_desc AS (
    SELECT client_id, purchase_datetime, ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY purchase_datetime DESC) AS data_desc FROM purchases
),
campaigns_received AS (
    SELECT client_id, id_campaign, ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY COUNT(*) DESC) AS most_campaign FROM campaigns WHERE return_status = "received" GROUP BY client_id, id_campaign
),
total_price AS (
    SELECT client_id, ROUND(SUM(price * amount * discount_applied),2) as total_price FROM purchases GROUP BY client_id
),
count_error AS (
    SELECT client_id, COUNT(return_status) as count_error FROM campaigns WHERE return_status = "error" GROUP BY client_id
)

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