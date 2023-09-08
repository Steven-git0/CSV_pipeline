from pyspark.sql import SparkSession
from datetime import timedelta, datetime
import sys

#date_str = sys.argv[1]

#creating spark session
#change to yarn for emr cluster
spark = SparkSession.builder.master("yarn").appName("demo").getOrCreate()

datestr = (datetime.today() - timedelta(days = 1)).strftime("%Y-%m-%d")
#read file from s3
#change to s3 bucket when running in emr
store_df = spark.read.option("header", "true").option("delimiter", ",").csv(f"s3://midterm-wcd0/store_{datestr}.csv")
sales_df = spark.read.option("header", "true").option("delimiter", ",").csv(f"s3://midterm-wcd0/sales_{datestr}.csv")
calendar_df = spark.read.option("header", "true").option("delimiter", ",").csv(f"s3://midterm-wcd0/calendar_{datestr}.csv")
product_df = spark.read.option("header", "true").option("delimiter", ",").csv(f"s3://midterm-wcd0/product_{datestr}.csv")
inventory_df = spark.read.option("header", "true").option("delimiter", ",").csv(f"s3://midterm-wcd0/inventory_{datestr}.csv")

#total sales quantity by week, store and product
store_df.createOrReplaceTempView("STORE")
sales_df.createOrReplaceTempView("SALES")
calendar_df.createOrReplaceTempView("CALENDAR")
product_df.createOrReplaceTempView("PRODUCT")
inventory_df.createOrReplaceTempView("INVENTORY")

df_sum_sales_quantity = spark.sql("""
                                  
WITH END_OF_WEEK_INVENTORY AS(
    SELECT CALENDAR.WK_NUM, CALENDAR.DAY_OF_WK_NUM, SALES.STORE_KEY, INVENTORY.INVENTORY_ON_HAND_QTY, INVENTORY.INVENTORY_ON_ORDER_QTY, INVENTORY.PROD_KEY
    FROM SALES
    JOIN CALENDAR ON SALES.TRANS_DT = CALENDAR.CAL_DT
    JOIN INVENTORY ON SALES.PROD_KEY = INVENTORY.PROD_KEY AND SALES.TRANS_DT = INVENTORY.CAL_DT AND INVENTORY.STORE_KEY = SALES.STORE_KEY
    WHERE CALENDAR.DAY_OF_WK_NUM = 6
)

SELECT CALENDAR.WK_NUM, SALES.STORE_KEY, SALES.PROD_KEY, 
SUM(SALES_QTY) AS SALES_QUANTITY, 
SUM(SALES_AMT) AS SALES_REVENUE, 
SUM(SALES_AMT)/SUM(SALES_QTY) AS AVG_SALE_PRICE,
END_OF_WEEK_INVENTORY.INVENTORY_ON_HAND_QTY AS END_WEEK_INVENTORY,
END_OF_WEEK_INVENTORY.INVENTORY_ON_ORDER_QTY AS ON_ORDER_INVENTORY,
SUM(SALES_COST) AS SALES_COSTS, 
1-SUM(INVENTORY.OUT_OF_STOCK_FLG)/7 AS IN_STOCK_PERC,
CASE 
    WHEN SUM(INVENTORY.INVENTORY_ON_HAND_QTY) < SUM(SALES_QTY) THEN 1 + SUM(INVENTORY.OUT_OF_STOCK_FLG)
    ELSE 0 +SUM(INVENTORY.OUT_OF_STOCK_FLG)
    END AS LOW_STOCK_IMPACT,
CASE 
    WHEN SUM(INVENTORY.INVENTORY_ON_HAND_QTY) < SUM(SALES_QTY) THEN SUM(SALES_AMT-INVENTORY.INVENTORY_ON_HAND_QTY)
    ELSE 0
    END AS POTENTIAL_LOW_STOCK_IMPACT,
CASE
    WHEN SUM(INVENTORY.OUT_OF_STOCK_FLG) = 1 THEN SUM(SALES_AMT)
    ELSE 0
    END AS NO_STOCK_IMPACT,
SUM(
    CASE
        WHEN INVENTORY.INVENTORY_ON_HAND_QTY < SALES.SALES_QTY THEN 1
        ELSE 0
    END
    ) AS LOW_STOCK_INSTANCES,
SUM(
    CASE
        WHEN INVENTORY.OUT_OF_STOCK_FLG = 1 THEN 1
        ELSE 0
    END
    ) AS NO_STOCK_INSTANCES,
END_OF_WEEK_INVENTORY.INVENTORY_ON_HAND_QTY/SUM(SALES_QTY) AS WEEKS_OF_SUPPLY
FROM SALES
JOIN CALENDAR ON SALES.TRANS_DT = CALENDAR.CAL_DT
JOIN PRODUCT ON SALES.PROD_KEY = PRODUCT.PROD_KEY
JOIN INVENTORY ON SALES.PROD_KEY = INVENTORY.PROD_KEY 
    AND INVENTORY.STORE_KEY = SALES.STORE_KEY 
    AND INVENTORY.CAL_DT = SALES.TRANS_DT
JOIN END_OF_WEEK_INVENTORY ON END_OF_WEEK_INVENTORY.WK_NUM = CALENDAR.WK_NUM 
    AND END_OF_WEEK_INVENTORY.STORE_KEY = SALES.STORE_KEY 
    AND INVENTORY.PROD_KEY = END_OF_WEEK_INVENTORY.PROD_KEY
GROUP BY 
    CALENDAR.WK_NUM, 
    SALES.STORE_KEY, 
    SALES.PROD_KEY,
    END_OF_WEEK_INVENTORY.INVENTORY_ON_HAND_QTY,
    END_OF_WEEK_INVENTORY.INVENTORY_ON_ORDER_QTY
ORDER BY CALENDAR.WK_NUM;                                  
                                  
                                  
                                  """)

df_sum_sales_quantity.show()

#write back to s3 for emr
df_sum_sales_quantity.repartition(1).write.mode('overwrite').option('compression', 'gzip').parquet(f"s3://midterm-result-wcd0/cleaned_{datestr}")
store_df.repartition(1).write.mode('overwrite').option('compression', 'gzip').parquet(f"s3://midterm-result-wcd0/store_{datestr}")
product_df.repartition(1).write.mode('overwrite').option('compression', 'gzip').parquet(f"s3://midterm-result-wcd0/product_{datestr}")
calendar_df.repartition(1).write.mode('overwrite').option('compression', 'gzip').parquet(f"s3://midterm-result-wcd0/calendar_{datestr}")