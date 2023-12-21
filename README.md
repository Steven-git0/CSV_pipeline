# CSV_pipeline

1.The schema and data source is from snowflake, from snowflake the data will be automatically uploaded to an aws s3 bucket a prespecified time

2. Using Aws lambda, the files that are uploaded will be checked and verified. In the event the files are not present an email will be sent out notifying the engineer of the fault.

3. After Lambda has confirmed the contents it sends a signal to airflow to begin processsing the data.

4. The Data gets cleaned by and proccessed by EMR, converting raw sales data into usable metrics such as, total revenue, total costs, high volume sellers, high margin items, store profit etc...
Afterwards it get sent back to the s3 bucket for further processing.

5. Glue to crawl the data and Athena for storing the cleaned data and final dimnesion tables

6. Superset(or Power Bi) is used to connect with athena and run queries and make tables on the data.

The pipeline is fully automated with all relavent code uploaded. Use for future reference. 
