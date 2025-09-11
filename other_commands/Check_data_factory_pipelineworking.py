# run in gold notebook after running silver and bronze and kafka together
# pipeline should be working, thus there should be auto ingestion into gold


from pyspark.sql import functions as F
from pyspark.sql.functions import lit, col, expr, current_timestamp, to_timestamp, sha2, concat_ws, coalesce, monotonically_increasing_id
from delta.tables import DeltaTable
from pyspark.sql import Window

#ADLS configuration 
spark.conf.set(
  "fs.azure.account.key.saihospitalstorage.dfs.core.windows.net",
  dbutils.secrets.get(scope="hospitalanalyticsvaultscope", key="storage-connection")
)
gold_dim_patient = "abfss://gold@saihospitalstorage.dfs.core.windows.net/dim_patient"
display(spark.read.format("delta").load(gold_dim_patient))