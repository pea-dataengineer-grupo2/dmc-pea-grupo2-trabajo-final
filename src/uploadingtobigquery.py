
from google.cloud import bigquery

project_id = 'dmc-pea-data-engineer-grupo2'

client = bigquery.Client(project=project_id)
table_id = "dmc-pea-data-engineer-grupo2.ds_ordenes_compra.ordenes_compra"

job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                                    source_format = bigquery.SourceFormat.PARQUET)

uri = "gs://dmc-pea-de-grupo-2-datalake/functional/OrdenesCompraDistritosLima/*.parquet"

load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)

load_job.result()

destination_table = client.get_table(table_id)

print("Loadad {} rows".format(destination_table.num_rows))
