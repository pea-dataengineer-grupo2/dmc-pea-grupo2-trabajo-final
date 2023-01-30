from google.cloud import storage


def copy_blob(
    bucket_name, blob_name, destination_bucket_name, destination_blob_name
):
    """Copies a blob from one bucket to another with a new name."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"
    # destination_bucket_name = "destination-bucket-name"
    # destination_blob_name = "destination-object-name"

    storage_client = storage.Client()

    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_name
    )

    print(
        "Blob {} in bucket {} copied to blob {} in bucket {}.".format(
            source_blob.name,
            source_bucket.name,
            blob_copy.name,
            destination_bucket.name,
        )
    )

def main():
    files = []

    src_bucket = "dmc-pea-de-grupo-2-input"
    dst_bucket = "dmc-pea-de-grupo-2-datalake"


    file_jesus_maria_2021_2 = "MunicipalidadJesusMaria/MuniJesusMaria2021_2.csv"
    file_jesus_maria_2022_1 = "MunicipalidadJesusMaria/MuniJesusMaria2022_1.csv"
    file_los_olivos = "MunicipalidadLosOlivos/MuniLosOlivos2019-2022.csv"
    file_lince = "MunicipalidadLince/MuniLince2022.csv"
    file_san_bartolo = "MunicipalidadSanBartolo/MuniSanBartolo2021.csv"

    files.append(file_jesus_maria_2021_2)
    files.append(file_jesus_maria_2022_1)
    files.append(file_los_olivos)
    files.append(file_lince)
    files.append(file_san_bartolo)

    for file in files:
        copy_blob(src_bucket, file, dst_bucket, "workload/" + file)

main()