project_id = 'dmc-pea-data-engineer-grupo2'

def copy_file_from_bucket_to_bucket(SOURCE_BUCKET_NAME, SOURCE_FOLDER_NAME, SOURCE_FILE_NAME,
                                    DESTINATION_BUCKET_NAME, DESTINATION_FOLDER_NAME):
  SOURCE_BUCKET = SOURCE_BUCKET_NAME + "/" + SOURCE_FOLDER_NAME + "/" + SOURCE_FILE_NAME
  DESTINATION_BUCKET = DESTINATION_BUCKET_NAME + "/" + DESTINATION_FOLDER_NAME + "/"
  !gcloud storage cp gs://{SOURCE_BUCKET} gs://{DESTINATION_BUCKET}/

## Municipalidad Jesus Maria

SOURCE_BUCKET_NAME = "dmc-pea-de-grupo-2-input"
DESTINATION_BUCKET_NAME = "dmc-pea-de-grupo-2-datalake"

SOURCE_FOLDER_NAME = "MunicipalidadJesusMaria"
DESTINATION_FOLDER_NAME = "workload/MunicipalidadJesusMaria"

SOURCE_FILE_NAME = "MuniJesusMaria2021_2.csv"

copy_file_from_bucket_to_bucket(SOURCE_BUCKET_NAME, SOURCE_FOLDER_NAME, SOURCE_FILE_NAME,
                                DESTINATION_BUCKET_NAME, DESTINATION_FOLDER_NAME)

SOURCE_FILE_NAME = "MuniJesusMaria2022_1.csv"

copy_file_from_bucket_to_bucket(SOURCE_BUCKET_NAME, SOURCE_FOLDER_NAME, SOURCE_FILE_NAME,
                                DESTINATION_BUCKET_NAME, DESTINATION_FOLDER_NAME)

## Municipalidad San Bartolo

SOURCE_BUCKET_NAME = "dmc-pea-de-grupo-2-input"
DESTINATION_BUCKET_NAME = "dmc-pea-de-grupo-2-datalake"

SOURCE_FOLDER_NAME = "MunicipalidadSanBartolo"
DESTINATION_FOLDER_NAME = "workload/MunicipalidadSanBartolo"

SOURCE_FILE_NAME = "MuniSanBartolo2021.csv"

copy_file_from_bucket_to_bucket(SOURCE_BUCKET_NAME, SOURCE_FOLDER_NAME, SOURCE_FILE_NAME,
                                DESTINATION_BUCKET_NAME, DESTINATION_FOLDER_NAME)

## Municipalidad Los Olivos

SOURCE_BUCKET_NAME = "dmc-pea-de-grupo-2-input"
DESTINATION_BUCKET_NAME = "dmc-pea-de-grupo-2-datalake"

SOURCE_FOLDER_NAME = "MunicipalidadLosOlivos"
DESTINATION_FOLDER_NAME = "workload/MunicipalidadLosOlivos"

SOURCE_FILE_NAME = "MuniLosOlivos2019-2022.csv"

copy_file_from_bucket_to_bucket(SOURCE_BUCKET_NAME, SOURCE_FOLDER_NAME, SOURCE_FILE_NAME,
                                DESTINATION_BUCKET_NAME, DESTINATION_FOLDER_NAME)

## Municipalidad Lince

SOURCE_BUCKET_NAME = "dmc-pea-de-grupo-2-input"
DESTINATION_BUCKET_NAME = "dmc-pea-de-grupo-2-datalake"

SOURCE_FOLDER_NAME = "MunicipalidadLince"
DESTINATION_FOLDER_NAME = "workload/MunicipalidadLince"

SOURCE_FILE_NAME = "MuniLince2022.csv"

copy_file_from_bucket_to_bucket(SOURCE_BUCKET_NAME, SOURCE_FOLDER_NAME, SOURCE_FILE_NAME,
                                DESTINATION_BUCKET_NAME, DESTINATION_FOLDER_NAME)