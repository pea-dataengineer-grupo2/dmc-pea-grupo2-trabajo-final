#!/usr/bin/env python
# coding: utf-8

# # PROYECTO PEA DE GRUPO2

# # POBLANDO DATALAKE - APACHE SPARK

# ## 1° POBLANDO CAPA LANDING
# **Importamos módulos de apache spark**

# In[176]:


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


# **Creamos las session de apache spark en una variable**

# In[177]:


spark = SparkSession.builder.master("local[*]").getOrCreate()


# **Verificamos la versión de apache spark**

# In[178]:


spark


# ### CAPA LANDING
# 
# **Creando los dataframe** 

# In[179]:


# Estructura del dataframe de archivo municipalidad Jesus Maria
df_schema_jesus_maria = StructType([
StructField("T_ORDEN", StringType(),True),
StructField("ORDEN_RUC", StringType(),True),
StructField("SIAF_O_COMPRA", IntegerType(),True),
StructField("FECHA_O_COMPRA", StringType(),True),
StructField("MONTO", DoubleType(),True),
StructField("NOMBRE_PROVEEDOR", StringType(),True),
StructField("DESCRIPCION", StringType(),True),
StructField("FECHA_COMPRA", StringType(),True),
StructField("AÑO_COMPRA", StringType(),True),
StructField("MES_COMPRA", StringType(),True),
StructField("DEPARTAMENTO", StringType(),True),
StructField("PROVINCIA", StringType(),True),
StructField("DISTRITO", StringType(),True),
])


# In[180]:


# Estructura del dataframe de archivo municipalidad Lince
df_schema_lince = StructType([
StructField("FECHA_CORTE", StringType(),True),
StructField("DEPARTAMENTO", StringType(),True),
StructField("PROVINCIA", StringType(),True),
StructField("DISTRITO", StringType(),True),
StructField("UBIGEO", IntegerType(),True),
StructField("TIPO_ORDEN", StringType(),True),
StructField("NUMERO_ORDEN", IntegerType(),True),
StructField("FECHA_ORDEN", StringType(),True),
StructField("RUC", StringType(),True),
StructField("RAZON_SOCIAL", StringType(),True),
StructField("PERIODO", StringType(),True),
StructField("MONTO", DoubleType(),True),
])


# In[181]:


# Estructura del dataframe de archivo municipalidad San Bartolo
df_schema_san_bartolo = StructType([
StructField("DEPARTAMENTO", StringType(),True),
StructField("PROVINCIA", StringType(),True),
StructField("DISTRITO", StringType(),True),
StructField("UBIGEO", IntegerType(),True),
StructField("GOBIERNO_LOCAL", StringType(),True),
StructField("RUC_GOBIERNO_LOCAL", StringType(),True),
StructField("TIPO_ORDEN", StringType(),True),
StructField("NUMERO_ORDEN", IntegerType(),True),
StructField("DESCRIPCION_ORDEN", StringType(),True),
StructField("FECHA_ORDEN", StringType(),True),
StructField("RUC_PROVEEDOR", StringType(),True),
StructField("MONTO_TOTAL", DoubleType(),True),
StructField("PERIODO_MENSUAL", StringType(),True),
StructField("PERIODO_ANUAL", StringType(),True),
StructField("NUMERO_SIAF", IntegerType(),True),
StructField("FECHA_CORTE", StringType(),True),

])


# In[182]:


# Estructura del dataframe de archivo municipalidad Los Olivos
df_schema_los_olivos = StructType([
StructField("DEPARTAMENTO", StringType(),True),
StructField("PROVINCIA", StringType(),True),
StructField("DISTRITO", StringType(),True),
StructField("UBIGEO", IntegerType(),True),
StructField("GOBIERNO_LOCAL", StringType(),True),
StructField("RUC_GOBIERNO_LOCAL", StringType(),True),
StructField("TIPO_ORDEN", StringType(),True),
StructField("NUMERO_ORDEN", IntegerType(),True),
StructField("DESCRIPCION_ORDEN", StringType(),True),
StructField("FECHA_ORDEN", StringType(),True),
StructField("RUC_PROVEEDOR", StringType(),True),
StructField("RAZON_SOCIAL", StringType(),True),
StructField("MONTO_TOTAL", DoubleType(),True),
StructField("PERIODO_MENSUAL", StringType(),True),
StructField("PERIODO_ANUAL", StringType(),True),
StructField("NUMERO_SIAF", IntegerType(),True),
StructField("FECHA_CORTE", StringType(),True),

])


# In[183]:


# Definiendo rutas workload
ruta_archivo_workload_jesus_maria_2021_2 = "gs://dmc-pea-de-grupo-2-datalake/workload/MunicipalidadJesusMaria/MuniJesusMaria2021_2.csv"
ruta_archivo_workload_jesus_maria_2022_1 = "gs://dmc-pea-de-grupo-2-datalake/workload/MunicipalidadJesusMaria/MuniJesusMaria2022_1.csv"

ruta_archivo_workload_lince = "gs://dmc-pea-de-grupo-2-datalake/workload/MunicipalidadLince/MuniLince2022.csv"

ruta_archivo_workload_san_bartolo = "gs://dmc-pea-de-grupo-2-datalake/workload/MunicipalidadSanBartolo/MuniSanBartolo2021.csv"

ruta_archivo_workload_los_olivos = "gs://dmc-pea-de-grupo-2-datalake/workload/MunicipalidadLosOlivos/MuniLosOlivos2019-2022.csv"


# In[184]:


# Leyendo los archivos
df_jesus_maria_2021_2 = spark.read.format("CSV").option("header","true").option("delimiter",";").option("encoding", "ISO-8859-1").schema(df_schema_jesus_maria).load(ruta_archivo_workload_jesus_maria_2021_2)
df_jesus_maria_2022_1 = spark.read.format("CSV").option("header","true").option("delimiter",";").option("encoding", "ISO-8859-1").schema(df_schema_jesus_maria).load(ruta_archivo_workload_jesus_maria_2022_1)

df_lince = spark.read.format("CSV").option("header","true").option("delimiter",";").option("encoding", "ISO-8859-1").schema(df_schema_lince).load(ruta_archivo_workload_lince)

df_san_bartolo = spark.read.format("CSV").option("header","true").option("delimiter",",").option("encoding", "ISO-8859-1").schema(df_schema_san_bartolo).load(ruta_archivo_workload_san_bartolo)

df_los_olivos = spark.read.format("CSV").option("header","true").option("delimiter",";").option("encoding", "ISO-8859-1").schema(df_schema_los_olivos).load(ruta_archivo_workload_los_olivos)


# **Definiendo las rutas de la capa landing y guardando los dataframe** 

# In[185]:


ruta_destino_landing_jesus_maria_2021_2 = "gs://dmc-pea-de-grupo-2-datalake/landing/MunicipalidadJesusMaria/2021-2/"
ruta_destino_landing_jesus_maria_2022_1 = "gs://dmc-pea-de-grupo-2-datalake/landing/MunicipalidadJesusMaria/2022-1/"

ruta_destino_landing_lince = "gs://dmc-pea-de-grupo-2-datalake/landing/MunicipalidadLince/"

ruta_destino_landing_san_bartolo = "gs://dmc-pea-de-grupo-2-datalake/landing/MunicipalidadSanBartolo/"

ruta_destino_landing_los_olivos = "gs://dmc-pea-de-grupo-2-datalake/landing/MunicipalidadLosOlivos/"


# In[186]:


df_jesus_maria_2021_2.write.mode("overwrite").format("parquet").save(ruta_destino_landing_jesus_maria_2021_2)
df_jesus_maria_2022_1.write.mode("overwrite").format("parquet").save(ruta_destino_landing_jesus_maria_2022_1)

df_lince.write.mode("overwrite").format("parquet").save(ruta_destino_landing_lince)

df_san_bartolo.write.mode("overwrite").format("parquet").save(ruta_destino_landing_san_bartolo)

df_los_olivos.write.mode("overwrite").format("parquet").save(ruta_destino_landing_los_olivos)


# ## 2° POBLANDO CAPA CURATED

# ### ORDENES DE COMPRA
# **Definiendo las rutas de la capa landing y leyendo los dataframe** 

# In[187]:


ruta_landing_jesus_maria_2021_2 = "gs://dmc-pea-de-grupo-2-datalake/landing/MunicipalidadJesusMaria/2021-2/"
ruta_landing_jesus_maria_2022_1 = "gs://dmc-pea-de-grupo-2-datalake/landing/MunicipalidadJesusMaria/2022-1/"

ruta_landing_lince = "gs://dmc-pea-de-grupo-2-datalake/landing/MunicipalidadLince/"

ruta_landing_san_bartolo = "gs://dmc-pea-de-grupo-2-datalake/landing/MunicipalidadSanBartolo/"

ruta_landing_los_olivos = "gs://dmc-pea-de-grupo-2-datalake/landing/MunicipalidadLosOlivos/"


# In[188]:


df_landing_jesus_maria_2021_2 = spark.read.format("parquet").option("header","true").load(ruta_landing_jesus_maria_2021_2)
df_landing_jesus_maria_2022_1 = spark.read.format("parquet").option("header","true").load(ruta_landing_jesus_maria_2022_1)

df_landing_lince = spark.read.format("parquet").option("header","true").load(ruta_landing_lince)

df_landing_san_bartolo = spark.read.format("parquet").option("header","true").load(ruta_landing_san_bartolo)

df_landing_los_olivos = spark.read.format("parquet").option("header","true").load(ruta_landing_los_olivos)


# **11° PASO Realizamos la limpieza de los dataframes y Reglas del Negocio**

# ### - Municipalidad Jesus Maria

# In[189]:


df_landing_jesus_maria=df_landing_jesus_maria_2021_2.union(df_landing_jesus_maria_2022_1)


# In[190]:


#Renombrando los nombres de las Columnas
df_landing_jesus_maria=df_landing_jesus_maria.withColumnRenamed("T_ORDEN","TIPO_ORDEN")
df_landing_jesus_maria=df_landing_jesus_maria.withColumnRenamed("ORDEN_RUC","RUC_GOBIERNO_LOCAL")
df_landing_jesus_maria=df_landing_jesus_maria.withColumnRenamed("NOMBRE_PROVEEDOR","RAZON_SOCIAL_PROVEEDOR")


# In[191]:


#Eliminamos " en el campo Descripcion
df_landing_jesus_maria = df_landing_jesus_maria.withColumn('DESCRIPCION', regexp_replace('DESCRIPCION', '"', ''))


# In[192]:


#Eliminamos . al inicio del campo Descripcion
df_landing_jesus_maria = df_landing_jesus_maria.withColumn('DESCRIPCION', regexp_replace('DESCRIPCION', '^\.', ''))


# In[193]:


#Eliminamos los espacios en blancos de los extremos 
df_landing_jesus_maria = df_landing_jesus_maria.withColumn('TIPO_ORDEN', trim('TIPO_ORDEN'))
df_landing_jesus_maria = df_landing_jesus_maria.withColumn('RAZON_SOCIAL_PROVEEDOR', trim('RAZON_SOCIAL_PROVEEDOR'))
df_landing_jesus_maria = df_landing_jesus_maria.withColumn('DESCRIPCION', trim('DESCRIPCION'))
df_landing_jesus_maria = df_landing_jesus_maria.withColumn('DEPARTAMENTO', trim('DEPARTAMENTO'))
df_landing_jesus_maria = df_landing_jesus_maria.withColumn('PROVINCIA', trim('PROVINCIA'))
df_landing_jesus_maria = df_landing_jesus_maria.withColumn('DISTRITO', trim('DISTRITO'))


# In[194]:


#Convertimos FECHA_O_COMPRA a tipo de dato date
df_landing_jesus_maria = df_landing_jesus_maria.withColumn("FECHA_O_COMPRA",to_date(col("FECHA_O_COMPRA"),"dd/MM/yyyy"))


# ### REGLA DE NEGOCIO:
# 1. Toda descripcion o caracteres en mayuscula.
# 2. FECHA_O_COMPRA será FECHA_COMPRA 
# 3. Agregar columna COD_DISTRITO (Iniciales del distrito)
# 4. Particionar por COD_PERIODO (AAAAMM)
# 

# In[195]:


#Convertimos los campos TIPO_ORDEN,DESCRIPCION a MAYUSCULA
df_landing_jesus_maria = df_landing_jesus_maria.withColumn("TIPO_ORDEN",upper("TIPO_ORDEN"))
df_landing_jesus_maria = df_landing_jesus_maria.withColumn("RAZON_SOCIAL_PROVEEDOR",upper("RAZON_SOCIAL_PROVEEDOR"))
df_landing_jesus_maria = df_landing_jesus_maria.withColumn("DESCRIPCION",upper("DESCRIPCION"))
df_landing_jesus_maria = df_landing_jesus_maria.withColumn("DEPARTAMENTO",upper("DEPARTAMENTO"))
df_landing_jesus_maria = df_landing_jesus_maria.withColumn("PROVINCIA",upper("PROVINCIA"))
df_landing_jesus_maria = df_landing_jesus_maria.withColumn("DISTRITO",upper("DISTRITO"))


# In[196]:


#Elimimando columna FECHA_COMPRA
df_landing_jesus_maria=df_landing_jesus_maria.drop("FECHA_COMPRA")

#Renombrando columna FECHA_O_COMPRA a FECHA_COMPRA
df_landing_jesus_maria=df_landing_jesus_maria.withColumnRenamed("FECHA_O_COMPRA","FECHA_COMPRA")


# In[197]:


#Agregando columna COD_PERIODO (AAAAMM) 
df_landing_jesus_maria=df_landing_jesus_maria.withColumn("AÑO_COMPRA",date_format("FECHA_COMPRA", "yyyy"))
df_landing_jesus_maria=df_landing_jesus_maria.withColumn("MES_COMPRA",date_format("FECHA_COMPRA", "MM"))
df_landing_jesus_maria=df_landing_jesus_maria.withColumn("COD_PERIODO",concat("AÑO_COMPRA","MES_COMPRA"))


# In[198]:


#Agregando columna COD_DISTRITO (Iniciales del distrito)
df_landing_jesus_maria=df_landing_jesus_maria.withColumn("COD_DISTRITO",lit("JM"))


# In[199]:


ruta_curated_jesus_maria_google_cloud = "gs://dmc-pea-de-grupo-2-datalake/curated/MunicipalidadJesusMaria/"


# In[200]:


df_landing_jesus_maria.write.mode("overwrite").format("parquet").partitionBy("COD_PERIODO").save(ruta_curated_jesus_maria_google_cloud)


# ### - Municipalidad Lince

# In[201]:


#Elimimando columna FECHA_CORTE
df_landing_lince_procesado=df_landing_lince.drop("FECHA_CORTE")


# In[202]:


#Renombrando los nombres de las Columnas
df_landing_lince_procesado=df_landing_lince_procesado.withColumnRenamed("RUC","RUC_PROVEEDOR")
df_landing_lince_procesado=df_landing_lince_procesado.withColumnRenamed("RAZON_SOCIAL","RAZON_SOCIAL_PROVEEDOR")
df_landing_lince_procesado=df_landing_lince_procesado.withColumnRenamed("FECHA_ORDEN","FECHA_COMPRA")
df_landing_lince_procesado=df_landing_lince_procesado.withColumnRenamed("PERIODO","MES_COMPRA")


# In[203]:


#Eliminamos los espacios en blancos de los extremos 
df_landing_lince_procesado = df_landing_lince_procesado.withColumn('TIPO_ORDEN', trim('TIPO_ORDEN'))
df_landing_lince_procesado = df_landing_lince_procesado.withColumn('RAZON_SOCIAL_PROVEEDOR', trim('RAZON_SOCIAL_PROVEEDOR'))
df_landing_lince_procesado = df_landing_lince_procesado.withColumn('DEPARTAMENTO', trim('DEPARTAMENTO'))
df_landing_lince_procesado = df_landing_lince_procesado.withColumn('PROVINCIA', trim('PROVINCIA'))
df_landing_lince_procesado = df_landing_lince_procesado.withColumn('DISTRITO', trim('DISTRITO'))


# In[204]:


#Convertimos FECHA_COMPRA a tipo de dato date
df_landing_lince_procesado = df_landing_lince_procesado.withColumn("FECHA_COMPRA",to_date(col("FECHA_COMPRA"),"yyyyMMdd"))


# ### REGLA DE NEGOCIO:
# 1. Toda descripcion o caracteres en mayuscula.
# 2. Agregar columna COD_DISTRITO (Iniciales del distrito)
# 3. Particionar por COD_PERIODO (AAAAMM)
# 

# In[205]:


#Convertimos los campos string a MAYUSCULA
df_landing_lince_procesado = df_landing_lince_procesado.withColumn("TIPO_ORDEN",upper("TIPO_ORDEN"))
df_landing_lince_procesado = df_landing_lince_procesado.withColumn("RAZON_SOCIAL_PROVEEDOR",upper("RAZON_SOCIAL_PROVEEDOR"))
df_landing_lince_procesado = df_landing_lince_procesado.withColumn("DEPARTAMENTO",upper("DEPARTAMENTO"))
df_landing_lince_procesado = df_landing_lince_procesado.withColumn("PROVINCIA",upper("PROVINCIA"))
df_landing_lince_procesado = df_landing_lince_procesado.withColumn("DISTRITO",upper("DISTRITO"))


# In[206]:


#Agregando columna COD_DISTRITO (Iniciales del distrito)
df_landing_lince_procesado=df_landing_lince_procesado.withColumn("COD_DISTRITO",lit("LINC"))


# In[207]:


#Agregando columna COD_PERIODO (AAAAMM) 
df_landing_lince_procesado=df_landing_lince_procesado.withColumn("AÑO_COMPRA",date_format("FECHA_COMPRA", "yyyy"))
df_landing_lince_procesado=df_landing_lince_procesado.withColumn("MES_COMPRA",date_format("FECHA_COMPRA", "MM"))
df_landing_lince_procesado=df_landing_lince_procesado.withColumn("COD_PERIODO",concat("AÑO_COMPRA","MES_COMPRA"))


# In[208]:


df_landing_lince_procesado_V2=df_landing_lince_procesado.select("TIPO_ORDEN","RUC_PROVEEDOR","NUMERO_ORDEN","FECHA_COMPRA","MONTO","RAZON_SOCIAL_PROVEEDOR","AÑO_COMPRA","MES_COMPRA","DEPARTAMENTO","PROVINCIA","DISTRITO","UBIGEO","COD_PERIODO","COD_DISTRITO")


# In[209]:


ruta_curated_lince_google_cloud = "gs://dmc-pea-de-grupo-2-datalake/curated/MunicipalidadLince/"


# In[210]:


df_landing_lince_procesado_V2.write.mode("overwrite").format("parquet").partitionBy("COD_PERIODO").save(ruta_curated_lince_google_cloud)


# ### - Municipalidad San Bartolo

# In[211]:


#Elimimando columna FECHA_CORTE
df_landing_san_bartolo_procesado=df_landing_san_bartolo.drop("FECHA_CORTE")


# In[212]:


#Renombrando los nombres de las Columnas
df_landing_san_bartolo_procesado=df_landing_san_bartolo_procesado.withColumnRenamed("DESCRIPCION_ORDEN","DESCRIPCION")
df_landing_san_bartolo_procesado=df_landing_san_bartolo_procesado.withColumnRenamed("FECHA_ORDEN","FECHA_COMPRA")
df_landing_san_bartolo_procesado=df_landing_san_bartolo_procesado.withColumnRenamed("MONTO_TOTAL","MONTO")
df_landing_san_bartolo_procesado=df_landing_san_bartolo_procesado.withColumnRenamed("PERIODO_MENSUAL","MES_COMPRA")
df_landing_san_bartolo_procesado=df_landing_san_bartolo_procesado.withColumnRenamed("PERIODO_ANUAL","AÑO_COMPRA")


# In[213]:


#Eliminamos los espacios en blancos de los extremos 
df_landing_san_bartolo_procesado = df_landing_san_bartolo_procesado.withColumn('GOBIERNO_LOCAL', trim('GOBIERNO_LOCAL'))
df_landing_san_bartolo_procesado = df_landing_san_bartolo_procesado.withColumn('TIPO_ORDEN', trim('TIPO_ORDEN'))
df_landing_san_bartolo_procesado = df_landing_san_bartolo_procesado.withColumn('DEPARTAMENTO', trim('DEPARTAMENTO'))
df_landing_san_bartolo_procesado = df_landing_san_bartolo_procesado.withColumn('PROVINCIA', trim('PROVINCIA'))
df_landing_san_bartolo_procesado = df_landing_san_bartolo_procesado.withColumn('DISTRITO', trim('DISTRITO'))
df_landing_san_bartolo_procesado = df_landing_san_bartolo_procesado.withColumn('DESCRIPCION', trim('DESCRIPCION'))


# In[214]:


#Convertimos FECHA_COMPRA a tipo de dato date
df_landing_san_bartolo_procesado = df_landing_san_bartolo_procesado.withColumn("FECHA_COMPRA",to_date(col("FECHA_COMPRA"),"yyyyMMdd"))


# ### REGLA DE NEGOCIO:
# 1. Toda descripcion o caracteres en mayuscula.
# 2. Agregar columna COD_DISTRITO (Iniciales del distrito)
# 3. Particionar por COD_PERIODO (AAAAMM)
# 

# In[215]:


#Convertimos los campos STRING a MAYUSCULA
df_landing_san_bartolo_procesado = df_landing_san_bartolo_procesado.withColumn("TIPO_ORDEN",upper("TIPO_ORDEN"))
df_landing_san_bartolo_procesado = df_landing_san_bartolo_procesado.withColumn("DESCRIPCION",upper("DESCRIPCION"))
df_landing_san_bartolo_procesado = df_landing_san_bartolo_procesado.withColumn("DEPARTAMENTO",upper("DEPARTAMENTO"))
df_landing_san_bartolo_procesado = df_landing_san_bartolo_procesado.withColumn("PROVINCIA",upper("PROVINCIA"))
df_landing_san_bartolo_procesado = df_landing_san_bartolo_procesado.withColumn("DISTRITO",upper("DISTRITO"))


# In[216]:


#Agregando columna COD_DISTRITO (Iniciales del distrito)
df_landing_san_bartolo_procesado=df_landing_san_bartolo_procesado.withColumn("COD_DISTRITO",lit("SB"))


# In[217]:


#Agregando columna COD_PERIODO (AAAAMM) 
df_landing_san_bartolo_procesado=df_landing_san_bartolo_procesado.withColumn("AÑO_COMPRA",date_format("FECHA_COMPRA", "yyyy"))
df_landing_san_bartolo_procesado=df_landing_san_bartolo_procesado.withColumn("MES_COMPRA",date_format("FECHA_COMPRA", "MM"))
df_landing_san_bartolo_procesado=df_landing_san_bartolo_procesado.withColumn("COD_PERIODO",concat("AÑO_COMPRA","MES_COMPRA"))


# In[218]:


df_landing_san_bartolo_procesado_V2=df_landing_san_bartolo_procesado.select("TIPO_ORDEN","RUC_GOBIERNO_LOCAL","GOBIERNO_LOCAL","NUMERO_ORDEN","NUMERO_SIAF","FECHA_COMPRA","MONTO","RUC_PROVEEDOR","DESCRIPCION","AÑO_COMPRA","MES_COMPRA","DEPARTAMENTO","PROVINCIA","DISTRITO","UBIGEO","COD_PERIODO","COD_DISTRITO")


# In[219]:


ruta_curated_san_bartolo_google_cloud = "gs://dmc-pea-de-grupo-2-datalake/curated/MunicipalidadSanBartolo/"


# In[220]:


df_landing_san_bartolo_procesado_V2.write.mode("overwrite").format("parquet").partitionBy("COD_PERIODO").save(ruta_curated_san_bartolo_google_cloud)


# ### - Municipalidad Los Olivos

# In[221]:


#Elimimando columna FECHA_CORTE
df_landing_los_olivos_procesado=df_landing_los_olivos.drop("FECHA_CORTE")


# In[222]:


#Renombrando los nombres de las Columnas
df_landing_los_olivos_procesado=df_landing_los_olivos_procesado.withColumnRenamed("DESCRIPCION_ORDEN","DESCRIPCION")
df_landing_los_olivos_procesado=df_landing_los_olivos_procesado.withColumnRenamed("FECHA_ORDEN","FECHA_COMPRA")
df_landing_los_olivos_procesado=df_landing_los_olivos_procesado.withColumnRenamed("MONTO_TOTAL","MONTO")
df_landing_los_olivos_procesado=df_landing_los_olivos_procesado.withColumnRenamed("PERIODO_MENSUAL","MES_COMPRA")
df_landing_los_olivos_procesado=df_landing_los_olivos_procesado.withColumnRenamed("PERIODO_ANUAL","AÑO_COMPRA")
df_landing_los_olivos_procesado=df_landing_los_olivos_procesado.withColumnRenamed("RAZON_SOCIAL","RAZON_SOCIAL_PROVEEDOR")


# In[223]:


#Eliminamos los espacios en blancos de los extremos 
df_landing_los_olivos_procesado = df_landing_los_olivos_procesado.withColumn('GOBIERNO_LOCAL', trim('GOBIERNO_LOCAL'))
df_landing_los_olivos_procesado = df_landing_los_olivos_procesado.withColumn('TIPO_ORDEN', trim('TIPO_ORDEN'))
df_landing_los_olivos_procesado = df_landing_los_olivos_procesado.withColumn('DEPARTAMENTO', trim('DEPARTAMENTO'))
df_landing_los_olivos_procesado = df_landing_los_olivos_procesado.withColumn('PROVINCIA', trim('PROVINCIA'))
df_landing_los_olivos_procesado = df_landing_los_olivos_procesado.withColumn('DISTRITO', trim('DISTRITO'))
df_landing_los_olivos_procesado = df_landing_los_olivos_procesado.withColumn('DESCRIPCION', trim('DESCRIPCION'))
df_landing_los_olivos_procesado = df_landing_los_olivos_procesado.withColumn('RAZON_SOCIAL_PROVEEDOR', trim('RAZON_SOCIAL_PROVEEDOR'))


# In[224]:


#Convertimos FECHA_COMPRA a tipo de dato date
df_landing_los_olivos_procesado = df_landing_los_olivos_procesado.withColumn("FECHA_COMPRA",to_date(col("FECHA_COMPRA"),"yyyyMMdd"))


# ### REGLA DE NEGOCIO:
# 1. Toda descripcion o caracteres en mayuscula.
# 2. Agregar columna COD_DISTRITO (Iniciales del distrito)
# 3. Particionar por COD_PERIODO (AAAAMM)
# 

# In[225]:


#Convertimos los campos STRING a MAYUSCULA
df_landing_los_olivos_procesado = df_landing_los_olivos_procesado.withColumn("TIPO_ORDEN",upper("TIPO_ORDEN"))
df_landing_los_olivos_procesado = df_landing_los_olivos_procesado.withColumn("DESCRIPCION",upper("DESCRIPCION"))
df_landing_los_olivos_procesado = df_landing_los_olivos_procesado.withColumn("DEPARTAMENTO",upper("DEPARTAMENTO"))
df_landing_los_olivos_procesado = df_landing_los_olivos_procesado.withColumn("PROVINCIA",upper("PROVINCIA"))
df_landing_los_olivos_procesado = df_landing_los_olivos_procesado.withColumn("DISTRITO",upper("DISTRITO"))
df_landing_los_olivos_procesado = df_landing_los_olivos_procesado.withColumn("GOBIERNO_LOCAL",upper("GOBIERNO_LOCAL"))
df_landing_los_olivos_procesado = df_landing_los_olivos_procesado.withColumn("RAZON_SOCIAL_PROVEEDOR",upper("RAZON_SOCIAL_PROVEEDOR"))


# In[226]:


#Agregando columna COD_DISTRITO (Iniciales del distrito)
df_landing_los_olivos_procesado=df_landing_los_olivos_procesado.withColumn("COD_DISTRITO",lit("LO"))


# In[227]:


#Agregando columna COD_PERIODO (AAAAMM) 
df_landing_los_olivos_procesado=df_landing_los_olivos_procesado.withColumn("AÑO_COMPRA",date_format("FECHA_COMPRA", "yyyy"))
df_landing_los_olivos_procesado=df_landing_los_olivos_procesado.withColumn("MES_COMPRA",date_format("FECHA_COMPRA", "MM"))
df_landing_los_olivos_procesado=df_landing_los_olivos_procesado.withColumn("COD_PERIODO",concat("AÑO_COMPRA","MES_COMPRA"))


# In[228]:


df_landing_los_olivos_procesado_V2=df_landing_los_olivos_procesado.select("TIPO_ORDEN","RUC_GOBIERNO_LOCAL","GOBIERNO_LOCAL","NUMERO_ORDEN","NUMERO_SIAF","FECHA_COMPRA","MONTO","RUC_PROVEEDOR","RAZON_SOCIAL_PROVEEDOR","DESCRIPCION","AÑO_COMPRA","MES_COMPRA","DEPARTAMENTO","PROVINCIA","DISTRITO","UBIGEO","COD_PERIODO","COD_DISTRITO")


# In[229]:


ruta_curated_los_olivos_google_cloud = "gs://dmc-pea-de-grupo-2-datalake/curated/MunicipalidadLosOlivos/"


# In[230]:


df_landing_los_olivos_procesado_V2.write.mode("overwrite").format("parquet").partitionBy("COD_PERIODO").save(ruta_curated_los_olivos_google_cloud)


# In[ ]:





# ## 3° POBLANDO CAPA FUNCTIONAL

# **15.1** Definimos ruta tablas requeridas apuntando a la capa curated 

# In[231]:


ruta_curated_JesusMaria = "gs://dmc-pea-de-grupo-2-datalake/curated/MunicipalidadJesusMaria/"
ruta_curated_Lince = "gs://dmc-pea-de-grupo-2-datalake/curated/MunicipalidadLince/"
ruta_curated_LosOlivos = "gs://dmc-pea-de-grupo-2-datalake/curated/MunicipalidadLosOlivos/"
ruta_curated_SanBartolo = "gs://dmc-pea-de-grupo-2-datalake/curated/MunicipalidadSanBartolo/"


# **15.2** Creamos el dataframe para cada tabla.

# In[232]:


df_jm_curated = spark.read.format("parquet").option("header","true").load(ruta_curated_JesusMaria)
df_lince_curated = spark.read.format("parquet").option("header","true").load(ruta_curated_Lince)
df_olivos_curated = spark.read.format("parquet").option("header","true").load(ruta_curated_LosOlivos)
df_sb_curated = spark.read.format("parquet").option("header","true").load(ruta_curated_SanBartolo)


# **15.3** Mostramos datos de los dataframes

# **Realizar la unión de las 4 tablas, con los siguientes campos requeridos para su posterior analisis:**
# 
# NOTA: En caso alguna de las tablas de los distritos no posea dichos campos detallados, colocar como valor INDEFINIDO
# * TIPO_ORDEN
# * RUC_GOBIERNO_LOCAL
# * NUMERO_SIAF
# * FECHA_COMPRA
# * MONTO
# * RUC_PROVEEDOR
# * RAZON_SOCIAL_PROVEEDOR
# * DESCRIPCION
# * AÑO_COMPRA
# * MES_COMPRA
# * DEPARTAMENTO
# * PROVINCIA
# * DISTRITO
# * COD_DISTRITO
# * COD_PERIODO

# In[234]:


df_jm_curated_v1 = df_jm_curated.withColumn('RUC_PROVEEDOR',lit("INDEFINIDO")).select(
    col('TIPO_ORDEN'),
    col('RUC_GOBIERNO_LOCAL'),
    col('SIAF_O_COMPRA').alias("NUMERO_SIAF"),
    col('FECHA_COMPRA'),
    col('MONTO'),
    col('RUC_PROVEEDOR'),
    col('RAZON_SOCIAL_PROVEEDOR'),
    col('DESCRIPCION'),
    col('AÑO_COMPRA'),
    col('MES_COMPRA'),
    col('DEPARTAMENTO'),
    col('PROVINCIA'),
    col('DISTRITO'),
    col('COD_DISTRITO'),
    col('COD_PERIODO')
    )


# In[235]:


df_lince_curated_v1 = df_lince_curated.withColumn('RUC_GOBIERNO_LOCAL',lit("INDEFINIDO")).withColumn('DESCRIPCION',lit("INDEFINIDO")).select(
    col('TIPO_ORDEN'),
    col('RUC_GOBIERNO_LOCAL'),
    col('NUMERO_ORDEN').alias("NUMERO_SIAF"),
    col('FECHA_COMPRA'),
    col('MONTO'),
    col('RUC_PROVEEDOR'),
    col('RAZON_SOCIAL_PROVEEDOR'),
    col('DESCRIPCION'),
    col('AÑO_COMPRA'),
    col('MES_COMPRA'),
    col('DEPARTAMENTO'),
    col('PROVINCIA'),
    col('DISTRITO'),
    col('COD_DISTRITO'),
    col('COD_PERIODO')
    )


# In[236]:


df_olivos_curated_v1 = df_olivos_curated.select(
    col('TIPO_ORDEN'),
    col('RUC_GOBIERNO_LOCAL'),
    col('NUMERO_SIAF'),
    col('FECHA_COMPRA'),
    col('MONTO'),
    col('RUC_PROVEEDOR'),
    col('RAZON_SOCIAL_PROVEEDOR'),
    col('DESCRIPCION'),
    col('AÑO_COMPRA'),
    col('MES_COMPRA'),
    col('DEPARTAMENTO'),
    col('PROVINCIA'),
    col('DISTRITO'),
    col('COD_DISTRITO'),
    col('COD_PERIODO')
    )


# In[237]:


df_sb_curated_v1 = df_sb_curated.withColumn('RAZON_SOCIAL_PROVEEDOR',lit("INDEFINIDO")).select(
    col('TIPO_ORDEN'),
    col('RUC_GOBIERNO_LOCAL'),
    col('NUMERO_SIAF'),
    col('FECHA_COMPRA'),
    col('MONTO'),
    col('RUC_PROVEEDOR'),
    col('RAZON_SOCIAL_PROVEEDOR'),
    col('DESCRIPCION'),
    col('AÑO_COMPRA'),
    col('MES_COMPRA'),
    col('DEPARTAMENTO'),
    col('PROVINCIA'),
    col('DISTRITO'),
    col('COD_DISTRITO'),
    col('COD_PERIODO')
    )


# **Realizando la union de la tablas**

# In[238]:


df_union_distritos=df_jm_curated_v1.union(df_lince_curated_v1).union(df_olivos_curated_v1).union(df_sb_curated_v1)


# **Estandarizando valores de la columna TIPO_ORDEN**

# In[239]:


df_bu = df_union_distritos


# In[240]:


df_bu.withColumn("TIPO_ORDEN", when(df_bu.TIPO_ORDEN == "COMPRA","COMPRAS")
                                    .when(df_bu.TIPO_ORDEN == "ORD. COMPRA","COMPRAS")
                                    .when(df_bu.TIPO_ORDEN == "ORD.COMPRA","COMPRAS")
                                    .when(df_bu.TIPO_ORDEN == "SERVICIO","SERVICIOS")
                                    .when(df_bu.TIPO_ORDEN == "ORD.SERVICIO","SERVICIOS")
                                    .when(df_bu.TIPO_ORDEN == "ORD. SERVICIO","SERVICIOS")
                                    .when(df_bu.TIPO_ORDEN == "SERVIVIOS","SERVICIOS")
                                 .otherwise(df_bu.TIPO_ORDEN))
tipo_orden = ['COMPRAS', 'SERVICIOS'] 
df_bu[df_bu['TIPO_ORDEN'].isin(tipo_orden)] 


# **Eliminando filas que no cuenten con el COD_PERIODO**

# In[241]:


df_bu = df_bu.where(col('COD_PERIODO').isNotNull())


# In[161]:


ruta_functional = "gs://dmc-pea-de-grupo-2-datalake/functional/OrdenesCompraDistritosLima/"
df_bu.write.mode("overwrite").format("parquet").save(ruta_functional)


# 
# #### **Elaborado por Grupo 2 del PEA Data Engineer :**
# *  Delgado Alba, Manuel
# * Vilcacuri Huamani Elizabeth 
# #### Linkedin: https://www.linkedin.com/in/juan-salinas/

# In[ ]:




