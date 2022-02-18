from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType,DateType


def refine_data(raw_data_path, refined_path):
    spark = SparkSession.builder.getOrCreate()

    print("[Refined] Loading Data")
    # Transforming csv data to dataframe
    df = spark.read.csv(raw_data_path, header=True, inferSchema=True)

    print("[Refined] Table Shape:", (df.count(), len(df.columns)))

    print("[Refined] Cleaning Data")
    #  Droping non decimal values from longitud column
    df = df.filter(df.longitud.rlike("^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)$"))

    #  Casting columns to the desired data type
    df = df.withColumn("precio", df.precio.cast(FloatType())) \
           .withColumn("latitud", df.latitud.cast(FloatType())) \
           .withColumn("longitud", df.longitud.cast(FloatType())) \
           .withColumn("fechaRegistro", df.fechaRegistro.cast(DateType()))

    #  Eliminating rows with no product name
    df = df.filter(df.producto.isNotNull())

    #  Replacing null values in all columns
    df = df.na.fill('unknown', ["presentacion", "marca", "categoria",
                                "catalogo", "cadenaComercial", "giro",
                                "nombreComercial", "direccion", "estado",
                                "municipio"]) \
        .na.fill("2011-05-18 00:00:00", ["fechaRegistro"]) \
        .na.fill(-1, ["precio"]) \
        .na.fill(-91, ["latitud"]) \
        .na.fill(-181, ["longitud"])

    #  Filtering only valid latitude and longitude values
    df = df.filter("latitud >= -91 and latitud <= 90")
    df = df.filter("longitud >= -181 and longitud <= 180")

    print("[Refined] Table Shape after cleaning:", (df.count(), len(df.columns)))

    print("[Refined] Saving Data")
    #  Saving dataframe data to a new csv file
    df.write.mode("overwrite").option("header", True).csv(refined_path)
    print(df.printSchema())
    print(df.show())


if __name__ == "__main__":
    print("[Refined] Start")
    raw_data_path = "../data/raw/all_data.csv"
    refined_path = "../data/refined/all_data_refined"
    refine_data(raw_data_path, refined_path)
    print("[Refined] End")