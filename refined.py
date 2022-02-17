from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType
from pyspark.sql.types import DateType


def refine_data(raw_data_path, refined_path):
    spark = SparkSession.builder.getOrCreate()

    print("[Refined] Loading Data")
    df = spark.read.csv(raw_data_path, header=True, inferSchema=True)

    print("[Refined] Table Shape:", (df.count(), len(df.columns)))

    print("[Refined] Cleaning Data")
    df = df.withColumn("precio", df["precio"].cast(FloatType())) \
           .withColumn("latitud", df["latitud"].cast(FloatType())) \
           .withColumn("longitud", df["longitud"].cast(FloatType())) \
           .withColumn("fechaRegistro", df["fechaRegistro"].cast(DateType()))

    df = df.filter(df.producto. isNotNull())
    df = df.na.fill('unknown', ["presentacion", "marca", "categoria",
                                "catalogo", "cadenaComercial", "giro",
                                "nombreComercial", "direccion", "estado",
                                "municipio"]) \
        .na.fill("2011-05-18 00:00:00", ["fechaRegistro"]) \
        .na.fill(-1, ["precio"]) \
        .na.fill(-91, ["latitud"]) \
        .na.fill(-181, ["longitud"])

    print("[Refined] Saving Data")
    df.write.mode("overwrite").option("header", True).csv(refined_path + "all_data_refined")
    print(df.printSchema())
    print(df.show())


if __name__ == "__main__":
    print("[Refined] Start")
    raw_data_path = "./data/raw/all_data.csv"
    refined_path = "./data/refined/"
    refine_data(raw_data_path, refined_path)
    print("[Refined] End")