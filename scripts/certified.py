from pyspark.sql import SparkSession


def certify_data(raw_data_path, refined_path):
    spark = SparkSession.builder.getOrCreate()

    print("[Certified] Loading Data")
    df = spark.read.csv(raw_data_path, header=True, inferSchema=True)

    print("[Certified] Saving data in long format")
    df.write.mode("overwrite").option("header", True).csv(refined_path + "all_data_long_format")

    print("[Certified] Saving data in wide format")
    df = df.groupBy('cadenaComercial').pivot('categoria').sum('precio').orderBy('cadenaComercial').na.fill(0)

    df.write.mode("overwrite").option("header", True).csv(refined_path + "all_data_wide_format")


if __name__ == "__main__":
    print("[Certified] Start")
    refined_data_path = "../data/refined/all_data_refined"
    certified_path = "../data/certified/"
    certify_data(refined_data_path, certified_path)
    print("[Certified] End")