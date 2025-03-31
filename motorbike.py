from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Crear sesión de Spark
    spark = SparkSession\
        .builder\
        .appName("motorcycle")\
        .getOrCreate()

    # Cargar el dataset
    path_bikes = "dataset.csv"
    df_bikes = spark.read.csv(path_bikes, header=True, inferSchema=True)

    # Crear vista temporal
    df_bikes.createOrReplaceTempView("bikes")

    # Lista de las mejores marcas de motocicletas
    top_brands = [
        "Honda", "Yamaha", "Kawasaki", "Suzuki", "Ducati", 
        "BMW", "KTM", "Harley-Davidson", "Triumph", "Aprilia"
    ]

    # Consulta SQL para filtrar marcas y ordenar alfabéticamente
    query = f"""
        SELECT Brand, Model
        FROM bikes
        WHERE Brand IN {tuple(top_brands)}
        ORDER BY Brand ASC
    """
    df_filtered = spark.sql(query)

    # Mostrar algunos resultados
    df_filtered.show(20)

    # Guardar los resultados en formato JSON
    df_filtered.write.mode("overwrite").json("results/motorcycle")

    # Cerrar sesión de Spark
    spark.stop()
