from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Crear sesión de Spark
    spark = SparkSession\
        .builder\
        .appName("motorcycle")\
        .getOrCreate()

    # Cargar el dataset
    path_bikes = "dataset.csv"
    df_bikes = spark.read.option("header", True).option("inferSchema", True).csv(path_bikes)

    # Verificar si se cargó correctamente
    df_bikes.show(5)
    print(df_bikes.columns)
    print(f"Total de filas en el dataset: {df_bikes.count()}")

    # Limpiar nombres de columnas
    df_bikes = df_bikes.toDF(*[c.strip() for c in df_bikes.columns])

    # Revisar si las marcas coinciden
    df_bikes.select("Brand").distinct().show(50)

    # Lista de las mejores marcas de motocicletas
    top_brands = [
        "Honda", "Yamaha", "Kawasaki", "Suzuki", "Ducati", 
        "BMW", "KTM", "Harley-Davidson", "Triumph", "Aprilia"
    ]

    # Consulta SQL para filtrar marcas y ordenar alfabéticamente
    query = f"""
        SELECT Brand, Model
        FROM bikes
        WHERE LOWER(Brand) IN {tuple(b.lower() for b in top_brands)}
        ORDER BY Brand ASC
    """
    df_filtered = spark.sql(query)

    # Mostrar algunos resultados
    df_filtered.show(10)
    print(f"Total de filas filtradas: {df_filtered.count()}")

    # Guardar los resultados en formato CSV para pruebas
    df_filtered.write.mode("overwrite").csv("results/motorcycle")

    # Cerrar sesión de Spark
    spark.stop()
