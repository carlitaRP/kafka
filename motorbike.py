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

    # Verificar si el DataFrame se cargó correctamente
    if df_bikes.isEmpty():
        print("El archivo CSV no tiene datos o no se cargó correctamente.")
        spark.stop()
        exit()

    # Crear vista temporal
    df_bikes.createOrReplaceTempView("bikes")

    # Lista de las mejores marcas de motocicletas
    top_brands = [
        "Honda", "Yamaha", "Kawasaki", "Suzuki", "Ducati", 
        "BMW", "KTM", "Harley-Davidson", "Triumph", "Aprilia"
    ]

    # Convertir la lista a una consulta SQL válida
    brands_str = "', '".join(top_brands)  # Convertir lista a cadena SQL
    query = f"""
        SELECT Brand, Model
        FROM bikes
        WHERE LOWER(Brand) IN ('{brands_str}')
        ORDER BY Brand ASC
    """

    df_filtered = spark.sql(query)

    # Mostrar algunos resultados
    df_filtered.show(20)

    # Guardar los resultados en formato JSON
    df_filtered.write.mode("overwrite").json("results/motorcycle")

    # Cerrar sesión de Spark
    spark.stop()
