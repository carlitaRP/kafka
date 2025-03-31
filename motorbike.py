from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Crear sesión de Spark
    spark = SparkSession\
        .builder\
        .appName("motorcycle_sales")\
        .getOrCreate()
    
    # Cargar el dataset de motocicletas
    path_motorcycles = "dataset.csv"
    df_motorcycles = spark.read.csv(path_motorcycles, header=True, inferSchema=True)
    
    # Crear vista temporal
    df_motorcycles.createOrReplaceTempView("motorcycles")
    
    # Lista de marcas populares o mejores marcas de motocicletas
    best_brands = [
        "aprilia", "bajaj", "benelli", "bmw", "ducati", "kawasaki"
    ]
    
    # Filtrar motocicletas por las mejores marcas y ordenar alfabéticamente por marca y modelo
    query = f"""
        SELECT Brand, Model
        FROM motorcycles
        WHERE Brand IN {tuple(best_brands)}
        ORDER BY Brand ASC, Model ASC
    """
    df_filtered = spark.sql(query)
    
    # Mostrar algunos resultados
    df_filtered.show(20)
    
    # Guardar los resultados en formato JSON
    df_filtered.write.mode("overwrite").json("results/motorcycle_sales")
    
    # Cerrar sesión de Spark
    spark.stop()
