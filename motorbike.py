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
        "Harley-Davidson", "Yamaha", "Honda", 
        "Ducati", "Kawasaki", "BMW", 
        "Suzuki", "Triumph", "KTM", "Indian"
    ]
    
    # Filtrar motocicletas por las mejores marcas (ignorando mayúsculas/minúsculas) y eliminar duplicados de modelo
    query_best_brands = f"""
        SELECT DISTINCT Brand, Model
        FROM motorcycles
        WHERE LOWER(Brand) IN ({', '.join([f"'{brand.lower()}" for brand in best_brands])})
        ORDER BY Brand ASC, Model ASC
    """
    df_best_brands = spark.sql(query_best_brands)
    
    # Mostrar algunos resultados del primer filtro
    df_best_brands.show(20)
    
    # Guardar los resultados en formato JSON
    df_best_brands.write.mode("overwrite").json("results/motorcycle_sales_best_brands")
    
    # Obtener la motocicleta con mayor HP por cada marca
    query_max_hp = """
        SELECT Brand, Model, `Power (hp)`
        FROM (
            SELECT Brand, Model, `Power (hp)`, 
                   ROW_NUMBER() OVER (PARTITION BY LOWER(Brand) ORDER BY `Power (hp)` DESC) AS rank
            FROM motorcycles
        )
        WHERE rank = 1
    """
    df_max_hp = spark.sql(query_max_hp)
    
    # Mostrar algunos resultados del segundo filtro
    df_max_hp.show(20)
    
    # Guardar los resultados en formato JSON
    df_max_hp.write.mode("overwrite").json("results/motorcycle_sales_max_hp")
    
    # Cerrar sesión de Spark
    spark.stop()
