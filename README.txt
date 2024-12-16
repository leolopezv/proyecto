1. docker-compose up --build (build la primera vez)

2. contenedor zookeeper, exec:
    kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic clima-en-guayaquil
    kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic output
PD: no abrir ventanas para monitorear lo que se manda. luego no agarra en el programa

3. terminal vscode nueva:
    docker cp libs/spark-sql-kafka-0-10_2.11-2.4.5.jar jupyter:/opt/spark/jars/
    docker cp libs/kafka-clients-2.4.1.jar jupyter:/opt/spark/jars/
estos son los jars necesarios para la integración de spark y kafka

4. para ir a jupyter -> localhost:8888
4.1. si pide un token -> contenedor jupyter, exec:
    jupyter notebook list
copiar el token que salga en el link

5. crear los archivos 'sensor', 'procesamiento' y 'visualizacion' (ver carpeta jupyter o archivos) en jupyter
    los bloques de código están separados por ###

6. orden ideal para mandar a correr archivos:
    'procesamiento' -> 'visualizacion' -> 'middleware' -> 'sensor'

extra:
- cuando se detenga el archivo 'procesamiento', ejecutar los dos últimos bloques de código:
    for stream in spark.streams.active:
    print(f"Stopping stream: {stream.id}")
    stream.stop()
-----------------------------------------------------
    import shutil
    shutil.rmtree('./check.txt', ignore_errors=True)
-----------------------------------------------------
    si no se hace esto, van a salir errores

- si solo se detiene 'procesamiento', y se lo manda a correr sin detener a los demás, no saldrá nada en 'visualizacion'
- tldr: si se detiene un archivo, es mejor detener todos

LOCAL HOST DE JUPYTER
http://localhost:8888/tree?#notebooks