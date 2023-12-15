# Usa la imagen base de spark
FROM datamechanics/spark:3.2.1-hadoop-3.3.1-java-11-scala-2.12-python-3.8-dm18

USER root

# Establece el directorio de trabajo como /app
WORKDIR /app

RUN pip install --upgrade pip

COPY requirements.txt .
RUN pip3 install -r requirements.txt

# Copia los archivos de la carpeta spark_app
COPY ratings.csv /app/ratings.csv

COPY logica_spark.py /app/logica_spark.py

# Establece la variable de entorno PYSPARK_PYTHON
ENV PYSPARK_PYTHON=python3

# CMD o ENTRYPOINT según sea necesario
# CMD jupyter-lab --allow-root --no-browser --ip=0.0.0.0
CMD jupyter-lab --allow-root --no-browser --ip=0.0.0.0
