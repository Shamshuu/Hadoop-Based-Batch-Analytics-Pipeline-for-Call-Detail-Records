FROM bitnamilegacy/spark:3.5.0 AS spark-source

FROM apache/airflow:2.8.0
USER root

# Install OpenJDK 17 and procps
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy Spark from the spark-source stage to avoid internet download issues
COPY --from=spark-source /opt/bitnami/spark /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Switch back to airflow user
USER airflow
RUN pip install --no-cache-dir pyspark==3.5.0
