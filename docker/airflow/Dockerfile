# Airflow + Java/PySpark (local Spark runner) + dbt (isolated venv).
ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.11
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION
# Must match the Hadoop client bundled with PySpark 3.5.x (3.3.4).
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262

USER root

# Java runtime for Spark (Debian bookworm default = OpenJDK 17).
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

# s3a connector so local Spark can read/write the S3 lake.
RUN mkdir -p /opt/spark-jars \
    && curl -fsSL -o /opt/spark-jars/hadoop-aws.jar \
       "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    && curl -fsSL -o /opt/spark-jars/aws-java-sdk-bundle.jar \
       "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"

# dbt lives in its own virtualenv: its pins conflict with Airflow's.
COPY docker/airflow/requirements-dbt.txt /tmp/requirements-dbt.txt
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt \
    && ln -s /opt/dbt-venv/bin/dbt /usr/local/bin/dbt

USER airflow

COPY requirements.txt docker/airflow/requirements-airflow.txt /tmp/
# Pin Airflow itself so pip never up/downgrades it while resolving extras.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" \
    -r /tmp/requirements.txt -r /tmp/requirements-airflow.txt
