# Airflow
FROM apache/airflow:2.11.0

USER root

# Env vars
ENV SPARK_VERSION=3.5.3 \
    HADOOP_VERSION=3 \
    HADOOP_AWS_VERSION=3.3.4 \
    AWS_SDK_BUNDLE_VERSION=1.12.262 \
    DELTA_VERSION=3.3.2 \
    SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=/usr/local/bin/python \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python
ENV PATH="${SPARK_HOME}/bin:${PATH}"

RUN apt-get update && \
    apt-get install -y curl openjdk-17-jre-headless && \
    rm -rf /var/lib/apt/lists/*

RUN ARCH=$(dpkg --print-architecture) && \
    if [ "$ARCH" = "amd64" ]; then \
        export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64; \
    elif [ "$ARCH" = "arm64" ]; then \
        export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64; \
    fi

# Spark
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME

# Add JARs
RUN curl -L -o "${SPARK_HOME}/jars/delta-spark_2.12-${DELTA_VERSION}.jar" \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar \
    && curl -L -o "${SPARK_HOME}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    && curl -L -o "${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar

USER airflow

# Install requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
