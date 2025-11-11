FROM apache/airflow:2.10.5-python3.9 AS airflow

ENV IMAGE_BUILD_VERSION=2.10.5.0

USER root

# Create a directory to store connector JARs
RUN mkdir -p /opt/spark/jars

# Download the GCS and BigQuery connector JARs
RUN curl -o /opt/spark/jars/gcs-connector-latest-hadoop2.jar https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop2.jar && \
    curl -o /opt/spark/jars/spark-3.3-bigquery-0.34.0.jar https://storage.googleapis.com/spark-lib/bigquery/spark-3.3-bigquery-0.34.0.jar

# Install Hadoop
ARG HADOOP_CLASSPATH
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_VERSION=3.3.5 \
    HADOOP_COMMON_HOME=${HADOOP_HOME} \
    HADOOP_HDFS_HOME=${HADOOP_HOME} \
    HADOOP_MAPRED_HOME=${HADOOP_HOME} \
    HADOOP_YARN_HOME=${HADOOP_HOME} \
    HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop \
    PATH=${PATH}:${HADOOP_HOME}/bin \
    CLASSPATH=${HADOOP_CLASSPATH}

RUN curl -fsSL https://ftp-stud.hs-esslingen.de/pub/Mirrors/ftp.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    --output /tmp/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar --directory /opt -zxf /tmp/hadoop-${HADOOP_VERSION}.tar.gz && \
    rm /tmp/hadoop-${HADOOP_VERSION}.tar.gz && \
    ln -s /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME}

# Install necessary dependencies
RUN apt-get update && \
    apt-get install -y \
        build-essential \
        git \
        locales \
        watch \
        unzip \
        gcc \
        openssl \
        openjdk-17-jdk \
        ca-certificates \
        wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Set the working directory
WORKDIR /opt

# Copy and install Python dependencies
COPY ./requirements.txt /opt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install -r requirements.txt

# Set the final working directory
WORKDIR /tmp