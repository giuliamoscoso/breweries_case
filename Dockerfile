FROM apache/airflow:2.10.5-python3.9 AS airflow

ENV IMAGE_BUILD_VERSION=2.10.5.0

USER root

# Install Apache Spark
RUN mkdir -p /opt/spark/jars

# Download Azure Storage connector JARs
RUN curl -o /opt/spark/jars/hadoop-azure-3.3.5.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.5/hadoop-azure-3.3.5.jar && \
    curl -o /opt/spark/jars/azure-storage-8.6.6.jar https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar && \
    curl -o /opt/spark/jars/azure-storage-blob-12.19.0.jar https://repo1.maven.org/maven2/com/azure/azure-storage-blob/12.19.0/azure-storage-blob-12.19.0.jar && \
    curl -o /opt/spark/jars/azure-storage-common-12.19.0.jar https://repo1.maven.org/maven2/com/azure/azure-storage-common/12.19.0/azure-storage-common-12.19.0.jar && \
    curl -o /opt/spark/jars/azure-core-1.34.0.jar https://repo1.maven.org/maven2/com/azure/azure-core/1.34.0/azure-core-1.34.0.jar && \
    curl -o /opt/spark/jars/azure-core-http-netty-1.12.8.jar https://repo1.maven.org/maven2/com/azure/azure-core-http-netty/1.12.8/azure-core-http-netty-1.12.8.jar

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

    
# Download Hadoop from a mirror
RUN curl -fsSL https://ftp-stud.hs-esslingen.de/pub/Mirrors/ftp.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    --output /tmp/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar --directory /opt -zxf /tmp/hadoop-${HADOOP_VERSION}.tar.gz && \
    rm /tmp/hadoop-${HADOOP_VERSION}.tar.gz && \
    ln -s /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME}

# Use cached Hadoop tarball if available to speed up builds
RUN if ! [ -f /tmp/hadoop-${HADOOP_VERSION}.tar.gz ]; then \
        curl -fsSL https://ftp-stud.hs-esslingen.de/pub/Mirrors/ftp.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
            --output /tmp/hadoop-${HADOOP_VERSION}.tar.gz; \
    fi  && \
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