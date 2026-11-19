# Local image: Flink 2.2.1 + PyFlink + Kafka support.
# We write Python and SQL. The JVM remains part of Flink's runtime.
FROM flink:2.2.1-scala_2.12-java21

ARG FLINK_VERSION=2.2.1
ARG KAFKA_CONNECTOR_VERSION=5.0.0-2.2
ARG KAFKA_CLIENTS_VERSION=4.2.0

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        python3 \
        python3-dev \
        python3-pip \
        python3-venv \
    && python3 -m venv /opt/pyflink-venv \
    && /opt/pyflink-venv/bin/python -c "import sys; v=sys.version_info[:2]; assert (3, 9) <= v <= (3, 12), f'PyFlink 2.2.1 requires Python 3.9-3.12; found {v}'" \
    && /opt/pyflink-venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/pyflink-venv/bin/pip install --no-cache-dir "apache-flink==${FLINK_VERSION}" \
    && ln -sf /opt/pyflink-venv/bin/python /usr/local/bin/python \
    && ln -sf /opt/pyflink-venv/bin/pip /usr/local/bin/pip \
    && mkdir -p /opt/flink/connectors/sql \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/opt/pyflink-venv/bin:${PATH}"
ENV PYFLINK_CLIENT_EXECUTABLE=/opt/pyflink-venv/bin/python
ENV PYFLINK_PYTHON=/opt/pyflink-venv/bin/python

# PyFlink DataStream Kafka connector and its Kafka client runtime dependency.
ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar /opt/flink/lib/
ADD https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar /opt/flink/lib/

# Flink SQL connector bundle and JSON format are loaded by the SQL Client with ADD JAR.
# Keeping them outside /opt/flink/lib avoids loading a second copy of the DataStream connector.
ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar /opt/flink/connectors/sql/
ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/${FLINK_VERSION}/flink-json-${FLINK_VERSION}.jar /opt/flink/connectors/sql/

# Remote ADD may preserve restrictive file modes. Make connector JARs readable
# by the non-root Flink process in every container built from this image.
RUN chmod 0644 \
    /opt/flink/lib/flink-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
    /opt/flink/lib/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar \
    /opt/flink/connectors/sql/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
    /opt/flink/connectors/sql/flink-json-${FLINK_VERSION}.jar

USER flink
