# Airflow worker image (Debian bookworm, which ships OpenJDK 17) with a JVM, PySpark and this provider. The Python minor must match the
# executor image (stock spark:<version>-python3 is 3.10).
ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.10
ARG PYSPARK_VERSION=4.2.0

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}
ARG PYSPARK_VERSION
ARG AIRFLOW_VERSION

LABEL org.opencontainers.image.source=https://github.com/sdaberdaku/pysparkonk8s \
      org.opencontainers.image.licenses=Apache-2.0

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java
ENV JAVA_HOME=/opt/java
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow
COPY --chown=airflow:0 dist/*.whl /tmp/wheels/
# pyspark-client must match pyspark: Spark Connect-aware code paths import it, and a skewed pair breaks at runtime.
# The apache-airflow pin keeps pip from moving Airflow while it resolves the wheel's requirements.
RUN pip install --no-cache-dir "pyspark==${PYSPARK_VERSION}" "pyspark-client==${PYSPARK_VERSION}" \
        "apache-airflow==${AIRFLOW_VERSION}" /tmp/wheels/*.whl \
    && pip check \
    && rm -rf /tmp/wheels
