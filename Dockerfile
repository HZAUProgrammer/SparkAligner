FROM gurvin/spark-base:2.0.1
MAINTAINER Pål Karlsrud <paal@128.no>

ENV SPARK_ALIGNER sparkaligner-1.1.5.jar
ENV SPARK_ALIGNER_SHA256 a4fc0b72b2847025b1d643576a8bcda394cae99a00c4265e8288646171745a3f

ENV TEST_DATA test_data.tar.gz
ENV TEST_DATA_SHA256 7b4fb7d72ea08c233d97a9dd7e07ab8dfaf0246725518604ec126fa62ebad985

RUN wget -q "https://f.128.no/genomics/${SPARK_ALIGNER}" && \
    echo "${SPARK_ALIGNER_SHA256} ${SPARK_ALIGNER}" | sha256sum -c - && \
    mv ${SPARK_ALIGNER} /usr/local/bin/${SPARK_ALIGNER} && \
    chmod +x /usr/local/bin/${SPARK_ALIGNER}

RUN wget -q "https://f.128.no/genomics/${TEST_DATA}" && \
    echo "${TEST_DATA_SHA256} ${TEST_DATA}" | sha256sum -c - && \
    mv ${TEST_DATA} /usr/local/share/${TEST_DATA} && \
    tar xvf /usr/local/share/${TEST_DATA} -C /usr/local/share/

ENTRYPOINT ["tini", "--", "spark-submit", "--conf",  "spark.task.cpus=4", "--class", "com.github.sparkaligner.SparkAligner", "/usr/local/bin/sparkaligner-1.1.5.jar"]
