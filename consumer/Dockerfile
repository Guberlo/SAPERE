FROM openjdk:11

ENV SPARK_DIR=/opt/spark-3.1.1-bin-hadoop2.7
ENV PATH ${SPARK_DIR}/bin:$PATH
ENV LOG4J_SETTINGS="-Dlog4j.configuration=file:log4j.properties"

# Download spark binaries and extract them
WORKDIR /opt
ADD https://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz .
RUN tar -xf spark-3.1.1-bin-hadoop2.7.tgz
RUN rm -f spark-3.1.1-bin-hadoop2.7.tgz

# Add FAT jar and config files
ARG JAR_DIR=consumer
COPY target/consumer-1.0-SNAPSHOT-jar-with-dependencies.jar app/consumer.jar
COPY config.yaml app/config.yaml
COPY src/main/resources/log4j.properties ${SPARK_DIR}/conf/log4j.properties
COPY src/main/resources/en-ner-person.bin ${SPARK_DIR}/en-ner-person.bin
COPY src/main/resources/deberta_v3_base_token_classifier_ontonotes_en_3.4.4_3.0_1651826367819 ${SPARK_DIR}/deberta_v3_base_token_classifier_ontonotes_en_3.4.4_3.0_1651826367819

WORKDIR ${SPARK_DIR}
ENTRYPOINT [ "./bin/spark-submit", \
            "--master", "local[2]", \
            "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1", \
            "--driver-memory", "3g", \
            "--conf", "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/opt/spark-3.1.1-bin-hadoop2.7/conf/log4j.properties", \
            "--conf", "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/opt/spark-3.1.1-bin-hadoop2.7/conf/log4j.properties", \
            "--files", "/opt/spark-3.1.1-bin-hadoop2.7/conf/log4j.properties", \
            "/opt/app/consumer.jar" ]