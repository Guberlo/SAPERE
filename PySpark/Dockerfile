FROM openjdk:8-jre

ENV SPARK_DIR=/opt/spark
ENV PATH $SPARK_DIR/bin:$PATH

RUN apt-get update && apt-get -y install bash python3 python3-pip
RUN pip3 install pyspark numpy elasticsearch pyyaml nltk

ADD dispatcher/dispatcher.py $SPARK_DIR/dispatcher/dispatcher.py
ADD config/ $SPARK_DIR/config
ADD consumer/ $SPARK_DIR/consumer
ADD utils/ $SPARK_DIR/utils
ADD spark-manager.sh $SPARK_DIR/bin/spark-manager

RUN chmod +x $SPARK_DIR/bin/spark-manager
RUN cp -r /usr/local/bin/* $SPARK_DIR/bin/

WORKDIR ${SPARK_DIR}
ENTRYPOINT [ "spark-manager" ] 
