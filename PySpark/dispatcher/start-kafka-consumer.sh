#!/bin/bash

docker exec -it sapere-kafka /opt/kafka/bin/kafka-console-consumer.sh --topic processRequest --bootstrap-server 10.0.100.23:9092 --from-beginning --property print.key=true --property key.separator=":"