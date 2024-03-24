#!/bin/bash
[[ -z "${SPARK_ACTION}" ]] && { echo "SPARK_ACTION required"; exit 1; }

echo "Running action ${SPARK_ACTION}"
case ${SPARK_ACTION} in
"spark-shell")
./bin/spark-shell --master local[2]
;;
"pyspark")
./bin/pyspark --master local[2]
;;
"spark-submit-python")
 ./bin/spark-submit --master local[2] --packages $2 --py-files /opt/spark/$3 /opt/spark/$1
;;
esac