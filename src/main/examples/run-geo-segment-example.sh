#!/bin/sh

${SPARK_HOME}/bin/spark-submit \
    --conf "spark.driver.memory=8G" \
    --class software.uncharted.contrib.tiling.jobs.XYSegmentJob \
    ../lib/salt-tiling-contrib.jar \
    geo-segment-example.conf
