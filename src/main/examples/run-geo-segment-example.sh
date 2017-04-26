#!/bin/sh

${SPARK_HOME}/bin/spark-submit \
    --conf "spark.driver.memory=8G" \
    --class software.uncharted.xdata.tiling.jobs.XYSegmentJob \
    ../lib/xdata-salt-tiling.jar \
    geo-segment-example.conf
