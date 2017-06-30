#!/bin/sh

${SPARK_HOME}/bin/spark-submit \
    --conf "spark.driver.memory=8G" \
    --class software.uncharted.contrib.tiling.jobs.XYHeatmapJob \
    ../lib/salt-tiling-contrib.jar \
    port-heatmap-example.conf
