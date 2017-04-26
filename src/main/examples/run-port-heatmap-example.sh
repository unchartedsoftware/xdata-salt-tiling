#!/bin/sh

${SPARK_HOME}/bin/spark-submit \
    --conf "spark.driver.memory=8G" \
    --class software.uncharted.xdata.tiling.jobs.XYHeatmapJob \
    ../lib/xdata-salt-tiling.jar \
    port-heatmap-example.conf
