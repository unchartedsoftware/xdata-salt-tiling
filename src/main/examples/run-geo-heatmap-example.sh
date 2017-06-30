#!/bin/sh

${SPARK_HOME}/bin/spark-submit \
    --class software.uncharted.contrib.tiling.jobs.XYHeatmapJob \
    ../lib/salt-tiling-contrib.jar \
    geo-heatmap-example.conf
