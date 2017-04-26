#!/bin/sh

${SPARK_HOME}/bin/spark-submit \
    --class software.uncharted.xdata.tiling.jobs.XYHeatmapJob \
    ../lib/xdata-salt-tiling.jar \
    geo-heatmap-example.conf
