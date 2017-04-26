#!/bin/sh

${SPARK_HOME}/bin/spark-submit \
    --class software.uncharted.xdata.tiling.jobs.IPHeatmapJob \
    ../lib/xdata-salt-tiling.jar \
    ip-heatmap-example.conf
