#!/bin/sh

${SPARK_HOME}/bin/spark-submit \
    --class software.uncharted.contrib.tiling.jobs.IPHeatmapJob \
    ../lib/salt-tiling-contrib.jar \
    ip-heatmap-example.conf
