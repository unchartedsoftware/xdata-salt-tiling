#!/usr/bin/env bash
/opt/spark-1.5.2-bin-hadoop2.6/bin/spark-submit \
    --num-executors 12 \
    --executor-memory 10g \
    --executor-cores 4 \
    --class software.uncharted.xdata.sparkpipe.jobs.MercatorTimeHeatmapJob \
    ./lib/xdata-pipeline-ops.jar \
    tiling-example.conf
