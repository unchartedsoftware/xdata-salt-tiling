#!/usr/bin/env bash
/opt/spark-2.0.1/bin/spark-submit \
    --num-executors 12 \
    --executor-memory 10g \
    --executor-cores 4 \
    --class software.uncharted.xdata.sparkpipe.jobs.XYTimeHeatmapJob \
    ../lib/xdata-pipeline-ops.jar \
    tiling-example.conf
