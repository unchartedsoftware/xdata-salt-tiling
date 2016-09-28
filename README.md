# xdata-pipeline-ops

Provides tiling jobs to create heatmaps and wordclouds using cartesian or web-mercator projections from CSV data.  Supported outputs include Amazon S3, HBase and local file system.

There are no install prerequisites as the Gradle wrapper is used.  To build run:

`./gradlew build`

To create a deployable jar to use with `spark-submit`:

`./gradlew distZip`

Continuous integration is run on Bamboo:

https://bamboo.uncharted.software/browse/XDATA-XPO


## Sparkpipe Jobs
The folder `xdata-pipeline-ops/src/main/scala/software/uncharted/xdata/sparkpipe/jobs`
contains a collection of spark jobs responsible for running one or more pipeline operations.
### Execution
The jobs are run on the cluster using spark-submit with the following steps:
1. build a distribution jar from the project root for xdata-pipeline-ops with
```bash
./gradlew jar
```
1. Put together a config file (some available in `src/main/scala/software/uncharted/xdata/sparkpipe/config/files`),
1. And run with
```bash
spark-submit --class <full class name of Job> xdata-pipeline-ops.jar <config-file>
```
NOTE: Some Jobs might take multiple config files

### Architecture
Each Job reads a config file using [typesafe-config](https://github.com/typesafehub/config)
and uses various classes in `.sparkpipe/config` to extract the given configuration.
The results are then constructed into pipeline operations and their arguments.
These operations are then made into a pipe and run.

### Current Jobs
Responsible for running operations in `src/main/scala/software/uncharted/xdata/ops/`:
- `TopicModellingJob`
  - Operation:
    - `TopicModelling`
- `TopicModellingParallelJob`
  - Operation:
    - `TopicModellingParallel`
- `XYTimeHeatmapJob`
  - Operation:
   - `salt/MercatorTimeHeatmap`, or
   - `salt/CartesianTimeHeatmap`
- `XYTimeTopicsJob`
  - Operation:
    - `salt/MercatorTimeTopics`, or
    - `salt/CartesianTimeTopics`
