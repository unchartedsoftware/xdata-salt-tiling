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

### TODO
A 3rd, XYSegmentJob is under construction. It is responsible for running `../../ops/salt/CartesianSegment.scala`
It has a helper config class `../config/XYSegmentConfig.scala`, and a config file `../config/files/XYSegment.conf`

- Write a job that runs CartesianSegmentOp (mirroring current jobs.
  - This means we should probably write a CartesianSegment class (mirroring CartesianTimeHeatmap), that inherits from CartesianSegmentOp (mirroring CartesianTimeOp)
  It will be responsible for filling in various arguments to CartesianSegmentOp

- Refactor to remove unnecessary inheritance
- move ALL config logic out of job, and into config class(es)
  - logging and exits?
  - pipeline operation construction (can put function in Config?
-
