# xdata-pipeline-ops

Provides tiling jobs to create heatmaps and wordclouds using cartesian or web-mercator projections from CSV data.  Supported outputs include Amazon S3, HBase and local file system.

There are no install prerequisites as the Gradle wrapper is used.  To build run:

`./gradlew build`

To create a deployable jar to use with `spark-submit`:

`./gradlew distZip`

Continuous integration is run on Bamboo:

https://bamboo.uncharted.software/browse/XDATA-XPO


