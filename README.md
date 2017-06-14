# Tile Based Visual Analytic Pipelines and Operations

`xdata-salt-tiling` provides a number of Tile Based Visual Analytics [[Cheng 2013](https://uncharted.software/assets/tile-based-visual-analytics-for-twitter.pdf),[Langevin 2015](https://uncharted.software/assets/global-to-local-pattern-of-life-analysis.pdf)] developed for the DARPA XDATA program using Uncharted's [Salt](http://unchartedsoftware.github.io/salt-core/) and [Sparkpipe](http://unchartedsoftware.github.io/sparkpipe-core/) libraries.  A number of core Sparkpipe operations are
 provided that perform Salt tiling tasks, and these operations are incorporated into larger pre-defined pipelines that implement loading, filtering, tiling and output steps.

The following tiling operations are supported:

  - Basic heatmap
  - Line segment heatmap
  - Heatmap over time
  - Term frequency topic
  - Term frequency topic over time
  - IP heatmap
  - IP segment heatmap
  - Term Frequency - Inverse Document Frequency (TFIDF) tile topic
  - Latent Dirichlet (LDA) tile topic

Each of these is incorporated into a configurable pipeline that can be executed via `spark-submit`.

## Building

Java 1.7+, Scala 2.11+ are required to build the library and run tests. The Gradle wrapper is used so there is no requirement to have Gradle installed.

As a pre-requisite, build and install the `sparkpipe-xdata-text` project following the instructions [here](https://github.com/unchartedsoftware/sparkpipe-xdata-text).  This artifact is currently not available in a public Maven repository and needs to be built from source.

After checking out the source code, the library binary can be built from the project root and installed locally as follows:

`./gradlew build install docs`

A full distribution will be available as tar and zip archives in the `project_root/build/distributions` directory. The distribution consists of a single "fat" JAR that contains the class binaries for the `xdata-salt-tiling` code, along with all dependencies it requires.  This single JAR can be used with the `spark-submit` command on it's own to run the included tiling pipelines (more below).  The distribution also contains example configuration files and run scripts that provide a starting point for running custom tiling jobs.

In addition to the distribution, a JAR consisting of `xdata-salt-tiling` class binaries only (suitable for inclusion as a dependency in other projects) will be available in `project_root/build/libs`, and a full set of archives (binary, sources, test sources, docs) can be published to a local Maven repository via:

`./gradlew publish`

Note that The above command requires that `MAVEN_REPO_URL`, `MAVEN_REPO_USERNAME` and `MAVEN_REPO_PASSWORD` be defined as environment variables.

## Running A Tiling Job

The following instructions assume that Spark 2.0+ has been installed locally, and that the `SPARK_HOME` environment variable is pointing to the installation directory.  The examples can easily be adapted to run on a Spark cluster.

Starting in the project root directory, execute the following to build and unzip the archive:

```bash
./gradlew build
mkdir -p ~/xdata-salt-tiling
unzip ./build/distributions/xdata-salt-tiling-0.2.0.zip -d ~/xdata/salt-tiling
```

Download the example NYC taxi data and unzip into the run directory:
```bash
cd ~/xdata-salt-tiling/xdata-salt-tiling-0.2.0
mkdir data
wget https://s3.ca-central-1.amazonaws.com/tiling-examples/taxi_micro.zip
unzip taxi_micro.zip -d ./data
```

Run the tiling job:
```bash
cd examples
./run-geo-heatmap-example.sh
```

The results of the tiling job will be available in `~/xdata-salt-tiling/xdata-salt-tiling-0.2.0/tiles/geo_heatmap`.  The tiles are organized into folders and files by zoom level, column and row using TMS indexing as follows: `{zoom_level}/{x}/{y}.bin`.  The tiles themselves are binaries consisting of 64 bit floating point values, arranged in row/major order.  The row and column count are equal, and the count value itself is stored in `tiles/geo_heatmap/metadata.json`.  Each bin element in this case is a count of the data values aggregated within that bin.

An in-depth explanation of TMS indexing is available on the [Open Street Map wiki](http://wiki.openstreetmap.org/wiki/TMS).

Examples also exist for:
 - segment based tiling (`run-geo-segment-example.sh` using the NYC tax data)
 - IP heatmap tiling (`run-ip-heatmap-example.sh` using a subset of [MACCDC2012](https://maccdc.org/) PCAP data (download [here](https://s3.ca-central-1.amazonaws.com/tiling-examples/maccdc2012_00008.zip)
 - Port heatmap tiling (`run-port-heatmap-example.sh` using PCAP data above)

 In each case, the shell script provides a `spark-submit` that uses the `xdata-salt-tiling.jar` along with a corresponding `*-example.conf` file to define and run a tiling job.  The scaladocs for the `software.uncharted.xdata.config` package detail values used to configure the jobs.
