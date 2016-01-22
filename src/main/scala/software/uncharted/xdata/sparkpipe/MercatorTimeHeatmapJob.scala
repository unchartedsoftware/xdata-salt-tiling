/**
 * Copyright (c) 2014-2015 Uncharted Software Inc. All rights reserved.
 *
 * Property of Uncharted(tm), formerly Oculus Info Inc.
 * http://uncharted.software/
 *
 * This software is the confidential and proprietary information of
 * Uncharted Software Inc. ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Uncharted Software Inc.
 */
package software.uncharted.xdata.sparkpipe

import com.typesafe.config.{ConfigException, Config, ConfigFactory}
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import software.uncharted.sparkpipe.Pipe
import software.uncharted.sparkpipe.ops.core.dataframe.temporal.parseDate
import software.uncharted.xdata.ops.io.serializeBinArray
import software.uncharted.xdata.ops.salt.{RangeDescription, MercatorTimeHeatmap}
import software.uncharted.xdata.sparkpipe.JobUtil.{dataframeFromSparkCsv, createOutputOperation}

// Parse config for geoheatmap sparkpipe op
case class MercatorTimeHeatmapConfig(lonCol: String, latCol: String, timeCol: String, timeRange: RangeDescription[Long], timeFormat: Option[String] = None)
object MercatorTimeHeatmapConfig extends Logging {

  val mercatorTimeHeatmapKey = "mercatorTimeHeatmap"
  val timeFormatKey = "timeFormat"
  val longitudeColumnKey = "longitudeColumn"
  val latitudeColumnKey = "latitudeColumn"
  val timeColumnKey = "timeColumn"
  val timeMinKey = "min"
  val timeStepKey = "step"
  val timeCountKey =  "count"

  def apply(config: Config): Option[MercatorTimeHeatmapConfig] = {
    try {
      val heatmapConfig = config.getConfig(mercatorTimeHeatmapKey)
      Some(MercatorTimeHeatmapConfig(
        heatmapConfig.getString(longitudeColumnKey),
        heatmapConfig.getString(latitudeColumnKey),
        heatmapConfig.getString(timeColumnKey),
        RangeDescription.fromMin(heatmapConfig.getLong(timeMinKey), heatmapConfig.getLong(timeStepKey), heatmapConfig.getInt(timeCountKey)),
        if (heatmapConfig.hasPath(timeFormatKey)) Some(heatmapConfig.getString(timeFormatKey)) else None)
      )
    } catch {
      case e: ConfigException =>
        error("Failure parsing arguments from [" + mercatorTimeHeatmapKey + "]", e)
        None
    }
  }
}


// scalastyle:off method.length
object MercatorTimeHeatmapJob extends Logging {

  private val convertedTime: String = "convertedTime"

  def execute(config: Config): Unit = {
    // parse the schema, and exit on any errors
    val schema = Schema(config).getOrElse {
      error("Couldn't create schema - exiting")
      sys.exit(-1)
    }

    // Parse tiling parameters out of supplied config
    val tilingConfig = TilingConfig(config).getOrElse {
      logger.error("Invalid tiling config")
      sys.exit(-1)
    }

    // Parse geo heatmap parameters out of supplied config
    val heatmapConfig = MercatorTimeHeatmapConfig(config).getOrElse {
      logger.error("Invalid heatmap op config")
      sys.exit(-1)
    }

    // Parse output parameters and return the correspoding write function
    val outputOperation = createOutputOperation(config).getOrElse {
      logger.error("Output opeation config")
      sys.exit(-1)
    }

    // Create the spark context from the supplied config
    val sqlc = SparkConfig(config)
    try {
      // Create the dataframe from the input config
      val df = dataframeFromSparkCsv(config, tilingConfig.source, schema, sqlc)

      // Pipe the dataframe
      Pipe(df)
        .to {
          heatmapConfig.timeFormat
            .map(p => parseDate(heatmapConfig.timeCol, convertedTime, heatmapConfig.timeFormat.get)(_))
            .getOrElse((df: DataFrame) => df)
        }
        .to(_.select(heatmapConfig.lonCol, heatmapConfig.latCol, heatmapConfig.timeCol))
        .to(_.cache())
        .to {
          MercatorTimeHeatmap(
            heatmapConfig.latCol,
            heatmapConfig.lonCol,
            heatmapConfig.timeFormat.map(s => convertedTime).getOrElse(heatmapConfig.timeCol),
            None,
            None,
            heatmapConfig.timeRange,
            tilingConfig.levels)
        }
        .to(serializeBinArray)
        .to(outputOperation)
        .run()
    } finally {
      sqlc.sparkContext.stop()
    }
  }

  def execute(args: Array[String]): Unit = {
    // get the properties file path
    if (args.length < 1) {
      logger.error("Path to conf file required")
      sys.exit(-1)
    }

    // load properties file from supplied URI
    val config = ConfigFactory.parseReader(scala.io.Source.fromFile(args(0)).bufferedReader())
    execute(config)
  }

  def main (args: Array[String]): Unit = {
    MercatorTimeHeatmapJob.execute(args)
  }
}
