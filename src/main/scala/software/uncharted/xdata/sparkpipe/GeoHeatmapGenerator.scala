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

import com.typesafe.config.{Config, ConfigFactory}
import grizzled.slf4j.Logging
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}
import scala.collection.JavaConverters._ // scalastyle:ignore
import software.uncharted.sparkpipe.Pipe
import software.uncharted.xdata.ops.io.BinArraySerializerOp
import software.uncharted.sparkpipe.ops.core.dataframe.temporal.parseDate
import software.uncharted.xdata.ops.numeric.numericRangeFilter
import software.uncharted.xdata.ops.salt.{GeoHeatmapOp, GeoHeatmapOpConf}

object GeoHeatmapGenerator extends Logging {

  def execute(args: Array[String]): Unit = {

    // get the properties file path
    if (args.length < 1) {
      logger.error("Path to conf file required")
      sys.exit(-1)
    }

    // load properties file from supplied URI
    val config = ConfigFactory.parseReader(scala.io.Source.fromFile(args(0)).bufferedReader())

    // parse the schema, and exit on any errors
    val schema = Schema(config).getOrElse {
      error("Couldn't create schema - exiting")
      sys.exit(-1)
    }

    // Create the spark context from the supplied config
    val sqlc = SparkConfig(config)

    // Parse tiling parameters out of supplied config
    val tilingConfig = TilingConfig(config).getOrElse {
      logger.error("Invalid tiling config")
      sys.exit(-1)
    }

    // Create the dataframe from the input config
    val df = setupDataframe(config, tilingConfig.source, schema, sqlc)

    // Parse geo heatmap parameters out of supplied config
    val geoHeatmapConfig = GeoHeatmapConfig(config).getOrElse {
      logger.error("Invalid heatmap op config")
      sys.exit(-1)
    }

    val nullOp = (df: DataFrame) => df

    // Pipe the dataframe
    Pipe(df)
      .to(numericRangeFilter(Seq((geoHeatmapConfig.timeCol, geoHeatmapConfig.timeRange.min, geoHeatmapConfig.timeRange.max))))
      .to(geoHeatmapConfig.timeFormat.map(p => parseDate(geoHeatmapConfig.timeCol, "dateCol", geoHeatmapConfig.timeFormat.get)(_)).getOrElse(nullOp))
      .to(GeoHeatmapOp(GeoHeatmapOpConf(tilingConfig.levels, 0, 1, 3, None, geoHeatmapConfig.timeRange, tilingConfig.xBins)))
      .to(BinArraySerializerOp.binArraySerializeOp)
      .to(OutputConfig(config))
      .run()

    sqlc.sparkContext.stop()
  }

  private def setupDataframe(config: Config, source: String, schema: StructType, sqlc: SQLContext) = {
    val sparkCsvConfig = config.getConfig("sparkCsv")
      .entrySet()
      .asScala
      .map(e => e.getKey -> e.getValue.unwrapped().toString)
      .toMap

    sqlc.read
      .format("com.databricks.spark.csv")
      .options(sparkCsvConfig)
      .schema(schema)
      .load(source)
  }

  def main (args: Array[String]): Unit = {
    GeoHeatmapGenerator.execute(args)
  }
}
