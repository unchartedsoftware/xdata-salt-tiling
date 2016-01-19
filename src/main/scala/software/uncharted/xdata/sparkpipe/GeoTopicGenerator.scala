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
import org.apache.spark.sql.{DataFrame, SQLContext}
import software.uncharted.xdata.ops.io.serializeElementScore
import scala.collection.JavaConverters._ // scalastyle:ignore
import software.uncharted.sparkpipe.Pipe
import software.uncharted.sparkpipe.ops.core.dataframe.temporal.parseDate
import software.uncharted.sparkpipe.ops.core.dataframe.text._
import software.uncharted.xdata.ops.salt.{GeoTopicOpConf, GeoTopicOp}

object GeoTopicGenerator extends Logging {

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
    val geoTopicConfig = GeoTopicConfig(config).getOrElse {
      logger.error("Invalid heatmap op config")
      sys.exit(-1)
    }

    val nullOp = (df: DataFrame) => df

    // Create the spark context from the supplied config
    val sqlc = SparkConfig(config)
    try {
      // Create the dataframe from the input config
      val df = setupDataframe(config, tilingConfig.source, schema, sqlc)

      // Pipe the dataframe
      Pipe(df)
        .to {
          geoTopicConfig.timeFormat
            .map(p => parseDate(geoTopicConfig.timeCol, "convertedTime", geoTopicConfig.timeFormat.get)(_))
            .getOrElse(nullOp)
        }
        .to(split(geoTopicConfig.textCol, "\\b+"))
        .to(includeTermFilter(geoTopicConfig.textCol, geoTopicConfig.termList.keySet))
        .to(x => {x.show(); x})
        .to {
          GeoTopicOp(GeoTopicOpConf(
            geoTopicConfig.lonCol,
            geoTopicConfig.latCol,
            geoTopicConfig.timeFormat.map(s => "convertedTime").getOrElse("time"),
            geoTopicConfig.textCol,
            geoTopicConfig.timeRange,
            geoTopicConfig.topicLimit,
            tilingConfig.levels,
            None))
        }
        .to(serializeElementScore)
        .to(OutputConfig(config))
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
