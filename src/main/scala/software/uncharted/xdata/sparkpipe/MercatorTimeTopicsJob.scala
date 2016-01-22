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
import software.uncharted.sparkpipe.Pipe
import software.uncharted.sparkpipe.ops.core.dataframe.temporal.parseDate
import software.uncharted.sparkpipe.ops.core.dataframe.text.{includeTermFilter, split}
import software.uncharted.xdata.ops.io.serializeElementScore
import software.uncharted.xdata.ops.salt.MercatorTimeTopics
import software.uncharted.xdata.sparkpipe.JobUtil.{dataframeFromSparkCsv, createOutputOperation}

import scala.collection.JavaConverters._ // scalastyle:ignore

// scalastyle:off method.length
object MercatorTimeTopicsJob extends Logging {

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
    val topicsConfig = MercatorTimeTopicsConfig(config).getOrElse {
      logger.error("Invalid heatmap op config")
      sys.exit(-1)
    }

    val outputOperation = createOutputOperation(config).getOrElse {
      logger.error("Output opeation config")
      sys.exit(-1)
    }

    val nullOp = (df: DataFrame) => df

    // Create the spark context from the supplied config
    val sqlc = SparkConfig(config)
    try {
      // Create the dataframe from the input config
      val df = dataframeFromSparkCsv(config, tilingConfig.source, schema, sqlc)

      // Pipe the dataframe
      Pipe(df)
        .to {
          topicsConfig.timeFormat
            .map(p => parseDate(topicsConfig.timeCol, "convertedTime", topicsConfig.timeFormat.get)(_))
            .getOrElse(nullOp)
        }
        .to(split(topicsConfig.textCol, "\\b+"))
        .to(includeTermFilter(topicsConfig.textCol, topicsConfig.termList.keySet))
        .to {
          MercatorTimeTopics(
            topicsConfig.latCol,
            topicsConfig.lonCol,
            topicsConfig.timeFormat.map(s => "convertedTime").getOrElse("time"),
            topicsConfig.textCol,
            None,
            topicsConfig.timeRange,
            topicsConfig.topicLimit,
            tilingConfig.levels)
        }
        .to(serializeElementScore)
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
    MercatorTimeTopicsJob.execute(args)
  }
}
