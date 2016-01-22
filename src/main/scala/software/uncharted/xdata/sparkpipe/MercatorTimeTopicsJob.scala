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

import java.io.FileReader

import com.typesafe.config.{ConfigException, Config, ConfigFactory}
import grizzled.slf4j.Logging
import org.apache.commons.csv.CSVFormat
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import software.uncharted.sparkpipe.Pipe
import software.uncharted.sparkpipe.ops.core.dataframe.temporal.parseDate
import software.uncharted.sparkpipe.ops.core.dataframe.text.{includeTermFilter, split}
import software.uncharted.xdata.ops.io.serializeElementScore
import software.uncharted.xdata.ops.salt.{RangeDescription, MercatorTimeTopics}
import software.uncharted.xdata.sparkpipe.JobUtil.{dataframeFromSparkCsv, createOutputOperation}
import software.uncharted.xdata.sparkpipe.MercatorTimeTopicsConfig

import scala.collection.JavaConverters._ // scalastyle:ignore


// Parse config for mercator time heatmap sparkpipe op
case class MercatorTimeTopicsConfig(lonCol: String,
                                    latCol: String,
                                    timeCol: String,
                                    textCol: String,
                                    timeRange: RangeDescription[Long],
                                    timeFormat: Option[String],
                                    topicLimit: Int,
                                    termList: Map[String, String])
object MercatorTimeTopicsConfig extends Logging {

  val mercatorTimeTopicKey = "mercatorTimeTopics"
  val timeFormatKey = "timeFormat"
  val longitudeColumnKey = "longitudeColumn"
  val latitudeColumnKey = "latitudeColumn"
  val timeColumnKey = "timeColumn"
  val timeMinKey = "min"
  val timeStepKey = "step"
  val timeCountKey =  "count"
  val textColumnKey = "textColumn"
  val topicLimitKey = "topicLimit"
  val termPathKey = "terms"

  def apply(config: Config): Option[MercatorTimeTopicsConfig] = {
    try {
      val topicConfig = config.getConfig(mercatorTimeTopicKey)

      Some(MercatorTimeTopicsConfig(
        topicConfig.getString(longitudeColumnKey),
        topicConfig.getString(latitudeColumnKey),
        topicConfig.getString(timeColumnKey),
        topicConfig.getString(textColumnKey),
        RangeDescription.fromMin(topicConfig.getLong(timeMinKey), topicConfig.getLong(timeStepKey), topicConfig.getInt(timeCountKey)),
        if (topicConfig.hasPath(timeFormatKey)) Some(topicConfig.getString(timeFormatKey)) else None,
        topicConfig.getInt(topicLimitKey),
        readTerms(topicConfig.getString(termPathKey)))
      )
    } catch {
      case e: ConfigException =>
        error(s"Failure parsing arguments from [$mercatorTimeTopicKey]", e)
        None
    }
  }



  private def readTerms(path: String) = {
    val in = new FileReader(path)
    val records = CSVFormat.DEFAULT
      .withAllowMissingColumnNames()
      .withCommentMarker('#')
      .withIgnoreSurroundingSpaces()
      .parse(in)
    records.iterator().asScala.map(x => (x.get(0), x.get(1))).toMap
  }
}



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
        .to(_.select(topicsConfig.lonCol, topicsConfig.latCol, topicsConfig.timeCol, topicsConfig.textCol))
        .to(_.cache())
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
