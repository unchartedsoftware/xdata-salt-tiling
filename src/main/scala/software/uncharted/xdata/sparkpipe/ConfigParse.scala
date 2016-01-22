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

import java.io.{File, FileReader}

import com.typesafe.config.{Config, ConfigException}
import grizzled.slf4j.Logging
import org.apache.commons.csv.{CSVParser, CSVFormat}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import software.uncharted.xdata.ops.io.{writeToFile, writeToS3}
import software.uncharted.xdata.ops.salt.RangeDescription

import scala.collection.JavaConverters._ //scalastyle:ignore

// Parse spark configuration and instantiate context from it
object SparkConfig {
  def apply(config: Config): SQLContext = {
    val sparkConfig = config.getConfig("spark")
    val conf = new SparkConf()
    sparkConfig.entrySet().asScala.foreach(e => conf.set(s"spark.${e.getKey}", e.getValue.unwrapped().asInstanceOf[String]))
    val sc = new SparkContext(conf)
    new SQLContext(sc)
  }
}


// Parse tiling parameter and store results
case class TilingConfig(levels: List[Int], xBins: Int, yBins: Int, source: String)
object TilingConfig extends Logging {
  def apply(config: Config): Option[TilingConfig] = {
    try {
      val tilingConfig = config.getConfig("tiling")
      Some(TilingConfig(
        tilingConfig.getIntList("levels").asScala.map(_.asInstanceOf[Int]).toList,
        tilingConfig.getInt("xBins"),
        tilingConfig.getInt("yBins"),
        tilingConfig.getString("source")))
    } catch {
      case e: ConfigException =>
        error("Failure parsing arguments from [tiling]", e)
        None
    }
  }
}


// Parse output configuration and return output function
object OutputConfig {
  def apply(config: Config): (RDD[((Int, Int, Int), Seq[Byte])]) => RDD[((Int, Int, Int), Seq[Byte])] = {
    if (config.hasPath("fileOutput")) {
      val serializerConfig = config.getConfig("fileOutput")
      writeToFile(serializerConfig.getString("dest"), serializerConfig.getString("layer"), "bin")
    } else if (config.hasPath("s3Output")) {
      val serializerConfig = config.getConfig("s3Output")
      writeToS3(sys.env("AWS_ACCESS_KEY"), sys.env("AWS_SECRET_KEY"), serializerConfig.getString("bucket"), serializerConfig.getString("layer"))
    } else {
      throw new ConfigException.Missing("Failure parsing output - [s3Output] or [fileOutput] required")
    }
  }
}


// Parse config for geoheatmap sparkpipe op
case class MercatorTimeHeatmapConfig(lonCol: String, latCol: String, timeCol: String, timeRange: RangeDescription[Long], timeFormat: Option[String] = None)
object MercatorTimeHeatmapConfig extends Logging {
  def apply(config: Config): Option[MercatorTimeHeatmapConfig] = {
    try {
      val heatmapConfig = config.getConfig("mercatorTimeHeatmap")
      Some(MercatorTimeHeatmapConfig(
        heatmapConfig.getString("longitudeColumn"),
        heatmapConfig.getString("latitudeColumn"),
        heatmapConfig.getString("timeColumn"),
        RangeDescription.fromMin(heatmapConfig.getLong("min"), heatmapConfig.getLong("step"), heatmapConfig.getInt("count")),
        if (heatmapConfig.hasPath("timeFormat")) Some(heatmapConfig.getString("timeFormat")) else None)
      )
    } catch {
      case e: ConfigException =>
        error("Failure parsing arguments from [mercatorTimeHeatmap]", e)
        None
    }
  }
}


// Parse config for geoheatmap sparkpipe op
case class MercatorTimeTopicsConfig(lonCol: String, latCol: String, timeCol: String, textCol: String,
                          timeRange: RangeDescription[Long], timeFormat: Option[String],
                          topicLimit: Int, termList: Map[String, String])
object MercatorTimeTopicsConfig extends Logging {
  def apply(config: Config): Option[MercatorTimeTopicsConfig] = {
    try {
      val topicConfig = config.getConfig("mercatorTimeTopics")

      Some(MercatorTimeTopicsConfig(
        topicConfig.getString("longitudeColumn"),
        topicConfig.getString("latitudeColumn"),
        topicConfig.getString("timeColumn"),
        topicConfig.getString("textColumn"),
        RangeDescription.fromMin(topicConfig.getLong("min"), topicConfig.getLong("step"), topicConfig.getInt("count")),
        if (topicConfig.hasPath("timeFormat")) Some(topicConfig.getString("timeFormat")) else None,
        topicConfig.getInt("topicLimit"),
        readTerms(topicConfig.getString("terms")))
      )
    } catch {
      case e: ConfigException =>
        error("Failure parsing arguments from [mercatorTimeTopics]", e)
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



