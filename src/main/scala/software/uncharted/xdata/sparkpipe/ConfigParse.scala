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

import com.typesafe.config.{Config, ConfigException}
import grizzled.slf4j.Logging
import org.apache.commons.csv.CSVFormat
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import software.uncharted.xdata.ops.salt.RangeDescription

import scala.collection.JavaConverters._ //scalastyle:ignore

// scalastyle:off multiple.string.literals

// Parse spark configuration and instantiate context from it
object SparkConfig {
  val sparkKey = "spark"

  def apply(config: Config): SQLContext = {
    val sparkConfig = config.getConfig(sparkKey)
    val conf = new SparkConf()
    sparkConfig.entrySet().asScala.foreach(e => conf.set(s"spark.${e.getKey}", e.getValue.unwrapped().asInstanceOf[String]))
    val sc = new SparkContext(conf)
    new SQLContext(sc)
  }
}


// Parse tiling parameter and store results
case class TilingConfig(levels: List[Int], xBins: Int, yBins: Int, source: String)
object TilingConfig extends Logging {
  val tilingKey= "tiling"
  val levelsKey = "levels"
  val xBinKey = "xBins"
  val yBinKey = "yBins"
  val sourceKey = "source"

  def apply(config: Config): Option[TilingConfig] = {
    try {
      val tilingConfig = config.getConfig(tilingKey)
      Some(TilingConfig(
        tilingConfig.getIntList(levelsKey).asScala.map(_.asInstanceOf[Int]).toList,
        tilingConfig.getInt(xBinKey),
        tilingConfig.getInt(yBinKey),
        tilingConfig.getString(sourceKey)))
    } catch {
      case e: ConfigException =>
        error(s"Failure parsing arguments from [$tilingKey]", e)
        None
    }
  }
}

case class FileOutputConfig(destPath: String, layer: String, extension: String)
object FileOutputConfig extends Logging {
  val fileOutputKey = "fileOutput"
  val pathKey = "dest"
  val layerKey = "layer"
  val extensionKey = "ext"
  val defaultExtensionKey = "bin"

  def apply(config: Config): Option[FileOutputConfig] = {
    try {
      val fileConfig = config.getConfig(fileOutputKey)
      val path = fileConfig.getString(pathKey)
      val layer = fileConfig.getString(layerKey)
      val extension = if (fileConfig.hasPath(extensionKey)) fileConfig.getString(extensionKey) else defaultExtensionKey
      Some(FileOutputConfig(path, layer, extension))
    } catch {
      case e: ConfigException =>
        error(s"Failure parsing arguments from [$fileOutputKey]", e)
        None
    }
  }
}


// parse S3 output config
case class S3OutputConfig(accessKey: String, secretKey: String, bucket: String, layer: String, extension: String)
object S3OutputConfig extends Logging {
  val s3OutputKey = "s3Output"
  val awsAccessKey = "awsAccessKey"
  val awsSecretKey = "awsSecretKey"
  val bucketKey = "bucket"
  val layerKey = "layer"
  val extensionKey = "ext"
  val defaultExtension = "bin"

  def apply(config: Config): Option[S3OutputConfig] = {
    try {
      val s3Config = config.getConfig(s3OutputKey)
      val awsAccess = s3Config.getString(awsAccessKey)
      val awsSecret = s3Config.getString(awsSecretKey)
      val bucket = s3Config.getString(bucketKey)
      val layer = s3Config.getString(layerKey)
      val extension = if (s3Config.hasPath(extensionKey)) s3Config.getString(extensionKey) else defaultExtension
      Some(S3OutputConfig(awsAccessKey, awsSecretKey, bucket, layer, extension))
    } catch {
      case e: ConfigException =>
        error(s"Failure parsing arguments from [$s3OutputKey]", e)
        None
    }
  }
}


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


// Parse config for mercator time heatmap sparkpipe op
case class MercatorTimeTopicsConfig(lonCol: String, latCol: String, timeCol: String, textCol: String,
                          timeRange: RangeDescription[Long], timeFormat: Option[String],
                          topicLimit: Int, termList: Map[String, String])
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



