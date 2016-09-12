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
package software.uncharted.xdata.sparkpipe.config

import com.typesafe.config.{Config, ConfigException}
import grizzled.slf4j.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._      // scalastyle:ignore

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
case class TilingConfig(levels: List[Int], source: String, bins: Option[Int] = None)
object TilingConfig extends Logging {
  val tilingKey= "tiling"
  val levelsKey = "levels"
  val binsKey = "bins"
  val sourceKey = "source"

  def apply(config: Config): Option[TilingConfig] = {
    try {
      val tilingConfig = config.getConfig(tilingKey)
      Some(TilingConfig(
        tilingConfig.getIntList(levelsKey).asScala.map(_.asInstanceOf[Int]).toList,
        tilingConfig.getString(sourceKey),
        if (tilingConfig.hasPath(binsKey)) Some(tilingConfig.getInt(binsKey)) else None))
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
      Some(S3OutputConfig(awsAccess, awsSecret, bucket, layer, extension))
    } catch {
      case e: ConfigException =>
        error(s"Failure parsing arguments from [$s3OutputKey]", e)
        None
    }
  }
}

case class HBaseOutputConfig (configFiles: Seq[String], layer: String, qualifier: String)
object HBaseOutputConfig extends Logging {
  val hBaseOutputKey = "hbaseOutput"
  val configFilesKey = "configFiles"
  val layerKey = "layer"
  val qualifierKey = "qualifier"

  def apply (config: Config): Option[HBaseOutputConfig] = {
    try {
      val hbaseConfig = config.getConfig(hBaseOutputKey)
      val configFilesList = hbaseConfig.getStringList(configFilesKey)
      val configFiles = configFilesList.toArray(new Array[String](configFilesList.size()))
      val layer = hbaseConfig.getString(layerKey)
      val qualifier = hbaseConfig.getString(qualifierKey)
      Some(HBaseOutputConfig(configFiles, layer, qualifier))
    } catch {
      case e: ConfigException =>
        error(s"Failure parsing arguments from [$hBaseOutputKey]", e)
        None
    }
  }
}
