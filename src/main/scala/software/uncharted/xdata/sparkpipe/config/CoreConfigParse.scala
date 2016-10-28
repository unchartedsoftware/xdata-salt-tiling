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

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import com.typesafe.config.Config
import scala.collection.JavaConverters._ // scalastyle:ignore
import scala.util.Try


// scalastyle:off multiple.string.literals

// Parse spark configuration and instantiate context from it
object SparkConfig {
  val sparkKey = "spark"

  def apply(config: Config): SparkSession = {
    val conf = new SparkConf()
    config.getConfig(sparkKey)
      .entrySet()
      .asScala
      .foreach(e => conf.set(s"spark.${e.getKey}", e.getValue.unwrapped().asInstanceOf[String]))

    SparkSession.builder.config(conf).getOrCreate()
  }
}


// Parse tiling parameter and store results
case class TilingConfig(levels: List[Int], source: String, bins: Option[Int] = None, tms: Boolean)
object TilingConfig {
  val tilingKey= "tiling"
  val levelsKey = "levels"
  val binsKey = "bins"
  val sourceKey = "source"
  val tmsKey = "tms"
  val defaultTMS = false

  def apply(config: Config): Try[TilingConfig] = {
    Try {
      val tilingConfig = config.getConfig(tilingKey)
      TilingConfig(
        tilingConfig.getIntList(levelsKey).asScala.map(_.asInstanceOf[Int]).toList,
        tilingConfig.getString(sourceKey),
        if (tilingConfig.hasPath(binsKey)) Some(tilingConfig.getInt(binsKey)) else None,
        if (tilingConfig.hasPath(tmsKey)) tilingConfig.getBoolean(tmsKey) else defaultTMS
      )
    }
  }
}



case class FileOutputConfig(destPath: String, layer: String, extension: String)
object FileOutputConfig {
  val fileOutputKey = "fileOutput"
  val pathKey = "dest"
  val layerKey = "layer"
  val extensionKey = "ext"
  val defaultExtensionKey = ".bin"

  def apply(config: Config): Try[FileOutputConfig] = {
    Try {
      val fileConfig = config.getConfig(fileOutputKey)
      val path = fileConfig.getString(pathKey)
      val layer = fileConfig.getString(layerKey)
      val extension = if (fileConfig.hasPath(extensionKey)) fileConfig.getString(extensionKey) else defaultExtensionKey
      FileOutputConfig(path, layer, extension)
    }
  }
}



// parse S3 output config
case class S3OutputConfig(accessKey: String, secretKey: String, bucket: String, layer: String, extension: String)
object S3OutputConfig {
  val s3OutputKey = "s3Output"
  val awsAccessKey = "awsAccessKey"
  val awsSecretKey = "awsSecretKey"
  val bucketKey = "bucket"
  val layerKey = "layer"
  val extensionKey = "ext"
  val defaultExtension = "bin"

  def apply(config: Config): Try[S3OutputConfig] = {
    Try {
      val s3Config = config.getConfig(s3OutputKey)
      val awsAccess = s3Config.getString(awsAccessKey)
      val awsSecret = s3Config.getString(awsSecretKey)
      val bucket = s3Config.getString(bucketKey)
      val layer = s3Config.getString(layerKey)
      val extension = if (s3Config.hasPath(extensionKey)) s3Config.getString(extensionKey) else defaultExtension
      S3OutputConfig(awsAccess, awsSecret, bucket, layer, extension)
    }
  }
}

case class HBaseOutputConfig (configFiles: Seq[String], layer: String, qualifier: String)
object HBaseOutputConfig {
  val hBaseOutputKey = "hbaseOutput"
  val configFilesKey = "configFiles"
  val layerKey = "layer"
  val qualifierKey = "qualifier"

  def apply (config: Config): Try[HBaseOutputConfig] = {
    Try {
      val hbaseConfig = config.getConfig(hBaseOutputKey)
      val configFilesList = hbaseConfig.getStringList(configFilesKey)
      val configFiles = configFilesList.toArray(new Array[String](configFilesList.size()))
      val layer = hbaseConfig.getString(layerKey)
      val qualifier = hbaseConfig.getString(qualifierKey)
      HBaseOutputConfig(configFiles, layer, qualifier)
    }
  }
}
