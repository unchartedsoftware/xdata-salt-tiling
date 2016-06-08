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
package software.uncharted.xdata.sparkpipe.jobs

import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import software.uncharted.xdata.ops.io.{writeBytesToFile, writeBytesToS3, writeToFile, writeToS3}
import software.uncharted.xdata.sparkpipe.config.{FileOutputConfig, S3OutputConfig}

import scala.collection.JavaConverters._ // scalastyle:ignore

object JobUtil {
  type OutputOperation = (RDD[((Int, Int, Int), Seq[Byte])]) => RDD[((Int, Int, Int), Seq[Byte])]

  def dataframeFromSparkCsv(config: Config, source: String, schema: StructType, sqlc: SQLContext): DataFrame = {
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


  def createTileOutputOperation(config: Config): Option[OutputOperation] = {
    if (config.hasPath(FileOutputConfig.fileOutputKey)) {
      FileOutputConfig(config).map(c => writeToFile(c.destPath, c.layer, c.extension))
    } else if (config.hasPath(S3OutputConfig.s3OutputKey)) {
      S3OutputConfig(config).map(c => writeToS3(c.accessKey, c.secretKey, c.bucket, c.layer))
    } else {
      None
    }
  }

  def createMetadataOutputOperation(config: Config): Option[(String, Seq[Byte]) => Unit] = {
    if (config.hasPath(FileOutputConfig.fileOutputKey)) {
      FileOutputConfig(config).map(c => writeBytesToFile(c.destPath, c.layer))
    } else if (config.hasPath(S3OutputConfig.s3OutputKey)) {
      S3OutputConfig(config).map(c => writeBytesToS3(c.accessKey, c.secretKey, c.bucket, c.layer))
    } else {
      None
    }
  }
}
