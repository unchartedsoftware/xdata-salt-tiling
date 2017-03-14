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
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import software.uncharted.xdata.ops.io.{writeBytesToFile, writeBytesToS3, writeBytesToHBase, writeToFile, writeToS3, writeToHBase}
import software.uncharted.xdata.sparkpipe.config.{HBaseOutputConfig, FileOutputConfig, S3OutputConfig}
import scala.collection.JavaConverters._ // scalastyle:ignore
import scala.util.{Failure, Try}



object JobUtil {
  type OutputOperation = (RDD[((Int, Int, Int), Seq[Byte])]) => RDD[((Int, Int, Int), Seq[Byte])]

  def dataframeFromSparkCsv(config: Config, source: String, schema: StructType, sparkSession: SparkSession): DataFrame = {
    val sparkCsvConfig = config.getConfig("sparkCsv")
      .entrySet()
      .asScala
      .map(e => e.getKey -> e.getValue.unwrapped().toString)
      .toMap

    sparkSession.read
      .format("com.databricks.spark.csv")
      .options(sparkCsvConfig)
      .schema(schema)
      .load(source)
  }


  def createTileOutputOperation(config: Config): Try[OutputOperation] = {
    if (config.hasPath(FileOutputConfig.fileOutputKey)) {
      FileOutputConfig.parse(config).map(c => writeToFile(c.destPath, c.layer, c.extension))
    } else if (config.hasPath(S3OutputConfig.s3OutputKey)) {
      S3OutputConfig.parse(config).map(c => writeToS3(c.accessKey, c.secretKey, c.bucket, c.layer))
    } else if (config.hasPath(HBaseOutputConfig.hBaseOutputKey)) {
      HBaseOutputConfig.parse(config).map(c => writeToHBase(c.configFiles, c.layer, c.qualifier))
    } else {
      Failure(new Exception("No output operation given"))
    }
  }

  def createMetadataOutputOperation(config: Config): Try[(String, Seq[Byte]) => Unit] = {
    if (config.hasPath(FileOutputConfig.fileOutputKey)) {
      FileOutputConfig.parse(config).map(c => writeBytesToFile(c.destPath, c.layer))
    } else if (config.hasPath(S3OutputConfig.s3OutputKey)) {
      S3OutputConfig.parse(config).map(c => writeBytesToS3(c.accessKey, c.secretKey, c.bucket, c.layer))
    } else if (config.hasPath(HBaseOutputConfig.hBaseOutputKey)) {
      HBaseOutputConfig.parse(config).map(c => writeBytesToHBase(c.configFiles, c.layer, c.qualifier))
    } else {
      Failure(new Exception("No metadata output operation given"))
    }
  }
}
