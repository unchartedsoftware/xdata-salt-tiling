/**
 * Copyright © 2013-2017 Uncharted Software Inc.
 *
 * Property of Uncharted™, formerly Oculus Info Inc.
 *
 * http://uncharted.software/
 *
 * Released under the MIT License.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package software.uncharted.xdata.tiling.jobs

import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import software.uncharted.sparkpipe.ops.xdata.io.{writeBytesToFile, writeBytesToHBase, writeBytesToS3, writeToFile, writeToHBase, writeToS3}
import software.uncharted.xdata.tiling.config.{FileOutputConfig, HBaseOutputConfig, S3OutputConfig}
import scala.collection.JavaConverters._ //scalastyle:ignore
import scala.util.{Failure, Try}

/**
  * Helper functions for configuring and instantiating sparkpipe tiling pipelines.
  */
object JobUtil {
  type OutputOperation = (RDD[((Int, Int, Int), Seq[Byte])]) => RDD[((Int, Int, Int), Seq[Byte])]

  /**
    * Creates a `DataFrame` from from a CSV file, leveraging Spark's internal CSV loading facilities.
    * Configuration values are taken from the `Config` object, and passed through to the `DataFrameReader`, so all args defined
    * at [[https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameReader.html#csv(scala.collection.Seq)]]
    * are valid.
    *
    * Example config in HOCON notation:
    *
    * {{{
    * sparkCsv {
    *   sep = "\t"
    *   header = false
    *   mode = DROPMALFORMED
    * }
    * }}}
    *
    * @param config `Config` object containing the spark CSV arguments.
    * @param source Source path, as an HDFS path, local file system, or any Hadoop-supported file system URI
    * @param schema The schema of DataFrame the CSV will be loaded into (as specified by the `csvSchema` portion
    *               of the config file).
    * @param sparkSession The target spark session.
    * @return A Dataframe containing the CSV data, following the caller supplied schema.
    */
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

  /**
    * Creates a tile output operation from a supplied `Config` object.  See documentation
    * of `FileOutputConfig`, `S3OutputConfig` and `HBaseConfig` for valid values.
    *
    * @param config The `Config` object containing the output parameters.
    * @return A tile output function that can be added to a sparkpipe pipeline.
    */
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

  /**
    * Creates a metatdata output operation from a supplied `Config` object.  See documentation
    * of `FileOutputConfig`, `S3OutputConfig` and `HBaseConfig` for valid values.
    *
    * @param config The `Config` object containing the output parameters.
    * @return A tile output function that can be added to a sparkpipe pipeline.
    */
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
