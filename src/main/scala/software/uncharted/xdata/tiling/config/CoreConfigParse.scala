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

package software.uncharted.xdata.tiling.config

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.typesafe.config.Config
import software.uncharted.sparkpipe.ops.xdata.salt.{TopicsOp, ZXYOp}

import scala.collection.JavaConverters._
import scala.util.Try

// scalastyle:off multiple.string.literals

// Parse spark configuration and instantiate context from it
object SparkConfig {
  val sparkKey = "spark"
  val checkpointDirectoryKey = sparkKey + "." + "checkpoint-directory"

  private[config] def applySparkConfigEntries(config: Config)(conf: SparkConf): SparkConf = {
    config.getConfig(sparkKey)
      .entrySet()
      .asScala
      .foreach(e => conf.set(s"spark.${e.getKey}", e.getValue.unwrapped().toString))

    conf
  }

  def apply(config: Config): SparkSession = {
    val session = SparkSession.builder.config(applySparkConfigEntries(config)(new SparkConf())).getOrCreate()

    if (config.hasPath(checkpointDirectoryKey)) {
      session.sparkContext.setCheckpointDir(config.getString(checkpointDirectoryKey))
    }

    session
  }
}

// Parse tiling parameter and store results
case class TilingConfig(levels: List[Int], source: String, bins: Option[Int])
object TilingConfig extends ConfigParser{
  private val tilingKey= "tiling"
  private val levelsKey = "levels"
  private val binsKey = "bins"
  private val sourceKey = "source"

  def parse(config: Config): Try[TilingConfig] = {
    Try {
      val tilingConfig = config.getConfig(tilingKey)
      TilingConfig(
        tilingConfig.getIntList(levelsKey).asScala.map(_.asInstanceOf[Int]).toList,
        tilingConfig.getString(sourceKey),
        getIntOption(tilingConfig, binsKey)
      )
    }
  }
}

case class FileOutputConfig(destPath: String, layer: String, extension: String)
object FileOutputConfig extends ConfigParser{
  val fileOutputKey = "fileOutput"
  private val pathKey = "dest"
  private val layerKey = "layer"
  private val extensionKey = "ext"
  private val defaultExtensionKey = ".bin"

  def parse(config: Config): Try[FileOutputConfig] = {
    Try {
      val fileConfig = config.getConfig(fileOutputKey)

      FileOutputConfig(
        fileConfig.getString(pathKey),
        fileConfig.getString(layerKey),
        getString(fileConfig, extensionKey, defaultExtensionKey)
      )
    }
  }
}

// parse S3 output config
case class S3OutputConfig(accessKey: String, secretKey: String, bucket: String, layer: String, extension: String)
object S3OutputConfig extends ConfigParser{
  val s3OutputKey = "s3Output"
  private val awsAccessKey = "awsAccessKey"
  private val awsSecretKey = "awsSecretKey"
  private val bucketKey = "bucket"
  private val layerKey = "layer"
  private val extensionKey = "ext"
  private val defaultExtension = "bin"

  def parse(config: Config): Try[S3OutputConfig] = {
    Try {
      val s3Config = config.getConfig(s3OutputKey)
      S3OutputConfig(
        s3Config.getString(awsAccessKey),
        s3Config.getString(awsSecretKey),
        s3Config.getString(bucketKey),
        s3Config.getString(layerKey),
        getString(s3Config, extensionKey, defaultExtension))
    }
  }
}

case class HBaseOutputConfig (configFiles: Seq[String], layer: String, qualifier: String)
object HBaseOutputConfig extends ConfigParser{
  val hBaseOutputKey = "hbaseOutput"
  private val configFilesKey = "configFiles"
  private val layerKey = "layer"
  private val qualifierKey = "qualifier"

  def parse (config: Config): Try[HBaseOutputConfig] = {
    Try {
      val hbaseConfig = config.getConfig(hBaseOutputKey)
      val configFilesList = hbaseConfig.getStringList(configFilesKey)
      val configFiles = configFilesList.toArray(new Array[String](configFilesList.size()))

      HBaseOutputConfig(
        configFiles,
        hbaseConfig.getString(layerKey),
        hbaseConfig.getString(qualifierKey)
      )
    }
  }
}
