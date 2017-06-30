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

package software.uncharted.contrib.tiling.config

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.typesafe.config.Config

import scala.collection.JavaConverters._ // scalastyle:off
import scala.util.Try

// scalastyle:off multiple.string.literals

/**
  * Provides a factory function to produce a spark session from a `com.typesafe.config.Config` object.
  * Values are taken from the object and passed through to the `SparkSession` builder, so all defined
  * at [[http://spark.apache.org/docs/latest/configuration.html]] are valid.  Note that the
  * properties do not have to be appended with "spark" - use `app.name` rather than
  * `spark.app.name`.
  *
  * Example config:
  *
  * {{{
  * spark {
  *   master = "local[*]"
  *   app.name = salt-ip-heatmap-test
  * }
  * }}}
  *
  */
object SparkConfig {
  val RootKey = "spark"
  val CheckpointDirectoryKey = RootKey + "." + "checkpoint-directory"

  private[config] def applySparkConfigEntries(config: Config)(conf: SparkConf): SparkConf = {
    config.getConfig(RootKey)
      .entrySet()
      .asScala
      .foreach(e => conf.set(s"spark.${e.getKey}", e.getValue.unwrapped().toString))

    conf
  }

  /**
    * Parses configuration values and instantiates a spark session from them.
 *
    * @param config A configuration container
    * @return A spark session based on the config settings.
    */
  def apply(config: Config): SparkSession = {
    val session = SparkSession.builder.config(applySparkConfigEntries(config)(new SparkConf())).getOrCreate()

    if (config.hasPath(CheckpointDirectoryKey)) {
      session.sparkContext.setCheckpointDir(config.getString(CheckpointDirectoryKey))
    }

    session
  }
}

/**
  * Generalized tiling job configuration.
  *
  * @param levels A [[scala.collection.Seq]] of levels, doesn't need to be ordered or
  *               contiguous.
  * @param source The path to the source data.  This can be an HDFS path, a file system
  *               location or any valid Hadoop URI.
  * @param bins The tile dimension in bins.
  */
case class TilingConfig(levels: List[Int], source: String, bins: Option[Int])

/**
  * Provides functions for parsing general tile data out of `com.typesafe.config.Config` objects.
  *
  * Valid properties are:
  *
  *   - `levels` - an array of levels to tile
  *   - `bins` - an integral value specifying the tile dimension [OPTIONAL]
  *   - `source` - the data source specified as a valid HDFS path, file system location, or Hadoop URI.
  *
  *  Example from config file (in [[https://github.com/typesafehub/config#using-hocon-the-json-superset HOCON]] notation):
  *
  *  {{{
  *  tiling {
  *    levels = [0,1,2]
  *    bins = 256
  *    source = "hdfs://cluster1.data.org:9000/data/maccdc2012_00008.csv"
  *  }
  *  }}}
  *
  */
object TilingConfig extends ConfigParser{
  private val RootKey= "tiling"
  private val LevelsKey = "levels"
  private val BinsKey = "bins"
  private val SourceKey = "source"

  /**
    * Parse general tiling parameters out of a config container and instantiates a `TilingConfig`
    * object from them.
    *
    * @param config The configuration container.
    * @return A `Try` containing the `TilingConfig` object.
    */
  def parse(config: Config): Try[TilingConfig] = {
    Try {
      val tilingConfig = config.getConfig(RootKey)
      TilingConfig(
        tilingConfig.getIntList(LevelsKey).asScala.map(_.asInstanceOf[Int]).toList,
        tilingConfig.getString(SourceKey),
        getIntOption(tilingConfig, BinsKey)
      )
    }
  }
}

/**
  * File system output configuration for saving tile results.
  *
  * @param destPath  The path to write to.
  * @param layer The layer name - will be appened to the path.
  * @param extension The extension to apply the tile files.
  */
case class FileOutputConfig(destPath: String, layer: String, extension: String)

/**
  * Provides functions for parsing file system output configuration data out of a `com.typesafe.config.Config` object.
  *
  * Valid properties are:
  *
  *   - `dest` - A path specifying the write location in the file system.
  *   - `layer` - The name of the layer to write.  Will be appended to the path sepcified by teh `dest` argument.
  *   - `ext` - The extension to assign to the create tile files. [OPTIONAL] - defaults to `bin`
  *
  *  Example from config file (in [[https://github.com/typesafehub/config#using-hocon-the-json-superset HOCON]] notation):
  *
  *  {{{
  *  fileOutput {
  *     dest = ~/data/tiles
  *     layer = taxi_heatmap
  *  }
  *  }}}
  *
  */
object FileOutputConfig extends ConfigParser{
  val fileOutputKey = "fileOutput"
  private val pathKey = "dest"
  private val layerKey = "layer"
  private val extensionKey = "ext"
  private val defaultExtension = ".bin"

/**
  * Parses file output parameters out of a config container and instantiates a `FileOutputConfig`
  * object from them.
  *
  * @param config The configuration container.
  * @return A `Try` containing the `FileOutputConfig` object.
  */
  def parse(config: Config): Try[FileOutputConfig] = {
    Try {
      val fileConfig = config.getConfig(fileOutputKey)

      FileOutputConfig(
        fileConfig.getString(pathKey),
        fileConfig.getString(layerKey),
        getString(fileConfig, extensionKey, defaultExtension)
      )
    }
  }
}

/**
  * S3 output configuration that specifies how tile results will be saved.
  *
  * @param accessKey An AWS access key
  * @param secretKey An AWS secret key
  * @param bucket The name of the bucket to save to
  * @param layer The name of the tile layer
  * @param extension The extension to use for save tiles
  */
case class S3OutputConfig(accessKey: String, secretKey: String, bucket: String, layer: String, extension: String)

/**
  * Provides functions for parsing Amazon S3 output configuration data out of a `com.typesafe.config.Config` object.
  *
  * Valid properties are:
  *
  *   - `awsAccessKey` - AWS access key string.
  *   - `awsSecretKey` - AWS secret key string.
  *   - `bucket` - The name of the S3 bucket to write to.
  *   - `layer` - The name of the layer to write to.
  *   - `ext` - The extension to assign to the create tile files. [OPTIONAL] - defaults to `bin`
  *
  *  Example from config file (in [[https://github.com/typesafehub/config#using-hocon-the-json-superset HOCON]] notation):
  *
  *  {{{
  *  s3Output {
  *    awsAccessKey = $AWS_ACCESS_KEY
  *    awsSecretKey = $AWS_SECRET_KEY
  *    bucket = tile_data
  *    layer = taxi_heatmap
  *  }
  *  }}}
  *
  *  In this case, we use HOCON notation to indicate that the key values should be read from env variables.
  */
object S3OutputConfig extends ConfigParser{
  val s3OutputKey = "s3Output"
  private val awsAccessKey = "awsAccessKey"
  private val awsSecretKey = "awsSecretKey"
  private val bucketKey = "bucket"
  private val layerKey = "layer"
  private val extensionKey = "ext"
  private val defaultExtension = "bin"

  /**
    * Parses file output parameters out of a config container and instantiates an `S3OutputConfig`
    * object from them.
    *
    * @param config The configuration container.
    * @return A `Try` containing the `S3OutputConfig` object.
    */
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

/**
  * HBase configuration that specifies how tiles will be saved.
  *
  * @param configFiles The paths to the HBase connection configuration files.
  * @param layer The name of the layer to save the tiles as - this translates to a table name in HBase.
  * @param qualifier An optional column qualifier
  */
case class HBaseOutputConfig (configFiles: Seq[String], layer: String, qualifier: Option[String])

/**
  * Provides functions for parsing HBase output configuration data out of a `com.typesafe.config.Config` object.
  *
  * Valid properties are:
  *
  *   - `configFiles` - An array of string paths specifying the HBase config file location.  These files be passed
  *                     directly to the HBase connection facilities.
  *   - `layer` - The name of the layer to write.  Translates a table name in HBase.
  *   - `qualifier` - A column qualifier string. [OPTIONAL]
  *
  *  Example from config file (in [[https://github.com/typesafehub/config#using-hocon-the-json-superset HOCON]] notation):
  *
  *  {{{
  *  hbaseOutput {
  *     configFiles = [~/hbase_local_conf/hbase-site.xml]
  *     layer = taxi_heatmap
  *  }
  *  }}}
  *
  */
object HBaseOutputConfig extends ConfigParser{
  val hBaseOutputKey = "hbaseOutput"
  private val configFilesKey = "configFiles"
  private val layerKey = "layer"
  private val qualifierKey = "qualifier"

  /**
    * Parses hbase output parameters out of a config container and instantiates an `HBaseOutputConfig`
    * object from them.
    *
    * @param config The configuration container.
    * @return A `Try` containing the `HBaseOutputConfig` object.
    */
  def parse (config: Config): Try[HBaseOutputConfig] = {
    Try {
      val hbaseConfig = config.getConfig(hBaseOutputKey)
      val configFilesList = hbaseConfig.getStringList(configFilesKey)
      val configFiles = configFilesList.toArray(new Array[String](configFilesList.size()))

      HBaseOutputConfig(
        configFiles,
        hbaseConfig.getString(layerKey),
        getStringOption(hbaseConfig, qualifierKey)
      )
    }
  }
}
