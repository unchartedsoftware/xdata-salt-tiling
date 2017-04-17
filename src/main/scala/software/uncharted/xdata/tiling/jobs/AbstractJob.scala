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

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import grizzled.slf4j.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import software.uncharted.xdata.tiling.config.{Schema, SparkConfig, TilingConfig}
import software.uncharted.xdata.tiling.jobs.JobUtil.{OutputOperation, createTileOutputOperation}

import scala.collection.JavaConverters._ //scalastyle:ignore
import scala.io.Source

/**
  * A basic job trait that standardizes reading and combining config files and execution
  */
trait AbstractJob extends Logging {
  var debug = false


  /**
    * Parse the schema, and exit on any errors
    * @param config The configuration from which to determine the schema
    * @return A fully determined schema
    */
  protected def parseSchema (config: Config): StructType = {
    Schema(config).recover { case err: Exception =>
      error("Couldn't create schema - exiting", err)
      sys.exit(-1)
    }.get
  }

  /**
    * Parse tiling parameters from supplied config
    * @param config The configuration from which to determine tiling parameters
    * @return A fully determined set of tiling parameters
    */
  protected def parseTilingParameters (config: Config): TilingConfig = {
    TilingConfig.parse(config).recover { case err: Exception =>
      logger.error("Invalid tiling config", err)
      sys.exit(-1)
    }.get
  }

  /**
    * Parse configuration parameters to determine a tiling output operation
    * @param config The configuration from which to determine the output operation
    * @return A fully determined output operation
    */
  protected def parseOutputOperation (config: Config): OutputOperation = {
    createTileOutputOperation(config).recover { case err: Exception =>
      logger.error("Output operation config", err)
      sys.exit(-1)
    }.get
  }

  /**
    * This function actually executes the task the job describes
    *
    * @param session A spark session in which to run spark processes in our job
    * @param config The job configuration
    */
  def execute(session: SparkSession, config: Config): Unit

  def readConfigArguments (args: Array[String]): Config = {
    // get the properties file path
    if (args.length < 1) {
      logger.error("Path to conf file required")
      sys.exit(-1)
    }

    val environmentalConfig = ConfigFactory.load()
    args.flatMap { cfgFileName =>
      if (cfgFileName == "debug") {
        debug = true
        None
      } else {
        val cfgFile = new File(cfgFileName)
        if (!cfgFile.exists()) {
          logger.warn(s"Config file $cfgFileName doesn't exist")
          None
        } else if (!cfgFile.isFile) {
          logger.warn(s"Config file $cfgFileName is a directory, not a file")
          None
        } else if (!cfgFile.canRead) {
          logger.warn(s"Can't read config file $cfgFileName")
          None
        } else {
          if (debug) {
            // scalastyle:off regex
            println(s"Reading config file $cfgFile")
            // scalastyle:on regex
          }
          Some(ConfigFactory.parseReader(Source.fromFile(cfgFile).bufferedReader()))
        }
      }
    }.fold(environmentalConfig) { (base, fallback) =>
      base.withFallback(fallback)
    }.resolve()
  }

  def execute(args: Array[String]): Unit = {
    // load properties file from supplied URI
    val config = readConfigArguments(args)

    if (debug) {
      debugConfig(config)
    }

    val sparkSession = SparkConfig(config)
    try {
      execute(sparkSession, config)
    } finally {
      sparkSession.sparkContext.stop()
    }
  }

  // scalastyle:off regex
  def debugConfig (config: Config): Unit = {
    println("Full config info:")
    config.entrySet().asScala.foreach{entry =>
      val key = entry.getKey
      val value = entry.getValue
      println(s"""\t"$key":"$value"""")
    }
  }
  // scalastyle:on regex

  /**
    * All jobs need a main function to allow them to run
    */
  def main (args: Array[String]): Unit = {
    execute(args)
  }
}
