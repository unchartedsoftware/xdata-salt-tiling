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
import org.apache.spark.sql.SparkSession
import software.uncharted.sparkpipe.Pipe
import software.uncharted.sparkpipe.ops.xdata.io.serializeElementDoubleScore
import software.uncharted.sparkpipe.ops.xdata.salt.{LDATopicsOp, TileTextOperations}
import software.uncharted.xdata.tiling.config.{DictionaryConfigParser, LDAConfigParser, TileTopicConfig}
import software.uncharted.xdata.tiling.jobs.JobUtil.dataframeFromSparkCsv

import scala.util.{Failure, Success}


/**
  * A job that takes tsv data, breaks out a document from each entry as a word bag, tiles the documents into
  * tile-based word bags, and runs Latent Dirichlet Allocation on those tile word bags to produce a set of
  * topic tiles.
  */
object XYTileLDAJob extends AbstractJob {
  // Parse tile topic parameters out of supplied config
  private def parseTileTopicConfig (config: Config) = {
    TileTopicConfig.parse(config) match {
      case Success(c) => c
      case Failure(e) =>
        logger.error("Error getting topic tiling configuration", e)
        sys.exit(-1)
    }
  }

  // Get LDA-specific configuration
  private def parseLDAConfig (config: Config) = {
    LDAConfigParser.parse(config) match {
      case Success(c) => c
      case Failure(e) =>
        logger.error("Error getting LDA configuration")
        sys.exit(-1)
    }
  }

  // Get dictionary creation configuration
  private def parseDictionaryConfig (config: Config) = {
    DictionaryConfigParser.parse(config)
  }

  /**
    * This function actually executes the task the job describes
    *
    * @param session A spark session in which to run spark processes in our job
    * @param config The job configuration
    */
  override def execute(session: SparkSession, config: Config): Unit = {
    val schema = parseSchema(config)
    val tilingConfig = parseTilingParameters(config)
    val outputOperation = parseOutputOperation(config)
    val tileTopicConfig = parseTileTopicConfig(config)
    val dictionaryConfig = parseDictionaryConfig(config)
    val ldaConfig = parseLDAConfig(config)

    val projection = tileTopicConfig.projectionConfig.createProjection(tilingConfig.levels)
    val wordCloudTileOp = TileTextOperations.termFrequencyOp(
      tileTopicConfig.xColumn,
      tileTopicConfig.yColumn,
      tileTopicConfig.textColumn,
      projection,
      tilingConfig.levels
    )(_)
    val ldaOperation = LDATopicsOp.ldaWordsByTile[Nothing](dictionaryConfig, ldaConfig)(_)

    // Create the dataframe from the input config
    val df = dataframeFromSparkCsv(config, tilingConfig.source, schema, session)

    // Process our data
    Pipe(df)
      .to(wordCloudTileOp)
      .to(ldaOperation)
      .to(serializeElementDoubleScore)
      .to(outputOperation)
      .run
  }
}
