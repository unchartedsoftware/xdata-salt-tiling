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
import software.uncharted.sparkpipe.ops.core.dataframe.text
import software.uncharted.sparkpipe.ops.core.dataframe.text.{includeTermFilter, split}
import software.uncharted.xdata.tiling.jobs.JobUtil.{createMetadataOutputOperation, dataframeFromSparkCsv}
import software.uncharted.sparkpipe.ops.xdata.io.serializeElementScore
import software.uncharted.sparkpipe.ops.xdata.salt.TimeTopicsOp
import software.uncharted.xdata.tiling.config.{TilingConfig, XYTimeTopicsConfig}

/**
  * A job to do x,y coordinate based topic tiling over time.  Topic tiling consists of computing word frequencies for
  * all documents in a tile in each configured time bucket, and selecting the N-most frequent terms from that bucket to
  * include in the tile.  The job loads data from HDFS, creates the time-based heatmap tiles, and writes the results
  * out to the configured destination.
  */
object XYTimeTopicsJob extends AbstractJob {
  def execute(session: SparkSession, config: Config): Unit = {
    val schema = parseSchema(config)
    val tilingConfig = parseTilingParameters(config)
    val outputOperation = parseOutputOperation(config)

    // Parse geo heatmap parameters out of supplied config
    val topicsConfig = XYTimeTopicsConfig.parse(config).recover { case err: Exception =>
      logger.error(s"Invalid '${XYTimeTopicsConfig.rootKey}' config", err)
      sys.exit(-1)
    }.get

    val projection = topicsConfig.projection.createProjection(tilingConfig.levels)
    val topicsOp = TimeTopicsOp(topicsConfig.xCol, topicsConfig.yCol, topicsConfig.timeCol, topicsConfig.textCol,
      projection, topicsConfig.timeRange, topicsConfig.topicLimit, tilingConfig.levels, tilingConfig.bins.getOrElse(1))(_)

    // Create the spark context from the supplied config
    // Create the dataframe from the input config
    val df = dataframeFromSparkCsv(config, tilingConfig.source, schema, session)

    // Pipe the dataframe
    Pipe(df)
      .to(text.includeRowTermFilter(topicsConfig.textCol, topicsConfig.termList))
      .to(split(topicsConfig.textCol, "\\b+"))
      .to(text.stopTermFilter(topicsConfig.textCol, topicsConfig.stopWordsList.toSet))
      .to(_.select(topicsConfig.xCol, topicsConfig.yCol, topicsConfig.timeCol, topicsConfig.textCol))
      .to(_.cache())
      .to(topicsOp)
      .to(serializeElementScore)
      .to(outputOperation)
      .run()

    writeMetadata(config, tilingConfig, topicsConfig)
  }

  private def writeMetadata(baseConfig: Config, tilingConfig: TilingConfig, topicsConfig: XYTimeTopicsConfig): Unit = {
    import net.liftweb.json.JsonAST._ //scalastyle:ignore
    import net.liftweb.json.JsonDSL._ //scalastyle:ignore

    val outputOp = createMetadataOutputOperation(baseConfig)

    val binCount = tilingConfig.bins.getOrElse(1)
    val levelMetadata =
      ("bins" -> binCount) ~
        ("range" ->
          (("start" -> topicsConfig.timeRange.min) ~
            ("count" -> topicsConfig.timeRange.count) ~
            ("step" -> topicsConfig.timeRange.step)))
    val jsonBytes = compactRender(levelMetadata).getBytes.toSeq
    outputOp.foreach(_("metadata.json", jsonBytes))

    val termJsonBytes = compactRender(topicsConfig.termList).toString.getBytes.toSeq
    outputOp.foreach(_("terms.json", termJsonBytes))
  }
}
