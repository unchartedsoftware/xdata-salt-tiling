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
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}
import software.uncharted.sparkpipe.Pipe
import software.uncharted.sparkpipe.ops.xdata.io.serializeBinArray
import software.uncharted.sparkpipe.ops.xdata.salt.HeatmapOp
import software.uncharted.xdata.tiling.config.XYHeatmapConfig
import software.uncharted.xdata.tiling.jobs.JobUtil.{createMetadataOutputOperation, dataframeFromSparkCsv}

/**
  * Simple job to do ordinary 2-d tiling
  */
object XYHeatmapJob extends AbstractJob {
  /**
    * This function actually executes the task the job describes
    *
    * @param sparkSession   A point of access to Apache Spark
    * @param config The job configuration
    */
  override def execute(sparkSession: SparkSession, config: Config): Unit = {
    val schema = parseSchema(config)
    val tilingConfig = parseTilingParameters(config)
    val outputOperation = parseOutputOperation(config)

    // Parse geo heatmap parameters out of supplied config
    val heatmapConfig = XYHeatmapConfig.parse(config).recover { case err: Exception =>
      logger.error("Invalid heatmapOp config", err)
      sys.exit(-1)
    }.get

    val tileSize = tilingConfig.bins.getOrElse(HeatmapOp.DefaultTileSize)

    // create the heatmap operation based on the projection
    val heatmapOperation = HeatmapOp(
      heatmapConfig.xCol,
      heatmapConfig.yCol,
      heatmapConfig.valueCol,
      heatmapConfig.projection.createProjection(tilingConfig.levels),
      tilingConfig.levels,
      tileSize)(_)

    // list of columns we want to filter down to for the computation
    val selectCols = Seq(Some(heatmapConfig.xCol),
                         Some(heatmapConfig.yCol),
                         heatmapConfig.valueCol).flatten.map(new Column(_))

    // Pipe the dataframe.
    Pipe(dataframeFromSparkCsv(config, tilingConfig.source, schema, sparkSession))
      .to(_.select(selectCols: _*))
      .to(_.cache)
      .to(heatmapOperation)
      .to(serializeBinArray)
      .to(outputOperation)
      .run()

    // create and save extra level metadata - the tile x,y dimensions in this case
    writeMetadata(config, tileSize)
  }

  private def writeMetadata(baseConfig: Config, binCount: Int): Unit = {
    import net.liftweb.json.JsonAST._ // scalastyle:ignore
    import net.liftweb.json.JsonDSL._ // scalastyle:ignore

    val levelMetadata = "bins" -> binCount
    val jsonBytes = compactRender(levelMetadata).getBytes.toSeq
    createMetadataOutputOperation(baseConfig).foreach(_("metadata.json", jsonBytes))
  }
}
