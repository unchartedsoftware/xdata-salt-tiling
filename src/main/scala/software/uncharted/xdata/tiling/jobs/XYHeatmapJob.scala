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
import software.uncharted.sparkpipe.ops.xdata.io.serializeBinArray
import software.uncharted.sparkpipe.ops.xdata.salt.{CartesianHeatmapOp, MercatorHeatmapOp, ZXYOp}
import software.uncharted.xdata.tiling.config.{CartesianProjectionConfig, MercatorProjectionConfig, XYHeatmapConfig}
import software.uncharted.xdata.tiling.jobs.JobUtil.{dataframeFromSparkCsv, createMetadataOutputOperation}

/**
  * Simple job to do ordinary 2-d tiling
  */
// scalastyle:off method.length
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
    val heatmapConfig = XYHeatmapConfig.parse(config).getOrElse {
      logger.error("Invalid heatmap op config")
      sys.exit(-1)
    }

    val xyBoundsFound = heatmapConfig.projection.xyBounds match {
      case ara: Some[(Double, Double, Double, Double)] => true
      case None => false
      case _ => logger.error("Invalid XYbounds"); sys.exit(-1)
    }

    val tileSize = tilingConfig.bins.getOrElse(ZXYOp.TILE_SIZE_DEFAULT)

    // create the heatmap operation based on the projection
    val heatmapOperation = heatmapConfig.projection match {
      case _: MercatorProjectionConfig => MercatorHeatmapOp(
        heatmapConfig.xCol,
        heatmapConfig.yCol,
        heatmapConfig.valueCol,
        tilingConfig.levels,
        if (xyBoundsFound) heatmapConfig.projection.xyBounds else None,
        tileSize
      )(_)
      case _: CartesianProjectionConfig  => CartesianHeatmapOp(
        heatmapConfig.xCol,
        heatmapConfig.yCol,
        heatmapConfig.valueCol,
        tilingConfig.levels,
        if (xyBoundsFound) heatmapConfig.projection.xyBounds else None,
        tileSize
      )(_)
      case _ => logger.error("Unknown projection ${topicsConfig.projection}"); sys.exit(-1)
    }

    // Pipe the dataframe
    Pipe(dataframeFromSparkCsv(config, tilingConfig.source, schema, sparkSession))
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
