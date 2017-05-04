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
import org.apache.spark.sql.{Column, SparkSession}
import software.uncharted.sparkpipe.Pipe
import software.uncharted.sparkpipe.ops.xdata.io.serializeBinArray
import software.uncharted.sparkpipe.ops.xdata.salt.IPSegmentOp
import software.uncharted.xdata.tiling.config.IPSegmentConfig
import software.uncharted.xdata.tiling.jobs.JobUtil.dataframeFromSparkCsv

/**
  * A basic job to do segment based IP tiling.  Loads data from HDFS, creates heatmap tiles with arcs between
  * the endpoints calculated using the IP projection and writes the results out to configured destination.
  */
// scalastyle:off method.length
// scalastyle:off cyclomatic.complexity
object IPSegmentJob extends AbstractJob {
  /**
    * This function actually executes the task the job describes
    *
    * @param sparkSession   A SparkSession from which to extract input data as a DataFrame
    * @param config The job configuration
    */
  override def execute(sparkSession: SparkSession, config: Config): Unit = {
    val schema = parseSchema(config)
    val tilingConfig = parseTilingParameters(config)
    val outputOperation = parseOutputOperation(config)

    // Parse IP tiling parameters out of supplied config
    val ipConfig = IPSegmentConfig.parse(config).recover { case err: Exception =>
      logger.error(s"Invalid '${IPSegmentConfig.RootKey}' config", err)
      sys.exit(-1)
    }.get

    val tilingOp = IPSegmentOp(ipConfig.ipFromCol,ipConfig.ipToCol, ipConfig.valueCol.get,
      ipConfig.projectionConfig, ipConfig.arcType, ipConfig.projectionConfig.xyBounds, ipConfig.minSegLen,
      ipConfig.maxSegLen, tilingConfig.levels)(_)

    val seqCols = Seq(ipConfig.ipFromCol, ipConfig.ipToCol, ipConfig.valueCol.get)
    val selectCols = seqCols.map(new Column(_))

    // Create the dataframe from the input config
    val df = dataframeFromSparkCsv(config, tilingConfig.source, schema, sparkSession)

    // Pipe the dataframe
    Pipe(df)
      .to(_.select(selectCols: _*))
      .to(_.cache())
      .to(tilingOp)
      .to(serializeBinArray)
      .to(outputOperation)
      .run()
  }
}

// scalastyle:on method.length
// scalastyle:on cyclomatic.complexity
