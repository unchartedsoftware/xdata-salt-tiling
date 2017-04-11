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
package software.uncharted.xdata.tiling.jobs

// scalastyle:off
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import software.uncharted.sparkpipe.Pipe
import software.uncharted.sparkpipe.ops.core.dataframe.text.{includeTermFilter, split}
import software.uncharted.sparkpipe.ops.xdata.io.serializeElementScore
import software.uncharted.sparkpipe.ops.xdata.salt.{CartesianTopics, MercatorTopics}
import software.uncharted.xdata.tiling.config.{CartesianProjectionConfig, MercatorProjectionConfig, TilingConfig, XYTopicsConfig}
import software.uncharted.xdata.tiling.jobs.JobUtil.{dataframeFromSparkCsv, createMetadataOutputOperation}

object XYTopicsJob extends AbstractJob {

  def execute(sparkSession: SparkSession, config: Config): Unit = {
    val schema = parseSchema(config)
    val tilingConfig = parseTilingParameters(config)
    val outputOperation = parseOutputOperation(config)

    // Parse geo heatmap parameters out of supplied config
    val topicsConfig = XYTopicsConfig.parse(config).getOrElse {
      logger.error("Invalid heatmap op config")
      sys.exit(-1)
    }

    val exists_xyBounds  = topicsConfig.projection.xyBounds match {
      case ara : Some[(Double, Double, Double, Double)] => true
      case None => false
      case _ => logger.error("Invalid XYbounds"); sys.exit(-1)
    }
    // when time format is used, need to pick up the converted time column
    val topicsOp = topicsConfig.projection match {
      case _: MercatorProjectionConfig => MercatorTopics(
        topicsConfig.yCol,
        topicsConfig.xCol,
        topicsConfig.textCol,
        if (exists_xyBounds) topicsConfig.projection.xyBounds else None,
        topicsConfig.topicLimit,
        tilingConfig.levels,
        tilingConfig.bins.getOrElse(1))(_)
      case _: CartesianProjectionConfig => CartesianTopics(
        topicsConfig.xCol,
        topicsConfig.yCol,
        topicsConfig.textCol,
        if (exists_xyBounds) topicsConfig.projection.xyBounds else None,
        topicsConfig.topicLimit,
        tilingConfig.levels,
        tilingConfig.bins.getOrElse(1))(_)
      case _ => logger.error("Unknown projection ${topicsConfig.projection}"); sys.exit(-1)
    }

    // Pipe the dataframe
    Pipe(dataframeFromSparkCsv(config, tilingConfig.source, schema, sparkSession))
      .to(split(topicsConfig.textCol, "\\b+"))
      .to(includeTermFilter(topicsConfig.textCol, topicsConfig.termList.keySet))
      .to(_.select(topicsConfig.xCol, topicsConfig.yCol, topicsConfig.textCol))
      .to(_.cache())
      .to(topicsOp)
      .to(serializeElementScore)
      .to(outputOperation)
      .run()

    writeMetadata(config, tilingConfig, topicsConfig)
  }
  private def writeMetadata(baseConfig: Config, tilingConfig: TilingConfig, topicsConfig: XYTopicsConfig): Unit = {
    import net.liftweb.json.JsonAST._
    import net.liftweb.json.JsonDSL._ // scalastyle:ignore

    val outputOp = createMetadataOutputOperation(baseConfig)

    val binCount = tilingConfig.bins.getOrElse(1)
    val levelMetadata = ("bins" -> binCount)
    val jsonBytes = compactRender(levelMetadata).getBytes.toSeq
    outputOp.foreach(_("metadata.json", jsonBytes))

    val termJsonBytes = compactRender(topicsConfig.termList).toString().getBytes.toSeq
    outputOp.foreach(_("terms.json", termJsonBytes))
  }
}
