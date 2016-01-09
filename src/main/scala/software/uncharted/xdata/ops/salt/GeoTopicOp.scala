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
package software.uncharted.xdata.ops.salt

import grizzled.slf4j.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{TimestampType, DoubleType}
import org.apache.spark.sql.{Row, Column, DataFrame}
import software.uncharted.salt.core.generation.Series
import software.uncharted.salt.core.generation.mapreduce.MapReduceTileGenerator
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.TileLevelRequest
import software.uncharted.sparkpipe.Pipe
import software.uncharted.sparkpipe.ops.core.dataframe.castColumns

case class GeoTopicOpConf(latCol: String,
                          lonCol: String,
                          timeCol: String,
                          countsCol: String,
                          latLonBounds: Option[(Double, Double, Double, Double)],
                          timeRange: RangeDescription[Long],
                          topicLimit: Int,
                          levels: Int)

object GeoTopicOp extends Logging {

  val maxLon = 180
  val maxLat = 85.05112878

  def apply(conf: GeoTopicOpConf)(input: DataFrame): RDD[SeriesData[(Int, Int, Int), List[(String, Int)], Nothing]] = {
    // Use the pipeline to cast columns to expected values and select them into a new dataframe
    val selectCols = Seq(conf.latCol, conf.lonCol, conf.timeCol, conf.countsCol).map(new Column(_))

    val castCols = Seq(
      conf.latCol -> DoubleType.simpleString,
      conf.lonCol -> DoubleType.simpleString,
      conf.timeCol -> TimestampType.simpleString).toMap

    val frame = Pipe(input)
      .to(castColumns(castCols))
      .to(_.select(selectCols:_*))
      .run()

    // Extracts lat, lon, time coordinates from row - can assume (0,1,2) indices given select above
    val coordExtractor = (r: Row) => {
      if (!r.isNullAt(0) && !r.isNullAt(1) && !r.isNullAt(2)) {
        Some(r.getDouble(0), r.getDouble(1), r.getTimestamp(2).getTime)
      } else {
        None
      }
    }

    // Extracts value data from row
    val valueExtractor: Option[(Row) => Option[Seq[(String, Int)]]] = Some((r: Row) => {
      if (!r.isNullAt(3)) Some(r.getMap(3).toSeq) else None
    })

    // create a default projection from data-space into mercator tile space
    val projection = conf.latLonBounds.map { b =>
      new MercatorTimeProjection((b._2, b._1, conf.timeRange.min), (b._4, b._3, conf.timeRange.max), conf.timeRange.count)
    }.getOrElse(new MercatorTimeProjection(conf.timeRange))

    val aggregator = new ElementScoreAggregator[String](conf.topicLimit)

    // create the series to tie everything together
    val series = new Series(
      (0, 0, conf.timeRange.count-1),
      coordExtractor,
      projection,
      valueExtractor,
      aggregator,
      None
    )

    val generator = new MapReduceTileGenerator(frame.sqlContext.sparkContext)
    val request = new TileLevelRequest(List.range(0, conf.levels), (tc: (Int, Int, Int)) => tc._1)
    generator.generate(frame.rdd, series, request).map(t => series(t))
  }
}
