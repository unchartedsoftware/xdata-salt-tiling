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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row}
import software.uncharted.salt.core.analytic.Aggregator
import software.uncharted.salt.core.generation.Series
import software.uncharted.salt.core.generation.mapreduce.MapReduceTileGenerator
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.TileRequest
import software.uncharted.sparkpipe.Pipe
import software.uncharted.sparkpipe.ops.core.dataframe.castColumns

trait MercatorTimeOp {

  val defaultTileSize = 256

  def apply[T, U, V, W, X](// scalastyle:ignore
                           latCol: String,
                           lonCol: String,
                           rangeCol: String,
                           latLonBounds: Option[(Double, Double, Double, Double)],
                           timeRange: RangeDescription[Long],
                           valueExtractor: Option[(Row) => Option[T]],
                           binAggregator: Aggregator[T, U, V],
                           tileAggregator: Option[Aggregator[V, W, X]],
                           tileSize: Int,
                           tms: Boolean)
                          (request: TileRequest[(Int, Int, Int)])(input: DataFrame):
  RDD[SeriesData[(Int, Int, Int), V, X]] = {

    // create a default projection from data-space into mercator tile space
    val projection = latLonBounds.map { b =>
      new MercatorTimeProjection((b._2, b._1, timeRange.min), (b._4, b._3, timeRange.max), timeRange.count, tms)
    }.getOrElse(new MercatorTimeProjection(timeRange))

    // Use the pipeline to cast columns to expected values and select them into a new dataframe
    val castCols = Map(latCol -> DoubleType.simpleString, lonCol -> DoubleType.simpleString, rangeCol ->  TimestampType.simpleString)
    val frame = Pipe(input)
      .to(castColumns(castCols))
      .run()

    // Extracts lat, lon, time coordinates from row
    val coordExtractor = (r: Row) => {
      val lonIndex = r.schema.fieldIndex(lonCol)
      val latIndex = r.schema.fieldIndex(latCol)
      val rangeIndex = r.schema.fieldIndex(rangeCol)
      if (!r.isNullAt(lonIndex) && !r.isNullAt(latIndex) && !r.isNullAt(rangeIndex)) {
        Some(r.getDouble(lonIndex), r.getDouble(latIndex), r.getTimestamp(rangeIndex).getTime())
      } else {
        None
      }
    }

    // create the series to tie everything together
    val series = new Series(
      (tileSize - 1, tileSize - 1, timeRange.count - 1),
      coordExtractor,
      projection,
      valueExtractor,
      binAggregator,
      tileAggregator
    )

    val generator = new MapReduceTileGenerator(frame.sqlContext.sparkContext)
    generator.generate(frame.rdd, series, request).map(t => series(t))
  }
}
