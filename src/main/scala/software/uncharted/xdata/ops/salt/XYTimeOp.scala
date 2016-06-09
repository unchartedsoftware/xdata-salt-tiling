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
import software.uncharted.salt.core.generation.{TileGenerator, Series}
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.TileRequest
import software.uncharted.sparkpipe.Pipe
import software.uncharted.sparkpipe.ops.core.dataframe.castColumns

trait XYTimeOp {

  val defaultTileSize = 256

  def apply[T, U, V, W, X](// scalastyle:ignore
                           xCol: String,
                           yCol: String,
                           rangeCol: String,
                           timeRange: RangeDescription[Long],
                           valueExtractor: (Row) => Option[T],
                           binAggregator: Aggregator[T, U, V],
                           tileAggregator: Option[Aggregator[V, W, X]],
                           zoomLevels: Seq[Int],
                           tileSize: Int,
                           projection: XYTimeProjection)
                          (request: TileRequest[(Int, Int, Int)])(input: DataFrame):
  RDD[SeriesData[(Int, Int, Int), (Int, Int, Int), V, X]] = {

    // Use the pipeline to cast columns to expected values and select them into a new dataframe
    val castCols = Map(xCol -> DoubleType.simpleString, yCol -> DoubleType.simpleString, rangeCol ->  TimestampType.simpleString)
    val frame = Pipe(input)
      .to(castColumns(castCols))
      .run()

    // Extracts lat, lon, time coordinates from row
    val coordExtractor = (r: Row) => {
      val xIndex = r.schema.fieldIndex(xCol)
      val yIndex = r.schema.fieldIndex(yCol)
      val rangeIndex = r.schema.fieldIndex(rangeCol)
      if (!r.isNullAt(xIndex) && !r.isNullAt(yIndex) && !r.isNullAt(rangeIndex)) {
        Some(r.getDouble(xIndex), r.getDouble(yIndex), r.getTimestamp(rangeIndex).getTime())
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

    val generator = TileGenerator(frame.sqlContext.sparkContext)
    generator.generate(frame.rdd, series, request).map(series(_).get)
  }
}

