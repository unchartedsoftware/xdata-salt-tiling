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
package software.uncharted.sparkpipe.ops.xdata.salt

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row}
import software.uncharted.salt.core.analytic.Aggregator
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.TileRequest
import software.uncharted.salt.core.generation.{Series, TileGenerator}
import software.uncharted.salt.core.projection.Projection
import software.uncharted.sparkpipe.Pipe
import software.uncharted.sparkpipe.ops.core.dataframe.castColumns



// scalastyle:off parameter.number

/**
  * An operation which generates tile layers of point locations, located by IP address, from a DataFrame, using Salt
  */
object IPHeatmapOp {
  val defaultTileSize = 256
  val STRING_TYPE = "string"
  def apply[T, U, V, W, X](projection: Projection[String, (Int, Int, Int), (Int, Int)],
                           tileSize: Int,
                           ipCol: String,
                           vCol: String,
                           binAggregator: Aggregator[T, U, V],
                           tileAggregator: Option[Aggregator[V, W, X]]
                          )(request: TileRequest[(Int, Int, Int)])(input: DataFrame): RDD[SeriesData[(Int, Int, Int), (Int, Int), V, X]] = {

    val selectCols = Seq(ipCol, vCol).map(new Column(_))

    // Use the pipeline to convert x/y cols to doubles, and select them along with v col first
    val frame = Pipe(input)
      .to(castColumns(Map(ipCol -> STRING_TYPE)))
      .to(_.select(selectCols: _*))
      .run()

    // ip address will always be column 0
    val cExtractor = (r: Row) => {
      if (!r.isNullAt(0)) {
        Some(r.getString(0))
      } else {
        None
      }
    }

    // value will always be column 1
    val vExtractor = (r: Row) => {
      if (!r.isNullAt(1)) {
        Some(r.getAs[T](1))
      } else {
        None
      }
    }

    // create a series for our heatmap
    val series = new Series(
      (tileSize - 1, tileSize - 1),
      cExtractor,
      projection,
      vExtractor,
      binAggregator,
      tileAggregator
    )

    val sc = frame.sqlContext.sparkContext
    val generator = TileGenerator(sc)

    generator.generate(frame.rdd, series, request).flatMap(t => series(t))
  }
}

/**
  * An operation which generates tile layers of line segments, located from pairs of IP addresses, from a DataFrame,
  * using Salt
  */
object IPSegmentOp {
  val STRING_TYPE = "string"
  def apply[T, U, V, W, X]( // scalastyle:ignore
                            projection: Projection[(String, String), (Int, Int, Int), (Int, Int)],
                            tileSize: Int,
                            ipFromCol: String,
                            ipToCol: String,
                            vCol: String,
                            binAggregator: Aggregator[T, U, V],
                            tileAggregator: Option[Aggregator[V, W, X]]
                          )(request: TileRequest[(Int, Int, Int)])(input: DataFrame): RDD[SeriesData[(Int, Int, Int), (Int, Int), V, X]] = {

    val selectCols = Seq(ipFromCol, ipToCol, vCol).map(new Column(_))

    // Use the pipeline to convert x/y cols to doubles, and select them along with v col first
    val frame = Pipe(input)
      .to(castColumns(Map(ipFromCol -> STRING_TYPE, ipToCol -> STRING_TYPE)))
      .to(_.select(selectCols: _*))
      .run()

    // ip addresses will always be columns 0 and 1
    val cExtractor = (r: Row) => {
      if (!r.isNullAt(0) && !r.isNullAt(1)) {
        Some((r.getString(0), r.getString(1)))
      } else {
        None
      }
    }

    // value will always be column 2
    val vExtractor = (r: Row) => {
      if (!r.isNullAt(2)) {
        Some(r.getAs[T](2))
      } else {
        None
      }
    }

    // create a series for our heatmap
    val series = new Series(
      (tileSize - 1, tileSize - 1),
      cExtractor,
      projection,
      vExtractor,
      binAggregator,
      tileAggregator
    )

    val sc = frame.sqlContext.sparkContext
    val generator = TileGenerator(sc)

    generator.generate(frame.rdd, series, request).flatMap(t => series(t))
  }
}
