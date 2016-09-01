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
import software.uncharted.salt.core.generation.{Series, TileGenerator}
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.rdd.RDDTileGenerator
import software.uncharted.salt.core.generation.request.{TileLevelRequest, TileRequest}

import scala.reflect.ClassTag

/**
  * Basic building-block operations from which other salt operations are built or composed.
  */
object BasicSaltOperations {
  def genericTiling[RT, DC, TC: ClassTag, BC, T, U, V, W, X] (series: Series[RT, DC, TC, BC, T, U, V, W, X])
                                                             (request: TileRequest[TC])
                                                             (data: RDD[RT]): RDD[SeriesData[TC, BC, V, X]] = {
    val generator = TileGenerator(data.sparkContext)
    generator.generate(data, series, request).flatMap(t => series(t))
  }

  def genericFullTilingRequest[RT, DC, TC: ClassTag, BC, T, U, V, W, X] (series: Series[RT, DC, TC, BC, T, U, V, W, X],
                                                                         levels: Seq[Int],
                                                                         getZoomLevel: TC => Int)
                                                                        (data: RDD[RT]): RDD[SeriesData[TC, BC, V, X]] = {
    genericTiling(series)(new TileLevelRequest[TC](levels, getZoomLevel))(data)
  }

}
