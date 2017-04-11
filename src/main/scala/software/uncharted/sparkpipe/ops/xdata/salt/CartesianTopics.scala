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
import org.apache.spark.sql.DataFrame
import software.uncharted.salt.core.analytic.collection.TopElementsAggregator
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.TileLevelRequest

object CartesianTopics extends CartesianOp {

    def apply(// scalastyle:ignore
              xCol: String,
              yCol: String,
              textCol: String,
              latLonBounds: Option[(Double, Double, Double, Double)],
              topicLimit: Int,
              zoomLevels: Seq[Int],
              tileSize: Int)
             (input: DataFrame):
    RDD[SeriesData[(Int, Int, Int), (Int, Int), List[(String, Int)], Nothing]] = {

    val aggregator = new TopElementsAggregator[String](topicLimit)

    val request = new TileLevelRequest(zoomLevels, (tc: (Int, Int, Int)) => tc._1)
    super.apply(
    tileSize,
    xCol,
    yCol,
    textCol,
    latLonBounds.get,
    zoomLevels,
    aggregator,
    None)(request)(input)
  }
}
