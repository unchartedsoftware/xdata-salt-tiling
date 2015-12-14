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

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSpec}

// scalastyle:off magic.number

case class TestData(lon: Double, lat: Double, value: Double, time: Long)

class GeoHeatmapOpTest extends FunSpec with BeforeAndAfter {

  @transient private var sc: SparkContext = _
  @transient private var sqlc: SQLContext = _

  before {
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set("spark.driver.allowMultipleContexts", "true")

    sc = new SparkContext(conf)
    sqlc = new SQLContext(sc)
  }

  after {
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
    sc.stop()
  }

  def genData: DataFrame = {
    val testData =
      // 1st time bucket
      TestData(-91.0, -46.0, 1.0, 101L) ::
        TestData(-89.0, -44.0, 2.0, 101L) ::
        TestData(91.0, -44.0, 3.0, 101L) ::
        TestData(89.0, -46.0, 4.0, 101L)::
        TestData(89.0, 44.0, 5.0, 101L) ::
        TestData(91.0, 46.0, 6.0, 101L)::
        TestData(-91.0, 44.0, 7.0, 101L) ::
        TestData(-89.0, 46.0, 8.0, 101L) ::
        // 2nd time bucket
        TestData(-91.0, -46.0, 1.0, 201L) ::
        TestData(-89.0, -44.0, 2.0, 201L) ::
        TestData(91.0, -44.0, 3.0, 201L) ::
        TestData(89.0, -46.0, 4.0, 201L)::
        TestData(89.0, 44.0, 5.0, 201L) ::
        TestData(91.0, 46.0, 6.0, 201L)::
        TestData(-91.0, 44.0, 7.0, 201L) ::
        TestData(-89.0, 46.0, 8.0, 201L) ::
        TestData(-179, MercatorTimeProjection.maxLat - 1.0, 0.5, 301L) ::
        TestData(-179, MercatorTimeProjection.maxLat - 1.0, 0.5, 301L) :: Nil

    val tsqlc = sqlc
    import tsqlc.implicits._

    sc.parallelize(testData).toDF()
  }

  describe("A GeoHeatmapOp") {
    it("should create a quadtree of tiles where empty tiles are skipped") {
      val conf = GeoHeatmapOpConf(3, 0, 1, 3, Some(2), RangeDescription.fromCount(0, 800, 10), 10)
      val result = GeoHeatmapOp.geoHeatmapOp(conf)(genData).collect()
      assertResult(14)(result.length)
    }

    it("should create time bins from a range and bucket count") {
      val conf = GeoHeatmapOpConf(1, 0, 1, 3, Some(2), RangeDescription.fromCount(0, 800, 10), 10)
      val result = GeoHeatmapOp.geoHeatmapOp(conf)(genData).collect()
      assertResult(10 * 10 * 10)(result(0).bins.length)
    }

    it("should sum values that are in the same bin ") {
      val conf = GeoHeatmapOpConf(1, 0, 1, 3, Some(2), RangeDescription.fromCount(0, 800, 10), 10)
      val result = GeoHeatmapOp.geoHeatmapOp(conf)(genData).collect()
      val proj = new MercatorTimeProjection(RangeDescription.fromCount(0, 800, 10))
      assertResult(1)(result(0).bins(proj.binTo1D((0, 0, 3), (9, 9, 9))))
    }
  }
}
