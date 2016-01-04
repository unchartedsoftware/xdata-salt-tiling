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

import org.apache.spark.sql.DataFrame
import software.uncharted.xdata.spark.SparkFunSpec

// scalastyle:off magic.number

case class TestData(lon: Double, lat: Double, value: Double, time: Long)

class GeoHeatmapOpTest extends SparkFunSpec {

  def genData: DataFrame = {

    val testData =
      // 1st time bucket
      List(TestData(-91.0, -67.0, 1.0, 101L),
        TestData(-89.0, -65.0, 2.0, 101L),
        TestData(91.0, -65.0, 3.0, 101L),
        TestData(89.0, -67.0, 4.0, 101L),
        TestData(89.0, 65.0, 5.0, 101L),
        TestData(91.0, 67.0, 6.0, 101L),
        TestData(-91.0, 65.0, 7.0, 101L),
        TestData(-89.0, 67.0, 8.0, 101L),
        // 2nd time bucket
        TestData(-91.0, -67.0, 9.0, 201L),
        TestData(-89.0, -65.0, 10.0, 201L),
        TestData(91.0, -65.0, 11.0, 201L),
        TestData(89.0, -67.0, 12.0, 201L),
        TestData(89.0, 65.0, 13.0, 201L),
        TestData(91.0, 67.0, 14.0, 201L),
        TestData(-91.0, 65.0, 15.0, 201L),
        TestData(-89.0, 67.0, 16.0, 201L),

        TestData(-179, MercatorTimeProjection.maxLat - 1.0, 0.5, 301L),
        TestData(-179, MercatorTimeProjection.maxLat - 1.0, 0.5, 301L))

    val tsqlc = sqlc
    import tsqlc.implicits._ // scalastyle:ignore

    sc.parallelize(testData).toDF()
  }

  describe("GeoHeatmapOpTest") {
    it("should create a quadtree of tiles where empty tiles are skipped") {
      val conf = GeoHeatmapOpConf(3, 0, 1, 3, Some(2), RangeDescription.fromCount(0, 800, 10), 10)
      val result = GeoHeatmapOp.geoHeatmapOp(conf)(genData).collect().map(_.coords).toSet
      val expectedSet = Set(
        (0,0,0), // l0
        (1,0,0), (1,1,0), (1,1,1), (1,0,1), // l1
        (2, 0, 0), (2, 2, 0), (2, 1, 1), (2, 3, 1), (2, 0, 2), (2, 2, 2), (2, 1, 3), (2, 3, 3), (2, 0, 3)) // l2
      assertResult((Set(), Set()))((expectedSet diff result, result diff expectedSet))
    }

    it("should create time bins from a range and bucket count") {
      val conf = GeoHeatmapOpConf(1, 0, 1, 3, Some(2), RangeDescription.fromCount(0, 800, 10), 10)
      val result = GeoHeatmapOp.geoHeatmapOp(conf)(genData).collect()
      assertResult(10 * 10 * 10)(result(0).bins.length)
    }

    it("should sum values that are in the same bin ") {
      val conf = GeoHeatmapOpConf(1, 0, 1, 3, Some(2), RangeDescription.fromCount(0, 800, 10), 10)
      val result = GeoHeatmapOp.geoHeatmapOp(conf)(genData).collect()
      val proj = new MercatorTimeProjection(RangeDescription.fromCount(0L, 800L, 10))
      assertResult(1)(result(0).bins(proj.binTo1D((0, 0, 3), (9, 9, 9))))
    }

    it("should not aggregate across time buckets") {
      val conf = GeoHeatmapOpConf(3, 0, 1, 3, None, RangeDescription.fromCount(0, 800, 10), 10)
      val result = GeoHeatmapOp.geoHeatmapOp(conf)(genData).collect()
      val proj = new MercatorTimeProjection(RangeDescription.fromCount(0L, 800L, 10))

      val tile = (t: (Int, Int, Int)) => result.find(s => s.coords == t)

      assertResult(2)(tile((0, 0, 0)).getOrElse(fail()).bins(proj.binTo1D((0, 0, 3), (9, 9, 9))))
      assertResult(2)(tile((1, 0, 1)).getOrElse(fail()).bins(proj.binTo1D((0, 0, 3), (9, 9, 9))))
      assertResult(2)(tile((2, 0, 3)).getOrElse(fail()).bins(proj.binTo1D((0, 1, 3), (9, 9, 9))))
    }

    it("should use a value of 1.0 for each bin when no value column is specified") {
      val conf = GeoHeatmapOpConf(1, 0, 1, 3, None, RangeDescription.fromCount(0, 800, 10), 10)
      val result = GeoHeatmapOp.geoHeatmapOp(conf)(genData).collect()
      val proj = new MercatorTimeProjection(RangeDescription.fromCount(0L, 800L, 10))
      assertResult(2)(result(0).bins(proj.binTo1D((0, 0, 3), (9, 9, 9))))
    }
  }
}
