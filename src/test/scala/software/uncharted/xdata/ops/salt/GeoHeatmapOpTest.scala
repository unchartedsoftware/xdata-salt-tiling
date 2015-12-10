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

case class TestData(lon: Double, lat: Double, value: Double)

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
      TestData(-91.0, -46.0, 1.0) ::
        TestData(-89.0, -44.0, 2.0) ::
        TestData(91.0, -44.0, 3.0) ::
        TestData(89.0, -46.0, 4.0)::
        TestData(89.0, 44.0, 5.0) ::
        TestData(91.0, 46.0, 6.0)::
        TestData(-91.0, 44.0, 7.0) ::
        TestData(-89.0, 46.0, 8.0) :: Nil

    val tsqlc = sqlc
    import tsqlc.implicits._

    sc.parallelize(testData).toDF()
  }

  describe("A GeoHeatmapOp") {
    it("should create a quad tree of tiles") {
      val geoOp = GeoHeatmapOp.geoHeatmapOp(3, 0, 1, Some(2))(genData).collect()
      assertResult(13)(geoOp.length)
    }

//    it("should create tiles without a value column set") {
//      val geoOp = GeoHeatmapOp.geoHeatmapOp(2, 0, 1, None)(testDf).collect()
//      pending
//    }
  }
}
