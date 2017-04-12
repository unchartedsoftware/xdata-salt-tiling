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

import software.uncharted.sparkpipe.ops.xdata.io.intScoreListToByteArray
import software.uncharted.xdata.spark.SparkFunSpec

case class MercatorTopicsTestData(lon: Double, lat: Double, text: List[String])

class MercatorTopicsTest extends SparkFunSpec{
  private val lonCol = "lon"
  private val latCol = "lat"
  private val textCol = "text"

  it ("should verify contents of tiles") {
    val session = sparkSession
    import session.implicits._ // scalastyle:ignore

    val testData =
      List(MercatorTopicsTestData(-99.0, 15.0, List("c", "c", "d")),
        MercatorTopicsTestData(-99.0, 40.0, List("a", "a", "a")),
        MercatorTopicsTestData(-99.0, 10, List("b", "b", "b")),
        MercatorTopicsTestData(95.0, -70.0, List("e", "e", "c")))

    val generatedData = sc.parallelize(testData).toDF()

    val topicsOp = MercatorTopics(
      latCol,
      lonCol,
      textCol,
      None,
      10,
      0 until 3,
      4
    )(_)

    val opsResult = topicsOp(generatedData)

    val coordsResult = opsResult.map(_.coords).collect().toSet
    val expectedCoords = Set(
      (0, 0, 0), // l0
      (1, 1, 0), (1, 0, 1), // l1
      (2, 3, 0), (2, 0, 2)) // l2
    assertResult((Set(), Set()))((expectedCoords diff coordsResult, coordsResult diff expectedCoords))

    val binValues = opsResult.map(elem => (elem.coords, elem.bins)).collect()
    val binCoords = binValues.map {
      elem => (elem._1, new String(intScoreListToByteArray(elem._2).toArray))
    }
    val selectedBinValue = binCoords.filter { input =>
      input._1 match {
        case Tuple3(2,0,2) => true
        case _ => false
      }
    }

    val binValCheck = Array(
      (Tuple3(2,0,2), """[{"binIndex": 11, "topics": {"a": 3}},{"binIndex": 15, "topics": {"b": 3, "c": 2, "d": 1}}]""")
    )
    assertResult(binValCheck)(selectedBinValue)

  }
}
