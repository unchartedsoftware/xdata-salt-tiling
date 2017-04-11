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

import software.uncharted.sparkpipe.ops.xdata.salt.CartesianTopics
import software.uncharted.xdata.spark.SparkFunSpec
import software.uncharted.sparkpipe.ops.xdata.io.intScoreListToByteArray

case class CartesianTopicTestData(x: Double, y: Double, text: List[String])

class CartesianTopicsTest extends SparkFunSpec {
  private val xCol = "x"
  private val yCol = "y"
  private val textCol = "text"

  it ("should verify contents of tiles") {
    val session = sparkSession
    import session.implicits._ // scalastyle:ignore

    val testData =
      List(CartesianTopicTestData(-99.0, 15.0, List("c", "c", "d", "d", "c", "c")),
        CartesianTopicTestData(-99.0, 40.0, List("a", "a", "a", "a", "a", "a")),
        CartesianTopicTestData(-99.0, 10, List("b", "b", "b", "b", "c", "a")),
        CartesianTopicTestData(95.0, -70.0, List("a", "a", "a", "a", "a", "a")))

    val generatedData = sc.parallelize(testData).toDF()

    val topicsOp = CartesianTopics(
      xCol,
      yCol,
      textCol,
      Some(-100.0, -80.0, 100.0, 80.0),
      10,
      0 until 3,
      4)(_)

    val opsResult = topicsOp(generatedData)

    val coordsResult = opsResult.map(_.coords).collect().toSet
    val expectedCoords = Set(
      (0, 0, 0), // l0
      (1, 1, 0), (1, 0, 1), // l1
      (2, 3, 0), (2, 0, 2), (2, 0, 3)) // l2
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
      (Tuple3(2,0,2), """[{"binIndex": 8, "topics": {"c": 5, "b": 4, "d": 2, "a": 1}}]""")
    )
    assertResult(binValCheck)(selectedBinValue)
  }
}
