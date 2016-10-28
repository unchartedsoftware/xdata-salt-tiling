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

package software.uncharted.xdata.ops.text

import software.uncharted.xdata.spark.SparkFunSpec
import org.apache.spark.sql.{DataFrame, Row}

class TextTest extends SparkFunSpec {
  describe("Pipeline Text Analytics Test") {
    it("should output terms & their TF/IDF values") {
      val rddData = sc.parallelize(Seq(
        TFIDFData(1, "this is a test".split(" ")),
        TFIDFData(2, "more testing done".split(" ")),
        TFIDFData(3, "test it again sam".split(" ")),
        TFIDFData(4, "i wish i were testing so that i could do it".split(" ")),
        TFIDFData(5, "this is the final test i did it".split(" "))
      ))

      val dfData = sparkSession.createDataFrame(rddData)

      val result = text.tfidf("text", "tfidfs")(dfData)

      assert(result.count() === 5)

      //Verify the relative tfidf values for a few pairs.
      //Extract the term + tfidf values and put them in a map.
      val resultMap = result.select("tfidfs").collect().map(x => x(0).asInstanceOf[Seq[Row]].map(t => (t(0), t(1).asInstanceOf[Double])).toMap)
      assert(resultMap(0)("test") < resultMap(0)("this"))
      assert(resultMap(0)("this") < resultMap(0)("a"))

      assert(resultMap(1)("testing") < resultMap(1)("more"))
      assert(resultMap(1)("testing") < resultMap(1)("done"))

      assert(resultMap(3)("wish") < resultMap(3)("i"))
      assert(resultMap(3)("it") < resultMap(3)("wish"))

      assert(resultMap(4)("it") < resultMap(4)("final"))

      assert(resultMap(4)("i") < resultMap(3)("i"))
    }
  }
}

case class TFIDFData (id: Integer, text: Seq[String])
