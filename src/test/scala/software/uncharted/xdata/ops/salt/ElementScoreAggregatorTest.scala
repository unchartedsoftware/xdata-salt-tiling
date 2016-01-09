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

import org.scalatest.FunSpec

// scalastyle:off magic.number
// scalastyle:off multiple.string.literals
class ElementScoreAggregatorTest extends FunSpec {
  describe("ElementScoreAggregatorTest") {
    describe("#add") {
      it("should add the counts from an input sequence to the current count set") {
        val agg = new ElementScoreAggregator[String](10)
        val result = agg.add(Map("a" -> 1, "b" -> 11), Some(Seq("c" -> 5, "a" -> 3)))
        assertResult(Map("a" -> 4, "b" -> 11, "c" -> 5))(result)
      }

      it("should leave the existing current count set unchanged if the input sequence is None") {
        val agg = new ElementScoreAggregator[String](10)
        val result = agg.add(Map("a" -> 1, "b" -> 11), None)
        assertResult(Map("a" -> 1, "b" -> 11))(result)
      }
    }

    describe("#merge") {
      it("should merge the two sets, adding count values from entries with the same key") {
        val agg = new ElementScoreAggregator[String](10)
        val result = agg.merge(Map("a" -> 1, "b" -> 11), Map("c" -> 5, "a" -> 3))
        assertResult(Map("a" -> 4, "b" -> 11, "c" -> 5))(result)
      }
    }

    describe("#finish") {
      it("should convert the input map into a List with limit ") {
        val agg = new ElementScoreAggregator[String](3)
        val result = agg.finish(Map("a" -> 5, "b" -> 3, "c" -> 4, "d" -> 2, "e" -> 1 ))
        assertResult(List("a" -> 5, "c" -> 4, "b" -> 3))(result)
      }
    }
  }
}
