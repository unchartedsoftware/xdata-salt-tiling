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
class RangeDescriptionTest extends FunSpec {
  describe("RangeDescriptionTest") {
    describe("#constructor") {
      it("should throw an IllegalArgumentException if min isn't less than max") {
        val min = 2d
        val max = 1d
        val caughtMsg = intercept[IllegalArgumentException] {
          RangeDescription(min, max, 0, .1d)
        }

        assert(caughtMsg.getMessage() == s"requirement failed: min ($min) must be less than max ($max)")
      }

      it("should throw an IllegalArgumentException if count isn't greater than 1") {
        val count = -1
        val caughtMsg =  intercept[IllegalArgumentException] {
          RangeDescription(0d, 1d, count, .1d)
        }

        assert(caughtMsg.getMessage() == s"requirement failed: count ($count) must be greater than 1")
      }

      it("should throw an IllegalArgumentException if step isn't positive") {
        val step = -0.1d
        val caughtMsg = intercept[IllegalArgumentException] {
          RangeDescription(0d, 1d, 1, step)
        }

        assert(caughtMsg.getMessage() == s"requirement failed: step ($step) must be greater than 0")
      }
    }

    describe("#fromMin") {
      it("should create a range with the correct max given min, count and step") {
        assertResult(RangeDescription(10.0, 20.0, 10, 1.0))(RangeDescription.fromMin(10.0, 1.0, 10))
      }
    }

    describe("#fromMax") {
      it("should create a range with the correct min given max, count and step") {
        assertResult(RangeDescription(10.0, 20.0, 10, 1.0))(RangeDescription.fromMax(20.0, 1.0, 10))
      }
    }

    describe("#fromCount") {
      it("should create a range with the correct count given min, max and step") {
        assertResult(RangeDescription(10.0, 20.0, 10, 1.0))(RangeDescription.fromCount(10.0, 20.0, 10))
      }
    }

    describe("#fromStep") {
      it("should create a range with the correct count given min, max, and step") {
        assertResult(RangeDescription(10.0, 20.0, 10, 1.0))(RangeDescription.fromStep(10.0, 20.0, 1.0))
      }
    }
  }
}
