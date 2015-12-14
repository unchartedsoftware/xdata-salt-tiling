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
      it("should throw an IllegalArgumentException if count isn't positive") {
        intercept[IllegalArgumentException] {
          RangeDescription(0d, 1d, 0, .1d)
        }
      }
      it("should throw an IllegalArgumentException if step isn't positive") {
        intercept[IllegalArgumentException] {
          RangeDescription(0d, 1d, 0, -0.1d)
        }
      }
      it("should throw an IllegalArgumentException if min isn't less than max") {
        intercept[IllegalArgumentException] {
          RangeDescription(1d, 2d, 0, .1d)
        }
      }
    }

    describe("#fromMin") {
      it("should create a range with the correct max given min, count and step") {
        assertResult(RangeDescription(0.0, 10.0, 10, 1.0))(RangeDescription.fromMin(0.0, 1.0, 10))
      }
    }

    describe("#fromMax") {
      it("should create a range with the correct min given max, count and step") {
        assertResult(RangeDescription(0.0, 10.0, 10, 1.0))(RangeDescription.fromMax(10.0, 1.0, 10))
      }
    }

    describe("#fromCount") {
      it("should create a range with the correct count given min, max and step") {
        assertResult(RangeDescription(0.0, 10.0, 10, 1.0))(RangeDescription.fromCount(0.0, 10.0, 10))
      }
    }

    describe("#fromStep") {
      it("should create a range with the correct count given min, max, and step") {
        assertResult(RangeDescription(0.0, 10.0, 10, 1.0))(RangeDescription.fromStep(0.0, 10.0, 1.0))
      }
    }
  }
}
