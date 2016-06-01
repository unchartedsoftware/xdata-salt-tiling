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
package software.uncharted.salt.core.generation.output

import software.uncharted.salt.core.projection.Projection
import software.uncharted.salt.core.util.SparseArray

// Series data constructor is package private, so we need to create this proxy in the same
// package so that test cases can call it.  Blech.
object TestSeriesData {
  def apply[TC, BC, V, X](projection: Projection[_, TC, BC],
                          maxBin: BC,
                          coords: TC,
                          bin: SparseArray[V],
                          tileMeta: Option[X]): SeriesData[TC, BC, V, X] = {
    new SeriesData(projection, maxBin, coords, bin, tileMeta)
  }
}
