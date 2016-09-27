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
package software.uncharted.xdata.sparkpipe.jobs

import org.scalatest.FunSpec

// scalastyle:off multiple.string.literals
class TopicModellingJobTest extends FunSpec {

  describe("TopicModellingParallelJobTest") {
    describe("#execute") {
      it("should do topic modelling", FileIOTest) {
        // When test are run from another project that includes this project, the current working directory is set such
        // that the data files referenced in tiling-file-io.conf can't be found.  To fix this we reset the CWD to the
        // xdata-pipeline-ops directory, and reset it afterwards.
        val oldDir = System.getProperty("user.dir")
        try {
          val path = classOf[TopicModellingJobTest].getResource("/topic-modelling/topic-modelling.conf").toURI.getPath
          // Run the test from path/to/xdata-pipeline-ops/
          val newDir = path.substring(0, path.indexOf("xdata-pipeline-ops") + 18)
          System.setProperty("user.dir", newDir)

          TopicModellingJob.main(Array(path))

        } finally {
          System.setProperty("user.dir", oldDir)
        }
      }
    }
  }
}
