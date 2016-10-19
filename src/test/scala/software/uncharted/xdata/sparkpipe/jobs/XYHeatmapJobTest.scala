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



class XYHeatmapJobTest extends FunSpec {
  private val testOutputDir: String = "build/tmp/test_file_output/test_heatmap"
  private val suffix: String = "bin"

  describe("XYHeatmapJobTest") {
    describe("#execute") {
      it("should create tiles from source csv data with time filter applied", FileIOTest) {
        // When test are run from another project that includes this project, the current working directory is set such
        // that the data files referenced in tiling-file-io.conf can't be found.  We reset the CWD to the
        // xdata-pipeline-ops directory, and reset it afterwards, to get around this problem.
        val oldDir = System.getProperty("user.dir")
        try {
          // run the job
          val config = classOf[XYHeatmapJobTest].getResource("/tiling-file-io.conf").toURI.getPath
          // Make sure to run the test from the correct directory
          val project = "xdata-pipeline-ops"
          val newDir = config.substring(0, config.indexOf(project) + project.length)
          System.setProperty("user.dir", newDir)
          XYHeatmapJob.execute(Array(config))

          val files = JobTestUtils.collectFiles(testOutputDir, suffix)
          // One more bin ((2, 0, 3) than the time test - there's a bin that's out of time range, but we don't care.
          val expected = Set(
            (0, 0, 0), // l0
            (1, 0, 0), (1, 1, 0), (1, 1, 1), (1, 0, 1), // l1
            (2, 0, 0), (2, 0, 2), (2, 0, 3), (2, 1, 1), (2, 1, 3), (2, 2, 0), (2, 2, 2), (2, 3, 1), (2, 3, 3)) // l2

          assertResult((Set(), Set()))((expected diff files, files diff expected))
        } finally {
          System.setProperty("user.dir", oldDir)
        }
      }
    }
  }
}
