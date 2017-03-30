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

import java.io.File

import org.apache.commons.io.FileUtils
import org.scalatest.FunSpec

class XYTileTFIDFJobTest extends FunSpec {
  private val testOutputDir: String = "build/tmp/test_file_output/tile_topics"
  private val suffix: String = "bin"

  describe("XYTileTFIDFJobTest test") {
    describe("#execute") {
      it("should create tiles from source csv data", FileIOTest) {
        // When test are run from another project that includes this project, the current working directory is set such
        // that the data files referenced in tiling-file-io.conf can't be found.  We reset the CWD to the
        // xdata-pipeline-ops directory, and reset it afterwards, to get around this problem.
        val oldDir = System.getProperty("user.dir")
        try {
          // run the job
          val config = classOf[XYTileTFIDFJobTest].getResource("/tile-topic-test.conf").toURI.getPath
          // Make sure to run the test from the correct directory
          val project = "xdata-pipeline-ops"
          val newDir = config.substring(0, config.indexOf(project) + project.length)
          System.setProperty("user.dir", newDir)
          XYTileTFIDFJob.execute(Array(config))

          val files = JobTestUtils.collectFiles(testOutputDir, suffix)
          // All tiles expected to have something
          val expected = (0 to 4).flatMap{level =>
            val size = 1 << level
            for (x <- 0 until size; y <- 0 until size) yield (level, x, y)
          }.toSet

          assertResult((Set(), Set()))((expected diff files, files diff expected))
        } finally {
          System.setProperty("user.dir", oldDir)
          FileUtils.deleteDirectory(new File(testOutputDir))
        }
      }
    }
  }
}
