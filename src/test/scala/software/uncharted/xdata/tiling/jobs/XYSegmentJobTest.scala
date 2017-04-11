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
package software.uncharted.xdata.tiling.jobs

import java.io.File

import org.apache.commons.io.FileUtils
import org.scalatest.FunSpec

class XYSegmentJobTest extends FunSpec {

  private val testOutputDir: String = "build/tmp/test_file_output/segment"
  private val suffix: String = "bin"

  describe("XYSegmentJobTest") {
    // scalastyle:ignore
    import net.liftweb.json.JsonDSL._
    describe("#execute") {
      it("should create tiles from source csv data", FileIOTest) {
        // When test are run from another project that includes this project, the current working directory is set such
        // that the data files referenced in tiling-file-io.conf can't be found.  We reset the CWD to the
        // xdata-pipeline-ops directory, and reset it afterwards, to get around this problem.
        val oldDir = System.getProperty("user.dir")
        try {
          // run the job
          val path = classOf[XYSegmentJobTest].getResource("/XYSegmentJobTest/xysegment.conf").toURI.getPath
          // Make sure to run the test from the correct directory
          val newDir = path.substring(0, path.indexOf("xdata-pipeline-ops") + 18)
          System.setProperty("user.dir", newDir)
          XYSegmentJob.main(Array(path))

          val files = JobTestUtils.collectFiles(testOutputDir, suffix)
          val expected = Set(
            (0,0,0),
            (1,0,0),
            (1,1,1),
            (2,1,1), (2,2,2)
          )
          assertResult((Set(), Set()))((expected diff files, files diff expected))
        } finally {
          System.setProperty("user.dir", oldDir)
          FileUtils.deleteDirectory(new File(testOutputDir))
        }
      }
      it("default to Cartesian since no projection is specified and use specified xyBounds", FileIOTest) {
        val oldDir = System.getProperty("user.dir")
        try {
          // run the job
          val path = classOf[XYSegmentJobTest].getResource("/XYSegmentJobTest/xysegment-defaultProjection.conf").toURI.getPath
          // Make sure to run the test from the correct directory
          val newDir = path.substring(0, path.indexOf("xdata-pipeline-ops") + 18)
          System.setProperty("user.dir", newDir)
          XYSegmentJob.main(Array(path))

          val files = JobTestUtils.collectFiles(testOutputDir, suffix)
          val expected = Set(
            (0,0,0),
            (1,0,0),
            (1,1,1),
            (2,1,1), (2,2,2)
          )
          assertResult((Set(), Set()))((expected diff files, files diff expected))
        } finally {
          System.setProperty("user.dir", oldDir)
          FileUtils.deleteDirectory(new File(testOutputDir))
        }
      }

      it("default to Mercator xyBounds since xyBounds are not specified", FileIOTest) {
        val oldDir = System.getProperty("user.dir")
        try {
          // run the job
          val path = classOf[XYSegmentJobTest].getResource("/XYSegmentJobTest/xysegment-xyBoundsDefault.conf").toURI.getPath
          // Make sure to run the test from the correct directory
          val newDir = path.substring(0, path.indexOf("xdata-pipeline-ops") + 18)
          System.setProperty("user.dir", newDir)
          XYSegmentJob.main(Array(path))

          val files = JobTestUtils.collectFiles(testOutputDir, suffix)
          val expected = Set(
            (0,0,0),
            (1,0,0),
            (1,1,1),
            (2,1,1), (2,2,2)
          )
          assertResult((Set(), Set()))((expected diff files, files diff expected))
        } finally {
          System.setProperty("user.dir", oldDir)
          FileUtils.deleteDirectory(new File(testOutputDir))
        }
      }

    }
  }
}
