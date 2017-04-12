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

class XYTopicsJobTest extends FunSpec {
  private val testOutputDir: String = "build/tmp/test_file_output/test_topics"
  private val suffix: String = "bin"

  describe("XYTopicsJobTest") {
    import net.liftweb.json.JsonDSL._ //scalastyle:ignore
    import net.liftweb.json._ //scalastyle:ignore

    describe("#execute") {
      it("should create tiles from source csv data", FileIOTest) {
        try {
          // run the job
          val path = classOf[XYTopicsJobTest].getResource("/XYTopicsJobTest/tiling-topic-file-io.conf").toURI.getPath
          XYTopicsJob.execute(Array(path))

          //validate created tiles
          val files = JobTestUtils.collectFiles(testOutputDir, suffix)
          val expected = Set(
            (0, 0, 0), // l0
            (1, 0, 0), (1, 1, 0), (1, 1, 1), (1, 0, 1), // l1
            (2, 0, 0), (2, 0, 2), (2, 0, 3), (2, 1, 1), (2, 1, 3), (2, 2, 0), (2, 2, 2), (2, 3, 1), (2, 3, 3)) // l2

          assertResult((Set(), Set()))((expected diff files, files diff expected))

          // check terms list
          val termsFileStr = FileUtils.readFileToString(new File(s"$testOutputDir/terms.json"))
          val termsJsonObject = parse(termsFileStr)
          val expectedTermsJson = ("a" -> "alpha") ~ ("b" -> "bravo") ~ ("e" -> "echo")

          assertResult(expectedTermsJson)(termsJsonObject)
        } finally {
          FileUtils.deleteDirectory(new File(testOutputDir))
        }
      }
      it("should use xyBounds specified in configuration file", FileIOTest) {
        try {
          val path = classOf[XYTopicsJobTest].getResource("/XYTopicsJobTest/tiling-topic-file-io-xyBoundsSpec.conf").toURI.getPath
          XYTopicsJob.execute(Array(path))

          // validate created tiles
          val files = JobTestUtils.collectFiles(testOutputDir, suffix)
          val expected = Set(
            (0, 0, 0), // l0
            (1, 0, 0), (1, 0, 1), (1, 1, 0), (1, 1, 1), // l1
            (2, 1, 1), (2, 1, 3), (2, 2, 0), (2, 2, 2)) // l2

          assertResult((Set(), Set()))((expected diff files, files diff expected))
        } finally {
          FileUtils.deleteDirectory(new File(testOutputDir))
        }
      }
      it ("use default Cartesian projection and xyBounds and tilesize of one since these three parameters are not specified in conf file", FileIOTest) {
        try {
          val path = classOf[XYTopicsJobTest].getResource("/XYTopicsJobTest/tiling-topic-file-io-defaultProjection.conf").toURI.getPath
          XYTopicsJob.execute(Array(path))

          val files = JobTestUtils.collectFiles(testOutputDir, suffix)
          val expected = Set(
            (0, 0, 0),
            (1, 0, 1), (1, 0, 0), (1, 1, 0), (1, 1, 1),
            (2, 0, 3), (2, 0, 0), (2, 1, 0), (2, 1, 3), (2, 2, 0), (2, 2, 3), (2, 3, 0), (2, 3, 3))

          assertResult((Set(), Set()))((expected diff files, files diff expected))
        } finally  {
          FileUtils.deleteDirectory(new File(testOutputDir))
        }
      }
      it("should extract the tile size parameter", FileIOTest) {
        try {
          val path = classOf[XYTopicsJobTest].getResource("/XYTopicsJobTest/tiling-topic-file-io-tileSize.conf").toURI.getPath
          XYTopicsJob.execute(Array(path))

          val files = JobTestUtils.collectFiles(testOutputDir, suffix)
          val expected = Set(
            (0, 0, 0),
            (1, 0, 1), (1, 0, 0), (1, 1, 0), (1, 1, 1),
            (2, 0, 3), (2, 0, 0), (2, 1, 0), (2, 1, 3), (2, 2, 0), (2, 2, 3), (2, 3, 0), (2, 3, 3))

          assertResult((Set(), Set()))((expected diff files, files diff expected))

          val fileStr = FileUtils.readFileToString(new File(s"$testOutputDir/metadata.json"))
          val jsonObject = parse(fileStr)

          val expectedJson = JObject(List(JField("bins",JInt(4))))
          assertResult(expectedJson)(jsonObject)

        } finally  {
          FileUtils.deleteDirectory(new File(testOutputDir))
        }
      }
    }
  }
}
