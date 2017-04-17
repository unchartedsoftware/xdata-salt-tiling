/**
 * Copyright © 2013-2017 Uncharted Software Inc.
 *
 * Property of Uncharted™, formerly Oculus Info Inc.
 *
 * http://uncharted.software/
 *
 * Released under the MIT License.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package software.uncharted.xdata.tiling.jobs

import java.io.File

import org.apache.commons.io.FileUtils
import org.scalatest.FunSpec

class XYTimeTopicsJobTest extends FunSpec {

  private val testOutputDir: String = "build/tmp/test_file_output/test_topics"
  private val suffix: String = "bin"

  describe("XYTimeTopicsJobTest") {
    import net.liftweb.json.JsonDSL._ //scalastyle:ignore
    import net.liftweb.json._ //scalastyle:ignore

    describe("#execute") {
      it("should create tiles from source csv data with time filter applied", FileIOTest) {
        try {
          // run the job
          val path = classOf[XYTimeTopicsJobTest].getResource("/XYTimeTopicsJobTest/tiling-time-topic-file-io.conf").toURI.getPath
          XYTimeTopicsJob.execute(Array(path))

          // validate created tiles
          val files = JobTestUtils.collectFiles(testOutputDir, suffix)
          val expected = Set(
            (0,0,0), // l0
            (1,0,0), (1,1,0), (1,1,1), (1,0,1), // l1
            (2, 0, 0), (2, 2, 0), (2, 1, 1), (2, 3, 1), (2, 0, 2), (2, 2, 2), (2, 1, 3), (2, 3, 3)) // l2

          assertResult((Set(), Set()))((expected diff files, files diff expected))

          // check metadata
          val fileStr = FileUtils.readFileToString(new File(s"$testOutputDir/metadata.json"))
          val jsonObject = parse(fileStr)
          val expectedJson =
            ("bins" -> 1) ~
            ("range" ->
              (("start" -> 1357016400000L) ~
              ("step" -> 86400000) ~
              ("count" -> 8)))
          assertResult(expectedJson)(jsonObject)

          // check terms list
          val termsFileStr = FileUtils.readFileToString(new File(s"$testOutputDir/terms.json"))
          val termsJsonObject = parse(termsFileStr)
          val expectedTermsJson = ("a" -> "alpha") ~ ("b" -> "bravo") ~ ("e" -> "echo")
          assertResult(expectedTermsJson)(termsJsonObject)

        } finally {
          FileUtils.deleteDirectory(new File(testOutputDir))
        }
      }

      it("should convert string date to timestamp", FileIOTest) {
        try {
          val path = classOf[XYTimeTopicsJobTest].getResource("/XYTimeTopicsJobTest/tiling-time-topic-file-io.conf").toURI.getPath
          XYTimeTopicsJob.execute(Array(path))

          val files = JobTestUtils.collectFiles(testOutputDir, suffix)
          val expected = Set(
            (0,0,0), // l0
            (1,0,0), (1,1,0), (1,1,1), (1,0,1), // l1
            (2, 0, 0), (2, 2, 0), (2, 1, 1), (2, 3, 1), (2, 0, 2), (2, 2, 2), (2, 1, 3), (2, 3, 3)) // l2

          assertResult((Set(), Set()))((expected diff files, files diff expected))
        } finally {
          FileUtils.deleteDirectory(new File(testOutputDir))
        }
      }

      it("should use xyBounds specified in configuration file", FileIOTest) {
        try {
          val path = classOf[XYTimeTopicsJobTest].getResource("/XYTimeTopicsJobTest/tiling-time-topic-file-io-xyBoundsSpec.conf").toURI.getPath
          XYTimeTopicsJob.execute(Array(path))

          // validate created tiles
          val files = JobTestUtils.collectFiles(testOutputDir, suffix)
          val expected = Set(
            (0,0,0), // l0
            (1,0,0), (1,1,0), (1,1,1), (1,0,1), // l1
            (2, 1, 1), (2, 1, 3), (2, 2, 0), (2, 2, 2)) // l2

          assertResult((Set(), Set()))((expected diff files, files diff expected))

        } finally {
          FileUtils.deleteDirectory(new File(testOutputDir))
        }
      }

      it("should default to Cartesian since no projection is specified and use tilesize equal to 1 since bins parameter is not specified", FileIOTest) {
        try {
          val path = classOf[XYTimeTopicsJobTest].getResource("/XYTimeTopicsJobTest/tiling-time-topic-file-io-defaultProjection.conf").toURI.getPath
          XYTimeTopicsJob.execute(Array(path))

          // validate created tiles
          val files = JobTestUtils.collectFiles(testOutputDir, suffix)
          val expected = Set(
            (0, 0, 0), // l0
            (1, 0, 0), (1, 1, 1), // l1
            (2, 0, 0), (2, 3, 3)) // l2

          assertResult((Set(), Set()))((expected diff files, files diff expected))

        } finally {
          FileUtils.deleteDirectory(new File(testOutputDir))
        }
      }

      it("should extract the tile size parameter", FileIOTest) {
        try {
          val path = classOf[XYTimeTopicsJobTest].getResource("/XYTimeTopicsJobTest/tiling-time-topic-file-io-tileSize.conf").toURI.getPath
          XYTimeTopicsJob.execute(Array(path))

          val files = JobTestUtils.collectFiles(testOutputDir, suffix)
          val expected = Set(
            (0, 0, 0), // l0
            (1, 0, 0), (1, 1, 1), // l1
            (2, 0, 0), (2, 3, 3)) // l2

          assertResult((Set(), Set()))((expected diff files, files diff expected))

          // check metadata
          val fileStr = FileUtils.readFileToString(new File(s"$testOutputDir/metadata.json"))
          val jsonObject = parse(fileStr)
          val expectedJson =
            ("bins" -> 4) ~
              ("range" ->
                (("start" -> 1357016400000L) ~
                  ("step" -> 86400000) ~
                  ("count" -> 8)))
          assertResult(expectedJson)(jsonObject)
        } finally {
          FileUtils.deleteDirectory(new File(testOutputDir))
        }
      }
    }
  }
}
