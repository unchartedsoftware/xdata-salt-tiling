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
package software.uncharted.contrib.tiling.jobs

import java.io.File

import org.apache.commons.io.FileUtils
import org.scalatest.FunSpec

class IPSegmentJobTest extends FunSpec {
  private val testOutputDir: String = "build/tmp/test_file_output/test_ip_segment"
  private val suffix: String = "bin"

  describe("IPSegmentJobTest") {
    describe("#execute") {
      it("should create tiles from source csv data", FileIOTest) {
        // When test are run from another project that includes this project, the current working directory is set such
        // that the data files referenced in tiling-file-io.conf can't be found.  We reset the CWD to the
        // salt-tiling-contrib directory, and reset it afterwards, to get around this problem.
        val oldDir = System.getProperty("user.dir")
        try {
          // run the job
          val config = classOf[IPSegmentJobTest].getResource("/IPSegmentJobTest/ipsegment.conf").toURI.getPath
          // Make sure to run the test from the correct directory
          val project = "salt-tiling-contrib"
          val newDir = config.substring(0, config.indexOf(project) + project.length)
          System.setProperty("user.dir", newDir)
          IPSegmentJob.execute(Array(config))

          val files = JobTestUtils.collectFiles(testOutputDir, suffix)
          val expected = Set(
            (0, 0, 0),  // l0
            (1, 0, 0),  //l1
            (2, 1, 0)   //l2
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
