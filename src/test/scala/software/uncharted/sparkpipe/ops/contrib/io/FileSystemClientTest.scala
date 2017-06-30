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

package software.uncharted.sparkpipe.ops.contrib.io

import java.io.File

import org.apache.commons.io.FileUtils
import software.uncharted.salt.contrib.spark.SparkFunSpec

class FileSystemClientTest extends SparkFunSpec {
  private val testDir = "build/tmp/test_file_output/test_data"
  private val testLayer = "test_layer"
  private val extension = ".tst"

  describe("#FileSystemClientTest") {
    it ("should write and read the same data") {
      try {
        val fsc = new FileSystemClient(testDir, Some(extension))

        val data = "hello world".getBytes
        val coordinates = (1, 2, 3)
        val dataLocation = mkRowId("", "/", "")(coordinates._1, coordinates._2, coordinates._3)

        fsc.writeRaw(testLayer, s"$dataLocation$extension", data)
        val result = fsc.readRaw(s"$testLayer/$dataLocation").get

        assertResult(data)(result)
      } finally {
        FileUtils.deleteDirectory(new File(testDir))
      }
    }
  }

}
