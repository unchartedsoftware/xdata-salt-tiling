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
import org.scalatest.Tag
import net.liftweb.json._
import scala.collection.JavaConversions._ // scalastyle:ignore

object FileIOTest extends Tag("fileio.test")

/**
  * Set of utility functions for testing jobs
  */
object JobTestUtils {
  def collectFiles(rootDir: String, suffix: String) : Set[(Int, Int, Int)] = {
    // convert produced filenames into indices
    val filename = s""".*[/\\\\](\\d+)[/\\\\](\\d+)[/\\\\](\\d+)\\.$suffix""".r
    val files = FileUtils.listFiles(new File(rootDir), Array(suffix), true)
      .flatMap(_.toString match {
        case filename(level, x, y) => Some((level.toInt, x.toInt, y.toInt))
        case _ => None
      })
      .toSet
    files
  }

  /**
    * Get coordinates (of all zoom levels) and their corresponding bin values
    *
    * @param rootDir Directory of generated tile data
    * @param suffix File suffix for extraction of data
    * @return List of tile coordinates and their bin values
    */
  def getBinValues(rootDir: String, suffix: String): List[((Int, Int, Int), JValue)] = {
    val filename = s""".*[/\\\\](\\d+)[/\\\\](\\d+)[/\\\\](\\d+)\\.$suffix""".r
    val results = FileUtils.listFiles(new File(rootDir), Array(suffix), true)
      .map { inputFile =>
        val byteAra = FileUtils.readFileToByteArray(inputFile)
        val binValue = new String(byteAra)
        val coord = Array(inputFile).flatMap(_.toString match {
            case filename(level, x, y) => Some((level.toInt, x.toInt, y.toInt))
            case _ => None
          }).head
        (coord, parse(binValue))
      }.toList
    results
  }
}
