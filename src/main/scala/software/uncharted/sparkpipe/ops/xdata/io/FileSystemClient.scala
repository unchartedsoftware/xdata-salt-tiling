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

package software.uncharted.sparkpipe.ops.xdata.io

import java.io.{File, FileInputStream, FileOutputStream}

import grizzled.slf4j.Logging
import org.apache.commons.io.IOUtils.toByteArray

import scala.util.Try

/**
  * A i/o client for writing a single tile set directly to the local file system.
  *
  * This client should <em>only</em> be used with a local master.
  *
  * @param baseFilePath The name of the base directory into which to write tile sets
  * @param extension    The extension to use for all tiles in the tile set
  */
class FileSystemClient(baseFilePath: String, extension: Option[String]) extends LocalIOClient[String] with Logging {
  /**
    * Prepare a dataset for writing
    *
    * @param datasetName The name of the dataset to write
    */
  override def prepare(datasetName: String): String = datasetName

  /**
    * Write out a single tile
    */
  override val writeRaw: (String, String, Array[Byte]) =>  Unit = {
    val localBaseFilePath = baseFilePath
    (datasetName, fileName, data) => {
      // sadly, scalastyle has problems with interpolated strings
      val path = s"$localBaseFilePath/$datasetName/$fileName"  // scalastyle:ignore
      try {
        val file = new File(path)
        // Create directories as necessary
        file.getParentFile.mkdirs()

        // Write out our data
        val fos = new FileOutputStream(file)
        fos.write(data)
        fos.flush()
        fos.close()
      } catch {
        case e: Exception =>
          new Exception(s"Failed to write file $path", e).printStackTrace()
      }
    }
  }

  /**
    * Provide a function that can read in a single tile
    */
  override val readRaw: (String => Try[Array[Byte]]) = {
    (fileLocation) =>
      val path = s"$baseFilePath/$fileLocation${extension.getOrElse(".bin")}"
      Try {
        val fileData = new File(path)
        val fis = new FileInputStream(fileData)
        val data = toByteArray(fis)
        fis.close()
        data
      }
  }

  /**
    * Perform any finishing actions that must be performed when writing a dataset.
    *
    * @param datasetName The name of the dataset
    */
  override def finalize(datasetName: String): Unit = {}
}
