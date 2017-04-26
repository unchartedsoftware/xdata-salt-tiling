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

import org.apache.spark.rdd.RDD

import scala.util.Try

/**
  * Generic I/O client that handles the general bookkeeping associated with tile sets, reducing the work of writing an
  * I/O client to the specific portions relevant to the specific format.
  *
  * @tparam T The type of information passed from the prepare function, through the write function, to the finalize function
  */
trait LocalIOClient[T] {
  /**
    * Write out an entire dataset
    *
    * @param indexFcn A function to translate from indices to differentiating i/o keys
    * @param dataSet  A set of data to write
    * @tparam I The type of index used to differentiate between data
    */
  def write[I](datasetName: String, dataSet: RDD[(I, Array[Byte])], indexFcn: (I) => String): Unit = {
    val setInfo = prepare(datasetName)
    val localWriteRaw = writeRaw

    dataSet.foreach { case (key, data) =>
      localWriteRaw(setInfo, indexFcn(key), data)
    }

    finalize(setInfo)
  }

  /**
    * Prepare a dataset for writing
    *
    * @param datasetName The name of the dataset to write
    * @return Type of information used in write and finalize function
    */
  def prepare(datasetName: String): T

  /**
    * Write out raw data
    */
  val writeRaw: (T, String, Array[Byte]) => Unit

  /**
    * Read raw data
    */
  val readRaw: String => Try[Array[Byte]]

  /**
    * Perform any finishing actions that must be performed when writing a dataset.
    *
    * @param datasetInfo Any information that might be needed about the dataset.
    */
  def finalize(datasetInfo: T): Unit
}
