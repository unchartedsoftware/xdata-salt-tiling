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


package software.uncharted.sparkpipe.ops.xdata.text

import software.uncharted.xdata.spark.SparkFunSpec
import org.apache.spark.mllib.feature.{HashingTF => TestTF}
import org.apache.spark.ml.linalg.SparseVector
import software.uncharted.sparkpipe.ops.xdata.text

class TextTest extends SparkFunSpec {
  describe("Pipeline Text Analytics Test") {
    it("should output terms & their TF/IDF values") {
      val rddData = sc.parallelize(Seq(
        TFIDFData(1, "this is a test".split(" ")),
        TFIDFData(2, "more testing done".split(" ")),
        TFIDFData(3, "test it again sam".split(" ")),
        TFIDFData(4, "i wish i were testing so that i could do it".split(" ")),
        TFIDFData(5, "this is the final test i did it".split(" "))
      ))

      val dfData = sparkSession.createDataFrame(rddData)

      val result = text.tfidf("text", "tfs", "tfidfs")(dfData)

      assert(result.count() === 5)

      //Verify the relative tfidf values for a few pairs.
      //Extract the term + tfidf values.
      val resultMap = result.select("tfidfs").collect().map(x => x(0).asInstanceOf[SparseVector])

      //Use the hashing TF to get the index in the sparse vectors returned.
      val testTF = new TestTF(resultMap(0).size)

      assert(resultMap(0)(testTF.indexOf("test")) < resultMap(0)(testTF.indexOf("this")))
      assert(resultMap(0)(testTF.indexOf("this")) < resultMap(0)(testTF.indexOf("a")))

      assert(resultMap(1)(testTF.indexOf("testing")) < resultMap(1)(testTF.indexOf("more")))
      assert(resultMap(1)(testTF.indexOf("testing")) < resultMap(1)(testTF.indexOf("done")))

      assert(resultMap(3)(testTF.indexOf("wish")) < resultMap(3)(testTF.indexOf("i")))
      assert(resultMap(3)(testTF.indexOf("it")) < resultMap(3)(testTF.indexOf("wish")))

      assert(resultMap(4)(testTF.indexOf("it")) < resultMap(4)(testTF.indexOf("final")))

      assert(resultMap(4)(testTF.indexOf("i")) < resultMap(3)(testTF.indexOf("i")))
    }
  }
}

case class TFIDFData (id: Integer, text: Seq[String])
