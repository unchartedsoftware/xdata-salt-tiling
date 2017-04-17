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


package software.uncharted.sparkpipe.ops.xdata

import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.{HashingTF, IDF}

package object text {
  /**
    * Calculate the TFIDF of the input column and store the TF & TFIDF results in the output columns.
    *
    * @param inputColumnName        Name of the input column. It should be a Seq[String].
    * @param outputColumnNameTF     Name of the output column that will contain the term frequencies.
    * @param outputColumnNameTFIDF  Name of the output column that will contain the TFIDF values.
    * @param input                  Input DataFrame
    * @return DataFrame with the calculated TFIDF values.
    */
  def tfidf(inputColumnName: String, outputColumnNameTF: String, outputColumnNameTFIDF: String)(input: DataFrame): DataFrame = {

    //Get the TF. MLLib needs an RDD of iterable.
    val hashingTF = new HashingTF().setInputCol(inputColumnName).setOutputCol(outputColumnNameTF)
    val featurizedData = hashingTF.transform(input)
    val idf = new IDF().setInputCol(outputColumnNameTF).setOutputCol(outputColumnNameTFIDF)
    val idfModel = idf.fit(featurizedData)
    idfModel.transform(featurizedData)
  }
}
