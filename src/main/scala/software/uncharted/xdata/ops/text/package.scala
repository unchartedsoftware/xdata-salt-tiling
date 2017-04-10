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

package software.uncharted.xdata.ops.text

import org.apache.spark.sql.{DataFrame, Row}
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
