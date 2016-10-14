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

import org.apache.spark.sql.types.{StructType, ArrayType, StructField, StringType, DoubleType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.mllib.feature.{HashingTF, IDF}

package object text {
  /**
    * Calculate the TFIDF of the input column and store the (term, tfidf) results in the output column.
    *
    * @param inputColumnName Name of the input column. It should be a Seq[String].
    * @param outputColumnName Name of the output column. It will be a Seq[Row] with each row having two columns: term (String) & tfidf (Double).
    * @param input Input DataFrame
    * @return DataFrame with the calculated TFIDF values.
    */
  def tfidf (inputColumnName: String, outputColumnName: String)(input: DataFrame): DataFrame = {

    //Get the TF. MLLib needs an RDD of iterable.
    val hashingTF = new HashingTF()
    val documents = input.select(inputColumnName).rdd.map(row => row(0).asInstanceOf[Seq[String]])

    val tf = hashingTF.transform(documents)

    //Calculate the IDF & TFIDF.
    val idf = new IDF().fit(tf)
    val tfidf = idf.transform(tf)

    //Create the term -> tfidf mapping.
    val combined = documents.zip(tfidf)
    val termTFIDF = combined.map(x => x._1.distinct.map(t => (t, x._2(hashingTF.indexOf(t)))))

    //Build the combined data frame.
    //ML would work seamlessly, but because MLLib operates on RDDs, need to push everything into a new DataFrame.
    val rows = input.rdd.zip(termTFIDF).map{case (rowLeft, rowRight) => Row.fromSeq(rowLeft.toSeq ++ Array(rowRight))}
    val schema = StructType(input.schema.fields :+
      StructField(
          outputColumnName,
          ArrayType(
            StructType(
              Seq(
                StructField("term", StringType),
                StructField("tfidf", DoubleType, false)
              )
            )
          )
      ))

    //Schema is actually input schema + Seq[Row] with Row having two columns (term, tfidf).
    //I can't seem to get it to recognize the tuples and be input schema + Seq[(String, Double)].
    input.sqlContext.createDataFrame(rows, schema)

  }
}
