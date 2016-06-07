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
package software.uncharted.xdata.ops

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.{Column, DataFrame}
import spire.math.Numeric
import spire.implicits._  // scalastyle:ignore

package object numeric {

  /**
   * A generalized n-dimensional range filter operation.
   *
   * @param filters Sequence of column (name, min, max) tuples, 1 for each dimension of the data
   * @param exclude Boolean indicating whether values in the range are excluded or included.
   * @param df Dataframe to apply filter to
   * @return Transformed dataframe, where records inside/outside the specified time range have been removed.
   */
  def numericRangeFilter[T: Numeric](filters: Seq[(String, T, T)], exclude: Boolean = false)(df: DataFrame): DataFrame = {
    require(filters.forall(p => p._2 <= p._3))
    val test: Column = filters.map { f =>
      val col = new Column(f._1)
      val result: Column = col >= f._2 && col <= f._3
      if (exclude) result.unary_! else result
    }.reduce(_ && _)
    df.filter(test)
  }

  /**
    * Add a column containing a constant value on every row.
    *
    * @param countColumnName The name of the column to use.  The caller is responsible for making sure this name is
    *                        unique in the columns of the DataFrame
    * @param constantValue The value to use for the contents of the new constant column
    * @param input The DataFrame to which to add the constant column
    * @return A new DataFrame, with the old data, plus a new constant column
    */
  def addConstantColumn (countColumnName: String, constantValue: Int)(input: DataFrame): DataFrame = {
    input.withColumn(countColumnName, new Column(Literal(constantValue)))
  }
}
