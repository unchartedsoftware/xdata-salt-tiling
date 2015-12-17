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
package software.uncharted.xdata.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.scalatest.{FunSpec, BeforeAndAfter}

/**
 * Makes a spark context available to test subclasses.  The context is created before
 * a test case is run, and destroyed after it completes.
 */
abstract class SparkFunSpec extends FunSpec with BeforeAndAfter {
  @transient protected var sc: SparkContext = _
  @transient protected var sqlc: SQLContext = _

  before {
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set("spark.driver.allowMultipleContexts", "true")

    sc = new SparkContext(conf)
    sqlc = new SQLContext(sc)
  }

  after {
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
    sc.stop()
  }
}
