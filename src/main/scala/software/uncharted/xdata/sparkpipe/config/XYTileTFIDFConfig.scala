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
package software.uncharted.xdata.sparkpipe.config

import com.typesafe.config.Config
import grizzled.slf4j.Logging

import scala.util.Try

case class XYTileTFIDFConfig (xColumn: String,
                              yColumn: String,
                              textColumn: String,
                              projection: String,
                              wordsToKeep: Int,
                              bounds: Option[(Double, Double, Double, Double)])
object XYTileTFIDFConfig extends Logging {
  val X_COLUMN_KEY = "xColumn"
  val Y_COLUMN_KEY = "yColumn"
  val TEXT_COLUMN_KEY = "textColumn"
  val PROJECTION_KEY = "projection"
  val WORDS_KEY = "wordsToKeep"
  val MIN_X_KEY = "xMinimum"
  val MIN_Y_KEY = "yMinimum"
  val MAX_X_KEY = "xMaximum"
  val MAX_Y_KEY = "yMaximum"

  def optional[T] (config: Config, key: String, getFcn: (Config, String) => T): Option[T] = {
    if (config.hasPath(key)) {
      Some(getFcn(config, key))
    } else {
      None
    }
  }
  def optionalString (config: Config, key: String): Option[String] =
    optional(config, key, (c, k) => c.getString(k))
  def optionalDouble (config: Config, key: String): Option[Double] =
    optional(config, key, (c, k) => c.getDouble(k))

  def apply (config: Config): Try[XYTileTFIDFConfig] = {
    Try {
      val xColumn = config.getString(X_COLUMN_KEY)
      val yColumn = config.getString(Y_COLUMN_KEY)
      val textColumn = config.getString(TEXT_COLUMN_KEY)
      val projection = config.getString(PROJECTION_KEY)
      val wordsToKeep = config.getInt(WORDS_KEY)

      val minX = optionalDouble(config, MIN_X_KEY)
      val minY = optionalDouble(config, MIN_Y_KEY)
      val maxX = optionalDouble(config, MAX_X_KEY)
      val maxY = optionalDouble(config, MAX_Y_KEY)
      val bounds =
        if (minX.isDefined && maxX.isDefined && minY.isDefined && maxY.isDefined) {
          Some((minX.get, minY.get, maxX.get, maxY.get))
        } else {
          None
        }

      XYTileTFIDFConfig(xColumn, yColumn, textColumn, projection, wordsToKeep, bounds)
    }
  }
}
