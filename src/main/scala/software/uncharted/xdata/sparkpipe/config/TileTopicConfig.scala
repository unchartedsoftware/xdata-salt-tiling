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

case class TileTopicConfig(xColumn: String,
                           yColumn: String,
                           textColumn: String,
                           wordsToKeep: Int,
                           projectionConfig: ProjectionConfig)
object TileTopicConfig extends Logging {
  val X_COLUMN_KEY = "xColumn"
  val Y_COLUMN_KEY = "yColumn"
  val TEXT_COLUMN_KEY = "textColumn"
  val WORDS_KEY = "wordsToKeep"
  val PROJECTION_KEY = "projection"

  def apply (config: Config): Try[TileTopicConfig] = {
    for (
      projectionConfig <- Try(config.getConfig(PROJECTION_KEY));
      projection <- ProjectionConfig(projectionConfig)
    ) yield {
      val xColumn = config.getString(X_COLUMN_KEY)
      val yColumn = config.getString(Y_COLUMN_KEY)
      val textColumn = config.getString(TEXT_COLUMN_KEY)
      val wordsToKeep = config.getInt(WORDS_KEY)

      TileTopicConfig(xColumn, yColumn, textColumn, wordsToKeep, projection)
    }
  }
}
