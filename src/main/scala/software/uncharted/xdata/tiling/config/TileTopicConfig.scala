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
package software.uncharted.xdata.tiling.config

import com.typesafe.config.Config

import scala.util.Try

case class TileTopicConfig(xColumn: String,
                           yColumn: String,
                           textColumn: String,
                           projectionConfig: ProjectionConfig)
object TileTopicConfig extends ConfigParser {
  private val SECTION_KEY = "topics"
  private val X_COLUMN_KEY = "xColumn"
  private val Y_COLUMN_KEY = "yColumn"
  private val TEXT_COLUMN_KEY = "textColumn"

  def parse (config: Config): Try[TileTopicConfig] = {
    for (
      section <- Try(config.getConfig(SECTION_KEY));
      projection <- ProjectionConfig.parse(section)
    ) yield {
      TileTopicConfig(
        section.getString(X_COLUMN_KEY),
        section.getString(Y_COLUMN_KEY),
        section.getString(TEXT_COLUMN_KEY),
        projection
      )
    }
  }
}
