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
import scala.util.Try

case class TileTopicConfig(xColumn: String,
                           yColumn: String,
                           textColumn: String,
                           projectionConfig: ProjectionConfig)
object TileTopicConfig extends ConfigParser {
  private val topics = "topics"
  private val xColumn = "xColumn"
  private val yColumn = "yColumn"
  private val textColumn = "textColumn"

  def parse (config: Config): Try[TileTopicConfig] = {
    for (
      tileTopicsConfig <- Try(config.getConfig(topics));
      projection <- ProjectionConfig.parse(tileTopicsConfig)
    ) yield {
      TileTopicConfig(
        tileTopicsConfig.getString(xColumn),
        tileTopicsConfig.getString(yColumn),
        tileTopicsConfig.getString(textColumn),
        projection
      )
    }
  }
}
