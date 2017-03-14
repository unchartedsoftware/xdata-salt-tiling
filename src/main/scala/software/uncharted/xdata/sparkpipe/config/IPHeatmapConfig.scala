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

/**
  * A config object representing information specific to IP tiling jobs
  * @param ipCol The column in which an IP address is to be found
  * @param valueCol The column in which the value to be used is found
  */
case class IPHeatmapConfig (ipCol: String, valueCol: String)
object IPHeatmapConfig extends ConfigParser {//extends Logging {
  private val CATEGORY_KEY = "ip-tiling"
  private val IP_COLUMN_KEY = "ipColumn"
  private val VALUE_COLUMN_KEY = "valueColumn"

  def parse (config: Config): Try[IPHeatmapConfig] = {
    Try{
      val ipConfig = config.getConfig(CATEGORY_KEY)

      IPHeatmapConfig(
        ipConfig.getString(IP_COLUMN_KEY),
        ipConfig.getString(VALUE_COLUMN_KEY)
      )
    }
  }
}
