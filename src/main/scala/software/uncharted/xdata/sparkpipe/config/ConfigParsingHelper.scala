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

/**
  * A class in which to place helper functions usable by config parsers
  */
trait ConfigParsingHelper {
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
  def optionalInt (config: Config, key: String): Option[Int] =
    optional(config, key, (c, k) => c.getInt(k))
  def optionalConfig (config: Config, key: String): Option[Config] =
    optional(config, key, (c, k) => c)
}
