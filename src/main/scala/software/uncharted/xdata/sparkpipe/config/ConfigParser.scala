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
  * A mix-in trait to with some helper functions for parsing config files
  */
trait ConfigParser {
  private def getOption[T] (config: Config, possibleKeys: String*)(extractFrom: (Config, String) => T): Option[T] = {
    possibleKeys.find(key => config.hasPath(key)).map(key => extractFrom(config, key))
  }

  def getConfigOption (config: Config, possibleKeys: String*): Option[Config] = {
    getOption(config, possibleKeys:_*)((c, k) => c.getConfig(k))
  }
  def getStringOption (config: Config, possibleKeys: String*): Option[String] = {
    getOption(config, possibleKeys:_*)((c, k) => c.getString(k))
  }
  def getIntOption (config: Config, possibleKeys: String*): Option[Int] = {
    getOption(config, possibleKeys:_*)((c, k) => c.getInt(k))
  }
  def getDoubleOption (config: Config, possibleKeys: String*): Option[Double] = {
    getOption(config, possibleKeys:_*)((c, k) => c.getDouble(k))
  }
  def getBooleanOption (config: Config, possibleKeys: String*): Option[Boolean] = {
    getOption(config, possibleKeys:_*)((c, k) => c.getBoolean(k))
  }



  private def getDefaulted[T] (config: Config, key: String, extractFrom: Config => T, defaultValue: T): T = {
    if (config.hasPath(key)) {
      extractFrom(config)
    } else {
      defaultValue
    }
  }

  def getString (config: Config, key: String, defaultValue: String): String = {
    getDefaulted(config, key, _.getString(key), defaultValue)
  }
  def getInt (config: Config, key: String, defaultValue: Int): Int = {
    getDefaulted(config, key, _.getInt(key), defaultValue)
  }
  def getDouble (config: Config, key: String, defaultValue: Double): Double = {
    getDefaulted(config, key, _.getDouble(key), defaultValue)
  }
  def getBoolean (config: Config, key: String, defaultValue: Boolean): Boolean = {
    getDefaulted(config, key, _.getBoolean(key), defaultValue)
  }
}
