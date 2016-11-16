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
class ConfigParser {
  private def getOption[T] (config: Config, key: String, extractFrom: Config => T): Option[T] = {
    if (config.hasPath(key)) {
      Some(extractFrom(config))
    } else {
      None
    }
  }

  def getConfigOption (config: Config, key: String): Option[Config] = {
    getOption(config, key, _.getConfig(key))
  }
  def getStringOption (config: Config, key: String): Option[String] = {
    getOption(config, key, _.getString(key))
  }
  def getIntOption (config: Config, key: String): Option[Int] = {
    getOption(config, key, _.getInt(key))
  }
  def getDoubleOption (config: Config, key: String): Option[Double] = {
    getOption(config, key, _.getDouble(key))
  }
  def getBooleanOption (config: Config, key: String): Option[Boolean] = {
    getOption(config, key, _.getBoolean(key))
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
