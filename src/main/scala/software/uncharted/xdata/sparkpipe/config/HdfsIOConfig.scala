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



import scala.collection.JavaConverters._ // scalastyle:ignore
import scala.util.Try
import com.typesafe.config.Config



/** config to describe a file of character-separated values */
case class HdfsCsvConfig (location: String, partitions: Option[Int], separator: String, neededColumns: Seq[Int])

object HdfsIOConfig {
  private val LOCATION_KEY = "location"
  private val PARTITIONS_KEY = "partitions"
  private val SEPARATOR_KEY = "separator"
  private val DEFAULT_SEPARATOR = ","
  private val RELEVANT_COLUMNS_KEY = "columns"
  /**
    * Read the config for a particular character-separated values file for input or output
    */
  def csv(key: String, defaultSeparator: String = DEFAULT_SEPARATOR)(config: Config): Try[HdfsCsvConfig] = {
    Try {
      val fileConfig = config.getConfig(key)
      val separator =
        if (fileConfig.hasPath(SEPARATOR_KEY)) {
          fileConfig.getString(SEPARATOR_KEY)
        } else {
          defaultSeparator
        }
      val neededColumns =
        if (fileConfig.hasPath(RELEVANT_COLUMNS_KEY)) {
          fileConfig.getIntList(RELEVANT_COLUMNS_KEY).asScala.toSeq.map(_.intValue())
        } else {
          Seq[Int]()
        }

      HdfsCsvConfig(
        fileConfig.getString(LOCATION_KEY),
        optionalInt(fileConfig, PARTITIONS_KEY),
        separator,
        neededColumns
      )
    }
  }

  def optionalInt (config: Config, path: String): Option[Int] =
    if (config.hasPath(path)) {
      Some(config.getInt(path))
    } else {
      None
    }
}
