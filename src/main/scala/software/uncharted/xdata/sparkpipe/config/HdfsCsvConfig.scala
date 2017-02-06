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



/**
  * A configuration object describing a file of character-separated values in HDFS
  *
  * @param location The HDFS location of the file.
  * @param partitions The number of partitions to read. If not specified, the default number of partitions will
  *                   be used.
  * @param separator A separator to use to separate columns in the CSV file.  Default is a comma.
  * @param neededColumns The columns from the CSV file that we need. Not optional.
  */
case class HdfsCsvConfig (location: String, partitions: Option[Int], separator: String, neededColumns: Seq[Int])

object HdfsCsvConfigParser extends ConfigParser {
  private val LOCATION_KEY = "location"
  private val PARTITIONS_KEY = "partitions"
  private val SEPARATOR_KEY = "separator"
  private val DEFAULT_SEPARATOR = ","
  private val RELEVANT_COLUMNS_KEY = "columns"
  /**
    * Read the config for a particular character-separated values file for input or output
    */
  def parse(key: String, defaultSeparator: String = DEFAULT_SEPARATOR)(config: Config): Try[HdfsCsvConfig] = {
    Try {
      val fileConfig = config.getConfig(key)
      HdfsCsvConfig(
        fileConfig.getString(LOCATION_KEY),
        getIntOption(fileConfig, PARTITIONS_KEY),
        getString(fileConfig, SEPARATOR_KEY, defaultSeparator),
        getIntList(fileConfig, RELEVANT_COLUMNS_KEY)
      )
    }
  }
}
