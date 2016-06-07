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
package software.uncharted.xdata.ops.io

import java.io.{BufferedOutputStream, FileOutputStream, File}

import grizzled.slf4j.Logging
import org.apache.spark.rdd.RDD

import scala.util.{Failure, Success, Try}

/**
  * A i/o client for writing a single tile set directly to the local file system.
  *
  * This client should <em>only</em> be used with a local master.
  *
  * @param baseFilePath The name of the base directory into which to write tile sets
  * @param extension    The extension to use for all tiles in the tile set
  */
class FileSystemClient(baseFilePath: String, extension: Option[String]) extends LocalIOClient[String] with Logging {
  override val standardTileIndexTranslator: (String, (Int, Int, Int)) => String = {
    val localExtension = extension.map("." + _).getOrElse("")
    (tileSetName, index) => {
      val (level, x, y) = index
      val digits = math.log10(1 << level).floor.toInt + 1
      s"""%02d/%0${digits}d/%0${digits}d${localExtension}""".format(level, x, y)
    }
  }

  /**
    * Prepare a dataset for writing
    *
    * @param datasetName The name of the dataset to write
    */
  override def prepare(datasetName: String): String = datasetName

  /**
    * Write out a single tile
    */
  override val writeRaw: (String, String, Array[Byte]) =>  Unit = {
    val localBaseFilePath = baseFilePath
    (datasetName, fileName, data) => {
      val path = s"$localBaseFilePath/$datasetName/$fileName"
      try {
        val file = new File(path)
        // Create directories as necessary
        file.getParentFile.mkdirs()

        // Write out our data
        val fos = new FileOutputStream(file)
        fos.write(data)
        fos.flush()
        fos.close()
      } catch {
        case e: Exception =>
          new Exception(s"Failed to write file $path", e).printStackTrace()
      }
    }
  }

  /**
    * Perform any finishing actions that must be performed when writing a dataset.
    *
    * @param datasetName The name of the dataset
    */
  override def finalize(datasetName: String): Unit = {}
}
