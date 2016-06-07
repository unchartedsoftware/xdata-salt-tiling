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

import java.io.{File, FileOutputStream}
import java.util.zip.{ZipEntry, ZipOutputStream, ZipFile}

import org.apache.spark.rdd.RDD

/**
  * A i/o client for writing a single tile set to a zip archive.
  *
  * This client should <em>only</em> be used with a local master.
  *
  * @param baseZipLocation The directory into which to drop zip files
  */
class ZipFileClient(baseZipLocation: File) extends LocalIOClient[ZipOutputStream] {

  override val standardTileIndexTranslator: (String, (Int, Int, Int)) => String = (tileSetName, index) => {
    val (level, x, y) = index
    val digits = math.log10(1 << level).floor.toInt + 1
    s"%02d/%0${digits}d/%0${digits}d".format(level, x, y)
  }


  /**
    * Prepare a dataset for writing
    *
    * @param datasetName The name of the dataset to write
    */
  override def prepare(datasetName: String): ZipOutputStream = {
    new ZipOutputStream(new FileOutputStream(new File(baseZipLocation, datasetName)))
  }

  /**
    * Write out a single tile
    */
  override val writeRaw: (ZipOutputStream, String, Array[Byte]) => Unit =
    (zipStream, key, data) => {
    val entry = new ZipEntry(key)
    zipStream.putNextEntry(entry)
    zipStream.write(data)
  }

  /**
    * Perform any finishing actions that must be performed when writing a dataset.
    *
    * @param zipStream The stream to which the dataset was written
    */
  override def finalize(zipStream: ZipOutputStream): Unit = {
    zipStream.flush()
    zipStream.close()
  }
}
