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
package software.uncharted.xdata.ops

import java.io.{BufferedOutputStream, FileOutputStream, File}
import java.util.ArrayList

import grizzled.slf4j.Logging
import org.apache.spark.rdd.RDD
import software.uncharted.salt.core.generation.output.SeriesData

import scala.util.parsing.json.JSONObject

package object io extends Logging {

  val doubleBytes = 8

  //to remove scalastyle:string literal error
  val slash = "/"

  /**
   * Write binary array data to the file system.  Folder structure is
   * baseFilePath/layerName/level/xIdx/yIdx.bin.  Indexing is TMS style
   * with (0,0) in lower left, y increasing as it moves north.
   *
   * @param baseFilePath Baseline output directory - can store multiple layers.
   * @param layerName Unique name for the layer .
   * @param input tile coordinate / byte array tuples of tile data
   * @return input data unchanged
   */
  def writeToFile(baseFilePath: String, layerName: String, extension: String)(input: RDD[((Int, Int, Int), Seq[Byte])]): RDD[((Int, Int, Int), Seq[Byte])] = {
    input.collect().foreach { tileData =>
      val coord = tileData._1
      val dirPath = s"$baseFilePath/$layerName/${coord._1}/${coord._2}" //scalastyle:ignore
      val path = s"$dirPath/${coord._3}.$extension"
      try {
        // create path if necessary
        val file = new File(dirPath)
        file.mkdirs()
        val fos = new FileOutputStream(new File(path))
        val bos = new BufferedOutputStream(fos)
        bos.write(tileData._2.toArray)
        bos.close()
      } catch {
        case e: Exception => error(s"Failed to write file $path", e)
      }
    }
    input
  }

  /**
   * Write binary array data to the file system.  Folder structure is
   * baseFilePath/filename.ext.
   *
   * @param baseFilePath Baseline output directory - can store multiple layer subdirectories.
   * @param layerName Unique name for the layer.
   * @param fileName Filename to write byte data into.
   * @param bytes byte array of data
   */
  def writeBytesToFile(baseFilePath: String, layerName: String)(fileName: String, bytes: Seq[Byte]): Unit = {
    val dirPath = s"$baseFilePath/$layerName"
    val path = s"$dirPath/$fileName"
    try {
      // create path if necessary
      val file = new File(dirPath)
      file.mkdirs()
      val fos = new FileOutputStream(new File(path))
      val bos = new BufferedOutputStream(fos)
      bos.write(bytes.toArray)
      bos.close()
    } catch {
      case e: Exception => error(s"Failed to write file $path", e)
    }
  }

  /**
   * Write binary array data to Amazon S3 bucket.  Key format is layerName/level/xIdx/yIdx.bin.
   * Indexing is TMS style with (0,0) in lower left, y increasing as it moves north
   *
   * @param accessKey AWS Access key
   * @param secretKey AWS Secret key
   * @param bucketName Name of S3 bucket to write to - will create a bucket if missing
   * @param layerName Unique name for the layer
   * @param input tile coordinate / byte array tuples of tile data
   * @return input data unchanged
   */
  def writeToS3(accessKey: String, secretKey: String, bucketName: String, layerName: String)(input: RDD[((Int, Int, Int), Seq[Byte])]):
  RDD[((Int, Int, Int), Seq[Byte])] = {
    // Upload tiles to S3 using the supplied bucket and layer.  Use foreachPartition to avoid incurring
    // the cost of initializing the S3Client per record.  This can't be done outside the RDD closure
    // because the Amazon S3 API classes are not marked serializable.
    input.foreachPartition { tileDataIter =>
      val s3Client = S3Client(accessKey, secretKey)
      tileDataIter.foreach { tileData =>
        val coord = tileData._1
        // store tile in bucket as layerName/level-xIdx-yIdx.bin
        val key = s"$layerName/${coord._1}/${coord._2}/${coord._3}.bin"
        s3Client.upload(tileData._2.toArray, bucketName, key)
      }
    }
    input
  }

  /**
   * Write binary array data to Amazon S3 bucket.  Key format is layerName/filename.ext.
   *
   * @param accessKey AWS Access key
   * @param secretKey AWS Secret key
   * @param bucketName Name of S3 bucket to write to - will create a bucket if missing
   * @param layerName Unique name for the layer
   * @param bytes byte array of data to write
   */
  def writeBytesToS3(accessKey: String, secretKey: String, bucketName: String, layerName: String)(fileName: String, bytes: Seq[Byte]): Unit = {
    val s3Client = S3Client(accessKey, secretKey)
    s3Client.upload(bytes, bucketName, layerName + slash + fileName)
  }
  /**
   *Write Tiling Data to the TileData column in an HBase Table
   *
   *RDD Layer Data is stored in their own table with tile info stored in a column and tile name as the rowID
   * This Data is first mapped to a list of tuples containing required data to store into HBase table
   * Then the list of tuples are sent to HBase connector write the RDDPairs into HBase
   * @param configFile HBaseConfig Files used to connect to HBase
   * @param layerName unique name for the layer, will be used as table name
   * @param qualifierName name of column qualifier where data will be updated
   * @param input RDD of tile data to be processed and stored into HBase
   */
  def writeToHBase(configFile: Seq[String], layerName: String, qualifierName: String)
  (input: RDD[((Int, Int, Int), Seq[Byte])]): RDD[((Int, Int, Int), Seq[Byte])] = {

    val results = input.mapPartitions { tileDataIter =>
      tileDataIter.map { tileData =>
        val coord = tileData._1
        val rowID = mkRowId(s"${layerName}/", slash, ".bin")(coord._1, coord._2, coord._3)
        (rowID, tileData._2)
      }
    }
    val hBaseConnector = HBaseConnector(configFile)
    hBaseConnector.writeTileData(layerName, qualifierName)(results)
    hBaseConnector.close
    input
  }

  private def mkRowId (prefix: String, separator: String, suffix: String)(level: Int, x: Int, y: Int): String = {
    val digits = math.log10(1 << level).floor.toInt + 1
    (prefix + "%02d" + separator + "%0" + digits + "d" + separator + "%0" + digits + "d" + suffix).format(level, x, y)
  }
  /**
   *Write binary array data to a MetaData column in an HBase Table
   *
   *RDD Layer Data is stored in their own table with tile info stored in a column and tile name as the rowID
   * @param configFile HBaseConfig Files used to connect to HBase
   * @param layerName unique name for the layer, will be used as table name
   * @param qualifierName name of column qualifier where data will be updated
   * @param fileName name of file that the data belongs to. Will be stored as the RowID
   * @param bytes sequence of bytes to be stored in HBase Table
   */
  def writeBytesToHBase(configFile: Seq[String], layerName: String, qualifierName: String)(fileName: String, bytes: Seq[Byte]): Unit = {
    val hBaseConnector = HBaseConnector(configFile)
    hBaseConnector.writeMetaData(tableName = layerName, rowID = (layerName + slash + fileName), data = bytes)
    hBaseConnector.close
  }

  /**
   * Serializes tile bins stored as a double array to tile index / byte sequence tuples.
   * @param tiles The input tile set.
   * @return Index/byte tuples.
   */
  def serializeBinArray(tiles: RDD[SeriesData[(Int, Int, Int), (Int, Int, Int), Double, (Double, Double)]]):
  RDD[((Int, Int, Int), Seq[Byte])] = {
    tiles.map { tile =>
        val data = for (bin <- tile.bins; i <- 0 until doubleBytes) yield {
          val data = java.lang.Double.doubleToLongBits(bin)
          ((data >> (i * doubleBytes)) & 0xff).asInstanceOf[Byte]
        }
        (tile.coords, data.toSeq)
      }
  }

  /**
   * Serializes tile bins stored as a double array to tile index / byte sequence tuples.
   * @param tiles The input tile set.
   * @return Index/byte tuples.
   */
  def serializeElementScore(tiles: RDD[SeriesData[(Int, Int, Int), (Int, Int, Int), List[(String, Int)], Nothing]]):
  RDD[((Int, Int, Int), Seq[Byte])] = {
    tiles.map { t =>
        val bytes = new JSONObject(t.bins.head.toMap).toString().getBytes
        (t.coords, bytes.toSeq)
      }
  }
}
