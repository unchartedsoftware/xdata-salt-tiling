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

import java.io.{BufferedOutputStream, File, FileOutputStream}
import java.nio.{ByteBuffer, ByteOrder, DoubleBuffer}
import java.util.ArrayList
import java.util.zip.ZipOutputStream

import grizzled.slf4j.Logging
import org.apache.spark.rdd.RDD
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.util.SparseArray

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
    * @param layerName    Unique name for the layer .
    * @param input        tile coordinate / byte array tuples of tile data
    * @return input data unchanged
    */
  def writeToFile(baseFilePath: String, layerName: String, extension: String)(input: RDD[((Int, Int, Int), Seq[Byte])]): RDD[((Int, Int, Int), Seq[Byte])] = {
    new FileSystemClient(baseFilePath, Some(extension)).write(layerName, input.map { case (index, data) => (index, data.toArray) })
    input
  }

  /**
    * Write binary array data to the file system.  Folder structure is
    * baseFilePath/filename.ext.
    *
    * @param baseFilePath Baseline output directory - can store multiple layer subdirectories.
    * @param layerName    Unique name for the layer.
    * @param fileName     Filename to write byte data into.
    * @param bytes        byte array of data
    */
  def writeBytesToFile(baseFilePath: String, layerName: String)(fileName: String, bytes: Seq[Byte]): Unit = {
    new FileSystemClient(baseFilePath, None).writeRaw(layerName, fileName, bytes.toArray)
  }

  /**
    * Write binary array data to the zip file baseZipDirectory/layerName.zip.  Entries will be level/xIdx/yIdx.  Indexing
    * is TMS style, with (0, 0) in the lower left, y increasing as it moves north.
    *
    * @param baseZipDirectory Baseline output directory - can store multiple layers
    * @param layerName        Unique name for this layer.  Data will go into the layer file layerName.zip
    * @param input            tile coordinate / byte array tuples of tile data
    * @return input data unchanged.
    */
  def writeToZip(baseZipDirectory: String, layerName: String)(input: RDD[((Int, Int, Int), Seq[Byte])]): RDD[((Int, Int, Int), Seq[Byte])] = {
    val zipDirectory = new File(baseZipDirectory)
    zipDirectory.mkdirs()

    new ZipFileClient(zipDirectory).write(layerName + ".zip", input.map { case (index, data) => (index, data.toArray) })
    input
  }

  /**
    * Write binary array data directly to a zip file.
    *
    * @param baseZipDirectory Baseline output directory - can store multiple layer zip files
    * @param layerName        Unique name for the layer.  Associated zip file will be layerName.zip
    * @param fileName         The key by which the data will be put into the zip file.
    * @param bytes            byte array of data
    */
  def writeBytesToZip(baseZipDirectory: String, layerName: String)(fileName: String, bytes: Seq[Byte]): Unit = {
    val zipDirectory = new File(baseZipDirectory)
    zipDirectory.mkdirs()
    val zipStream = new ZipOutputStream(new FileOutputStream(new File(zipDirectory, s"${layerName}.zip"), true))

    new ZipFileClient(zipDirectory).writeRaw(zipStream, fileName, bytes.toArray)

    zipStream.flush()
    zipStream.close()
  }

  /**
    * Write binary array data to Amazon S3 bucket.  Key format is layerName/level/xIdx/yIdx.bin.
    * Indexing is TMS style with (0,0) in lower left, y increasing as it moves north
    *
    * @param accessKey  AWS Access key
    * @param secretKey  AWS Secret key
    * @param bucketName Name of S3 bucket to write to - will create a bucket if missing
    * @param layerName  Unique name for the layer
    * @param input      tile coordinate / byte array tuples of tile data
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
    * @param accessKey  AWS Access key
    * @param secretKey  AWS Secret key
    * @param bucketName Name of S3 bucket to write to - will create a bucket if missing
    * @param layerName  Unique name for the layer
    * @param bytes      byte array of data to write
    */
  def writeBytesToS3(accessKey: String, secretKey: String, bucketName: String, layerName: String)(fileName: String, bytes: Seq[Byte]): Unit = {
    val s3Client = S3Client(accessKey, secretKey)
    s3Client.upload(bytes, bucketName, layerName + slash + fileName)
  }

  /**
    * Write Tiling Data to the TileData column in an HBase Table
    *
    * RDD Layer Data is stored in their own table with tile info stored in a column and tile name as the rowID
    * This Data is first mapped to a list of tuples containing required data to store into HBase table
    * Then the list of tuples are sent to HBase connector write the RDDPairs into HBase
    *
    * @param configFile    HBaseConfig Files used to connect to HBase
    * @param layerName     unique name for the layer, will be used as table name
    * @param qualifierName name of column qualifier where data will be updated
    * @param input         RDD of tile data to be processed and stored into HBase
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

  private def mkRowId(prefix: String, separator: String, suffix: String)(level: Int, x: Int, y: Int): String = {
    val digits = math.log10(1 << level).floor.toInt + 1
    (prefix + "%02d" + separator + "%0" + digits + "d" + separator + "%0" + digits + "d" + suffix).format(level, x, y)
  }

  /**
    * Write binary array data to a MetaData column in an HBase Table
    *
    * RDD Layer Data is stored in their own table with tile info stored in a column and tile name as the rowID
    *
    * @param configFile    HBaseConfig Files used to connect to HBase
    * @param layerName     unique name for the layer, will be used as table name
    * @param qualifierName name of column qualifier where data will be updated
    * @param fileName      name of file that the data belongs to. Will be stored as the RowID
    * @param bytes         sequence of bytes to be stored in HBase Table
    */
  def writeBytesToHBase(configFile: Seq[String], layerName: String, qualifierName: String)(fileName: String, bytes: Seq[Byte]): Unit = {
    val hBaseConnector = HBaseConnector(configFile)
    hBaseConnector.writeMetaData(tableName = layerName, rowID = (layerName + slash + fileName), data = bytes)
    hBaseConnector.close
  }

  /**
    * Serializes tile bins stored as a double array to tile index / byte sequence tuples.
    *
    * @param tiles The input tile set.
    * @return Index/byte tuples.
    */
  def serializeBinArray[TC, BC, X](tiles: RDD[SeriesData[TC, BC, Double, X]]):
  RDD[(TC, Seq[Byte])] = {
    tiles.filter(tile => tile.bins.density > 0)
      .map { tile =>
        (tile.coords, doubleTileToByteArrayDense2(tile.bins))
      }
  }

  // Serialize a single tile's data
  val doubleTileToByteArrayDense1: SparseArray[Double] => Seq[Byte] = sparseData => {
    for (bin <- sparseData.seq;  i <- 0 until doubleBytes) yield {
      val datum = java.lang.Double.doubleToLongBits(bin);
      ((datum >> (i * doubleBytes)) & 0xff).asInstanceOf[Byte];
    }
  }

  // Serialize a single tile's data - an alternate version that does the same thing in a tenth the time
  // See unit test in PackageTest for confirmation
  val doubleTileToByteArrayDense2: SparseArray[Double] => Seq[Byte] = sparseData => {
    val data = sparseData.seq.toArray
    val byteBuffer = ByteBuffer.allocate(data.length * doubleBytes).order(ByteOrder.LITTLE_ENDIAN)
    byteBuffer.asDoubleBuffer().put(DoubleBuffer.wrap(data))
    byteBuffer.array().toSeq
  }

  /**
    * Serializes tile bins stored as a list of scored strings to tile index / byte sequence tuples.
    *
    * @param tiles The input tile set.
    * @return Index/byte tuples.
    */
  def serializeElementScore[TC, BC, X](tiles: RDD[SeriesData[TC, BC, List[(String, Int)], X]]):
  RDD[(TC, Seq[Byte])] = {
    tiles.filter(t => t.bins.density() > 0)
      .map { t =>
        val bytes = new JSONObject(t.bins.head.toMap).toString().getBytes
        (t.coords, bytes.toSeq)
      }
  }

  /**
    * Serializes tile bins according to an arbitrarily specified serialization function
    *
    * @param serializationFcn The serialization function by which a single tile is serialized
    * @param tiles The input tile set
    * @tparam TC The tile coordinate type
    * @tparam BC The bin coordinate type
    * @tparam V The value type
    * @tparam X Any associated analytic type
    * @return Index/byte-array tuples
    */
  def serializeTiles[TC, BC, V, X](serializationFcn: SparseArray[V] => Seq[Byte])(tiles: RDD[SeriesData[TC, BC, V, X]]): RDD[(TC, Seq[Byte])] = {
    tiles.filter(t => t.bins.density() > 0)
      .map { tile =>
        (tile.coords, serializationFcn(tile.bins))
      }
  }
}
