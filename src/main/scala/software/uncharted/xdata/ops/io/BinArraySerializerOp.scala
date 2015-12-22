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

import java.io.{File, FileOutputStream, BufferedOutputStream}

import grizzled.slf4j.Logging
import org.apache.spark.rdd.RDD
import software.uncharted.salt.core.generation.output.SeriesData

object BinArraySerializerOp extends Logging {

  val doubleBytes = 8

  /**
   * Serializes series data from a sequence of Doubles into a sequence of Bytes, where
   * each Double is converted into its byte-based equivalent via java.Double.doubleToLongBits.
   *
   * @param tiles input series data
   * @return An RDD of tile coord / byte array tuples
   */
  def binArraySerializeOp(tiles: RDD[SeriesData[(Int, Int, Int), java.lang.Double, (java.lang.Double, java.lang.Double)]]): RDD[((Int, Int, Int), Seq[Byte])] = {
    tiles.map { tile =>
      val data = for (bin <- tile.bins; i <- 0 until doubleBytes) yield {
        val data = java.lang.Double.doubleToLongBits(bin)
        ((data >> (i * doubleBytes)) & 0xff).asInstanceOf[Byte]
      }
      (tile.coords, data)
    }
  }

  /**
   * Write binary array data to the file system.  Folder structure is
   * baseFilePath/layerName/level/xIdx/yIdx.bin.  Indexing is TMS style
   * with (0,0) in lower left, y increasing as it moves north.
   *
   * @param baseFilePath Baseline output directory - can store multiple layers.
   * @param layerName Unique name for the layer .
   * @param data tile coordinate / byte array tuples of tile data
   * @return input data unchanged
   */
  def binArrayFileStoreOp(baseFilePath: String, layerName: String)(data: RDD[((Int, Int, Int), Seq[Byte])]): RDD[((Int, Int, Int), Seq[Byte])] = {
    data.collect().foreach { tileData =>
      val coord = tileData._1
      val dirPath = s"$baseFilePath/$layerName/${coord._1}/${coord._2}"
      val path = s"$dirPath/${coord._3}.bin"
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
    data
  }

  /**
   * Write binary array data to Amazon S3 bucket.  Key format is layerName/level-xIdx-yIdx.bin.
   * Indexing is TMS style with (0,0) in lower left, y increasing as it moves north
   *
   * @param layerName Unique name for the layer .
   * @param data tile coordinate / byte array tuples of tile data
   * @return input data unchanged
   */
  def binArrayS3StoreOp(accessKey: String, secretKey: String, bucketName: String, layerName: String)
               (data: RDD[((Int, Int, Int), Seq[Byte])]):
  RDD[((Int, Int, Int), Seq[Byte])] = {
    // Upload tiles to S3 using the supplied bucket and layer.  Use foreachPartition to avoid incurring
    // the cost of initializing the S3Client per record.  This can't be done outside the RDD closure
    // because the Amazon S3 API classes are not marked serializable.
    data.foreachPartition { tileDataIter =>
      val s3Client = S3Client(accessKey, secretKey)
      tileDataIter.foreach { tileData =>
        val coord = tileData._1
        // store tile in bucket as layerName/level-xIdx-yIdx.bin
        val key = s"$layerName/${coord._1}-${coord._2}-${coord._3}.bin"
        s3Client.upload(tileData._2.toArray, bucketName, key)
      }
    }
    data
  }
}
